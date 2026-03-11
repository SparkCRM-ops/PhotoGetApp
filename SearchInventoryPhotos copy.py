#!/usr/bin/env python3
# To build into a macOS app:
# 1. Ensure pyinstaller is installed: pip install pyinstaller
# 2. Run: pyinstaller --noconfirm --windowed --icon=install_512x512.icns --name "PhotoGetApp" SearchInventoryPhotos.py
# NOTE: Ensure you have a `credentials.json` file in your working directory with the proper Google service account credentials.
import threading
import tkinter as tk
from tkinter import ttk
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import logging
logging.basicConfig(level=logging.INFO)
import re
import os
import json
import signal
import shutil

DEBUG = False

BASE_URL = "https://www.queencityharley.com"
INVENTORY_URL = BASE_URL + "/--inventory?layout=grid&pg={}"

TOTAL_PAGES = 7  # Adjust this if more pages are added
stock_keywords = [
    "click for a quote",
    "no image available",
    "stock",
    "image coming soon",
    "nimg/400x300/no-image-generic.jpg",
    "imglib/nimg",  # catch base path for stock images
    "/no-image",    # slightly generalized pattern
    "trimsdb",      # for generic trimsdb image references
    "cdn.dealerspike.com/imglib/nimg"
]

def is_stock_image(img_tag):
    style = img_tag.get("style", "").lower()

    if "no-image-generic" in style:
        if DEBUG:
            print(f"Matched keyword in inline style: {style}")
        return True

    if "no-image-generic" in style:
        if DEBUG:
            print(f"Matched keyword in inline style: {style}")
        return True

    data_img = img_tag.get("data-dsp-small-image", "").lower()

    # Extract background-image URL from style
    match = re.search(r"background-image:\s*url\(['\"]?(.*?)['\"]?\)", style)
    bg_img = match.group(1).strip() if match else ""

    if "no-image-generic" in bg_img:
        if DEBUG:
            print(f"Matched keyword in background-image URL: {bg_img}")
        return True

    # Normalize and combine everything we have
    combined = " ".join([bg_img, data_img, style]).lower()
    combined = combined.replace("url(", "").replace(")", "").replace('"', '').replace("'", '')

    # Special check for missing photo marker
    if "no-image-generic" in combined:
        if DEBUG:
            print(f"Matched missing photo keyword in: {combined}")
        return True

    # Keep existing checks for other stock images
    for kw in stock_keywords:
        if kw != "no-image-generic" and kw in combined:
            if DEBUG:
                print(f"Matched keyword: {kw} in {combined}")
            return True

    # NEW fallback if only 'style' contains the stock path
    if not data_img and not bg_img:
        if any(kw in style for kw in stock_keywords):
            if DEBUG:
                print(f"Matched fallback keyword in style: {style}")
            return True

    # Catch unparsed no-image-generic URLs
    if "no-image-generic.jpg" in str(img_tag).lower():
        if DEBUG:
            print(f"Matched fallback in raw tag HTML: {img_tag}")
        return True

    return False

class AppUI:
    def __init__(self, root):
        self.root = root
        self.root.title("PhotoGetApp")
        self.status_label = ttk.Label(root, text="Ready", anchor="center")
        self.status_label.pack(padx=20, pady=10)
        self.progress = ttk.Progressbar(root, mode="indeterminate")
        self.progress.pack(padx=20, pady=10)
        self.progress_label = ttk.Label(root, text="Page 0 of 0 (auto) | Listings Found: 0", anchor="center")
        self.progress_label.pack(padx=20, pady=5)
        self.export_format = tk.StringVar(value="CSV")
        self.format_combobox = ttk.Combobox(root, textvariable=self.export_format, values=["CSV", "XLSX"], state="readonly")
        self.format_combobox.pack(padx=20, pady=5)

        # Page range controls
        self.page_frame = ttk.Frame(root)
        self.page_frame.pack(padx=20, pady=5, fill="x")
        ttk.Label(self.page_frame, text="Pages").pack(side="left")
        self.start_page = tk.IntVar(value=1)
        self.end_page = tk.IntVar(value=0)  # 0 means auto until empty page
        self.start_entry = ttk.Entry(self.page_frame, width=5, textvariable=self.start_page)
        self.start_entry.pack(side="left", padx=(6, 4))
        ttk.Label(self.page_frame, text="-").pack(side="left")
        self.end_entry = ttk.Entry(self.page_frame, width=5, textvariable=self.end_page)
        self.end_entry.pack(side="left", padx=(4, 0))
        ttk.Label(self.page_frame, text=" (End 0 = auto; keeps going until an empty page)").pack(side="left", padx=(6, 0))

        # Debug toggle
        self.debug_var = tk.BooleanVar(value=False)
        self.debug_check = ttk.Checkbutton(root, text="Verbose (Debug)", variable=self.debug_var)
        self.debug_check.pack(padx=20, pady=(0, 5), anchor="w")

        self.output_path = tk.StringVar(value=os.getcwd())
        self.path_frame = ttk.Frame(root)
        self.path_frame.pack(padx=20, pady=5)
        ttk.Label(self.path_frame, text="Save to:").pack(side="left")
        self.path_entry = ttk.Entry(self.path_frame, textvariable=self.output_path, width=40)
        self.path_entry.pack(side="left", padx=(5, 0))
        self.browse_button = ttk.Button(self.path_frame, text="Browse", command=self.select_output_path)
        self.browse_button.pack(side="left", padx=(5, 0))

        self.start_button = ttk.Button(root, text="Start", command=self.start_process)
        self.start_button.pack(side="left", padx=10, pady=10)
        self.cancel_button = ttk.Button(root, text="Cancel", command=self.cancel_process, state="disabled")
        self.cancel_button.pack(side="right", padx=10, pady=10)
        self.process_thread = None
        self.cancel_flag = False

    def select_output_path(self):
        from tkinter import filedialog
        selected_path = filedialog.askdirectory()
        if selected_path:
            self.output_path.set(selected_path)

    def start_process(self):
        if self.process_thread and self.process_thread.is_alive():
            return
        self.cancel_flag = False
        self.status_label.config(text="Running...")
        self.progress.start()
        self.start_button.config(state="disabled")
        self.cancel_button.config(state="normal")
        self.process_thread = threading.Thread(target=self.run_scraper)
        self.process_thread.start()

    def cancel_process(self):
        self.cancel_flag = True
        self.status_label.config(text="Cancelling...")

    def update_progress(self, page, total_pages, count):
        self.root.after(0, lambda: self.progress_label.config(
            text=f"Page {page} of {total_pages} | Listings Found: {count}"
        ))

    def run_scraper(self):
        try:
            run_scraper(self)
            # schedule UI updates on main thread
            self.root.after(0, lambda: self.status_label.config(text="Completed"))
        except Exception as e:
            self.root.after(0, lambda: self.status_label.config(text=f"Error: {str(e)}"))
        finally:
            self.root.after(0, self.progress.stop)
            self.root.after(0, lambda: self.start_button.config(state="normal"))
            self.root.after(0, lambda: self.cancel_button.config(state="disabled"))


# --- App main entry point for bundling ---
def run_scraper(ui=None):
    import os
    import atexit
    import csv
    import subprocess
    import platform
    from datetime import datetime
    # Ensure DEBUG is declared before any references/prints in this function
    global DEBUG
    if ui and hasattr(ui, "debug_var"):
        try:
            DEBUG = bool(ui.debug_var.get())
        except Exception:
            pass
    if ui:
        try:
            import openpyxl
            from openpyxl import Workbook
        except ImportError:
            pass

    import tempfile
    import time

    lock_file = os.path.join(tempfile.gettempdir(), "photogetapp.lock")

    def is_running(pid: int) -> bool:
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False

    # Read any existing lock
    old = {}
    if os.path.exists(lock_file):
        try:
            with open(lock_file, "r") as f:
                old = json.load(f)
        except Exception:
            old = {}

    stale_seconds = 3600  # consider stale after 1 hour; tune as needed
    now = time.time()
    old_pid = int(old.get("pid", 0)) if old.get("pid") else 0
    old_ts = float(old.get("ts", 0.0)) if old.get("ts") else 0.0

    if old_pid and is_running(old_pid) and (now - old_ts) < stale_seconds:
        def _tmp_dbg(msg: str):
            try:
                print(msg)
            except Exception:
                pass
        if DEBUG:
            _tmp_dbg("Another instance is already running.")
        return

    # Clear stale/invalid lock and write a new one
    try:
        if os.path.exists(lock_file):
            os.remove(lock_file)
    except Exception:
        pass

    with open(lock_file, "w") as f:
        json.dump({"pid": os.getpid(), "ts": now}, f)

    def remove_lock(*_args):
        try:
            if os.path.exists(lock_file):
                os.remove(lock_file)
        except Exception:
            pass

    atexit.register(remove_lock)
    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        try:
            signal.signal(sig, lambda *_: remove_lock())
        except Exception:
            pass

    bikes_with_missing_images_local = []
    options = Options()
    options.add_argument("--headless=new")  # modern headless
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1280,1200")
    options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15")

    driver = webdriver.Chrome(options=options)
    try:
        # Determine page range from UI (if provided)
        user_start_page = 1
        user_end_page = 0
        if ui:
            try:
                user_start_page = max(1, int(ui.start_page.get()))
            except Exception:
                user_start_page = 1
            try:
                user_end_page = int(ui.end_page.get())
            except Exception:
                user_end_page = 0

        # (DEBUG already initialized at top)

        # Configure file logging when DEBUG is on
        logger = logging.getLogger("PhotoGetApp")
        for h in list(logger.handlers):
            logger.removeHandler(h)
        logger.setLevel(logging.INFO if DEBUG else logging.WARNING)
        log_path = None
        if DEBUG:
            try:
                log_dir = ui.output_path.get() if ui else os.getcwd()
            except Exception:
                log_dir = os.getcwd()
            ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            log_path = os.path.join(log_dir, f"scan_{ts}.log")
            fh = logging.FileHandler(log_path, encoding='utf-8')
            fh.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
            logger.addHandler(fh)

        def dbg(msg: str):
            if DEBUG:
                try:
                    print(msg)
                except Exception:
                    pass
                try:
                    logger.info(msg)
                except Exception:
                    pass

        # Set fixed_total once, to be reused (for hard end page)
        fixed_total = user_end_page if user_end_page and user_end_page > 0 else None
        # Safety controls to avoid infinite loops if pagination is ignored or content repeats
        max_pages = 50  # hard stop
        empty_limit = 5  # stop after N consecutive truly empty pages (auto mode only)
        empty_streak = 0
        dup_sig_limit = 2  # stop after N consecutive identical page signatures (auto mode only)
        dup_sig_streak = 0
        # Timing safety variables
        start_time = time.time()
        max_seconds = 600  # hard time cap (10 minutes)

        def listings_signature(soup):
            try:
                ids = [li.get("data-unit-id", "") for li in soup.select("li[data-unit-id]")]
                return ",".join(ids)
            except Exception:
                return ""

        seen_stocks = set()
        page_metrics = []  # list of dicts: {page, pre, post, added}
        last_nonempty_page = 0  # highest page index that had any listings (pre-filter)
        visited_max_page = 0    # highest page index we attempted to visit
        page = user_start_page
        last_sig = None
        last_url = None
        while True:
            visited_max_page = max(visited_max_page, page)
            dbg(f"Checking page {page}...")
            # Early exits before fetching the page
            if fixed_total and page > fixed_total:
                if DEBUG:
                    print(f"Reached user-specified end page {fixed_total}. Stopping.")
                break
            if (page - user_start_page) >= max_pages:
                if DEBUG:
                    print(f"Hit max page safety limit ({max_pages}). Stopping.")
                break
            if (time.time() - start_time) > max_seconds:
                if DEBUG:
                    print(f"Hit max runtime safety limit ({max_seconds}s). Stopping.")
                break

            # Retry page load up to 3 times
            for attempt in range(3):
                try:
                    driver.get(INVENTORY_URL.format(page))
                    WebDriverWait(driver, 12).until(EC.presence_of_element_located((By.CSS_SELECTOR, "li[data-unit-id]")))
                    break
                except Exception as _e:
                    if attempt == 2:
                        raise
                    time.sleep(1.0 + attempt * 0.8)

            # Record current URL; if it repeats across pages, we'll stop after a small streak in auto mode
            try:
                cur = driver.current_url
            except Exception:
                cur = None

            # Scroll once to trigger any lazy loading, then parse
            try:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1.2)
            except Exception:
                pass
            soup = BeautifulSoup(driver.page_source, 'html.parser')

            sig = listings_signature(soup)
            if page > user_start_page and sig and sig == last_sig and not fixed_total:
                dup_sig_streak += 1
                dbg(f"Page {page} signature identical to previous. Streak {dup_sig_streak}/{dup_sig_limit}.")
                if dup_sig_streak >= dup_sig_limit:
                    dbg("Identical content repeated. Stopping (auto mode).")
                    break
            else:
                dup_sig_streak = 0
            last_sig = sig

            # Track repeating URLs as another end signal in auto mode
            if page > user_start_page and cur and last_url and cur == last_url and not fixed_total:
                dup_sig_streak += 1
                dbg(f"URL unchanged from previous page. Streak {dup_sig_streak}/{dup_sig_limit}.")
                if dup_sig_streak >= dup_sig_limit:
                    dbg("URL repeated across pages. Stopping (auto mode).")
                    break
            last_url = cur

            listings = soup.select("li[data-unit-id]")
            pre_filter_total = len(listings)
            # Empty page (no listings at all on this page)
            if not listings:
                if fixed_total:
                    dbg(f"No listings on page {page}. Continuing (fixed range {user_start_page}-{fixed_total}).")
                else:
                    empty_streak += 1
                    dbg(f"No listings on page {page}. Empty streak = {empty_streak}/{empty_limit} (auto mode)")
                    if empty_streak >= empty_limit:
                        dbg(f"Reached {empty_limit} consecutive empty pages. Stopping.")
                        break
                    # advance and continue without filtering
                    if ui:
                        # In auto mode, show the last page that actually had listings as the total
                        total_for_ui = fixed_total if fixed_total else (last_nonempty_page if last_nonempty_page > 0 else page)
                        ui.update_progress(page, total_for_ui, len(bikes_with_missing_images_local))
                    page += 1
                    if ui and ui.cancel_flag:
                        dbg("Cancelled by user.")
                        return
                    continue
            # This page has listings; remember it for progress display in auto mode
            last_nonempty_page = page
            filtered_listings = []
            for listing in listings:
                img_tag = listing.select_one("a.vehicle__image")
                if not img_tag or not is_stock_image(img_tag):
                    continue
                if listing.select_one("span.vehicle-image__overlay-text") and \
                   "sale pending" in listing.select_one("span.vehicle-image__overlay-text").get_text(strip=True).lower():
                    continue
                if "sale pending" in listing.get_text().lower() or "sold" in listing.get_text().lower():
                    continue
                filtered_listings.append(listing)
            listings = filtered_listings
            # This page had listings pre-filter, so reset empty streak
            empty_streak = 0
            post_filter_count = len(listings)

            before_count = len(seen_stocks)

            dbg(f"Found {len(listings)} listings on page {page} (pre-filter {pre_filter_total})")
            for listing in listings:
                if listing.find(string=lambda s: s and "sale pending" in s.lower()):
                    continue
                name_link = listing.select_one("a.vehicle-heading__link")
                if name_link:
                    year = name_link.select_one("span.vehicle-heading__year")
                    make = name_link.select_one("span.vehicle-heading__name")
                    model = name_link.select_one("span.vehicle-heading__model")
                    title = " ".join([
                        year.get_text(strip=True) if year else "",
                        make.get_text(strip=True) if make else "",
                        model.get_text(strip=True) if model else ""
                    ]).strip()
                else:
                    title = "Unknown"

                stock_el = listing.select_one("li.vehicle-specs__item--stock-number span.vehicle-specs__value")
                stock_number = stock_el.get_text(strip=True).upper() if stock_el else "N/A"

                color_el = listing.select_one("li.vehicle-specs__item--color span.vehicle-specs__value")
                color = color_el.get_text(strip=True).title() if color_el else "N/A"

                if stock_number not in seen_stocks:
                    seen_stocks.add(stock_number)
                    bikes_with_missing_images_local.append([title, stock_number, color])

            added_this_page = len(seen_stocks) - before_count
            page_metrics.append({
                "page": page,
                "pre": pre_filter_total,
                "post": post_filter_count,
                "added": added_this_page,
            })

            # Update progress in UI
            if ui:
                total_for_ui = fixed_total if fixed_total else (last_nonempty_page if last_nonempty_page > 0 else page)
                ui.update_progress(page, total_for_ui, len(bikes_with_missing_images_local))

            page += 1
            # Check for cancellation after each page
            if ui and ui.cancel_flag:
                dbg("Cancelled by user.")
                return

        # Final UI progress update to ensure progress bar/label doesn't show a trailing empty page
        if ui:
            final_total = fixed_total if fixed_total else (last_nonempty_page if last_nonempty_page > 0 else visited_max_page)
            ui.update_progress(last_nonempty_page or visited_max_page, final_total, len(bikes_with_missing_images_local))

        if DEBUG:
            dbg("\n=== Bikes with Stock or Missing Images ===")
            for bike in bikes_with_missing_images_local:
                dbg("- " + bike[0])

        # End-of-run page summary (debug only)
        if DEBUG and page_metrics:
            pre_series = ", ".join(str(m["pre"]) for m in page_metrics)
            post_series = ", ".join(str(m["post"]) for m in page_metrics)
            added_series = ", ".join(str(m["added"]) for m in page_metrics)
            total_pre = sum(m["pre"] for m in page_metrics)
            total_post = sum(m["post"] for m in page_metrics)
            unique_total = len(bikes_with_missing_images_local)
            dbg("\nPages summary (pre-filter totals): [" + pre_series + "]")
            dbg("Pages summary (post-filter matches on page): [" + post_series + "]")
            dbg("Pages summary (unique added per page): [" + added_series + "]")
            dbg(f"Total pre-filter = {total_pre} | Total post-filter (page matches) = {total_post} | Unique (deduped) = {unique_total}")
            dbg(f"Visited up to page {visited_max_page}; pages with listings: {len(page_metrics)}; last page with listings: {last_nonempty_page}")

        export_format = "CSV"
        if ui:
            export_format = ui.export_format.get().upper()

        filename_base = f"bikes_missing_photos_{datetime.now().strftime('%Y-%m-%d')}"
        output_dir = os.getcwd()
        if ui:
            output_dir = ui.output_path.get()

        if export_format == "XLSX":
            filename = os.path.join(output_dir, filename_base + ".xlsx")
            wb = Workbook()
            ws = wb.active
            ws.title = "Missing Photos"
            ws.append(["Year", "Make", "Model", "Stock Number", "Color"])
            for row in bikes_with_missing_images_local:
                year_make_model = row[0].split(" ", 2)
                year = year_make_model[0] if len(year_make_model) > 0 else ""
                make = year_make_model[1] if len(year_make_model) > 1 else ""
                model = year_make_model[2] if len(year_make_model) > 2 else ""
                ws.append([year, make, model, row[1], row[2]])
            tmpfile = filename + ".tmp"
            wb.save(tmpfile)
            shutil.move(tmpfile, filename)
            if DEBUG and log_path:
                dbg(f"Log written to: {log_path}")
        else:
            filename = os.path.join(output_dir, filename_base + ".csv")
            tmpfile = filename + ".tmp"
            with open(tmpfile, mode="w", newline="") as file:
                writer = csv.writer(file)
                writer.writerow(["Year", "Make", "Model", "Stock Number", "Color"])
                for row in bikes_with_missing_images_local:
                    year_make_model = row[0].split(" ", 2)
                    year = year_make_model[0] if len(year_make_model) > 0 else ""
                    make = year_make_model[1] if len(year_make_model) > 1 else ""
                    model = year_make_model[2] if len(year_make_model) > 2 else ""
                    writer.writerow([year, make, model, row[1], row[2]])
            shutil.move(tmpfile, filename)
            if DEBUG and log_path:
                dbg(f"Log written to: {log_path}")

        dbg(f"\n{export_format} file '{filename}' created with {len(bikes_with_missing_images_local)} entries.")
    finally:
        driver.quit()


if __name__ == "__main__":
    root = tk.Tk()
    app = AppUI(root)
    root.mainloop()