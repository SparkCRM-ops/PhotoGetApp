from __future__ import annotations

import atexit
import csv
import json
import logging
import os
import re
import shutil
import signal
import tempfile
import time
from datetime import datetime
from pathlib import Path

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

DEBUG = False

BASE_URL = "https://www.queencityharley.com"
INVENTORY_URL = BASE_URL + "/--inventory?layout=grid&pg={}"

stock_keywords = [
    "click for a quote",
    "no image available",
    "stock",
    "image coming soon",
    "nimg/400x300/no-image-generic.jpg",
    "imglib/nimg",
    "/no-image",
    "trimsdb",
    "cdn.dealerspike.com/imglib/nimg",
]


def is_stock_image(img_tag) -> bool:
    style = img_tag.get("style", "").lower()

    data_img = img_tag.get("data-dsp-small-image", "").lower()

    match = re.search(r"background-image:\s*url\(['\"]?(.*?)['\"]?\)", style)
    bg_img = match.group(1).strip() if match else ""

    combined = " ".join([bg_img, data_img, style]).lower()
    combined = (
        combined.replace("url(", "")
        .replace(")", "")
        .replace('"', "")
        .replace("'", "")
    )

    if "no-image-generic" in combined:
        return True

    for kw in stock_keywords:
        if kw != "no-image-generic" and kw in combined:
            return True

    if not data_img and not bg_img:
        if any(kw in style for kw in stock_keywords):
            return True

    if "no-image-generic.jpg" in str(img_tag).lower():
        return True

    return False


def run_photo_check(
    export_format: str = "csv",
    start_page: int = 1,
    end_page: int = 0,
    debug: bool = False,
) -> tuple[str, str]:
    """
    Run the inventory photo check and generate a downloadable file.

    Args:
        export_format: "csv" or "xlsx"
        start_page: first page to scan
        end_page: last page to scan, 0 means auto until empty/repeated pages
        debug: whether to log verbose output

    Returns:
        (output_path, filename)
    """
    global DEBUG
    DEBUG = bool(debug)

    export_format = export_format.lower().strip()
    if export_format not in {"csv", "xlsx"}:
        raise ValueError("export_format must be 'csv' or 'xlsx'")

    if export_format == "xlsx":
        try:
            from openpyxl import Workbook
        except ImportError as exc:
            raise RuntimeError("openpyxl is required for XLSX export") from exc

    temp_dir = Path("temp")
    temp_dir.mkdir(exist_ok=True)

    lock_file = os.path.join(tempfile.gettempdir(), "photogetapp.lock")

    def is_running(pid: int) -> bool:
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False

    old = {}
    if os.path.exists(lock_file):
        try:
            with open(lock_file, "r", encoding="utf-8") as f:
                old = json.load(f)
        except Exception:
            old = {}

    stale_seconds = 3600
    now = time.time()
    old_pid = int(old.get("pid", 0)) if old.get("pid") else 0
    old_ts = float(old.get("ts", 0.0)) if old.get("ts") else 0.0

    if old_pid and is_running(old_pid) and (now - old_ts) < stale_seconds:
        raise RuntimeError("Another PhotoGetApp scan is already running.")

    try:
        if os.path.exists(lock_file):
            os.remove(lock_file)
    except Exception:
        pass

    with open(lock_file, "w", encoding="utf-8") as f:
        json.dump({"pid": os.getpid(), "ts": now}, f)

    def remove_lock(*_args):
        try:
            if os.path.exists(lock_file):
                os.remove(lock_file)
        except Exception:
            pass

    atexit.register(remove_lock)
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, lambda *_: remove_lock())
        except Exception:
            pass

    logger = logging.getLogger("PhotoGetApp")
    for h in list(logger.handlers):
        logger.removeHandler(h)
    logger.setLevel(logging.INFO if DEBUG else logging.WARNING)

    log_path = None
    if DEBUG:
        ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        log_path = temp_dir / f"scan_{ts}.log"
        fh = logging.FileHandler(log_path, encoding="utf-8")
        fh.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
        logger.addHandler(fh)

    def dbg(msg: str):
        if DEBUG:
            print(msg)
            try:
                logger.info(msg)
            except Exception:
                pass

    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1280,1200")
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
    )

    driver = webdriver.Chrome(options=options)

    try:
        bikes_with_missing_images_local: list[list[str]] = []

        user_start_page = max(1, int(start_page))
        user_end_page = int(end_page)

        fixed_total = user_end_page if user_end_page and user_end_page > 0 else None
        max_pages = 50
        empty_limit = 5
        empty_streak = 0
        dup_sig_limit = 2
        dup_sig_streak = 0
        start_time = time.time()
        max_seconds = 600

        def listings_signature(soup):
            try:
                ids = [li.get("data-unit-id", "") for li in soup.select("li[data-unit-id]")]
                return ",".join(ids)
            except Exception:
                return ""

        seen_stocks = set()
        page_metrics = []
        last_nonempty_page = 0
        visited_max_page = 0
        page = user_start_page
        last_sig = None
        last_url = None

        while True:
            visited_max_page = max(visited_max_page, page)
            dbg(f"Checking page {page}...")

            if fixed_total and page > fixed_total:
                dbg(f"Reached user-specified end page {fixed_total}. Stopping.")
                break

            if (page - user_start_page) >= max_pages:
                dbg(f"Hit max page safety limit ({max_pages}). Stopping.")
                break

            if (time.time() - start_time) > max_seconds:
                dbg(f"Hit max runtime safety limit ({max_seconds}s). Stopping.")
                break

            for attempt in range(3):
                try:
                    driver.get(INVENTORY_URL.format(page))
                    WebDriverWait(driver, 12).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "li[data-unit-id]"))
                    )
                    break
                except Exception:
                    if attempt == 2:
                        raise
                    time.sleep(1.0 + attempt * 0.8)

            try:
                cur = driver.current_url
            except Exception:
                cur = None

            try:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1.2)
            except Exception:
                pass

            soup = BeautifulSoup(driver.page_source, "html.parser")

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

            if page > user_start_page and cur and last_url and cur == last_url and not fixed_total:
                dup_sig_streak += 1
                dbg(f"URL unchanged from previous page. Streak {dup_sig_streak}/{dup_sig_limit}.")
                if dup_sig_streak >= dup_sig_limit:
                    dbg("URL repeated across pages. Stopping (auto mode).")
                    break
            last_url = cur

            listings = soup.select("li[data-unit-id]")
            pre_filter_total = len(listings)

            if not listings:
                if fixed_total:
                    dbg(f"No listings on page {page}. Continuing (fixed range {user_start_page}-{fixed_total}).")
                else:
                    empty_streak += 1
                    dbg(f"No listings on page {page}. Empty streak = {empty_streak}/{empty_limit} (auto mode)")
                    if empty_streak >= empty_limit:
                        dbg(f"Reached {empty_limit} consecutive empty pages. Stopping.")
                        break
                    page += 1
                    continue

            last_nonempty_page = page

            filtered_listings = []
            for listing in listings:
                img_tag = listing.select_one("a.vehicle__image")
                if not img_tag or not is_stock_image(img_tag):
                    continue

                overlay = listing.select_one("span.vehicle-image__overlay-text")
                if overlay and "sale pending" in overlay.get_text(strip=True).lower():
                    continue

                listing_text = listing.get_text().lower()
                if "sale pending" in listing_text or "sold" in listing_text:
                    continue

                filtered_listings.append(listing)

            listings = filtered_listings
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
                    title = " ".join(
                        [
                            year.get_text(strip=True) if year else "",
                            make.get_text(strip=True) if make else "",
                            model.get_text(strip=True) if model else "",
                        ]
                    ).strip()
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
            page_metrics.append(
                {
                    "page": page,
                    "pre": pre_filter_total,
                    "post": post_filter_count,
                    "added": added_this_page,
                }
            )

            page += 1

        if DEBUG:
            dbg("\n=== Bikes with Stock or Missing Images ===")
            for bike in bikes_with_missing_images_local:
                dbg("- " + bike[0])

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

        filename_base = f"bikes_missing_photos_{datetime.now().strftime('%Y-%m-%d')}"
        file_ext = "xlsx" if export_format == "xlsx" else "csv"
        filename = f"{filename_base}.{file_ext}"
        output_path = temp_dir / filename

        if export_format == "xlsx":
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

            tmpfile = str(output_path) + ".tmp"
            wb.save(tmpfile)
            shutil.move(tmpfile, output_path)

        else:
            tmpfile = str(output_path) + ".tmp"
            with open(tmpfile, mode="w", newline="", encoding="utf-8") as file:
                writer = csv.writer(file)
                writer.writerow(["Year", "Make", "Model", "Stock Number", "Color"])

                for row in bikes_with_missing_images_local:
                    year_make_model = row[0].split(" ", 2)
                    year = year_make_model[0] if len(year_make_model) > 0 else ""
                    make = year_make_model[1] if len(year_make_model) > 1 else ""
                    model = year_make_model[2] if len(year_make_model) > 2 else ""
                    writer.writerow([year, make, model, row[1], row[2]])

            shutil.move(tmpfile, output_path)

        dbg(f"{export_format.upper()} file '{output_path}' created with {len(bikes_with_missing_images_local)} entries.")
        if DEBUG and log_path:
            dbg(f"Log written to: {log_path}")

        return str(output_path), filename

    finally:
        driver.quit()
        remove_lock()