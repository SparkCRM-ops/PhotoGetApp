from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, FileResponse, PlainTextResponse
from fastapi.templating import Jinja2Templates

from scraper import run_photo_check

app = FastAPI()
templates = Jinja2Templates(directory="templates")


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/run")
def run_check(
    export_format: str = Form("csv"),
    start_page: int = Form(1),
    end_page: int = Form(0),
):
    try:
        output_path, filename = run_photo_check(
            export_format=export_format,
            start_page=start_page,
            end_page=end_page,
            debug=False,
        )
    except Exception as e:
        return PlainTextResponse(str(e), status_code=500)

    media_type = (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        if filename.endswith(".xlsx")
        else "text/csv"
    )

    return FileResponse(
        path=output_path,
        filename=filename,
        media_type=media_type,
    )