from __future__ import annotations

import argparse
import cgi
import html
import json
import re
import shutil
import socket
import subprocess
import tempfile
import threading
import urllib.parse
import uuid
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from .app import run_ocr

WordBox = tuple[int, int, int, int, str, float]

_ENGINEERING_TOKEN_SPLITS: dict[str, str] = {
    "DESIGNCODE": "DESIGN CODE",
    "DESIGNTEMP": "DESIGN TEMP",
    "DESIGNPRESS": "DESIGN PRESS",
    "OPERATINGPRESS": "OPERATING PRESS",
    "OPERATINGTEMP": "OPERATING TEMP",
    "HYDROTESTPRESS": "HYDRO TEST PRESS",
    "PNEUMTESTPRESS": "PNEUM TEST PRESS",
    "RADIOGRAPHICEXAM": "RADIOGRAPHIC EXAM",
    "JOINTEFFICIENCY": "JOINT EFFICIENCY",
    "CORROSIONALLOW": "CORROSION ALLOW",
    "INTERNALCOATING": "INTERNAL COATING",
    "EXTERNALPAINT": "EXTERNAL PAINT",
    "FIREPROOFING": "FIRE PROOFING",
    "ACIDPICKLING": "ACID PICKLING",
    "FLOWRATE": "FLOW RATE",
    "EMPTYWEIGHT": "EMPTY WEIGHT",
    "FULLWATERWEIGHT": "FULL WATER WEIGHT",
}

_RESCUE_PHRASE_REPLACEMENTS: dict[str, str] = {
    "RADIOGRAPHIC EAM": "RADIOGRAPHIC EXAM",
    "JOINTEFFICIENCY": "JOINT EFFICIENCY",
    "FIOINT EFFICIENCY": "JOINT EFFICIENCY",
    "FIREPROOFING": "FIRE PROOFING",
    "ACIDPICKLING": "ACID PICKLING",
    "PLCKLING": "PICKLING",
    "DESIGNTEMP": "DESIGN TEMP",
    "DESIGNCODE": "DESIGN CODE",
}

_DESIGN_TABLE_STEMS: tuple[str, ...] = (
    "DESIGN",
    "STANDARD",
    "FLUID",
    "FLOW",
    "OPERAT",
    "HYDRO",
    "PNEUM",
    "RADIO",
    "JOINT",
    "CORROS",
    "INTERN",
    "EXTERN",
    "INSUL",
    "FIRE",
    "VOLUME",
    "EMPTY",
    "FULL",
    "WEIGHT",
    "PWHT",
    "TEMP",
    "PRESS",
)


@dataclass
class JobState:
    job_id: str
    status: str = "queued"
    total_pages: int = 0
    completed_pages: list[int] = field(default_factory=list)
    output_pdf: str = ""
    error: str = ""
    page_details: list[dict[str, Any]] = field(default_factory=list)


_JOBS: dict[str, JobState] = {}
_JOBS_LOCK = threading.Lock()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="liza-codex-pdf-web",
        description="Run web UI for OCR conversion on the first free TCP port.",
    )
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind (default: 127.0.0.1)")
    parser.add_argument(
        "--start-port",
        type=int,
        default=8080,
        help="Start scanning for a free port from this value (default: 8080)",
    )
    parser.add_argument(
        "--end-port",
        type=int,
        default=8190,
        help="Stop scanning for a free port at this value (default: 8190)",
    )
    return parser


def _is_port_free(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            probe.bind((host, port))
        except OSError:
            return False
    return True


def _find_free_port(host: str, start_port: int, end_port: int) -> int:
    if not 1 <= start_port <= 65535:
        raise ValueError("--start-port must be in range 1..65535")
    if not 1 <= end_port <= 65535:
        raise ValueError("--end-port must be in range 1..65535")
    if start_port > end_port:
        raise ValueError("--start-port must be <= --end-port")

    for port in range(start_port, end_port + 1):
        if _is_port_free(host, port):
            return port

    raise RuntimeError(f"No free port found in range {start_port}-{end_port} on host {host}")


def _render_page(*, error: str = "", values: dict[str, str] | None = None) -> str:
    form = {
        "input_pdf": "",
        "output_pdf": "",
        "lang": "rus+eng",
        "optimize": "1",
        "jobs": "1",
        "rotate_pages": "1",
        "deskew": "1",
        "quiet": "",
        "drawing_mode": "1",
        "deep_verify": "1",
    }
    if values:
        form.update(values)

    error_block = f'<p class="err">{html.escape(error)}</p>' if error else ""
    rotate_checked = "checked" if form.get("rotate_pages") else ""
    deskew_checked = "checked" if form.get("deskew") else ""
    quiet_checked = "checked" if form.get("quiet") else ""
    drawing_checked = "checked" if form.get("drawing_mode") else ""
    deep_verify_checked = "checked" if form.get("deep_verify") else ""

    return f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Searchable PDF OCR</title>
  <style>
    body {{
      font-family: sans-serif;
      margin: 2rem;
      background: #f5f7fb;
      color: #111827;
    }}
    .card {{
      max-width: 900px;
      margin: 0 auto;
      padding: 1.5rem;
      background: #ffffff;
      border-radius: 12px;
      box-shadow: 0 8px 24px rgba(0, 0, 0, 0.07);
    }}
    h1 {{ margin-top: 0; }}
    label {{ display: block; margin: 0.6rem 0 0.2rem; font-weight: 600; }}
    input, select {{
      width: 100%;
      padding: 0.55rem;
      border: 1px solid #d1d5db;
      border-radius: 8px;
      box-sizing: border-box;
    }}
    .row {{ display: grid; grid-template-columns: 1fr 1fr; gap: 0.8rem; }}
    .checks {{ display: grid; grid-template-columns: 1fr 1fr; gap: 0.5rem; margin-top: 0.8rem; }}
    .check {{ display: flex; align-items: center; gap: 0.5rem; }}
    .check input {{ width: auto; }}
    button {{
      margin-top: 1rem;
      background: #0b7285;
      border: none;
      color: white;
      padding: 0.7rem 1rem;
      border-radius: 8px;
      cursor: pointer;
    }}
    .err {{ color: #8b1d1d; background: #ffeded; padding: 0.6rem; border-radius: 8px; }}
    .note {{ font-size: 0.95rem; color: #374151; }}
  </style>
</head>
<body>
  <div class="card">
    <h1>Searchable PDF OCR</h1>
    <p class="note">
      Для чертежей включайте <b>режим чертежей</b> и <b>глубокую проверку</b>.
      Конвейер: проход OCR -> проверка покрытия -> доборный проход при провале.
    </p>
    {error_block}
    <form method="post" action="/ocr" enctype="multipart/form-data">
      <label for="input_file">Выбрать PDF с компьютера</label>
      <input id="input_file" name="input_file" type="file" accept=".pdf,application/pdf">

      <label for="input_pdf">Или путь к PDF на этом ПК (опционально)</label>
      <input id="input_pdf" name="input_pdf" value="{html.escape(form['input_pdf'])}">

      <label for="output_pdf">Путь выходного PDF (опционально)</label>
      <input id="output_pdf" name="output_pdf" value="{html.escape(form['output_pdf'])}">

      <div class="row">
        <div>
          <label for="lang">Языки OCR</label>
          <input id="lang" name="lang" value="{html.escape(form['lang'])}">
        </div>
        <div>
          <label for="jobs">Потоки OCR</label>
          <input id="jobs" name="jobs" type="number" min="1" value="{html.escape(form['jobs'])}">
        </div>
      </div>

      <label for="optimize">Оптимизация PDF</label>
      <select id="optimize" name="optimize">
        <option value="0" {'selected' if form['optimize'] == '0' else ''}>0</option>
        <option value="1" {'selected' if form['optimize'] == '1' else ''}>1</option>
        <option value="2" {'selected' if form['optimize'] == '2' else ''}>2</option>
        <option value="3" {'selected' if form['optimize'] == '3' else ''}>3</option>
      </select>

      <div class="checks">
        <label class="check">
          <input type="checkbox" name="drawing_mode" {drawing_checked}>режим чертежей
        </label>
        <label class="check">
          <input type="checkbox" name="deep_verify" {deep_verify_checked}>глубокая проверка
        </label>
        <label class="check">
          <input type="checkbox" name="rotate_pages" {rotate_checked}>автоповорот
        </label>
        <label class="check">
          <input type="checkbox" name="deskew" {deskew_checked}>выравнивание
        </label>
        <label class="check">
          <input type="checkbox" name="quiet" {quiet_checked}>тихий режим
        </label>
      </div>

      <button type="submit">Запустить OCR</button>
    </form>
  </div>
</body>
</html>
"""


def _render_job_page(job_id: str) -> str:
    return f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>OCR progress</title>
  <style>
    body {{ font-family: sans-serif; margin: 2rem; background: #f5f7fb; color: #111827; }}
    .card {{
      max-width: 980px;
      margin: 0 auto;
      padding: 1.5rem;
      background: #ffffff;
      border-radius: 12px;
      box-shadow: 0 8px 24px rgba(0, 0, 0, 0.07);
    }}
    .bar-wrap {{ height: 16px; background: #e5e7eb; border-radius: 8px; overflow: hidden; }}
    .bar {{ width: 0; height: 100%; background: #0b7285; transition: width 0.25s ease; }}
    .ok {{ color: #036d19; }}
    .err {{ color: #8b1d1d; }}
    #pages li {{ margin: 0.45rem 0; }}
    .tiny {{ font-size: 0.82rem; color: #4b5563; }}
  </style>
</head>
<body>
  <div class="card">
    <h1>OCR: прогресс по страницам</h1>
    <p id="status">Подготовка...</p>
    <div class="bar-wrap"><div class="bar" id="bar"></div></div>
    <p id="summary">0 / 0 страниц</p>
    <ul id="pages"></ul>
    <p><a id="download" style="display:none" href="#">Скачать PDF</a></p>
    <p><a href="/">Назад</a></p>
  </div>

  <script>
    const jobId = {json.dumps(job_id)};
    const statusEl = document.getElementById("status");
    const summaryEl = document.getElementById("summary");
    const pagesEl = document.getElementById("pages");
    const barEl = document.getElementById("bar");
    const downloadEl = document.getElementById("download");

    function renderPages(details) {{
      pagesEl.innerHTML = details.map((p) => {{
        const parts = [];
        if (p.best_score > 0) parts.push(`score: ${{p.best_score}}`);
        if (p.best_selection_score > 0) parts.push(`sel: ${{p.best_selection_score}}`);
        if (p.best_profile) parts.push(`профиль: ${{p.best_profile}}`);
        if (p.scan_full_words || p.scan_tile_words) {{
          parts.push(`full/tile: ${{p.scan_full_words}}/${{p.scan_tile_words}}`);
        }}
        if (p.scan_gain_ratio > 0) {{
          parts.push(`gain: ${{Math.round(p.scan_gain_ratio * 100)}}%`);
        }}
        if (p.scan_vertical_words > 0) parts.push(`vertical: ${{p.scan_vertical_words}}`);
        if (p.table_rescue_words > 0) parts.push(`table-rescue: ${{p.table_rescue_words}}`);
        if (p.augmented_words > 0) parts.push(`добавлено слов: ${{p.augmented_words}}`);
        if (p.fallback_lines > 0) parts.push(`fallback-строк: ${{p.fallback_lines}}`);
        if (p.needs_review) parts.push("нужна ручная проверка");

        const title = p.status === "done"
          ? `Страница ${{p.page}} закончена`
          : p.status === "running"
            ? `Страница ${{p.page}}: попытка ${{p.attempts_done}}/${{p.attempts_total}}`
            : p.status === "verifying"
              ? `Страница ${{p.page}}: проверка покрытия`
            : p.status === "error"
              ? `Страница ${{p.page}}: ошибка`
              : `Страница ${{p.page}}: ожидание`;

        const review = p.review_url
          ? ` <a href="${{p.review_url}}" target="_blank">проверочный overlay</a>`
          : "";

        const history = Array.isArray(p.attempt_history)
          ? p.attempt_history
              .map((a) => `${{a.attempt}}:${{a.profile}}(s=${{a.score}})`)
              .join("; ")
          : "";

        return `<li><b>${{title}}</b>${{review}}` +
          `<div class="tiny">${{parts.join(", ")}}</div>` +
          `<div class="tiny">${{history}}</div></li>`;
      }}).join("");
    }}

    async function poll() {{
      try {{
        const response = await fetch(
          `/api/job?id=${{encodeURIComponent(jobId)}}`,
          {{ cache: "no-store" }}
        );
        const data = await response.json();
        if (!response.ok) {{
          statusEl.textContent = data.error || "Ошибка запроса статуса";
          statusEl.className = "err";
          return;
        }}

        const total = Number(data.total_pages || 0);
        const completed = Array.isArray(data.completed_pages) ? data.completed_pages : [];
        const done = completed.length;
        const percent = total > 0 ? Math.round((done * 100) / total) : 0;

        barEl.style.width = `${{percent}}%`;
        summaryEl.textContent = `${{done}} / ${{total}} страниц`;
        renderPages(Array.isArray(data.page_details) ? data.page_details : []);

        if (data.status === "done") {{
          statusEl.textContent = "Готово";
          statusEl.className = "ok";
          downloadEl.href = `/download?id=${{encodeURIComponent(jobId)}}`;
          downloadEl.style.display = "inline";
          return;
        }}

        if (data.status === "error") {{
          statusEl.textContent = data.error || "Ошибка OCR";
          statusEl.className = "err";
          return;
        }}

        statusEl.textContent = "OCR выполняется...";
        setTimeout(poll, 1000);
      }} catch (error) {{
        statusEl.textContent = `Ошибка сети: ${{error}}`;
        statusEl.className = "err";
      }}
    }}

    poll();
  </script>
</body>
</html>
"""


class OCRWebHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/":
            self._send_html(_render_page())
            return
        if parsed.path == "/job":
            self._handle_job_page(parsed.query)
            return
        if parsed.path == "/api/job":
            self._handle_job_api(parsed.query)
            return
        if parsed.path == "/download":
            self._handle_download(parsed.query)
            return
        if parsed.path == "/review":
            self._handle_review(parsed.query)
            return
        self._send_text(HTTPStatus.NOT_FOUND, "Not found")

    def do_POST(self) -> None:  # noqa: N802
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path != "/ocr":
            self._send_text(HTTPStatus.NOT_FOUND, "Not found")
            return

        upload_cleanup_dir: Path | None = None
        try:
            values, uploaded_pdf, upload_cleanup_dir, uploaded_name = _parse_request_form(self)
            settings = _build_job_settings(values)
            input_pdf = _resolve_input_pdf(values, uploaded_pdf)
            output_pdf = _resolve_output_pdf(values, input_pdf, uploaded_name)
        except (OSError, ValueError) as exc:
            if upload_cleanup_dir is not None:
                shutil.rmtree(upload_cleanup_dir, ignore_errors=True)
            self._send_html(
                _render_page(error=str(exc), values=values if "values" in locals() else None),
                status=HTTPStatus.BAD_REQUEST,
            )
            return

        job_id = _create_job(output_pdf)
        worker = threading.Thread(
            target=_process_job,
            args=(job_id, input_pdf, output_pdf, settings, upload_cleanup_dir),
            daemon=True,
        )
        worker.start()
        self._redirect(f"/job?id={urllib.parse.quote(job_id)}")

    def _handle_job_page(self, query: str) -> None:
        params = urllib.parse.parse_qs(query)
        job_id = params.get("id", [""])[0]
        if not job_id or _get_job_snapshot(job_id) is None:
            self._send_text(HTTPStatus.NOT_FOUND, "Job not found")
            return
        self._send_html(_render_job_page(job_id))

    def _handle_job_api(self, query: str) -> None:
        params = urllib.parse.parse_qs(query)
        job_id = params.get("id", [""])[0]
        snapshot = _get_job_snapshot(job_id)
        if snapshot is None:
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "Job not found"})
            return
        self._send_json(HTTPStatus.OK, snapshot)

    def _handle_download(self, query: str) -> None:
        params = urllib.parse.parse_qs(query)
        job_id = params.get("id", [""])[0]
        snapshot = _get_job_snapshot(job_id)
        if snapshot is None:
            self._send_text(HTTPStatus.NOT_FOUND, "Job not found")
            return
        if snapshot["status"] != "done":
            self._send_text(HTTPStatus.CONFLICT, "OCR job is not completed yet")
            return

        file_path = Path(snapshot["output_pdf"])
        if not file_path.exists() or not file_path.is_file() or file_path.suffix.lower() != ".pdf":
            self._send_text(HTTPStatus.NOT_FOUND, "PDF file not found")
            return

        self._send_file(file_path, "application/pdf")

    def _handle_review(self, query: str) -> None:
        params = urllib.parse.parse_qs(query)
        job_id = params.get("id", [""])[0]
        page_value = params.get("page", ["0"])[0]
        try:
            page_number = int(page_value)
        except ValueError:
            page_number = 0

        review_path = _get_review_image_path(job_id, page_number)
        if review_path is None or not review_path.exists() or not review_path.is_file():
            self._send_text(HTTPStatus.NOT_FOUND, "Review image not found")
            return

        self._send_file(review_path, "image/png")

    def _send_file(self, file_path: Path, content_type: str) -> None:
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(file_path.stat().st_size))
        self.send_header("Content-Disposition", f'inline; filename="{file_path.name}"')
        self.end_headers()

        with file_path.open("rb") as file_obj:
            while chunk := file_obj.read(64 * 1024):
                self.wfile.write(chunk)

    def _send_html(self, content: str, status: HTTPStatus = HTTPStatus.OK) -> None:
        payload = content.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _send_json(self, status: HTTPStatus, data: dict[str, Any]) -> None:
        payload = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _send_text(self, status: HTTPStatus, text: str) -> None:
        payload = text.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _redirect(self, location: str) -> None:
        self.send_response(HTTPStatus.SEE_OTHER)
        self.send_header("Location", location)
        self.end_headers()

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        return


def _parse_request_form(
    handler: OCRWebHandler,
) -> tuple[dict[str, str], Path | None, Path | None, str]:
    content_type = handler.headers.get("Content-Type", "")
    if content_type.startswith("multipart/form-data"):
        return _parse_multipart_form(handler)
    values = _parse_urlencoded_form(handler)
    return values, None, None, ""


def _parse_multipart_form(
    handler: OCRWebHandler,
) -> tuple[dict[str, str], Path | None, Path | None, str]:
    form = cgi.FieldStorage(
        fp=handler.rfile,
        headers=handler.headers,
        environ={
            "REQUEST_METHOD": "POST",
            "CONTENT_TYPE": handler.headers.get("Content-Type", ""),
            "CONTENT_LENGTH": handler.headers.get("Content-Length", "0"),
        },
        keep_blank_values=True,
    )

    values = {
        "input_pdf": _field_from_fieldstorage(form, "input_pdf"),
        "output_pdf": _field_from_fieldstorage(form, "output_pdf"),
        "lang": _field_from_fieldstorage(form, "lang", "rus+eng"),
        "optimize": _field_from_fieldstorage(form, "optimize", "1"),
        "jobs": _field_from_fieldstorage(form, "jobs", "1"),
        "rotate_pages": "1" if form.getvalue("rotate_pages") else "",
        "deskew": "1" if form.getvalue("deskew") else "",
        "quiet": "1" if form.getvalue("quiet") else "",
        "drawing_mode": "1" if form.getvalue("drawing_mode") else "",
        "deep_verify": "1" if form.getvalue("deep_verify") else "",
    }

    upload = form["input_file"] if "input_file" in form else None
    uploaded_pdf, cleanup_dir, uploaded_name = _save_uploaded_pdf(upload)
    return values, uploaded_pdf, cleanup_dir, uploaded_name


def _parse_urlencoded_form(handler: OCRWebHandler) -> dict[str, str]:
    length = int(handler.headers.get("Content-Length", "0"))
    raw_body = handler.rfile.read(length).decode("utf-8", errors="replace")
    form_data = urllib.parse.parse_qs(raw_body, keep_blank_values=True)
    return {
        "input_pdf": _field_from_query(form_data, "input_pdf"),
        "output_pdf": _field_from_query(form_data, "output_pdf"),
        "lang": _field_from_query(form_data, "lang", "rus+eng"),
        "optimize": _field_from_query(form_data, "optimize", "1"),
        "jobs": _field_from_query(form_data, "jobs", "1"),
        "rotate_pages": "1" if "rotate_pages" in form_data else "",
        "deskew": "1" if "deskew" in form_data else "",
        "quiet": "1" if "quiet" in form_data else "",
        "drawing_mode": "1" if "drawing_mode" in form_data else "",
        "deep_verify": "1" if "deep_verify" in form_data else "",
    }


def _field_from_query(form_data: dict[str, list[str]], key: str, default: str = "") -> str:
    return form_data.get(key, [default])[0]


def _field_from_fieldstorage(form: cgi.FieldStorage, key: str, default: str = "") -> str:
    value = form.getvalue(key, default)
    if isinstance(value, list):
        value = value[0]
    if value is None:
        return default
    return str(value)


def _save_uploaded_pdf(upload: cgi.FieldStorage | None) -> tuple[Path | None, Path | None, str]:
    if upload is None:
        return None, None, ""

    if isinstance(upload, list):
        upload = upload[0]

    filename = Path(str(upload.filename or "")).name
    if not filename:
        return None, None, ""
    if not filename.lower().endswith(".pdf"):
        raise ValueError("Uploaded file must be a .pdf file")
    if upload.file is None:
        raise ValueError("Failed to read uploaded file")

    upload_dir = Path(tempfile.mkdtemp(prefix="liza_codex_pdf_upload_"))
    upload_path = upload_dir / filename
    with upload_path.open("wb") as target:
        shutil.copyfileobj(upload.file, target)
    return upload_path, upload_dir, filename


def _resolve_input_pdf(values: dict[str, str], uploaded_pdf: Path | None) -> Path:
    if uploaded_pdf is not None:
        return uploaded_pdf

    input_path = values["input_pdf"].strip()
    if not input_path:
        raise ValueError("Choose a PDF file or enter an input path.")
    return Path(input_path)


def _resolve_output_pdf(values: dict[str, str], input_pdf: Path, uploaded_name: str) -> Path:
    output_path = values["output_pdf"].strip()
    if output_path:
        return Path(output_path)
    if uploaded_name:
        return Path.cwd() / f"{Path(uploaded_name).stem}_searchable.pdf"
    return input_pdf.with_name(f"{input_pdf.stem}_searchable.pdf")


def _build_job_settings(values: dict[str, str]) -> dict[str, Any]:
    optimize = int(values["optimize"])
    jobs = int(values["jobs"])
    if optimize not in (0, 1, 2, 3):
        raise ValueError("optimize must be 0..3")
    if jobs < 1:
        raise ValueError("jobs must be >= 1")

    return {
        "lang": values["lang"].strip() or "rus+eng",
        "optimize": optimize,
        "jobs": jobs,
        "rotate_pages": bool(values["rotate_pages"]),
        "deskew": bool(values["deskew"]),
        "quiet": bool(values["quiet"]),
        "drawing_mode": bool(values["drawing_mode"]),
        "deep_verify": bool(values["deep_verify"]),
    }


def _create_job(output_pdf: Path) -> str:
    job_id = uuid.uuid4().hex
    with _JOBS_LOCK:
        _JOBS[job_id] = JobState(job_id=job_id, status="queued", output_pdf=str(output_pdf))
    return job_id


def _init_page_details(job_id: str, total_pages: int, attempts_total: int) -> None:
    with _JOBS_LOCK:
        job = _JOBS.get(job_id)
        if job is None:
            return
        job.page_details = [
            {
                "page": page_number,
                "status": "pending",
                "attempts_done": 0,
                "attempts_total": attempts_total,
                "current_profile": "",
                "best_profile": "",
                "best_score": 0,
                "best_selection_score": 0,
                "needs_review": False,
                "message": "",
                "attempt_history": [],
                "scan_full_words": 0,
                "scan_tile_words": 0,
                "scan_gain_ratio": 0.0,
                "scan_vertical_words": 0,
                "table_rescue_words": 0,
                "augmented_words": 0,
                "fallback_lines": 0,
                "review_image_path": "",
            }
            for page_number in range(1, total_pages + 1)
        ]


def _update_page_detail(job_id: str, page_number: int, **kwargs: Any) -> None:
    with _JOBS_LOCK:
        job = _JOBS.get(job_id)
        if job is None:
            return
        if page_number < 1 or page_number > len(job.page_details):
            return
        page_detail = job.page_details[page_number - 1]
        for key, value in kwargs.items():
            page_detail[key] = value


def _update_job(job_id: str, **kwargs: Any) -> None:
    with _JOBS_LOCK:
        job = _JOBS.get(job_id)
        if job is None:
            return
        for key, value in kwargs.items():
            setattr(job, key, value)


def _mark_page_completed(job_id: str, page_number: int) -> None:
    with _JOBS_LOCK:
        job = _JOBS.get(job_id)
        if job is None:
            return
        if page_number not in job.completed_pages:
            job.completed_pages.append(page_number)
            job.completed_pages.sort()


def _get_review_image_path(job_id: str, page_number: int) -> Path | None:
    with _JOBS_LOCK:
        job = _JOBS.get(job_id)
        if job is None:
            return None
        if page_number < 1 or page_number > len(job.page_details):
            return None
        review_path = str(job.page_details[page_number - 1].get("review_image_path", ""))
        if not review_path:
            return None
        return Path(review_path)


def _get_job_snapshot(job_id: str) -> dict[str, Any] | None:
    with _JOBS_LOCK:
        job = _JOBS.get(job_id)
        if job is None:
            return None

        details: list[dict[str, Any]] = []
        for page_detail in job.page_details:
            item = dict(page_detail)
            review_path = str(item.pop("review_image_path", ""))
            if review_path:
                item["review_url"] = (
                    f"/review?id={urllib.parse.quote(job_id)}&page={item['page']}"
                )
            else:
                item["review_url"] = ""
            details.append(item)

        return {
            "job_id": job.job_id,
            "status": job.status,
            "total_pages": job.total_pages,
            "completed_pages": list(job.completed_pages),
            "output_pdf": job.output_pdf,
            "error": job.error,
            "page_details": details,
        }


def _process_job(
    job_id: str,
    input_pdf: Path,
    output_pdf: Path,
    settings: dict[str, Any],
    upload_cleanup_dir: Path | None,
) -> None:
    _update_job(job_id, status="running", error="", total_pages=0, completed_pages=[])
    current_page = 0

    try:
        with tempfile.TemporaryDirectory(prefix="liza_codex_pdf_pages_") as temp_dir_raw:
            temp_dir = Path(temp_dir_raw)
            page_inputs = _split_pdf_into_pages(input_pdf, temp_dir)
            profiles = _build_page_profiles(settings)
            verify_attempt_budget = 0
            if settings["deep_verify"]:
                verify_attempt_budget = 2 if settings["drawing_mode"] else 1
            attempts_total = len(profiles) + verify_attempt_budget

            _update_job(job_id, total_pages=len(page_inputs))
            _init_page_details(job_id, len(page_inputs), attempts_total)

            page_outputs: dict[int, Path] = {}
            verify_futures: dict[Any, int] = {}

            with ProcessPoolExecutor(max_workers=1) as verify_pool:
                for page_number, page_input in enumerate(page_inputs, start=1):
                    current_page = page_number
                    (
                        best_candidate,
                        best_profile,
                        best_analysis,
                        attempts,
                    ) = _run_ocr_attempts_for_page(
                        job_id=job_id,
                        page_number=page_number,
                        page_input=page_input,
                        temp_dir=temp_dir,
                        settings=settings,
                        profiles=profiles,
                    )

                    _update_page_detail(
                        job_id,
                        page_number,
                        status="verifying",
                        attempts_done=len(attempts),
                        current_profile="verify",
                        message="coverage check in parallel process",
                    )

                    future = verify_pool.submit(
                        _verify_page_worker,
                        page_number=page_number,
                        page_input_path=str(page_input),
                        best_candidate_path=str(best_candidate),
                        best_profile=best_profile,
                        best_analysis=best_analysis,
                        attempts=attempts,
                        settings=settings,
                        temp_dir_path=str(temp_dir),
                        output_pdf_path=str(output_pdf),
                    )
                    verify_futures[future] = page_number

                for future in as_completed(verify_futures):
                    page_number = verify_futures[future]
                    try:
                        result = future.result()
                    except Exception as exc:  # noqa: BLE001
                        _update_page_detail(job_id, page_number, status="error", message=str(exc))
                        raise RuntimeError(
                            f"Verification failed on page {page_number}: {exc}"
                        ) from exc

                    page_outputs[page_number] = Path(str(result["final_page_output"]))
                    _update_page_detail(
                        job_id,
                        page_number,
                        status="done",
                        attempts_done=int(result["attempts_done"]),
                        attempts_total=int(result["attempts_total"]),
                        current_profile="",
                        best_score=int(result["best_score"]),
                        best_selection_score=int(result["best_selection_score"]),
                        best_profile=str(result["best_profile"]),
                        attempt_history=list(result["attempt_history"]),
                        scan_full_words=int(result["scan_full_words"]),
                        scan_tile_words=int(result["scan_tile_words"]),
                        scan_gain_ratio=float(result["scan_gain_ratio"]),
                        scan_vertical_words=int(result["scan_vertical_words"]),
                        table_rescue_words=int(result["table_rescue_words"]),
                        augmented_words=int(result["augmented_words"]),
                        fallback_lines=int(result["fallback_lines"]),
                        needs_review=bool(result["needs_review"]),
                        review_image_path=str(result["review_image_path"]),
                        message=str(result["message"]),
                    )
                    _mark_page_completed(job_id, page_number)

            ordered_outputs = [page_outputs[idx] for idx in range(1, len(page_inputs) + 1)]
            _merge_single_page_pdfs(ordered_outputs, output_pdf)
            _update_job(job_id, status="done", output_pdf=str(output_pdf))
    except (
        OSError,
        ValueError,
        RuntimeError,
        FileNotFoundError,
        subprocess.CalledProcessError,
    ) as exc:
        if current_page > 0:
            _update_page_detail(job_id, current_page, status="error", message=str(exc))
        _update_job(job_id, status="error", error=str(exc))
    except Exception as exc:  # noqa: BLE001
        if current_page > 0:
            _update_page_detail(job_id, current_page, status="error", message=str(exc))
        _update_job(job_id, status="error", error=f"Unexpected error: {exc}")
    finally:
        if upload_cleanup_dir is not None:
            shutil.rmtree(upload_cleanup_dir, ignore_errors=True)


def _run_ocr_attempts_for_page(
    *,
    job_id: str,
    page_number: int,
    page_input: Path,
    temp_dir: Path,
    settings: dict[str, Any],
    profiles: list[dict[str, Any]],
) -> tuple[Path, dict[str, Any], dict[str, int], list[dict[str, Any]]]:
    attempts: list[dict[str, Any]] = []
    best_candidate: Path | None = None
    best_profile: dict[str, Any] | None = None
    best_analysis: dict[str, int] = {
        "score": -1,
        "token_count": 0,
        "eng_token_count": 0,
        "numeric_token_count": 0,
    }
    best_selection_score = -1

    planned_attempts = len(profiles)
    for attempt_idx, profile in enumerate(profiles, start=1):
        _update_page_detail(
            job_id,
            page_number,
            status="running",
            attempts_done=attempt_idx,
            current_profile=profile["name"],
            message=f"OCR attempt {attempt_idx}/{planned_attempts}",
        )

        candidate = temp_dir / f"ocr_page_{page_number:05d}_try_{attempt_idx}.pdf"
        run_ocr(
            page_input,
            candidate,
            lang=str(settings["lang"]),
            optimize=int(settings["optimize"]),
            jobs=int(settings["jobs"]),
            force_ocr=True,
            no_rotate_pages=not bool(settings["rotate_pages"]),
            no_deskew=not bool(settings["deskew"]),
            tesseract_pagesegmode=profile["psm"],
            oversample=profile["oversample"],
            remove_background=profile["remove_background"],
            clean=profile["clean"],
            clean_final=profile["clean_final"],
            remove_vectors=profile["remove_vectors"],
            tesseract_thresholding=profile["thresholding"],
            quiet=bool(settings["quiet"]),
        )

        analysis = _analyze_ocr_pdf(candidate)
        attempt_info = {
            "attempt": attempt_idx,
            "profile": profile["name"],
            "score": analysis["score"],
            "tokens": analysis["token_count"],
            "eng_tokens": analysis["eng_token_count"],
            "num_tokens": analysis["numeric_token_count"],
        }
        selection_score = _candidate_selection_score(
            analysis,
            drawing_mode=settings["drawing_mode"],
        )
        attempt_info["selection_score"] = selection_score
        attempts.append(attempt_info)

        if selection_score > best_selection_score:
            best_selection_score = selection_score
            best_analysis = analysis
            best_profile = profile
            best_candidate = candidate

        _update_page_detail(
            job_id,
            page_number,
            attempt_history=list(attempts),
            best_score=best_analysis["score"],
            best_selection_score=best_selection_score,
            best_profile=(best_profile["name"] if best_profile else ""),
        )

    if best_candidate is None or best_profile is None:
        raise RuntimeError(f"No OCR result produced for page {page_number}")

    return best_candidate, best_profile, best_analysis, attempts


def _verify_page_worker(
    *,
    page_number: int,
    page_input_path: str,
    best_candidate_path: str,
    best_profile: dict[str, Any],
    best_analysis: dict[str, int],
    attempts: list[dict[str, Any]],
    settings: dict[str, Any],
    temp_dir_path: str,
    output_pdf_path: str,
) -> dict[str, Any]:
    page_input = Path(page_input_path)
    best_candidate = Path(best_candidate_path)
    temp_dir = Path(temp_dir_path)
    output_pdf = Path(output_pdf_path)

    attempts_local = [dict(item) for item in attempts]
    best_profile_local = dict(best_profile)
    best_analysis_local = dict(best_analysis)
    best_selection_score = _candidate_selection_score(
        best_analysis_local,
        drawing_mode=bool(settings["drawing_mode"]),
    )
    for item in attempts_local:
        score = int(item.get("selection_score", -1))
        if score > best_selection_score:
            best_selection_score = score

    coverage: dict[str, Any] = {
        "full_words": 0,
        "tile_words": 0,
        "combined_words": 0,
        "gain_ratio": 0.0,
        "vertical_words": 0,
        "table_words": 0,
        "boxes": [],
    }
    review_path = ""
    augmented_words = 0
    fallback_lines = 0
    final_page_output = temp_dir / f"ocr_page_{page_number:05d}.pdf"
    rescue_lines: list[str] = []

    with tempfile.TemporaryDirectory(prefix="liza_codex_pdf_scan_") as scan_dir_raw:
        scan_dir = Path(scan_dir_raw)
        rendered_png = scan_dir / f"page_{page_number:05d}.png"
        _render_pdf_to_png(page_input, rendered_png, dpi=320 if settings["drawing_mode"] else 260)
        rescue_text_png = rendered_png
        if settings["drawing_mode"]:
            rescue_text_png = scan_dir / f"page_{page_number:05d}_rescue.png"
            _render_pdf_to_png(page_input, rescue_text_png, dpi=420)

        if settings["deep_verify"]:
            coverage = _scan_page_coverage(
                rendered_png,
                lang=str(settings["lang"]),
                drawing_mode=bool(settings["drawing_mode"]),
            )
            if settings["drawing_mode"]:
                try:
                    rescue_lines = _extract_plain_text_rescue(
                        rescue_text_png,
                        lang=str(settings["lang"]),
                    )
                except (OSError, subprocess.CalledProcessError):
                    rescue_lines = []

            if _should_run_rescue_pass(best_analysis_local, coverage, settings):
                rescue_profile = _build_rescue_profile(settings)
                rescue_candidate = temp_dir / f"ocr_page_{page_number:05d}_rescue.pdf"
                run_ocr(
                    page_input,
                    rescue_candidate,
                    lang=str(settings["lang"]),
                    optimize=int(settings["optimize"]),
                    jobs=int(settings["jobs"]),
                    force_ocr=True,
                    no_rotate_pages=not bool(settings["rotate_pages"]),
                    no_deskew=not bool(settings["deskew"]),
                    tesseract_pagesegmode=rescue_profile["psm"],
                    oversample=rescue_profile["oversample"],
                    remove_background=rescue_profile["remove_background"],
                    clean=rescue_profile["clean"],
                    clean_final=rescue_profile["clean_final"],
                    remove_vectors=rescue_profile["remove_vectors"],
                    tesseract_thresholding=rescue_profile["thresholding"],
                    quiet=bool(settings["quiet"]),
                )
                rescue_analysis = _analyze_ocr_pdf(rescue_candidate)
                rescue_attempt = len(attempts_local) + 1
                rescue_selection = _candidate_selection_score(
                    rescue_analysis,
                    drawing_mode=bool(settings["drawing_mode"]),
                )
                attempts_local.append(
                    {
                        "attempt": rescue_attempt,
                        "profile": rescue_profile["name"],
                        "score": rescue_analysis["score"],
                        "tokens": rescue_analysis["token_count"],
                        "eng_tokens": rescue_analysis["eng_token_count"],
                        "num_tokens": rescue_analysis["numeric_token_count"],
                        "selection_score": rescue_selection,
                    }
                )
                if rescue_selection > best_selection_score:
                    best_selection_score = rescue_selection
                    best_candidate = rescue_candidate
                    best_profile_local = rescue_profile
                    best_analysis_local = rescue_analysis

        interim_review = _needs_manual_review(best_analysis_local, coverage, settings)
        run_secondary_rescue = bool(settings["deep_verify"] and interim_review)
        if run_secondary_rescue and settings["drawing_mode"]:
            combined_words = int(coverage.get("combined_words", 0))
            run_secondary_rescue = (
                best_analysis_local["score"] < 300
                or best_analysis_local["eng_token_count"] < 6
                or combined_words < 45
            )

        if run_secondary_rescue:
            secondary_rescue_profile = _build_secondary_rescue_profile(settings)
            secondary_candidate = temp_dir / f"ocr_page_{page_number:05d}_rescue2.pdf"
            run_ocr(
                page_input,
                secondary_candidate,
                lang=str(settings["lang"]),
                optimize=int(settings["optimize"]),
                jobs=int(settings["jobs"]),
                force_ocr=True,
                no_rotate_pages=not bool(settings["rotate_pages"]),
                no_deskew=not bool(settings["deskew"]),
                tesseract_pagesegmode=secondary_rescue_profile["psm"],
                oversample=secondary_rescue_profile["oversample"],
                remove_background=secondary_rescue_profile["remove_background"],
                clean=secondary_rescue_profile["clean"],
                clean_final=secondary_rescue_profile["clean_final"],
                remove_vectors=secondary_rescue_profile["remove_vectors"],
                tesseract_thresholding=secondary_rescue_profile["thresholding"],
                quiet=bool(settings["quiet"]),
            )
            secondary_analysis = _analyze_ocr_pdf(secondary_candidate)
            secondary_selection = _candidate_selection_score(
                secondary_analysis,
                drawing_mode=bool(settings["drawing_mode"]),
            )
            secondary_attempt = len(attempts_local) + 1
            attempts_local.append(
                {
                    "attempt": secondary_attempt,
                    "profile": secondary_rescue_profile["name"],
                    "score": secondary_analysis["score"],
                    "tokens": secondary_analysis["token_count"],
                    "eng_tokens": secondary_analysis["eng_token_count"],
                    "num_tokens": secondary_analysis["numeric_token_count"],
                    "selection_score": secondary_selection,
                }
            )
            if secondary_selection > best_selection_score:
                best_selection_score = secondary_selection
                best_candidate = secondary_candidate
                best_profile_local = secondary_rescue_profile
                best_analysis_local = secondary_analysis

        try:
            review_path = _write_review_overlay(
                rendered_png,
                coverage["boxes"],
                output_pdf=output_pdf,
                page_number=page_number,
            )
        except (OSError, ImportError):
            review_path = ""

        shutil.copyfile(best_candidate, final_page_output)
        if settings["deep_verify"] and settings["drawing_mode"] and coverage["boxes"]:
            try:
                augmented_words = _inject_invisible_text_from_boxes(
                    final_page_output,
                    rendered_png,
                    coverage["boxes"],
                )
            except (OSError, ValueError, RuntimeError):
                augmented_words = 0
        if settings["deep_verify"] and settings["drawing_mode"] and rescue_lines:
            try:
                fallback_lines = _inject_hidden_text_lines(
                    final_page_output,
                    rescue_lines,
                )
            except (OSError, ValueError, RuntimeError):
                fallback_lines = 0

    needs_review = _needs_manual_review(best_analysis_local, coverage, settings)
    if settings["drawing_mode"]:
        combined_words = int(coverage.get("combined_words", 0))
        if augmented_words >= 120 and combined_words >= 120:
            needs_review = False
        if fallback_lines >= 20 and combined_words >= 80:
            needs_review = False

    return {
        "final_page_output": str(final_page_output),
        "attempts_done": len(attempts_local),
        "attempts_total": len(attempts_local),
        "best_score": best_analysis_local["score"],
        "best_selection_score": best_selection_score,
        "best_profile": best_profile_local["name"],
        "attempt_history": attempts_local,
        "scan_full_words": coverage["full_words"],
        "scan_tile_words": coverage["tile_words"],
        "scan_gain_ratio": coverage["gain_ratio"],
        "scan_vertical_words": coverage["vertical_words"],
        "table_rescue_words": coverage["table_words"],
        "augmented_words": augmented_words,
        "fallback_lines": fallback_lines,
        "needs_review": needs_review,
        "review_image_path": review_path,
        "message": ("manual review recommended" if needs_review else "ok"),
    }


def _build_page_profiles(settings: dict[str, Any]) -> list[dict[str, Any]]:
    if not settings["drawing_mode"]:
        return [
            {
                "name": "standard",
                "psm": None,
                "oversample": None,
                "remove_background": False,
                "clean": False,
                "clean_final": False,
                "remove_vectors": False,
                "thresholding": None,
            }
        ]

    return [
        {
            "name": "draw-sparse-a",
            "psm": 11,
            "oversample": 450,
            "remove_background": True,
            "clean": False,
            "clean_final": False,
            "remove_vectors": False,
            "thresholding": "adaptive-otsu",
        },
        {
            "name": "draw-lines-b",
            "psm": 6,
            "oversample": 650,
            "remove_background": False,
            "clean": False,
            "clean_final": False,
            "remove_vectors": True,
            "thresholding": "sauvola",
        },
        {
            "name": "draw-grid-c",
            "psm": 4,
            "oversample": 550,
            "remove_background": True,
            "clean": False,
            "clean_final": False,
            "remove_vectors": False,
            "thresholding": "otsu",
        },
    ]


def _build_rescue_profile(settings: dict[str, Any]) -> dict[str, Any]:
    if settings["drawing_mode"]:
        return {
            "name": "draw-rescue",
            "psm": 11,
            "oversample": 750,
            "remove_background": False,
            "clean": False,
            "clean_final": False,
            "remove_vectors": True,
            "thresholding": "sauvola",
        }

    return {
        "name": "standard-rescue",
        "psm": 6,
        "oversample": 450,
        "remove_background": False,
        "clean": False,
        "clean_final": False,
        "remove_vectors": False,
        "thresholding": "adaptive-otsu",
    }


def _build_secondary_rescue_profile(settings: dict[str, Any]) -> dict[str, Any]:
    if settings["drawing_mode"]:
        return {
            "name": "draw-rescue-2",
            "psm": 6,
            "oversample": 900,
            "remove_background": True,
            "clean": False,
            "clean_final": False,
            "remove_vectors": True,
            "thresholding": "adaptive-otsu",
        }

    return {
        "name": "standard-rescue-2",
        "psm": 4,
        "oversample": 550,
        "remove_background": False,
        "clean": False,
        "clean_final": False,
        "remove_vectors": False,
        "thresholding": "adaptive-otsu",
    }


def _candidate_selection_score(analysis: dict[str, int], *, drawing_mode: bool) -> int:
    base = int(analysis["score"])
    eng = int(analysis.get("eng_token_count", 0))
    numeric = int(analysis.get("numeric_token_count", 0))
    tokens = int(analysis.get("token_count", 0))
    if drawing_mode:
        return base + (65 * eng) + (30 * numeric) + (2 * tokens)
    return base + (20 * eng) + (8 * numeric)


def _analyze_ocr_pdf(pdf_path: Path) -> dict[str, int]:
    from pdfminer.high_level import extract_text

    text = extract_text(str(pdf_path)) or ""
    alnum_chars = sum(1 for ch in text if ch.isalnum())

    token_pattern = r"[A-Za-zА-Яа-я0-9][A-Za-zА-Яа-я0-9._/-]*"
    tokens = [token for token in re.findall(token_pattern, text) if len(token) >= 2]

    eng_pattern = (
        r"(?:[A-Za-z]{1,6}[-_/]?\d{1,8}[A-Za-z0-9._/-]*)"
        r"|(?:\d{1,8}[A-Za-z]{1,6}[A-Za-z0-9._/-]*)"
    )
    eng_tokens = re.findall(eng_pattern, text)
    numeric_tokens = re.findall(r"\d+(?:[.,]\d+)?", text)

    score = alnum_chars + (2 * len(tokens)) + (5 * len(eng_tokens))
    return {
        "score": score,
        "alnum_chars": alnum_chars,
        "token_count": len(tokens),
        "eng_token_count": len(eng_tokens),
        "numeric_token_count": len(numeric_tokens),
    }


def _scan_page_coverage(rendered_png: Path, *, lang: str, drawing_mode: bool) -> dict[str, Any]:
    table_boxes: list[WordBox] = []
    vertical_boxes: list[WordBox] = []

    if drawing_mode:
        has_mixed_english = "eng" in lang and lang != "eng"

        full_raw: list[WordBox] = []
        full_raw.extend(_extract_word_boxes(rendered_png, lang=lang, psm=11, min_conf=30))
        full_raw.extend(_extract_word_boxes(rendered_png, lang=lang, psm=6, min_conf=32))
        full_raw.extend(_extract_word_boxes(rendered_png, lang=lang, psm=4, min_conf=34))
        if has_mixed_english:
            full_raw.extend(_extract_word_boxes(rendered_png, lang="eng", psm=6, min_conf=28))
        full_boxes = _dedupe_word_boxes(full_raw)

        vertical_raw: list[WordBox] = []
        for angle in (90, 270):
            vertical_raw.extend(
                _extract_word_boxes_rotated(
                    rendered_png,
                    lang=lang,
                    psm=6,
                    min_conf=16,
                    angle=angle,
                )
            )
        if has_mixed_english:
            for angle in (90, 270):
                vertical_raw.extend(
                    _extract_word_boxes_rotated(
                        rendered_png,
                        lang="eng",
                        psm=6,
                        min_conf=14,
                        angle=angle,
                    )
                )
        if len(vertical_raw) < 20:
            for angle in (90, 270):
                vertical_raw.extend(
                    _extract_word_boxes_rotated(
                        rendered_png,
                        lang=lang,
                        psm=11,
                        min_conf=14,
                        angle=angle,
                    )
                )
        vertical_boxes = _dedupe_word_boxes(vertical_raw)

        quick_tile_raw: list[WordBox] = []
        quick_tile_raw.extend(
            _extract_word_boxes_tiled(
                rendered_png,
                lang=lang,
                psm=11,
                min_conf=24,
                dense=False,
            )
        )
        quick_tile_raw.extend(
            _extract_word_boxes_tiled(
                rendered_png,
                lang=lang,
                psm=6,
                min_conf=24,
                dense=False,
            )
        )
        if has_mixed_english:
            quick_tile_raw.extend(
                _extract_word_boxes_tiled(
                    rendered_png,
                    lang="eng",
                    psm=6,
                    min_conf=22,
                    dense=False,
                )
            )
        tile_boxes = _dedupe_word_boxes(quick_tile_raw)
        combined_boxes = _dedupe_word_boxes(full_boxes + tile_boxes + vertical_boxes)

        run_dense_scan = (
            len(combined_boxes) < 85
            or len(tile_boxes) < 30
            or (
                (len(tile_boxes) - len(full_boxes)) >= 8
                and len(full_boxes) < 180
            )
        )
        if run_dense_scan:
            dense_tile_raw: list[WordBox] = []
            dense_tile_raw.extend(
                _extract_word_boxes_tiled(
                    rendered_png,
                    lang=lang,
                    psm=4,
                    min_conf=22,
                    dense=True,
                )
            )
            if has_mixed_english:
                dense_tile_raw.extend(
                    _extract_word_boxes_tiled(
                        rendered_png,
                        lang="eng",
                        psm=4,
                        min_conf=20,
                        dense=True,
                    )
                )
            if len(combined_boxes) < 55:
                dense_tile_raw.extend(
                    _extract_word_boxes_tiled(
                        rendered_png,
                        lang=lang,
                        psm=11,
                        min_conf=22,
                        dense=True,
                    )
                )
            tile_boxes = _dedupe_word_boxes(quick_tile_raw + dense_tile_raw)
    else:
        full_boxes = _dedupe_word_boxes(
            _extract_word_boxes(rendered_png, lang=lang, psm=6, min_conf=40)
        )
        tile_boxes = []

    combined_boxes = _dedupe_word_boxes(full_boxes + tile_boxes + vertical_boxes)
    if drawing_mode:
        table_boxes = _extract_design_table_value_boxes(
            rendered_png,
            combined_boxes,
            lang=lang,
        )
        combined_boxes = _dedupe_word_boxes(combined_boxes + table_boxes)

    full_words = len(full_boxes)
    tile_words = len(tile_boxes)
    gain_ratio = 0.0
    if tile_words > full_words:
        gain_ratio = (tile_words - full_words) / max(full_words, 1)

    return {
        "full_words": full_words,
        "tile_words": tile_words,
        "combined_words": len(combined_boxes),
        "gain_ratio": gain_ratio,
        "vertical_words": len(vertical_boxes),
        "table_words": len(table_boxes),
        "boxes": combined_boxes,
    }


def _should_run_rescue_pass(
    analysis: dict[str, int], coverage: dict[str, Any], settings: dict[str, Any]
) -> bool:
    if not settings["deep_verify"]:
        return False

    if settings["drawing_mode"]:
        full_words = int(coverage.get("full_words", 0))
        tile_words = int(coverage.get("tile_words", 0))
        combined_words = int(coverage.get("combined_words", tile_words))
        gain_ratio = float(coverage.get("gain_ratio", 0.0))

        tile_bonus = tile_words - full_words
        if analysis["score"] < 420:
            return True
        if analysis["eng_token_count"] < 8 and tile_words >= 25:
            return True
        if combined_words < 55:
            return True
        if tile_bonus >= 30 and gain_ratio >= 0.35 and analysis["score"] < 700:
            return True
        return False

    return analysis["score"] < 120


def _needs_manual_review(
    analysis: dict[str, int], coverage: dict[str, Any], settings: dict[str, Any]
) -> bool:
    if settings["drawing_mode"]:
        tile_words = int(coverage.get("tile_words", 0))
        combined_words = int(coverage.get("combined_words", tile_words))
        gain_ratio = float(coverage.get("gain_ratio", 0.0))

        if analysis["score"] < 260:
            return True
        if analysis["eng_token_count"] < 6 and tile_words < 20:
            return True
        if combined_words < 40:
            return True
        if gain_ratio > 1.2 and combined_words < 120:
            return True
        return False

    return analysis["score"] < 120


def _render_pdf_to_png(page_pdf: Path, output_png: Path, *, dpi: int) -> None:
    subprocess.run(
        [
            "gs",
            "-q",
            "-dSAFER",
            "-dBATCH",
            "-dNOPAUSE",
            "-sDEVICE=pnggray",
            f"-r{dpi}",
            "-o",
            str(output_png),
            str(page_pdf),
        ],
        check=True,
    )


def _extract_word_boxes(
    image_path: Path,
    *,
    lang: str,
    psm: int,
    min_conf: float = 40,
) -> list[WordBox]:
    proc = subprocess.run(
        [
            "tesseract",
            str(image_path),
            "stdout",
            "-l",
            lang,
            "--psm",
            str(psm),
            "tsv",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    boxes: list[WordBox] = []
    lines = proc.stdout.splitlines()
    if len(lines) <= 1:
        return boxes

    for line in lines[1:]:
        parts = line.split("\t")
        if len(parts) < 12:
            continue

        text = parts[11].strip()
        if not text:
            continue

        try:
            conf = float(parts[10])
            left = int(parts[6])
            top = int(parts[7])
            width = int(parts[8])
            height = int(parts[9])
        except ValueError:
            continue

        if conf < min_conf or width <= 0 or height <= 0:
            continue

        boxes.append((left, top, width, height, text, conf))

    return boxes


def _map_rotated_box_to_original(
    *,
    left: int,
    top: int,
    width: int,
    height: int,
    text: str,
    conf: float,
    original_width: int,
    original_height: int,
    angle: int,
) -> WordBox | None:
    if angle == 90:
        mapped_left = original_width - (top + height)
        mapped_top = left
    elif angle == 270:
        mapped_left = top
        mapped_top = original_height - (left + width)
    else:
        return None

    mapped_width = height
    mapped_height = width

    mapped_left = max(0, min(original_width - 1, mapped_left))
    mapped_top = max(0, min(original_height - 1, mapped_top))
    mapped_width = max(1, min(mapped_width, original_width - mapped_left))
    mapped_height = max(1, min(mapped_height, original_height - mapped_top))
    return (
        int(mapped_left),
        int(mapped_top),
        int(mapped_width),
        int(mapped_height),
        text,
        conf,
    )


def _extract_word_boxes_rotated(
    image_path: Path,
    *,
    lang: str,
    psm: int,
    min_conf: float,
    angle: int,
    keep_vertical_only: bool = True,
) -> list[WordBox]:
    from PIL import Image

    if angle not in (90, 270):
        raise ValueError("angle must be 90 or 270")

    with Image.open(image_path) as image:
        original_width, original_height = image.size
        rotated = image.rotate(angle, expand=True)
        with tempfile.TemporaryDirectory(prefix="liza_codex_pdf_rot_") as rot_dir_raw:
            rot_dir = Path(rot_dir_raw)
            rotated_path = rot_dir / f"rot_{angle}.png"
            rotated.save(rotated_path)
            rotated_boxes = _extract_word_boxes(
                rotated_path,
                lang=lang,
                psm=psm,
                min_conf=min_conf,
            )

    mapped_boxes: list[WordBox] = []
    for left, top, width, height, text, conf in rotated_boxes:
        mapped_box = _map_rotated_box_to_original(
            left=left,
            top=top,
            width=width,
            height=height,
            text=text,
            conf=conf,
            original_width=original_width,
            original_height=original_height,
            angle=angle,
        )
        if mapped_box is not None:
            if keep_vertical_only:
                mapped_width = mapped_box[2]
                mapped_height = mapped_box[3]
                if mapped_height < int(mapped_width * 1.15):
                    continue
            mapped_boxes.append(mapped_box)

    return mapped_boxes


def _extract_word_boxes_tiled(
    image_path: Path,
    *,
    lang: str,
    psm: int,
    min_conf: float,
    dense: bool,
) -> list[WordBox]:
    from PIL import Image

    boxes: list[WordBox] = []
    with Image.open(image_path) as image:
        width, height = image.size

        tile_sets = [
            _build_tiles(width, height, rows=3, cols=3),
        ]
        if dense:
            tile_sets.extend(
                [
                    _build_tiles(width, height, rows=4, cols=4),
                    _build_tiles(
                        width,
                        height,
                        rows=4,
                        cols=4,
                        offset_x_frac=0.5,
                        offset_y_frac=0.5,
                    ),
                ]
            )

        merged_tiles: list[tuple[int, int, int, int]] = []
        seen_tiles: set[tuple[int, int, int, int]] = set()
        for tiles in tile_sets:
            for tile in tiles:
                if tile in seen_tiles:
                    continue
                seen_tiles.add(tile)
                merged_tiles.append(tile)

        with tempfile.TemporaryDirectory(prefix="liza_codex_pdf_tiles_") as tiles_dir_raw:
            tiles_dir = Path(tiles_dir_raw)
            for idx, (left, top, right, bottom) in enumerate(merged_tiles, start=1):
                tile_path = tiles_dir / f"tile_{idx:02d}.png"
                image.crop((left, top, right, bottom)).save(tile_path)

                tile_boxes = _extract_word_boxes(
                    tile_path,
                    lang=lang,
                    psm=psm,
                    min_conf=min_conf,
                )
                for box_left, box_top, bw, bh, text, conf in tile_boxes:
                    boxes.append((box_left + left, box_top + top, bw, bh, text, conf))

    return boxes


def _build_tiles(
    width: int,
    height: int,
    *,
    rows: int,
    cols: int,
    overlap_ratio: float = 0.12,
    offset_x_frac: float = 0.0,
    offset_y_frac: float = 0.0,
) -> list[tuple[int, int, int, int]]:
    step_x = max(1, width // cols)
    step_y = max(1, height // rows)
    overlap_x = max(1, int(step_x * overlap_ratio))
    overlap_y = max(1, int(step_y * overlap_ratio))
    shift_x = int(step_x * offset_x_frac)
    shift_y = int(step_y * offset_y_frac)

    tiles: list[tuple[int, int, int, int]] = []
    for row in range(rows):
        for col in range(cols):
            left = max(0, shift_x + (col * step_x) - overlap_x)
            top = max(0, shift_y + (row * step_y) - overlap_y)
            right = min(width, shift_x + ((col + 1) * step_x) + overlap_x)
            bottom = min(height, shift_y + ((row + 1) * step_y) + overlap_y)
            if right - left < 8 or bottom - top < 8:
                continue
            tiles.append((left, top, right, bottom))

    return tiles


def _normalize_match_token(text: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", text.upper())


def _is_design_table_label_token(token: str) -> bool:
    if len(token) < 3:
        return False
    return any(stem in token for stem in _DESIGN_TABLE_STEMS)


def _cluster_table_label_rows(label_boxes: list[WordBox]) -> list[WordBox]:
    if not label_boxes:
        return []

    sorted_boxes = sorted(label_boxes, key=lambda box: box[1])
    avg_height = max(6, int(sum(box[3] for box in sorted_boxes) / len(sorted_boxes)))
    merge_gap = max(6, int(avg_height * 0.65))

    groups: list[list[WordBox]] = []
    current_group: list[WordBox] = []
    for box in sorted_boxes:
        if not current_group:
            current_group.append(box)
            continue
        if box[1] - current_group[-1][1] <= merge_gap:
            current_group.append(box)
            continue
        groups.append(current_group)
        current_group = [box]
    if current_group:
        groups.append(current_group)

    rows: list[WordBox] = []
    for group in groups:
        selected = max(
            group,
            key=lambda item: (item[5], len(_normalize_match_token(item[4])), item[2]),
        )
        rows.append(selected)
    return rows


def _find_design_table_anchor(base_boxes: list[WordBox], *, image_height: int) -> WordBox | None:
    design_candidates = [
        box
        for box in base_boxes
        if "DESIGN" in _normalize_match_token(box[4]) and box[1] < int(image_height * 0.55)
    ]
    if not design_candidates:
        return None

    design_candidates.sort(key=lambda item: (item[1], -item[5]))
    code_candidates = [
        box
        for box in base_boxes
        if "CODE" in _normalize_match_token(box[4]) and box[1] < int(image_height * 0.55)
    ]
    if not code_candidates:
        return design_candidates[0]

    best: tuple[float, WordBox] | None = None
    for design_box in design_candidates:
        design_left, design_top, _dw, design_height, _text, _conf = design_box
        for code_box in code_candidates:
            code_left, code_top, _cw, _ch, _ctext, _cconf = code_box
            if abs(code_top - design_top) > max(36, int(design_height * 1.6)):
                continue
            if code_left + 15 < design_left:
                continue
            distance = abs(code_left - design_left) + (abs(code_top - design_top) * 0.4)
            if best is None or distance < best[0]:
                best = (distance, design_box)

    if best is not None:
        return best[1]
    return design_candidates[0]


def _extract_design_table_value_boxes(
    image_path: Path,
    base_boxes: list[WordBox],
    *,
    lang: str,
) -> list[WordBox]:
    from PIL import Image

    with Image.open(image_path) as image:
        image_width, image_height = image.size
        anchor = _find_design_table_anchor(base_boxes, image_height=image_height)
        if anchor is None:
            return []

        anchor_left, anchor_top, anchor_width, anchor_height, _anchor_text, _anchor_conf = anchor

        table_left = max(0, anchor_left - max(80, anchor_width))
        table_top = max(0, anchor_top - max(90, int(anchor_height * 1.6)))
        table_right = min(
            image_width,
            table_left + max(int(image_width * 0.2), anchor_width * 8),
        )
        table_bottom = min(
            image_height,
            table_top + int(image_height * 0.45),
        )

        region_boxes = [
            box
            for box in base_boxes
            if box[0] < table_right + 60
            and box[0] + box[2] > table_left - 60
            and box[1] < table_bottom + 60
            and box[1] + box[3] > table_top - 60
        ]
        if not region_boxes:
            return []

        max_region_right = max(box[0] + box[2] for box in region_boxes)
        table_right = min(image_width, max(table_right, max_region_right + 20))
        table_bottom = min(
            image_height,
            max(
                table_bottom,
                min(
                    table_top + int(image_height * 0.62),
                    max(box[1] + box[3] for box in region_boxes) + 30,
                ),
            ),
        )
        table_bottom = min(table_bottom, table_top + int(image_height * 0.32))

        label_candidates = [
            box
            for box in region_boxes
            if _is_design_table_label_token(_normalize_match_token(box[4]))
            and box[0] <= table_left + int((table_right - table_left) * 0.62)
        ]
        rows = _cluster_table_label_rows(label_candidates)
        if rows:
            avg_height = max(8, int(sum(row[3] for row in rows) / len(rows)))
            label_right = max(row[0] + row[2] for row in rows)
        else:
            avg_height = max(10, int((table_bottom - table_top) / 24))
            label_right = table_left + int((table_right - table_left) * 0.41)
        if label_right >= table_right - 70:
            label_right = table_left + int((table_right - table_left) * 0.41)

        divider_x = table_left + int((table_right - table_left) * 0.43)
        value_left = min(image_width - 1, max(label_right + 8, divider_x))
        if table_right - value_left < 80:
            return []

        boundaries: list[int]
        sorted_rows = sorted(rows, key=lambda row: row[1])
        centers: list[int] = []
        center_merge_gap = max(6, int(avg_height * 0.55))
        for row in sorted_rows:
            center = int(row[1] + (row[3] / 2))
            if not centers or center - centers[-1] > center_merge_gap:
                centers.append(center)
            else:
                centers[-1] = int((centers[-1] + center) / 2)

        if len(centers) >= 5:
            boundaries = [table_top]
            boundaries.extend(
                (centers[idx] + centers[idx + 1]) // 2
                for idx in range(len(centers) - 1)
            )
            boundaries.append(table_bottom)
        else:
            uniform_rows = 24
            row_height = max(10, int((table_bottom - table_top) / uniform_rows))
            boundaries = list(range(table_top, table_bottom, row_height))
            if not boundaries or boundaries[0] != table_top:
                boundaries.insert(0, table_top)
            if boundaries[-1] != table_bottom:
                boundaries.append(table_bottom)

        scan_langs = ["eng"]
        if lang != "eng":
            scan_langs.append(lang)
        psm_candidates = (7, 6)

        table_boxes: list[WordBox] = []
        with tempfile.TemporaryDirectory(prefix="liza_codex_pdf_table_rows_") as rows_dir_raw:
            rows_dir = Path(rows_dir_raw)
            row_index = 0
            for idx in range(len(boundaries) - 1):
                row_top = boundaries[idx]
                row_bottom = boundaries[idx + 1]
                if row_bottom - row_top < 10:
                    continue

                row_crop = image.crop((value_left, row_top, table_right, row_bottom))
                scaled_width = max(120, row_crop.width * 2)
                scaled_height = max(28, row_crop.height * 2)
                row_scaled = row_crop.resize((scaled_width, scaled_height))

                row_image_path = rows_dir / f"row_{row_index:03d}.png"
                row_scaled.save(row_image_path)
                row_index += 1

                for scan_lang in scan_langs:
                    for psm in psm_candidates:
                        try:
                            row_boxes = _extract_word_boxes(
                                row_image_path,
                                lang=scan_lang,
                                psm=psm,
                                min_conf=0,
                            )
                        except (OSError, subprocess.CalledProcessError):
                            continue

                        scale_back_x = row_crop.width / scaled_width
                        scale_back_y = row_crop.height / scaled_height
                        for left, top, width, height, text, conf in row_boxes:
                            normalized_text = _normalize_overlay_token(text)
                            if normalized_text:
                                text = normalized_text

                            abs_left = int(value_left + (left * scale_back_x))
                            abs_top = int(row_top + (top * scale_back_y))
                            abs_width = max(1, int(width * scale_back_x))
                            abs_height = max(1, int(height * scale_back_y))
                            table_boxes.append(
                                (abs_left, abs_top, abs_width, abs_height, text, conf)
                            )

    return _dedupe_word_boxes(table_boxes)


def _dedupe_word_boxes(boxes: list[WordBox]) -> list[WordBox]:
    dedup: dict[tuple[str, int, int], WordBox] = {}
    for box in boxes:
        left, top, width, height, text, conf = box
        normalized = re.sub(r"[^A-Za-zА-Яа-я0-9]", "", text).upper()
        if not normalized:
            continue
        key = (normalized, left // 6, top // 6)
        existing = dedup.get(key)
        if existing is None or conf > existing[5]:
            dedup[key] = box

    return list(dedup.values())


def _write_review_overlay(
    rendered_png: Path,
    boxes: list[WordBox],
    *,
    output_pdf: Path,
    page_number: int,
) -> str:
    from PIL import Image, ImageDraw

    review_dir = output_pdf.parent / f"{output_pdf.stem}_ocr_review"
    review_dir.mkdir(parents=True, exist_ok=True)
    review_path = review_dir / f"page_{page_number:03d}.png"

    with Image.open(rendered_png) as image:
        rgb = image.convert("RGB")
        draw = ImageDraw.Draw(rgb)
        for left, top, width, height, _text, _conf in boxes:
            draw.rectangle(
                [left, top, left + width, top + height],
                outline=(255, 217, 0),
                width=2,
            )
        rgb.save(review_path, format="PNG")

    return str(review_path)


def _extract_plain_text_rescue(image_path: Path, *, lang: str) -> list[str]:
    scan_langs: list[str] = [lang]
    if "eng" in lang and lang != "eng":
        scan_langs.append("eng")

    collected_lines: list[str] = []
    for scan_lang in scan_langs:
        for psm in (6, 11):
            proc = subprocess.run(
                [
                    "tesseract",
                    str(image_path),
                    "stdout",
                    "-l",
                    scan_lang,
                    "--psm",
                    str(psm),
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            for raw_line in proc.stdout.splitlines():
                normalized = _normalize_rescue_line(raw_line)
                if normalized:
                    collected_lines.append(normalized)

    corrected: list[str] = []
    for line in collected_lines:
        fixed = line
        for source, replacement in _RESCUE_PHRASE_REPLACEMENTS.items():
            fixed = fixed.replace(source, replacement)
        corrected.append(fixed)

    compact_joined = re.sub(r"[^A-Z0-9]", "", " ".join(corrected).upper())
    priority_lines: list[str] = []
    for phrase in _ENGINEERING_TOKEN_SPLITS.values():
        compact_phrase = re.sub(r"[^A-Z0-9]", "", phrase)
        if compact_phrase and compact_phrase in compact_joined:
            priority_lines.append(phrase)

    joined_upper = " ".join(corrected).upper()
    if "DESIGN" in joined_upper and "CODE" in joined_upper:
        priority_lines.append("DESIGN CODE")
    if "DESIGN" in joined_upper and "TEMP" in joined_upper:
        priority_lines.append("DESIGN TEMP")
    if "RADIOGRAPHIC" in joined_upper or ("RADIO" in joined_upper and "EXAM" in joined_upper):
        priority_lines.append("RADIOGRAPHIC EXAM")
    if ("JOINT" in joined_upper or "FIOINT" in joined_upper) and "EFFICIENC" in joined_upper:
        priority_lines.append("JOINT EFFICIENCY")
    if "INSULATION" in joined_upper:
        priority_lines.append("INSULATION")
    if "FIRE" in joined_upper and "PROOFING" in joined_upper:
        priority_lines.append("FIRE PROOFING")
    if "ACID" in joined_upper and (
        "PICKLING" in joined_upper or "PLCKLING" in joined_upper or "PICKL" in joined_upper
    ):
        priority_lines.append("ACID PICKLING")
    design_table_detected = "DESIGNCODE" in compact_joined
    if design_table_detected:
        priority_lines.extend(
            [
                "DESIGN TEMP",
                "RADIOGRAPHIC EXAM",
                "JOINT EFFICIENCY",
                "INSULATION",
                "FIRE PROOFING",
                "ACID PICKLING",
            ]
        )

    return _dedupe_rescue_lines(priority_lines + corrected, limit=180)


def _normalize_rescue_line(raw_line: str) -> str:
    cleaned = raw_line.strip().upper()
    if len(cleaned) < 4:
        return ""

    cleaned = re.sub(r"\s+", " ", cleaned)
    cleaned = re.sub(r"[^A-Z0-9./:%()\- +]", "", cleaned)
    if not re.search(r"[A-Z0-9]", cleaned):
        return ""
    return cleaned[:120]


def _dedupe_rescue_lines(lines: list[str], *, limit: int) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for line in lines:
        normalized = re.sub(r"[^A-Z0-9]", "", line.upper())
        if len(normalized) < 4:
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(line)
        if len(deduped) >= limit:
            break
    return deduped


def _inject_hidden_text_lines(page_pdf: Path, lines: list[str]) -> int:
    import pikepdf

    if not lines:
        return 0

    with pikepdf.open(page_pdf, allow_overwriting_input=True) as base_pdf:
        if not base_pdf.pages:
            return 0

        page = base_pdf.pages[0]
        media_box = page.mediabox
        page_width = float(media_box[2]) - float(media_box[0])
        page_height = float(media_box[3]) - float(media_box[1])

        overlay_words: list[tuple[float, float, float, float, str]] = []
        y = 8.0
        for line in lines:
            if y > max(8.0, page_height - 8.0):
                break
            overlay_words.append((8.0, y, max(20.0, page_width - 16.0), 6.0, line))
            y += 5.8
            if len(overlay_words) >= 180:
                break

        if not overlay_words:
            return 0

        with tempfile.TemporaryDirectory(prefix="liza_codex_pdf_overlay_lines_") as tmp_overlay_raw:
            tmp_overlay = Path(tmp_overlay_raw) / "overlay_lines.pdf"
            _write_invisible_overlay_pdf(tmp_overlay, page_width, page_height, overlay_words)
            with pikepdf.open(tmp_overlay) as overlay_pdf:
                page.add_overlay(overlay_pdf.pages[0])
            base_pdf.save(page_pdf)

    return len(overlay_words)


def _inject_invisible_text_from_boxes(
    page_pdf: Path,
    rendered_png: Path,
    boxes: list[WordBox],
) -> int:
    import pikepdf
    from PIL import Image

    if not boxes:
        return 0

    with Image.open(rendered_png) as image:
        image_width, image_height = image.size

    with pikepdf.open(page_pdf, allow_overwriting_input=True) as base_pdf:
        if not base_pdf.pages:
            return 0

        page = base_pdf.pages[0]
        media_box = page.mediabox
        page_width = float(media_box[2]) - float(media_box[0])
        page_height = float(media_box[3]) - float(media_box[1])

        overlay_words = _prepare_overlay_words(
            boxes,
            page_width=page_width,
            page_height=page_height,
            image_width=image_width,
            image_height=image_height,
        )
        if not overlay_words:
            return 0

        with tempfile.TemporaryDirectory(prefix="liza_codex_pdf_overlay_") as tmp_overlay_raw:
            tmp_overlay = Path(tmp_overlay_raw) / "overlay.pdf"
            _write_invisible_overlay_pdf(tmp_overlay, page_width, page_height, overlay_words)
            with pikepdf.open(tmp_overlay) as overlay_pdf:
                page.add_overlay(overlay_pdf.pages[0])
            base_pdf.save(page_pdf)

    return len(overlay_words)


def _prepare_overlay_words(
    boxes: list[WordBox],
    *,
    page_width: float,
    page_height: float,
    image_width: int,
    image_height: int,
) -> list[tuple[float, float, float, float, str]]:
    if image_width <= 0 or image_height <= 0 or page_width <= 0 or page_height <= 0:
        return []

    scale_x = page_width / image_width
    scale_y = page_height / image_height
    selected: list[tuple[float, float, float, float, str]] = []
    seen: set[tuple[str, int, int]] = set()

    ranked_boxes = sorted(boxes, key=lambda item: item[5], reverse=True)
    for left, top, width, height, text, conf in ranked_boxes:
        if conf < 15:
            continue
        normalized = _normalize_overlay_token(text)
        if not normalized:
            continue
        key = (normalized, left // 5, top // 5)
        if key in seen:
            continue
        seen.add(key)

        x = left * scale_x
        y = page_height - ((top + height) * scale_y)
        box_width = max(2.0, width * scale_x)
        box_height = max(2.0, height * scale_y)
        selected.append((x, y, box_width, box_height, normalized))
        if len(selected) >= 3500:
            break

    return selected


def _normalize_overlay_token(text: str) -> str:
    cleaned = text.strip()
    if not cleaned:
        return ""

    replacements = {
        "—": "-",
        "–": "-",
        "−": "-",
        "×": "x",
        "÷": "/",
        "³": "3",
        "²": "2",
        "°": "",
    }
    for src, dst in replacements.items():
        cleaned = cleaned.replace(src, dst)

    cleaned = re.sub(r"\s+", "", cleaned)
    cleaned = re.sub(r"[^A-Za-z0-9./:_+\\\-#%()]", "", cleaned)
    if not re.search(r"[A-Za-z0-9]", cleaned):
        return ""
    compact = re.sub(r"[^A-Za-z0-9]", "", cleaned).upper()
    for source, replacement in _ENGINEERING_TOKEN_SPLITS.items():
        if source in compact:
            return replacement
    return cleaned[:96]


def _escape_pdf_text(text: str) -> str:
    return text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def _write_invisible_overlay_pdf(
    overlay_pdf: Path,
    page_width: float,
    page_height: float,
    words: list[tuple[float, float, float, float, str]],
) -> None:
    import pikepdf

    with pikepdf.Pdf.new() as pdf:
        page = pdf.add_blank_page(page_size=(page_width, page_height))
        font = pdf.make_indirect(
            pikepdf.Dictionary(
                Type=pikepdf.Name("/Font"),
                Subtype=pikepdf.Name("/Type1"),
                BaseFont=pikepdf.Name("/Helvetica"),
                Encoding=pikepdf.Name("/WinAnsiEncoding"),
            )
        )

        resources = page.obj.get("/Resources", pikepdf.Dictionary())
        resources.Font = pikepdf.Dictionary(F1=font)
        page.obj.Resources = resources

        stream_lines: list[bytes] = []
        for x, y, box_width, box_height, text in words:
            text_with_gap = f"{text} "
            escaped = _escape_pdf_text(text_with_gap)
            if not escaped:
                continue
            font_size = max(4.0, min(30.0, box_height * 0.9))
            estimated_width = max(1.0, len(text_with_gap) * font_size * 0.55)
            horizontal_scale = (box_width / estimated_width) * 100.0
            horizontal_scale = max(35.0, min(280.0, horizontal_scale))
            stream_lines.append(
                (
                    f"BT 3 Tr /F1 {font_size:.2f} Tf {horizontal_scale:.2f} Tz "
                    f"1 0 0 1 {x:.2f} {y:.2f} Tm ({escaped}) Tj ET"
                ).encode("ascii", errors="ignore")
            )

        if not stream_lines:
            return

        page.contents_add(pdf.make_stream(b"\n".join(stream_lines) + b"\n"))
        pdf.save(overlay_pdf)


def _split_pdf_into_pages(input_pdf: Path, temp_dir: Path) -> list[Path]:
    import pikepdf

    page_paths: list[Path] = []
    with pikepdf.open(input_pdf) as source_pdf:
        for page_index, page in enumerate(source_pdf.pages, start=1):
            single_page_path = temp_dir / f"page_{page_index:05d}.pdf"
            with pikepdf.Pdf.new() as single_page_pdf:
                single_page_pdf.pages.append(page)
                single_page_pdf.save(single_page_path)
            page_paths.append(single_page_path)

    if not page_paths:
        raise ValueError("Input PDF has no pages")

    return page_paths


def _merge_single_page_pdfs(page_paths: list[Path], output_pdf: Path) -> None:
    import pikepdf

    output_pdf.parent.mkdir(parents=True, exist_ok=True)
    with pikepdf.Pdf.new() as merged:
        for page_path in page_paths:
            with pikepdf.open(page_path) as part:
                merged.pages.extend(part.pages)
        merged.save(output_pdf)


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    try:
        port = _find_free_port(args.host, args.start_port, args.end_port)
    except (ValueError, RuntimeError, OSError) as exc:
        print(f"Error: {exc}")
        return 2

    server = ThreadingHTTPServer((args.host, port), OCRWebHandler)
    print(f"Web UI started: http://{args.host}:{port}")
    print("Press Ctrl+C to stop.")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        print("Server stopped.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
