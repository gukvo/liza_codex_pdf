from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from liza_codex_pdf.web import (
    _build_job_settings,
    _build_page_profiles,
    _find_free_port,
    _map_rotated_box_to_original,
    _needs_manual_review,
    _normalize_overlay_token,
    _prepare_overlay_words,
    _render_job_page,
    _render_page,
    _resolve_input_pdf,
    _resolve_output_pdf,
    _should_run_rescue_pass,
)


def test_find_free_port_picks_first_available(monkeypatch):
    def fake_is_free(_host: str, port: int) -> bool:
        return port == 8082

    monkeypatch.setattr("liza_codex_pdf.web._is_port_free", fake_is_free)

    assert _find_free_port("127.0.0.1", 8080, 8085) == 8082


def test_find_free_port_raises_when_range_busy(monkeypatch):
    monkeypatch.setattr("liza_codex_pdf.web._is_port_free", lambda _host, _port: False)

    with pytest.raises(RuntimeError, match="No free port found"):
        _find_free_port("127.0.0.1", 8080, 8081)


def test_find_free_port_rejects_bad_range():
    with pytest.raises(ValueError, match="start-port"):
        _find_free_port("127.0.0.1", 9000, 8999)


def test_render_page_has_file_picker_and_verify_mode():
    page = _render_page()
    assert 'type="file"' in page
    assert 'enctype="multipart/form-data"' in page
    assert "режим чертежей" in page
    assert "глубокая проверка" in page


def test_render_job_page_has_progress_polling():
    page = _render_job_page("abc123")
    assert "/api/job" in page
    assert "overlay" in page
    assert "Страница" in page


def test_resolve_input_pdf_prefers_uploaded_path(tmp_path: Path):
    uploaded = tmp_path / "upload.pdf"
    uploaded.write_bytes(b"%PDF-1.4\n")
    values = {"input_pdf": ""}

    assert _resolve_input_pdf(values, uploaded) == uploaded


def test_resolve_input_pdf_requires_source():
    with pytest.raises(ValueError, match="Choose a PDF file"):
        _resolve_input_pdf({"input_pdf": "   "}, None)


def test_resolve_output_pdf_for_uploaded_input(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    resolved = _resolve_output_pdf(
        values={"output_pdf": ""},
        input_pdf=tmp_path / "input.pdf",
        uploaded_name="drawing.pdf",
    )
    assert resolved == tmp_path / "drawing_searchable.pdf"


def test_build_job_settings_validates_numbers_and_flags():
    settings = _build_job_settings(
        {
            "lang": "rus+eng",
            "optimize": "1",
            "jobs": "2",
            "rotate_pages": "1",
            "deskew": "",
            "quiet": "",
            "drawing_mode": "1",
            "deep_verify": "1",
        }
    )
    assert settings["drawing_mode"] is True
    assert settings["deep_verify"] is True
    assert settings["jobs"] == 2


def test_build_job_settings_rejects_bad_jobs():
    with pytest.raises(ValueError, match="jobs"):
        _build_job_settings(
            {
                "lang": "rus+eng",
                "optimize": "1",
                "jobs": "0",
                "rotate_pages": "1",
                "deskew": "1",
                "quiet": "",
                "drawing_mode": "1",
                "deep_verify": "1",
            }
        )


def test_build_page_profiles_returns_multi_for_drawing():
    profiles = _build_page_profiles({"drawing_mode": True})
    assert len(profiles) >= 3
    assert profiles[0]["psm"] is not None


def test_build_page_profiles_returns_standard_for_non_drawing():
    profiles = _build_page_profiles({"drawing_mode": False})
    assert len(profiles) == 1
    assert profiles[0]["name"] == "standard"


def test_should_run_rescue_pass_for_drawing_gain():
    analysis = {"score": 200, "eng_token_count": 5}
    coverage = {"gain_ratio": 0.4, "tile_words": 40}
    settings = {"deep_verify": True, "drawing_mode": True}

    assert _should_run_rescue_pass(analysis, coverage, settings) is True


def test_needs_manual_review_flags_low_quality_drawing():
    analysis = {"score": 180, "eng_token_count": 3}
    coverage = {"tile_words": 10, "gain_ratio": 0.1}
    settings = {"drawing_mode": True}

    assert _needs_manual_review(analysis, coverage, settings) is True


def test_normalize_overlay_token_keeps_engineering_tokens():
    assert _normalize_overlay_token(" m³/h ") == "m3/h"
    assert _normalize_overlay_token("TAG-101(A)") == "TAG-101(A)"
    assert _normalize_overlay_token("JOINTEFFICIENCY") == "JOINT EFFICIENCY"
    assert _normalize_overlay_token("JOINTEFFICIENCYSUS304") == "JOINT EFFICIENCY"
    assert _normalize_overlay_token("   ") == ""


def test_prepare_overlay_words_dedupes_and_maps_coordinates():
    boxes = [
        (10, 10, 20, 10, "TAG-101", 90.0),
        (11, 10, 20, 10, "TAG-101", 80.0),
        (30, 20, 10, 8, "###", 95.0),
    ]
    words = _prepare_overlay_words(
        boxes,
        page_width=100,
        page_height=200,
        image_width=50,
        image_height=100,
    )

    assert len(words) == 1
    x, y, width, height, text = words[0]
    assert text == "TAG-101"
    assert x >= 0
    assert y >= 0
    assert width > 0
    assert height > 0


def test_map_rotated_box_to_original_for_90_and_270():
    mapped_90 = _map_rotated_box_to_original(
        left=10,
        top=20,
        width=30,
        height=40,
        text="TXT",
        conf=88.0,
        original_width=200,
        original_height=100,
        angle=90,
    )
    assert mapped_90 == (140, 10, 40, 30, "TXT", 88.0)

    mapped_270 = _map_rotated_box_to_original(
        left=10,
        top=20,
        width=30,
        height=40,
        text="TXT",
        conf=88.0,
        original_width=200,
        original_height=100,
        angle=270,
    )
    assert mapped_270 == (20, 60, 40, 30, "TXT", 88.0)
