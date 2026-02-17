from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from liza_codex_pdf.app import _build_ocrmypdf_cmd, main


def test_build_command_includes_defaults(tmp_path: Path):
    input_pdf = tmp_path / "scan.pdf"
    input_pdf.write_bytes(b"%PDF-1.4\n")

    args = argparse.Namespace(
        input_pdf=input_pdf,
        output_pdf=None,
        lang="rus+eng",
        optimize=1,
        jobs=2,
        force_ocr=True,
        no_rotate_pages=False,
        no_deskew=False,
        tesseract_pagesegmode=None,
        oversample=None,
        remove_background=False,
        clean=False,
        clean_final=False,
        remove_vectors=False,
        tesseract_thresholding=None,
        quiet=False,
    )

    cmd = _build_ocrmypdf_cmd(args)

    assert cmd[:3] == [sys.executable, "-m", "ocrmypdf"]
    assert "--force-ocr" in cmd
    assert "--skip-text" not in cmd
    assert "--rotate-pages" in cmd
    assert "--deskew" in cmd
    assert "--clean" not in cmd
    assert "--remove-vectors" not in cmd
    assert str(input_pdf) == cmd[-2]
    assert str(tmp_path / "scan_searchable.pdf") == cmd[-1]


def test_main_success(monkeypatch, tmp_path: Path, capsys):
    input_pdf = tmp_path / "in.pdf"
    output_pdf = tmp_path / "out.pdf"
    input_pdf.write_bytes(b"%PDF-1.4\n")

    executed = {}

    def fake_run(cmd, check):
        executed["cmd"] = cmd
        executed["check"] = check

    monkeypatch.setattr("liza_codex_pdf.app.importlib.util.find_spec", lambda _: object())
    monkeypatch.setattr("liza_codex_pdf.app.shutil.which", lambda _: "/usr/bin/mock")
    monkeypatch.setattr("liza_codex_pdf.app.subprocess.run", fake_run)

    rc = main([str(input_pdf), str(output_pdf), "--lang", "rus"])

    assert rc == 0
    assert executed["check"] is True
    assert executed["cmd"][-2:] == [str(input_pdf), str(output_pdf)]
    assert "Done: searchable PDF written to" in capsys.readouterr().out


def test_main_fails_when_dependencies_missing(monkeypatch, tmp_path: Path, capsys):
    input_pdf = tmp_path / "in.pdf"
    input_pdf.write_bytes(b"%PDF-1.4\n")

    monkeypatch.setattr("liza_codex_pdf.app.importlib.util.find_spec", lambda _: object())
    monkeypatch.setattr("liza_codex_pdf.app.shutil.which", lambda _: None)

    rc = main([str(input_pdf)])

    assert rc == 2
    assert "Missing required executables in PATH" in capsys.readouterr().err
