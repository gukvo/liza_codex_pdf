from __future__ import annotations

import argparse
import importlib.util
import shutil
import subprocess
import sys
from pathlib import Path


def _default_output_path(input_pdf: Path) -> Path:
    return input_pdf.with_name(f"{input_pdf.stem}_searchable.pdf")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="liza-codex-pdf",
        description=(
            "Convert scanned PDFs into searchable PDFs by adding an invisible OCR text layer "
            "(works with Ctrl+F in PDF readers)."
        ),
    )
    parser.add_argument("input_pdf", type=Path, help="Path to source scanned PDF")
    parser.add_argument(
        "output_pdf",
        type=Path,
        nargs="?",
        help="Output searchable PDF path (default: <input>_searchable.pdf)",
    )
    parser.add_argument(
        "-l",
        "--lang",
        default="rus+eng",
        help="Tesseract language(s), e.g. rus, eng, rus+eng (default: rus+eng)",
    )
    parser.add_argument(
        "--optimize",
        type=int,
        choices=(0, 1, 2, 3),
        default=1,
        help="PDF optimization level for output size/quality tradeoff (default: 1)",
    )
    parser.add_argument(
        "--jobs",
        type=int,
        default=1,
        help="Number of parallel OCR workers (default: 1)",
    )
    parser.add_argument(
        "--skip-text",
        action="store_true",
        help="Allow skipping OCR on pages that already have text",
    )
    parser.add_argument(
        "--no-rotate-pages",
        action="store_true",
        help="Disable automatic page rotation detection",
    )
    parser.add_argument(
        "--no-deskew",
        action="store_true",
        help="Disable deskew (de-tilt) preprocessing",
    )
    parser.add_argument(
        "--tesseract-psm",
        type=int,
        choices=tuple(range(0, 14)),
        default=None,
        help="Tesseract page segmentation mode (0-13)",
    )
    parser.add_argument(
        "--oversample",
        type=int,
        default=None,
        help="Rasterize at this DPI before OCR (useful for technical drawings)",
    )
    parser.add_argument(
        "--remove-background",
        action="store_true",
        help="Try to clean uneven gray backgrounds before OCR",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Run unpaper cleanup before OCR (may improve scans with noise)",
    )
    parser.add_argument(
        "--clean-final",
        action="store_true",
        help="Apply cleanup to final image output too (can remove desired details)",
    )
    parser.add_argument(
        "--remove-vectors",
        action="store_true",
        help="Mask vector graphics before OCR (useful for some technical drawings)",
    )
    parser.add_argument(
        "--tesseract-thresholding",
        choices=("auto", "otsu", "adaptive-otsu", "sauvola"),
        default=None,
        help="Tesseract thresholding mode for difficult images",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Reduce output from ocrmypdf",
    )
    return parser


def _build_ocrmypdf_cmd(args: argparse.Namespace) -> list[str]:
    output_pdf: Path = args.output_pdf or _default_output_path(args.input_pdf)

    cmd: list[str] = [
        sys.executable,
        "-m",
        "ocrmypdf",
        "--language",
        args.lang,
        "--output-type",
        "pdf",
        "--optimize",
        str(args.optimize),
        "--jobs",
        str(args.jobs),
    ]

    if args.force_ocr:
        cmd.append("--force-ocr")
    else:
        cmd.append("--skip-text")

    if not args.no_rotate_pages:
        cmd.append("--rotate-pages")
    if not args.no_deskew:
        cmd.append("--deskew")
    if args.tesseract_pagesegmode is not None:
        cmd.extend(["--tesseract-pagesegmode", str(args.tesseract_pagesegmode)])
    if args.oversample is not None:
        cmd.extend(["--oversample", str(args.oversample)])
    if args.remove_background:
        cmd.append("--remove-background")
    if args.clean:
        cmd.append("--clean")
    if args.clean_final:
        cmd.append("--clean-final")
    if args.remove_vectors:
        cmd.append("--remove-vectors")
    if args.tesseract_thresholding is not None:
        cmd.extend(["--tesseract-thresholding", args.tesseract_thresholding])
    if args.quiet:
        cmd.append("--quiet")

    cmd.extend([str(args.input_pdf), str(output_pdf)])
    return cmd


def _validate_args(args: argparse.Namespace) -> None:
    if not args.input_pdf.exists():
        raise FileNotFoundError(f"Input file not found: {args.input_pdf}")
    if args.input_pdf.suffix.lower() != ".pdf":
        raise ValueError(f"Input file is not a PDF: {args.input_pdf}")
    if args.jobs < 1:
        raise ValueError("--jobs must be >= 1")
    if args.oversample is not None and args.oversample < 1:
        raise ValueError("--oversample must be >= 1")


def _ensure_runtime_dependencies() -> None:
    missing: list[str] = []
    if importlib.util.find_spec("ocrmypdf") is None:
        missing.append("ocrmypdf (python package)")

    required_bins = ("tesseract", "qpdf", "gs")
    missing.extend(bin_name for bin_name in required_bins if shutil.which(bin_name) is None)
    if missing:
        joined = ", ".join(missing)
        raise RuntimeError(
            "Missing required executables in PATH: "
            f"{joined}. Install OCR system dependencies first."
        )


def run_ocr(
    input_pdf: Path,
    output_pdf: Path | None = None,
    *,
    lang: str = "rus+eng",
    optimize: int = 1,
    jobs: int = 1,
    force_ocr: bool = True,
    no_rotate_pages: bool = False,
    no_deskew: bool = False,
    tesseract_pagesegmode: int | None = None,
    oversample: int | None = None,
    remove_background: bool = False,
    clean: bool = False,
    clean_final: bool = False,
    remove_vectors: bool = False,
    tesseract_thresholding: str | None = None,
    quiet: bool = False,
) -> Path:
    args = argparse.Namespace(
        input_pdf=input_pdf,
        output_pdf=output_pdf,
        lang=lang,
        optimize=optimize,
        jobs=jobs,
        force_ocr=force_ocr,
        no_rotate_pages=no_rotate_pages,
        no_deskew=no_deskew,
        tesseract_pagesegmode=tesseract_pagesegmode,
        oversample=oversample,
        remove_background=remove_background,
        clean=clean,
        clean_final=clean_final,
        remove_vectors=remove_vectors,
        tesseract_thresholding=tesseract_thresholding,
        quiet=quiet,
    )

    _validate_args(args)
    _ensure_runtime_dependencies()
    cmd = _build_ocrmypdf_cmd(args)
    subprocess.run(cmd, check=True)
    return output_pdf or _default_output_path(input_pdf)


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    try:
        output_pdf = run_ocr(
            args.input_pdf,
            args.output_pdf,
            lang=args.lang,
            optimize=args.optimize,
            jobs=args.jobs,
            force_ocr=not args.skip_text,
            no_rotate_pages=args.no_rotate_pages,
            no_deskew=args.no_deskew,
            tesseract_pagesegmode=args.tesseract_psm,
            oversample=args.oversample,
            remove_background=args.remove_background,
            clean=args.clean,
            clean_final=args.clean_final,
            remove_vectors=args.remove_vectors,
            tesseract_thresholding=args.tesseract_thresholding,
            quiet=args.quiet,
        )
    except (FileNotFoundError, RuntimeError, ValueError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2
    except subprocess.CalledProcessError as exc:
        print(f"ocrmypdf failed with exit code {exc.returncode}", file=sys.stderr)
        return exc.returncode

    print(f"Done: searchable PDF written to {output_pdf}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
