from liza_codex_pdf.app import main

def test_main_runs(capsys):
    main()
    assert "Hello from liza_codex_pdf" in capsys.readouterr().out
