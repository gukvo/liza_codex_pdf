from __future__ import annotations

import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from liza_codex_pdf.backup import main


def test_backup_fails_without_git(monkeypatch, capsys):
    monkeypatch.setattr("liza_codex_pdf.backup.shutil.which", lambda _name: None)

    rc = main([])

    assert rc == 2
    assert "git is not found" in capsys.readouterr().err


def test_backup_commits_and_pushes_when_dirty(monkeypatch, capsys):
    calls: list[list[str]] = []

    def fake_run(cmd, check, text, capture_output):  # noqa: ANN001
        calls.append(list(cmd))
        command = cmd[1:]
        if command == ["rev-parse", "--is-inside-work-tree"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="true\n", stderr="")
        if command == ["remote", "get-url", "origin"]:
            return subprocess.CompletedProcess(
                cmd,
                0,
                stdout="git@github.com:user/demo.git\n",
                stderr="",
            )
        if command == ["rev-parse", "--abbrev-ref", "HEAD"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="main\n", stderr="")
        if command == ["add", "-A"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")
        if command == ["status", "--porcelain"]:
            return subprocess.CompletedProcess(cmd, 0, stdout=" M src/file.py\n", stderr="")
        if command[:2] == ["commit", "-m"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="[main abc] backup\n", stderr="")
        if command == ["push", "origin", "main"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")
        raise AssertionError(f"Unexpected command: {cmd}")

    monkeypatch.setattr("liza_codex_pdf.backup.shutil.which", lambda _name: "/usr/bin/git")
    monkeypatch.setattr("liza_codex_pdf.backup.subprocess.run", fake_run)

    rc = main(["--message", "manual backup"])

    assert rc == 0
    assert ["git", "commit", "-m", "manual backup"] in calls
    assert ["git", "push", "origin", "main"] in calls
    stdout = capsys.readouterr().out
    assert "Commit created" in stdout
    assert "Backup pushed" in stdout


def test_backup_pushes_without_commit_when_clean(monkeypatch, capsys):
    calls: list[list[str]] = []

    def fake_run(cmd, check, text, capture_output):  # noqa: ANN001
        calls.append(list(cmd))
        command = cmd[1:]
        if command == ["rev-parse", "--is-inside-work-tree"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="true\n", stderr="")
        if command == ["remote", "get-url", "origin"]:
            return subprocess.CompletedProcess(
                cmd,
                0,
                stdout="https://github.com/user/demo.git\n",
                stderr="",
            )
        if command == ["rev-parse", "--abbrev-ref", "HEAD"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="main\n", stderr="")
        if command == ["add", "-A"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")
        if command == ["status", "--porcelain"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")
        if command == ["push", "origin", "main"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")
        raise AssertionError(f"Unexpected command: {cmd}")

    monkeypatch.setattr("liza_codex_pdf.backup.shutil.which", lambda _name: "/usr/bin/git")
    monkeypatch.setattr("liza_codex_pdf.backup.subprocess.run", fake_run)

    rc = main([])

    assert rc == 0
    commit_calls = [call for call in calls if call[1] == "commit"]
    assert not commit_calls
    stdout = capsys.readouterr().out
    assert "No local changes to commit." in stdout
    assert "Backup pushed" in stdout


def test_backup_rejects_non_github_remote(monkeypatch, capsys):
    def fake_run(cmd, check, text, capture_output):  # noqa: ANN001
        command = cmd[1:]
        if command == ["rev-parse", "--is-inside-work-tree"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="true\n", stderr="")
        if command == ["remote", "get-url", "origin"]:
            return subprocess.CompletedProcess(
                cmd,
                0,
                stdout="git@gitlab.example.com:user/demo.git\n",
                stderr="",
            )
        raise AssertionError(f"Unexpected command: {cmd}")

    monkeypatch.setattr("liza_codex_pdf.backup.shutil.which", lambda _name: "/usr/bin/git")
    monkeypatch.setattr("liza_codex_pdf.backup.subprocess.run", fake_run)

    rc = main([])

    assert rc == 2
    assert "not GitHub" in capsys.readouterr().err
