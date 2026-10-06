"""Resolve code identity for a development checkout or an immutable release image."""
import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def code_revision():
    if (ROOT / ".git").exists():
        command = ["git", "-c", f"safe.directory={ROOT}"]
        commit = subprocess.check_output(command + ["rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
        dirty = bool(subprocess.check_output(command + ["status", "--porcelain"], cwd=ROOT, text=True).strip())
        return {"git_commit": commit, "git_dirty": dirty}
    return {"git_commit": os.environ.get("DEMO_CODE_REVISION", "unknown"), "git_dirty": False}
