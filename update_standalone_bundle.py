#!/usr/bin/env python3
"""Update standalone bundle from GitHub.

Behavior:
1. If this folder is a git worktree and git is available: run `git pull --ff-only`.
2. Otherwise: download latest GitHub ZIP for the standalone repo and copy only
   changed files into this folder.

This keeps ZIP users up to date without requiring git.
"""

from __future__ import annotations

import hashlib
import os
import shutil
import subprocess
import sys
import tempfile
import urllib.request
import zipfile
from pathlib import Path


REPO_OWNER = os.getenv("QE_STANDALONE_REPO_OWNER", "w-s-bitcoin")
REPO_NAME = os.getenv("QE_STANDALONE_REPO_NAME", "webapps-quantum-exposure")
REPO_BRANCH = os.getenv("QE_STANDALONE_REPO_BRANCH", "main")

SKIP_PATHS = {
    ".git",
    ".standalone_prefs.json",
}


def _hash_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            chunk = fh.read(1024 * 1024)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def _same_file_content(left: Path, right: Path) -> bool:
    if not left.exists() or not right.exists():
        return False
    if left.stat().st_size != right.stat().st_size:
        return False
    return _hash_file(left) == _hash_file(right)


def _is_skipped(relative_path: Path) -> bool:
    parts = set(relative_path.parts)
    if "__pycache__" in parts:
        return True
    return any(part in SKIP_PATHS for part in parts)


def _git_pull_if_possible(root: Path) -> bool:
    git_available = shutil.which("git") is not None
    if not git_available:
        return False

    inside_worktree = subprocess.run(
        ["git", "rev-parse", "--is-inside-work-tree"],
        cwd=root,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    ).returncode == 0

    if not inside_worktree:
        return False

    print("Auto update: checking for updates via git pull...")
    result = subprocess.run(["git", "pull", "--ff-only"], cwd=root, check=False)
    if result.returncode == 0:
        print("Auto update: git pull completed.")
        return True

    print("Warning: git pull failed, trying ZIP update fallback.")
    return False


def _find_extracted_repo_root(extract_dir: Path) -> Path:
    dirs = [child for child in extract_dir.iterdir() if child.is_dir()]
    if len(dirs) == 1:
        return dirs[0]

    for child in dirs:
        if (child / "standalone_server.py").exists() and (child / "webapps").exists():
            return child

    raise RuntimeError("Could not locate extracted repository root in ZIP")


def _zip_sync_update(root: Path) -> None:
    zip_url = os.getenv(
        "QE_STANDALONE_REPO_ZIP_URL",
        f"https://github.com/{REPO_OWNER}/{REPO_NAME}/archive/refs/heads/{REPO_BRANCH}.zip",
    )

    print(f"Auto update: downloading latest bundle ZIP from {zip_url}")
    with tempfile.TemporaryDirectory(prefix="qe-standalone-update-") as temp_dir_str:
        temp_dir = Path(temp_dir_str)
        zip_path = temp_dir / "bundle.zip"

        with urllib.request.urlopen(zip_url, timeout=60) as response, zip_path.open("wb") as out:
            shutil.copyfileobj(response, out)

        extract_dir = temp_dir / "extract"
        extract_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(extract_dir)

        source_root = _find_extracted_repo_root(extract_dir)

        created = 0
        updated = 0
        unchanged = 0

        for source_file in source_root.rglob("*"):
            if source_file.is_dir():
                continue

            relative_path = source_file.relative_to(source_root)
            if _is_skipped(relative_path):
                continue

            target_file = root / relative_path
            target_file.parent.mkdir(parents=True, exist_ok=True)

            if not target_file.exists():
                shutil.copy2(source_file, target_file)
                created += 1
            elif _same_file_content(source_file, target_file):
                unchanged += 1
            else:
                shutil.copy2(source_file, target_file)
                updated += 1

        print(
            "Auto update: ZIP sync complete "
            f"(created: {created}, updated: {updated}, unchanged: {unchanged})."
        )


def main() -> int:
    root = Path(__file__).resolve().parent

    try:
        if _git_pull_if_possible(root):
            return 0
        _zip_sync_update(root)
        return 0
    except Exception as exc:
        print(f"Warning: auto update failed ({exc}). Launching with local files.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
