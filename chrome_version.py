"""
Helpers for detecting the locally installed Chrome version.

The login scripts use ``undetected_chromedriver``, which needs a matching
Chrome major version when ``version_main`` is provided. Hardcoding that value
breaks whenever Chrome auto-updates, so we resolve it at runtime instead.
"""

from __future__ import annotations

import re
import shutil
import subprocess
from typing import Iterable


_CHROME_CANDIDATES = (
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "/Applications/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing",
    "google-chrome",
    "chrome",
    "chromium",
    "chromium-browser",
)


def _iter_binaries() -> Iterable[str]:
    for candidate in _CHROME_CANDIDATES:
        if candidate.startswith("/"):
            yield candidate
            continue

        resolved = shutil.which(candidate)
        if resolved:
            yield resolved


def detect_chrome_major_version() -> int | None:
    for binary in _iter_binaries():
        try:
            result = subprocess.run(
                [binary, "--version"],
                capture_output=True,
                text=True,
                check=False,
                timeout=5,
            )
        except (FileNotFoundError, subprocess.SubprocessError, OSError):
            continue

        version_output = (result.stdout or result.stderr or "").strip()
        match = re.search(r"(\d+)\.\d+\.\d+\.\d+", version_output)
        if match:
            return int(match.group(1))

    return None
