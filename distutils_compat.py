"""
Compatibility helpers for third-party packages that still import ``distutils``.

Python 3.13 removed ``distutils`` from the standard library. Some packages used
in this repo, notably ``undetected_chromedriver``, still import it directly.
This module restores that import path by exposing ``setuptools._distutils``
under the old module name when needed.
"""

from __future__ import annotations

import sys


def ensure_distutils() -> None:
    try:
        import distutils  # noqa: F401
    except ModuleNotFoundError:
        import setuptools._distutils as setuptools_distutils

        sys.modules["distutils"] = setuptools_distutils


ensure_distutils()
