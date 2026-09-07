"""Single source of truth for the running Piply version.

Read from installed package metadata rather than written down here, because a
hardcoded copy drifts: the CLI and the API had fallen three and one releases
behind `pyproject.toml` respectively before this module existed.
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _package_version

#: Reported when running from a source tree that was never installed. Chosen to
#: be obviously not a release, rather than a plausible-looking stale number.
UNKNOWN_VERSION = "0.0.0+local"


def get_version() -> str:
    """Return the installed version, or a clearly-unreleased placeholder."""
    try:
        return _package_version("mr-piply")
    except PackageNotFoundError:
        return UNKNOWN_VERSION
