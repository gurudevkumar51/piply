"""Cross-platform process liveness checks used by startup recovery."""

from __future__ import annotations

import os
import sys

_WINDOWS_QUERY_LIMITED_INFORMATION = 0x1000
_WINDOWS_ERROR_INVALID_PARAMETER = 87
_WINDOWS_STILL_ACTIVE = 259


def _is_alive_windows(pid: int) -> bool:
    """Return whether a Windows process id is still running.

    ``os.kill`` cannot be used here: on Windows it terminates the target for
    every signal other than the console-control events. Opening a handle is not
    sufficient on its own either, because a terminated process whose handle is
    still held by a parent remains openable, so the exit code is checked too.
    """
    import ctypes

    kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
    handle = kernel32.OpenProcess(_WINDOWS_QUERY_LIMITED_INFORMATION, False, pid)
    if not handle:
        # ERROR_INVALID_PARAMETER means the pid does not exist; any other error
        # (typically access denied) means the process exists but is not ours.
        return kernel32.GetLastError() != _WINDOWS_ERROR_INVALID_PARAMETER
    try:
        exit_code = ctypes.c_ulong()
        if not kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code)):
            return True
        return exit_code.value == _WINDOWS_STILL_ACTIVE
    finally:
        kernel32.CloseHandle(handle)


def is_process_alive(pid: int | None) -> bool:
    """Return whether the supplied process id is still running.

    An unknown pid is reported as dead so that legacy rows recorded before
    owner tracking existed are still recovered on startup.
    """
    if pid is None or pid <= 0:
        return False
    if pid == os.getpid():
        return True
    if sys.platform == "win32":  # pragma: no cover - platform specific
        try:
            return _is_alive_windows(pid)
        except Exception:
            return True
    try:  # pragma: no cover - platform specific
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return True
    return True
