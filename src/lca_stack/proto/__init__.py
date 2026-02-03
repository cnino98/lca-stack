"""LCA Stack protobuf bindings.

The core package depends on the standard `protobuf` runtime. For local
development in constrained Python environments (e.g., embedded runtimes that do
not have site-packages), this module can optionally fall back to a vendored
runtime if one exists at `lca_stack/vendor/`.

The vendored runtime is **not** part of the core package distribution.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path


def _ensure_protobuf_runtime() -> None:
    try:
        import google.protobuf  # type: ignore
        return
    except ModuleNotFoundError:
        pass

    vendor_root = Path(__file__).resolve().parent.parent / "vendor"
    if vendor_root.exists():
        sys.path.insert(0, str(vendor_root))
        # Use the pure-Python implementation for maximum portability.
        os.environ.setdefault("PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION", "python")

        # Import again to validate the vendored runtime is discoverable.
        import google.protobuf  # type: ignore  # noqa: F401
        return

    raise ModuleNotFoundError(
        "google.protobuf is required. Install dependencies (pip install protobuf)"
    )


_ensure_protobuf_runtime()

from .lca_stack_pb2 import *  # noqa: F401,F403
