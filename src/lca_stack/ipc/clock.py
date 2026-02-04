from __future__ import annotations

import time


def now_wall_ns() -> int:
    return int(time.time_ns())


def now_mono_ns() -> int:
    return int(time.monotonic_ns())


class MonotonicWallClock:
    """Wall-clock-like timestamps that never go backward.

    This anchors the current wall time at initialization, then advances using
    monotonic deltas. It prevents occasional wall-clock adjustments (e.g. NTP)
    from making timestamps non-monotonic.
    """

    __slots__ = ("_wall0", "_mono0")

    def __init__(self) -> None:
        self._wall0 = int(time.time_ns())
        self._mono0 = int(time.monotonic_ns())

    @property
    def wall0_ns(self) -> int:
        return int(self._wall0)

    @property
    def mono0_ns(self) -> int:
        return int(self._mono0)

    def now_wall_ns(self) -> int:
        return int(self._wall0 + (int(time.monotonic_ns()) - self._mono0))
