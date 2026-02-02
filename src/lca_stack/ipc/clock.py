from __future__ import annotations

import time


def now_wall_ns() -> int:
    return time.time_ns()


def now_mono_ns() -> int:
    return time.monotonic_ns()


class MonotonicWallClock:
    """
    Wall-clock-like timestamps that never go backward:
    anchor system wall time at init, then advance using monotonic time deltas.
    """

    __slots__ = ("_wall0", "_mono0")

    def __init__(self) -> None:
        self._wall0 = time.time_ns()
        self._mono0 = time.monotonic_ns()

    @property
    def wall0_ns(self) -> int:
        return int(self._wall0)

    @property
    def mono0_ns(self) -> int:
        return int(self._mono0)

    def now_wall_ns(self) -> int:
        return int(self._wall0 + (time.monotonic_ns() - self._mono0))
