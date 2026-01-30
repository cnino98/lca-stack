from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True)
class LegGeometry:
    shoulder_length: float
    elbow_length: float
    wrist_length: float


class LegIK:
    def __init__(self, side: str, geometry: LegGeometry) -> None:
        side_norm = side.strip().upper()
        if side_norm not in {"LEFT", "RIGHT"}:
            raise ValueError("side must be 'LEFT' or 'RIGHT'")
        self._side = side_norm
        self._geom = geometry
        self._shoulder_sign = 1.0 if self._side == "LEFT" else -1.0

    def solve(self, hip_to_foot: np.ndarray) -> np.ndarray:
        x, y, z = (float(v) for v in np.asarray(hip_to_foot, dtype=float).reshape(3))

        D = self._domain(x, y, z)
        D = float(np.clip(D, -1.0, 1.0))

        knee = float(np.arctan2(-np.sqrt(max(0.0, 1.0 - D * D)), D))

        shoulder_sq = y * y + (-z) * (-z) - self._geom.shoulder_length * self._geom.shoulder_length
        r = float(np.sqrt(max(0.0, shoulder_sq)))

        hip = -float(np.arctan2(z, y)) - float(np.arctan2(r, self._shoulder_sign * self._geom.shoulder_length))
        hip = -hip

        shoulder = float(np.arctan2(-x, r)) - float(
            np.arctan2(
                self._geom.wrist_length * np.sin(knee),
                self._geom.elbow_length + self._geom.wrist_length * np.cos(knee),
            )
        )

        return np.array([hip, shoulder, knee], dtype=float)

    def _domain(self, x: float, y: float, z: float) -> float:
        g = self._geom
        numerator = (
            y * y
            + (-z) * (-z)
            - g.shoulder_length * g.shoulder_length
            + (-x) * (-x)
            - g.elbow_length * g.elbow_length
            - g.wrist_length * g.wrist_length
        )
        denominator = 2.0 * g.wrist_length * g.elbow_length
        if abs(denominator) < 1e-12:
            return 1.0
        return numerator / denominator
