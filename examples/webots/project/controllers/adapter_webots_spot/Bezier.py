from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Mapping, MutableMapping, OrderedDict as OrderedDictType

import numpy as np
from collections import OrderedDict
from math import atan2, cos, comb, pi, sin, sqrt


LEG_ORDER = ("FL", "FR", "BL", "BR")


@dataclass(frozen=True)
class GaitTiming:
    swing_time: float = 0.30
    max_stance_ratio: float = 1.30


@dataclass(frozen=True)
class PhaseOffsets:
    FL: float = 0.0
    FR: float = 0.5
    BL: float = 0.5
    BR: float = 0.0

    def as_dict(self) -> Dict[str, float]:
        return {"FL": self.FL, "FR": self.FR, "BL": self.BL, "BR": self.BR}


class BezierGait:
    def __init__(self, dt: float, timing: GaitTiming | None = None, offsets: PhaseOffsets | None = None) -> None:
        self._dt = float(dt)
        self._timing = timing or GaitTiming()
        self._offsets = (offsets or PhaseOffsets()).as_dict()

        self._phase_time = 0.0
        self._rot_mod = {leg: 0.0 for leg in LEG_ORDER}

        self._order = 11
        self._binom = np.array([comb(self._order, k) for k in range(self._order + 1)], dtype=float)

    @property
    def swing_time(self) -> float:
        return self._timing.swing_time

    @swing_time.setter
    def swing_time(self, value: float) -> None:
        self._timing = GaitTiming(swing_time=float(value), max_stance_ratio=self._timing.max_stance_ratio)

    def reset(self) -> None:
        self._phase_time = 0.0
        for k in self._rot_mod:
            self._rot_mod[k] = 0.0

    def step(
        self,
        half_stride: float,
        lateral_fraction: float,
        yaw_rate: float,
        step_speed: float,
        base_body_to_foot: Mapping[str, np.ndarray],
        clearance_height: float,
        penetration_depth: float,
        dt: float | None = None,
    ) -> "OrderedDict[str, np.ndarray]":
        dt_s = float(self._dt if dt is None else dt)

        moving = (abs(half_stride) > 1e-9) or (abs(yaw_rate) > 1e-9)
        if not moving or step_speed <= 1e-6:
            self.reset()
            return OrderedDict((k, v.copy()) for k, v in base_body_to_foot.items())

        stance_time = 2.0 * abs(float(half_stride)) / float(step_speed)
        max_stance = self._timing.max_stance_ratio * self._timing.swing_time
        stance_time = float(np.clip(stance_time, 2.0 * dt_s, max_stance))
        stride_time = stance_time + self._timing.swing_time

        self._phase_time = (self._phase_time + dt_s) % stride_time
        yaw_delta = float(yaw_rate) * dt_s

        out: "OrderedDict[str, np.ndarray]" = OrderedDict()
        for leg_id in LEG_ORDER:
            base = np.asarray(base_body_to_foot[leg_id], dtype=float).reshape(4, 4)
            _, p_bf0 = base[:3, :3], base[:3, 3]

            step = self._leg_step(
                leg_id,
                p_bf0,
                stance_time,
                stride_time,
                float(half_stride),
                float(lateral_fraction),
                yaw_delta,
                float(clearance_height),
                float(penetration_depth),
            )

            T = base.copy()
            T[0, 3] = float(p_bf0[0] + step[0])
            T[1, 3] = float(p_bf0[1] + step[1])
            T[2, 3] = float(p_bf0[2] + step[2])
            out[leg_id] = T

        return out

    def _leg_step(
        self,
        leg_id: str,
        default_p_bf: np.ndarray,
        stance_time: float,
        stride_time: float,
        half_stride: float,
        lateral_fraction: float,
        yaw_delta: float,
        clearance_height: float,
        penetration_depth: float,
    ) -> np.ndarray:
        offset = float(self._offsets[leg_id])
        ti = self._phase_time - offset * stride_time

        while ti < -self._timing.swing_time:
            ti += stride_time
        while ti > stance_time:
            ti -= stride_time

        in_stance = (0.0 <= ti) and (ti <= stance_time)
        if in_stance:
            phase = 0.0 if stance_time <= 1e-9 else ti / stance_time
            return self._stance_delta(leg_id, phase, half_stride, lateral_fraction, yaw_delta, penetration_depth, default_p_bf)
        phase = (ti + self._timing.swing_time) / self._timing.swing_time
        phase = float(np.clip(phase, 0.0, 1.0))
        return self._swing_delta(leg_id, phase, half_stride, lateral_fraction, yaw_delta, clearance_height, default_p_bf)

    def _swing_delta(
        self,
        leg_id: str,
        phase: float,
        half_stride: float,
        lateral_fraction: float,
        yaw_delta: float,
        clearance_height: float,
        default_p_bf: np.ndarray,
    ) -> np.ndarray:
        lin = self._bezier_swing(phase, half_stride, lateral_fraction, clearance_height)
        rot = self._bezier_swing(phase, yaw_delta, self._rotation_arc_angle(leg_id, default_p_bf), clearance_height)
        return self._modulated_sum(leg_id, default_p_bf, lin, rot)

    def _stance_delta(
        self,
        leg_id: str,
        phase: float,
        half_stride: float,
        lateral_fraction: float,
        yaw_delta: float,
        penetration_depth: float,
        default_p_bf: np.ndarray,
    ) -> np.ndarray:
        lin = self._sine_stance(phase, half_stride, lateral_fraction, penetration_depth)
        rot = self._sine_stance(phase, yaw_delta, self._rotation_arc_angle(leg_id, default_p_bf), penetration_depth)
        return self._modulated_sum(leg_id, default_p_bf, lin, rot)

    def _rotation_arc_angle(self, leg_id: str, default_p_bf: np.ndarray) -> float:
        x, y = float(default_p_bf[0]), float(default_p_bf[1])
        base_dir = atan2(y, x)
        return pi / 2.0 + base_dir + float(self._rot_mod[leg_id])

    def _modulated_sum(self, leg_id: str, default_p_bf: np.ndarray, lin: np.ndarray, rot: np.ndarray) -> np.ndarray:
        default_mag = sqrt(float(default_p_bf[0] ** 2 + default_p_bf[1] ** 2))
        xy_mag = sqrt(float((lin[0] + rot[0]) ** 2 + (lin[1] + rot[1]) ** 2))
        self._rot_mod[leg_id] = atan2(xy_mag, default_mag) if default_mag > 1e-9 else 0.0
        return lin + rot

    def _bezier_swing(self, t: float, half_stride: float, lateral_fraction: float, clearance_height: float) -> np.ndarray:
        x_polar = cos(lateral_fraction)
        y_polar = sin(lateral_fraction)

        L = float(half_stride)
        ctrl_x = np.array(
            [-L, -1.4 * L, -1.5 * L, -1.5 * L, -1.5 * L, 0.0, 0.0, 0.0, 1.5 * L, 1.5 * L, 1.4 * L, L],
            dtype=float,
        )
        ctrl_y = ctrl_x * y_polar
        ctrl_x = ctrl_x * x_polar

        h = float(clearance_height)
        ctrl_z = np.array(
            [0.0, 0.0, 0.9 * h, 0.9 * h, 0.9 * h, 0.9 * h, 0.9 * h, 1.1 * h, 1.1 * h, 1.1 * h, 0.0, 0.0],
            dtype=float,
        )

        return np.array([self._bezier_eval(t, ctrl_x), self._bezier_eval(t, ctrl_y), self._bezier_eval(t, ctrl_z)], dtype=float)

    def _sine_stance(self, phase: float, half_stride: float, lateral_fraction: float, penetration_depth: float) -> np.ndarray:
        L = float(half_stride)
        x_polar = cos(lateral_fraction)
        y_polar = sin(lateral_fraction)

        step = L * (1.0 - 2.0 * float(phase))
        x = step * x_polar
        y = step * y_polar
        z = -float(penetration_depth) * cos(pi * float(phase))
        return np.array([x, y, z], dtype=float)

    def _bezier_eval(self, t: float, ctrl: np.ndarray) -> float:
        t = float(np.clip(t, 0.0, 1.0))
        k = np.arange(self._order + 1, dtype=float)
        basis = self._binom * (t ** k) * ((1.0 - t) ** (self._order - k))
        return float(np.dot(basis, ctrl))
