from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, OrderedDict as OrderedDictType

import numpy as np
from collections import OrderedDict

from LegKinematics import LegGeometry, LegIK
from LieAlgebra import rp_to_trans, rpy_to_rot, trans_inv, trans_to_rp


LEG_ORDER = ("FL", "FR", "BL", "BR")


@dataclass(frozen=True)
class SpotGeometry:
    shoulder_length: float = 0.113
    elbow_length: float = 0.368
    wrist_length: float = 0.352 + 0.035094
    hip_x: float = 0.364 + 0.308
    hip_y: float = 2 * 0.053
    foot_x: float = 0.364 + 0.308
    foot_y: float = 2 * 0.166
    height: float = 0.52
    com_offset_x: float = -0.023


class SpotModel:
    def __init__(self, geometry: SpotGeometry | None = None) -> None:
        self._geom = geometry or SpotGeometry()

        g = self._geom
        leg_geom = LegGeometry(g.shoulder_length, g.elbow_length, g.wrist_length)

        self._legs = OrderedDict(
            [
                ("FL", LegIK("LEFT", leg_geom)),
                ("FR", LegIK("RIGHT", leg_geom)),
                ("BL", LegIK("LEFT", leg_geom)),
                ("BR", LegIK("RIGHT", leg_geom)),
            ]
        )

        self._body_to_hip_home = self._build_body_to_hip_home()
        self._body_to_foot_home = self._build_body_to_foot_home()

    @property
    def body_to_foot_home(self) -> "OrderedDict[str, np.ndarray]":
        return OrderedDict((k, v.copy()) for k, v in self._body_to_foot_home.items())

    def inverse_kinematics(self, body_rpy: np.ndarray, body_pos: np.ndarray, body_to_foot: Mapping[str, np.ndarray]) -> np.ndarray:
        roll, pitch, yaw = (float(v) for v in np.asarray(body_rpy, dtype=float).reshape(3))
        pos = np.asarray(body_pos, dtype=float).reshape(3).copy()
        pos[0] += self._geom.com_offset_x

        Rb = rpy_to_rot(roll, pitch, yaw)
        T_wb = rp_to_trans(Rb, pos)

        angles = np.zeros((4, 3), dtype=float)
        for i, leg_id in enumerate(LEG_ORDER):
            T_wh = self._body_to_hip_home[leg_id]
            T_bf = np.asarray(body_to_foot[leg_id], dtype=float).reshape(4, 4)

            T_bh = trans_inv(T_wb) @ T_wh
            T_hf = trans_inv(T_bh) @ T_bf
            _, p_hf = trans_to_rp(T_hf)

            angles[i, :] = self._legs[leg_id].solve(p_hf)

        return angles

    def _build_body_to_hip_home(self) -> "OrderedDict[str, np.ndarray]":
        g = self._geom
        R = np.eye(3, dtype=float)
        return OrderedDict(
            [
                ("FL", rp_to_trans(R, np.array([g.hip_x / 2.0, g.hip_y / 2.0, 0.0], dtype=float))),
                ("FR", rp_to_trans(R, np.array([g.hip_x / 2.0, -g.hip_y / 2.0, 0.0], dtype=float))),
                ("BL", rp_to_trans(R, np.array([-g.hip_x / 2.0, g.hip_y / 2.0, 0.0], dtype=float))),
                ("BR", rp_to_trans(R, np.array([-g.hip_x / 2.0, -g.hip_y / 2.0, 0.0], dtype=float))),
            ]
        )

    def _build_body_to_foot_home(self) -> "OrderedDict[str, np.ndarray]":
        g = self._geom
        R = np.eye(3, dtype=float)
        return OrderedDict(
            [
                ("FL", rp_to_trans(R, np.array([g.foot_x / 2.0, g.foot_y / 2.0, -g.height], dtype=float))),
                ("FR", rp_to_trans(R, np.array([g.foot_x / 2.0, -g.foot_y / 2.0, -g.height], dtype=float))),
                ("BL", rp_to_trans(R, np.array([-g.foot_x / 2.0, g.foot_y / 2.0, -g.height], dtype=float))),
                ("BR", rp_to_trans(R, np.array([-g.foot_x / 2.0, -g.foot_y / 2.0, -g.height], dtype=float))),
            ]
        )
