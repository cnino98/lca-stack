from __future__ import annotations

from typing import Tuple

import numpy as np


def rp_to_trans(rotation: np.ndarray, position: np.ndarray) -> np.ndarray:
    rotation = np.asarray(rotation, dtype=float).reshape(3, 3)
    position = np.asarray(position, dtype=float).reshape(3)

    transform = np.eye(4, dtype=float)
    transform[:3, :3] = rotation
    transform[:3, 3] = position
    return transform


def trans_to_rp(transform: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    transform = np.asarray(transform, dtype=float).reshape(4, 4)
    return transform[:3, :3].copy(), transform[:3, 3].copy()


def trans_inv(transform: np.ndarray) -> np.ndarray:
    rotation, position = trans_to_rp(transform)
    rotation_t = rotation.T

    inv = np.eye(4, dtype=float)
    inv[:3, :3] = rotation_t
    inv[:3, 3] = -rotation_t @ position
    return inv


def rpy_to_rot(roll: float, pitch: float, yaw: float) -> np.ndarray:
    cr, sr = float(np.cos(roll)), float(np.sin(roll))
    cp, sp = float(np.cos(pitch)), float(np.sin(pitch))
    cy, sy = float(np.cos(yaw)), float(np.sin(yaw))

    Rx = np.array(
        [
            [1.0, 0.0, 0.0],
            [0.0, cr, -sr],
            [0.0, sr, cr],
        ],
        dtype=float,
    )
    Ry = np.array(
        [
            [cp, 0.0, sp],
            [0.0, 1.0, 0.0],
            [-sp, 0.0, cp],
        ],
        dtype=float,
    )
    Rz = np.array(
        [
            [cy, -sy, 0.0],
            [sy, cy, 0.0],
            [0.0, 0.0, 1.0],
        ],
        dtype=float,
    )

    return (Rx @ Ry) @ Rz


def rpy_to_trans(roll: float, pitch: float, yaw: float) -> np.ndarray:
    return rp_to_trans(rpy_to_rot(roll, pitch, yaw), np.zeros(3, dtype=float))
