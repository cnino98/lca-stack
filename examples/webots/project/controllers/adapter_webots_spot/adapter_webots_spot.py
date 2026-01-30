from __future__ import annotations

import argparse
import sys
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Tuple, cast

import numpy as np

from controller import Motor, Robot  # type: ignore[import-untyped]

from Bezier import BezierGait, GaitTiming, PhaseOffsets
from SpotKinematics import SpotModel

try:
    from lca_stack.ipc import JsonlClient, make_header
except ModuleNotFoundError:
    REPO_ROOT = Path(__file__).resolve().parents[5]
    sys.path.insert(0, str(REPO_ROOT / "src"))
    from lca_stack.ipc import JsonlClient, make_header


MOTOR_NAMES = (
    "front left shoulder abduction motor",
    "front left shoulder rotation motor",
    "front left elbow motor",
    "front right shoulder abduction motor",
    "front right shoulder rotation motor",
    "front right elbow motor",
    "rear left shoulder abduction motor",
    "rear left shoulder rotation motor",
    "rear left elbow motor",
    "rear right shoulder abduction motor",
    "rear right shoulder rotation motor",
    "rear right elbow motor",
)

MOTOR_OFFSETS = (0.0, 0.52, -1.182)


@dataclass(frozen=True)
class VelocityCommand:
    vx: float
    vy: float
    wz: float

    def is_zero(self, eps_lin: float, eps_yaw: float) -> bool:
        return abs(self.vx) < eps_lin and abs(self.vy) < eps_lin and abs(self.wz) < eps_yaw


@dataclass(frozen=True)
class TeleopLimits:
    max_vx: float = 0.45
    max_vy: float = 0.25
    max_wz: float = 0.90
    key_hold_timeout_s: float = 0.22  # kept for parity with original controller


@dataclass(frozen=True)
class MotionFilter:
    max_linear_accel: float = 1.20
    max_yaw_accel: float = 3.50
    deadband_linear: float = 0.02
    deadband_yaw: float = 0.03


@dataclass(frozen=True)
class GaitLimits:
    step_length_gain: float = 0.12
    max_half_stride: float = 0.09
    min_half_stride: float = 0.02
    clearance_height: float = 0.025
    penetration_depth: float = 0.003
    rotate_in_place_speed: float = 0.18
    min_step_speed: float = 0.12
    max_step_speed: float = 0.75


def _clamp(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x


def _slew(current: float, target: float, max_delta: float) -> float:
    delta = target - current
    if delta > max_delta:
        return current + max_delta
    if delta < -max_delta:
        return current - max_delta
    return target


class MotorGroup:
    def __init__(self, robot: Robot, names: Iterable[str], offsets: Tuple[float, float, float]) -> None:
        self._motors: Tuple[Motor, ...] = tuple(robot.getDevice(n) for n in names)  # type: ignore[arg-type]
        self._offsets = offsets
        self._min = np.array([m.getMinPosition() for m in self._motors], dtype=float)
        self._max = np.array([m.getMaxPosition() for m in self._motors], dtype=float)
        self._has_limits = np.isfinite(self._min) & np.isfinite(self._max) & (self._min < self._max)

        for m in self._motors:
            m.setVelocity(m.getMaxVelocity())

    def set_joint_targets(self, joint_angles: np.ndarray) -> None:
        flat = joint_angles.reshape(12)
        for i, m in enumerate(self._motors):
            cmd = float(flat[i]) + float(self._offsets[i % 3])
            if bool(self._has_limits[i]):
                cmd = float(np.clip(cmd, self._min[i], self._max[i]))
            m.setPosition(cmd)


class DaemonVelocitySource:
    """Reads the latest velocity command from the LCA daemon (non-blocking)."""

    def __init__(self, link: JsonlClient, limits: TeleopLimits) -> None:
        self._link = link
        self._limits = limits
        self._desired = VelocityCommand(0.0, 0.0, 0.0)

    def poll(self) -> VelocityCommand:
        self._drain_incoming_commands()
        return self._desired

    def _drain_incoming_commands(self) -> None:
        try:
            self._link.set_timeout(0.0)
            while True:
                msg = self._link.read_json()
                if msg is None:
                    return  # EOF; let caller continue stepping until Webots exits
                self._apply_message(msg)
        except Exception:
            # No more buffered data (timeout / would-block / etc).
            return
        finally:
            try:
                self._link.set_timeout(None)
            except Exception:
                pass

    def _apply_message(self, msg: Any) -> None:
        if not isinstance(msg, dict):
            return
        kind = msg.get("kind")
        if kind != "spot_cmd_vel_v1":
            return

        vx = _clamp(float(msg.get("vx_mps", self._desired.vx)), -self._limits.max_vx, self._limits.max_vx)
        vy = _clamp(float(msg.get("vy_mps", self._desired.vy)), -self._limits.max_vy, self._limits.max_vy)
        wz = _clamp(float(msg.get("wz_rps", self._desired.wz)), -self._limits.max_wz, self._limits.max_wz)
        self._desired = VelocityCommand(vx=vx, vy=vy, wz=wz)


class CommandLimiter:
    def __init__(self, cfg: MotionFilter) -> None:
        self._cfg = cfg
        self._cmd = VelocityCommand(0.0, 0.0, 0.0)

    def update(self, desired: VelocityCommand, dt: float) -> VelocityCommand:
        max_lin_delta = self._cfg.max_linear_accel * dt
        max_yaw_delta = self._cfg.max_yaw_accel * dt

        vx = _slew(self._cmd.vx, desired.vx, max_lin_delta)
        vy = _slew(self._cmd.vy, desired.vy, max_lin_delta)
        wz = _slew(self._cmd.wz, desired.wz, max_yaw_delta)

        if abs(vx) < self._cfg.deadband_linear:
            vx = 0.0
        if abs(vy) < self._cfg.deadband_linear:
            vy = 0.0
        if abs(wz) < self._cfg.deadband_yaw:
            wz = 0.0

        self._cmd = VelocityCommand(vx=vx, vy=vy, wz=wz)
        return self._cmd


class GaitMapper:
    def __init__(self, teleop: TeleopLimits, gait: GaitLimits) -> None:
        self._teleop = teleop
        self._gait = gait

    def map(self, cmd: VelocityCommand) -> Tuple[float, float, float, float]:
        vx_n = 0.0 if abs(self._teleop.max_vx) < 1e-9 else cmd.vx / self._teleop.max_vx
        vy_n = 0.0 if abs(self._teleop.max_vy) < 1e-9 else cmd.vy / self._teleop.max_vy

        vx_n = float(np.clip(vx_n, -1.0, 1.0))
        vy_n = float(np.clip(vy_n, -1.0, 1.0))

        half_stride = self._gait.step_length_gain * float(np.hypot(vx_n, vy_n))
        if vx_n < 0.0:
            half_stride *= -1.0

        if abs(cmd.wz) > 1e-6 and abs(half_stride) < 1e-9:
            half_stride = self._gait.step_length_gain * 0.10

        if abs(half_stride) > 1e-9:
            half_stride = float(np.clip(half_stride, -self._gait.max_half_stride, self._gait.max_half_stride))
            if abs(half_stride) < self._gait.min_half_stride:
                half_stride = float(np.copysign(self._gait.min_half_stride, half_stride))

        denom = abs(vx_n) if abs(vx_n) > 1e-6 else 1e-6
        lateral_fraction = float(np.arctan2(vy_n, denom))
        if vx_n < 0.0:
            lateral_fraction *= -1.0

        speed_linear = float(np.hypot(cmd.vx, cmd.vy))
        if abs(cmd.wz) > 1e-6:
            speed_linear = max(speed_linear, self._gait.rotate_in_place_speed)

        step_speed = float(np.clip(speed_linear, 0.0, self._gait.max_step_speed))
        if step_speed > 0.0:
            step_speed = max(step_speed, self._gait.min_step_speed)

        return half_stride, lateral_fraction, cmd.wz, step_speed


class SpotAdapterController:
    def __init__(self, robot: Robot, *, agent_id: str, daemon_host: str, daemon_port: int) -> None:
        self._robot = robot
        self._agent_id = agent_id
        self._timestep_ms = int(robot.getBasicTimeStep())
        self._dt = self._timestep_ms / 1000.0

        self._teleop_limits = TeleopLimits()
        self._filter_cfg = MotionFilter()
        self._gait_cfg = GaitLimits()

        self._daemon_link = JsonlClient(daemon_host, daemon_port)
        self._command_source = DaemonVelocitySource(self._daemon_link, self._teleop_limits)
        self._limiter = CommandLimiter(self._filter_cfg)

        self._motors = MotorGroup(robot, MOTOR_NAMES, MOTOR_OFFSETS)

        self._spot = SpotModel()
        self._gait = BezierGait(dt=self._dt, timing=GaitTiming(swing_time=0.30), offsets=PhaseOffsets())
        self._mapper = GaitMapper(self._teleop_limits, self._gait_cfg)

        self._run_id = str(uuid.uuid4())
        self._seq = 0

        self._base_foot = self._spot.body_to_foot_home
        self._body_pos = np.zeros(3, dtype=float)
        self._body_rpy = np.zeros(3, dtype=float)

    def step(self) -> bool:
        if self._robot.step(self._timestep_ms) == -1:
            return False

        sim_time_s = float(self._robot.getTime())

        desired = self._command_source.poll()
        cmd = self._limiter.update(desired, self._dt)

        half_stride, lateral_fraction, yaw_rate, step_speed = self._mapper.map(cmd)

        body_to_foot = self._gait.step(
            half_stride=half_stride,
            lateral_fraction=lateral_fraction,
            yaw_rate=yaw_rate,
            step_speed=step_speed,
            base_body_to_foot=self._base_foot,
            clearance_height=self._gait_cfg.clearance_height,
            penetration_depth=self._gait_cfg.penetration_depth,
            dt=self._dt,
        )

        joint_angles = -self._spot.inverse_kinematics(self._body_rpy, self._body_pos, body_to_foot)
        self._motors.set_joint_targets(joint_angles)

        self._publish_observation(
            sim_time_s=sim_time_s,
            desired=desired,
            cmd=cmd,
            half_stride=half_stride,
            lateral_fraction=lateral_fraction,
            yaw_rate=yaw_rate,
            step_speed=step_speed,
        )
        return True

    def _publish_observation(
        self,
        *,
        sim_time_s: float,
        desired: VelocityCommand,
        cmd: VelocityCommand,
        half_stride: float,
        lateral_fraction: float,
        yaw_rate: float,
        step_speed: float,
    ) -> None:
        self._seq += 1
        obs = {
            "header": make_header(self._run_id, self._agent_id, self._seq),
            "kind": "spot_obs_v1",
            "sim_time_s": sim_time_s,
            "desired": {"vx_mps": desired.vx, "vy_mps": desired.vy, "wz_rps": desired.wz},
            "cmd": {"vx_mps": cmd.vx, "vy_mps": cmd.vy, "wz_rps": cmd.wz},
            "gait": {
                "half_stride": half_stride,
                "lateral_fraction": lateral_fraction,
                "yaw_rate": yaw_rate,
                "step_speed": step_speed,
            },
        }
        self._daemon_link.write_json(obs)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--daemon-host", default="127.0.0.1")
    parser.add_argument("--daemon-port", type=int, required=True)
    parser.add_argument("--agent-id", default="cf1")
    args = parser.parse_args()

    robot = Robot()
    controller = SpotAdapterController(
        robot,
        agent_id=str(args.agent_id),
        daemon_host=str(args.daemon_host),
        daemon_port=int(args.daemon_port),
    )
    while controller.step():
        pass


if __name__ == "__main__":
    main()
