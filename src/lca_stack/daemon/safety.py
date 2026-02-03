from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from lca_stack.ipc.clock import now_wall_ns
from lca_stack.ipc.structs import struct_to_dict
from lca_stack.proto import lca_stack_pb2 as pb


@dataclass(frozen=True, slots=True)
class SafetyDecision:
    """Result of Safety Guard evaluation."""

    safety_applied: bool
    safety_reason: str
    final_data: dict[str, Any]
    event_type: str | None
    event_data: dict[str, Any] | None


class SafetyGuard:
    """Safety Guard MVP.

    The Safety Guard is intentionally *not* responsible for low-level actuation
    limits (speed/accel/etc). Those are adapter-specific and belong in the
    adapter.

    This MVP focuses on team / run-level safety checks that are portable across
    platforms.
    """

    def __init__(self, *, agent_id: str) -> None:
        self._agent_id = str(agent_id)

    def evaluate(self, *, mode: pb.Mode, estop: bool, req_env: pb.Envelope) -> SafetyDecision:
        if req_env.WhichOneof("payload") != "actuation_request":
            raise ValueError("SafetyGuard.evaluate expects an actuation_request envelope")

        req = req_env.actuation_request
        data = struct_to_dict(req.data)

        target = str(req.target_agent_id) if str(req.target_agent_id) else self._agent_id
        now_ns = int(now_wall_ns())
        expires_ns = int(req.expires_wall_ns)

        # Collaborative/run-level checks.
        if bool(estop):
            return self._apply_stop(
                reason="estop_latched",
                req_env=req_env,
                data=data,
                extra={"target_agent_id": target},
            )

        if int(mode) != int(pb.MODE_RUNNING):
            return self._apply_stop(
                reason="mode_not_running",
                req_env=req_env,
                data=data,
                extra={"mode": int(mode)},
            )

        if target != self._agent_id:
            return self._apply_stop(
                reason="target_agent_mismatch",
                req_env=req_env,
                data=data,
                extra={"target_agent_id": target, "local_agent_id": self._agent_id},
            )

        if expires_ns != 0 and now_ns > expires_ns:
            return self._apply_stop(
                reason="request_expired",
                req_env=req_env,
                data=data,
                extra={"expires_wall_ns": expires_ns, "now_wall_ns": now_ns},
            )

        return SafetyDecision(
            safety_applied=False,
            safety_reason="",
            final_data=data,
            event_type=None,
            event_data=None,
        )

    def _apply_stop(self, *, reason: str, req_env: pb.Envelope, data: Mapping[str, Any], extra: Mapping[str, Any]) -> SafetyDecision:
        final = _safe_stop_data(req_env.kind, data)

        event_data: dict[str, Any] = {"reason": str(reason), "kind": str(req_env.kind)}
        event_data.update(dict(extra))

        return SafetyDecision(
            safety_applied=True,
            safety_reason=str(reason),
            final_data=final,
            event_type="safety_intervention",
            event_data=event_data,
        )


def _safe_stop_data(kind: str, data: Mapping[str, Any]) -> dict[str, Any]:
    """Best-effort 'stop' actuation for common velocity-like payloads.

    This is intentionally conservative and adapter-agnostic. If a command uses
    different field names, the adapter should still protect itself.
    """

    out: dict[str, Any] = dict(data)

    def _zero_keys(d: dict[str, Any]) -> None:
        for k in (
            "vx_mps",
            "vy_mps",
            "wz_rps",
            "vx",
            "vy",
            "wz",
            "linear_x",
            "linear_y",
            "angular_z",
        ):
            if k in d:
                d[k] = 0.0

    _zero_keys(out)

    cmd = out.get("cmd")
    if isinstance(cmd, dict):
        cmd2 = dict(cmd)
        _zero_keys(cmd2)
        out["cmd"] = cmd2

    # Mark that this message was transformed into a stop.
    if "safety_stop" not in out:
        out["safety_stop"] = True
    if "safety_stop_kind" not in out and kind:
        out["safety_stop_kind"] = str(kind)

    return out
