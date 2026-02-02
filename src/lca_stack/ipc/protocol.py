from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Mapping, TypedDict, cast

from .clock import now_mono_ns, now_wall_ns
from .header import HeaderDict, make_header

# IPC protocol version.
#
# Bump when the daemon<->process JSONL framing or required fields change.
PROTOCOL_VERSION: int = 1

# Well-known message kinds produced by the daemon.
KIND_HANDSHAKE_V1 = "lca_handshake_v1"
KIND_RUN_EVENT_V1 = "lca_run_event_v1"


class HandshakeDict(TypedDict):
    header: HeaderDict
    topic: str
    kind: str
    protocol_version: int
    role: str
    run_id: str
    agent_id: str
    ports: dict[str, int]
    scenario: str | None
    seed: int | None


@dataclass(frozen=True, slots=True)
class Handshake:
    run_id: str
    agent_id: str
    protocol_version: int
    role: str
    adapter_port: int
    autonomy_port: int
    scenario: str | None
    seed: int | None


def make_handshake(
    *,
    run_id: str,
    agent_id: str,
    role: str,
    adapter_port: int,
    autonomy_port: int,
    scenario: str | None,
    seed: int | None,
) -> HandshakeDict:
    # Sequence 0 is reserved for daemon lifecycle messages.
    header = make_header(run_id, agent_id, seq=0)
    return HandshakeDict(
        header=header,
        topic="run/event",
        kind=KIND_HANDSHAKE_V1,
        protocol_version=int(PROTOCOL_VERSION),
        role=str(role),
        run_id=str(run_id),
        agent_id=str(agent_id),
        ports={"adapter": int(adapter_port), "autonomy": int(autonomy_port)},
        scenario=str(scenario) if scenario is not None else None,
        seed=int(seed) if seed is not None else None,
    )


def extract_handshake(msg: Any) -> Handshake | None:
    """Parse a handshake message.

    Returns None if the object is not a handshake.
    Raises ValueError if the object looks like a handshake but is malformed.
    """
    if not isinstance(msg, dict):
        return None
    kind = msg.get("kind")
    if kind != KIND_HANDSHAKE_V1:
        return None

    run_id = _as_str(msg.get("run_id"), field="run_id")
    agent_id = _as_str(msg.get("agent_id"), field="agent_id")
    protocol_version = _as_int(msg.get("protocol_version"), field="protocol_version")
    role = _as_str(msg.get("role"), field="role")

    ports_raw = msg.get("ports")
    if not isinstance(ports_raw, dict):
        raise ValueError("ports must be an object")
    ports = cast(Mapping[str, object], ports_raw)
    adapter_port = _as_int(ports.get("adapter"), field="ports.adapter")
    autonomy_port = _as_int(ports.get("autonomy"), field="ports.autonomy")

    scenario_val = msg.get("scenario")
    scenario = str(scenario_val) if isinstance(scenario_val, str) and scenario_val else None
    seed_val = msg.get("seed")
    seed = int(seed_val) if isinstance(seed_val, int) else None

    return Handshake(
        run_id=run_id,
        agent_id=agent_id,
        protocol_version=protocol_version,
        role=role,
        adapter_port=adapter_port,
        autonomy_port=autonomy_port,
        scenario=scenario,
        seed=seed,
    )


def make_run_event(
    *,
    run_id: str,
    agent_id: str,
    event: str,
    level: str = "INFO",
    message: str | None = None,
    fields: dict[str, Any] | None = None,
) -> dict[str, Any]:
    header = HeaderDict(
        run_id=str(run_id),
        agent_id=str(agent_id),
        seq=0,
        t_mono_ns=int(now_mono_ns()),
        t_wall_ns=int(now_wall_ns()),
    )
    obj: dict[str, Any] = {
        "header": header,
        "topic": "run/event",
        "kind": KIND_RUN_EVENT_V1,
        "event": str(event),
        "level": str(level),
    }
    if message is not None:
        obj["message"] = str(message)
    if fields:
        obj["fields"] = dict(fields)
    return obj


def dumps_json(obj: Any) -> str:
    # Stable compact encoding for IPC.
    return json.dumps(obj, separators=(",", ":"), sort_keys=True)


def _as_str(value: object, *, field: str) -> str:
    if not isinstance(value, str) or value == "":
        raise ValueError(f"missing {field}")
    return value


def _as_int(value: object, *, field: str) -> int:
    if value is None:
        raise ValueError(f"missing {field}")
    if isinstance(value, bool):
        raise ValueError(f"{field} must be an integer")
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not value.is_integer():
            raise ValueError(f"{field} must be an integer")
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError as e:
            raise ValueError(f"{field} must be an integer") from e
    raise ValueError(f"{field} must be an integer")
