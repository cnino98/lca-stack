from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Mapping, TypedDict, cast

from .clock import now_mono_ns, now_wall_ns


class HeaderDict(TypedDict):
    run_id: str
    agent_id: str
    seq: int
    t_mono_ns: int
    t_wall_ns: int


@dataclass(frozen=True, slots=True)
class Header:
    run_id: str
    agent_id: str
    seq: int
    t_mono_ns: int
    t_wall_ns: int


@dataclass(frozen=True, slots=True)
class HeaderLite:
    run_id: str
    agent_id: str
    t_wall_ns: int


def make_header(run_id: str, agent_id: str, seq: int) -> HeaderDict:
    return HeaderDict(
        run_id=str(run_id),
        agent_id=str(agent_id),
        seq=int(seq),
        t_mono_ns=int(now_mono_ns()),
        t_wall_ns=int(now_wall_ns()),
    )


def extract_header(json_line: str) -> Header:
    message = _load_json_object(json_line, context="message")
    return extract_header_from_obj(message)


def extract_header_from_obj(message: Mapping[str, object]) -> Header:
    header_obj_raw = message.get("header")
    if not isinstance(header_obj_raw, dict):
        raise ValueError("missing header object")

    header_obj = cast(Mapping[str, object], header_obj_raw)

    run_id = _as_str(header_obj.get("run_id"), field="header.run_id")
    agent_id = _as_str(header_obj.get("agent_id"), field="header.agent_id")
    seq = _as_int(header_obj.get("seq"), field="header.seq")
    t_mono_ns = _as_int(header_obj.get("t_mono_ns"), field="header.t_mono_ns")
    t_wall_ns = _as_int(header_obj.get("t_wall_ns"), field="header.t_wall_ns")

    return Header(run_id=run_id, agent_id=agent_id, seq=seq, t_mono_ns=t_mono_ns, t_wall_ns=t_wall_ns)


def try_extract_header_from_obj(message: Mapping[str, object]) -> Header | None:
    try:
        return extract_header_from_obj(message)
    except Exception:
        return None


def extract_header_lite(json_line: str) -> HeaderLite:
    header = extract_header(json_line)
    return HeaderLite(run_id=header.run_id, agent_id=header.agent_id, t_wall_ns=header.t_wall_ns)


def _load_json_object(json_line: str, *, context: str) -> dict[str, object]:
    parsed: Any = json.loads(json_line)
    if not isinstance(parsed, dict):
        raise ValueError(f"{context} root must be an object")
    # json.loads() produces dict[str, Any] in practice, but enforce str keys defensively
    for k in parsed.keys():
        if not isinstance(k, str):
            raise ValueError(f"{context} keys must be strings")
    return cast(dict[str, object], parsed)


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
