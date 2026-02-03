from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping


def wall_ns_to_iso8601(wall_ns: int) -> str:
    dt = datetime.fromtimestamp(int(wall_ns) / 1_000_000_000, tz=timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


def write_manifest(path: Path, data: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = _dump_yaml(data)
    path.write_text(text, encoding="utf-8")


def _dump_yaml(obj: Any, *, indent: int = 0) -> str:
    """Very small YAML emitter for run manifests.

    We keep manifests dependency-free (no PyYAML) and only support the
    data types we write: dict / list / str / int / float / bool / None.
    """
    pad = "  " * indent

    if obj is None:
        return "null\n" if indent == 0 else "null"
    if isinstance(obj, bool):
        return ("true\n" if obj else "false\n") if indent == 0 else ("true" if obj else "false")
    if isinstance(obj, int):
        return f"{obj}\n" if indent == 0 else str(obj)
    if isinstance(obj, float):
        return f"{obj}\n" if indent == 0 else repr(obj)
    if isinstance(obj, str):
        return (f"{_quote_yaml_str(obj)}\n" if indent == 0 else _quote_yaml_str(obj))

    if isinstance(obj, list):
        if not obj:
            return "[]\n" if indent == 0 else "[]"
        lines: list[str] = []
        for item in obj:
            if _is_scalar(item):
                lines.append(f"{pad}- {_dump_yaml(item, indent=indent + 1).rstrip()}\n")
            else:
                lines.append(f"{pad}-\n")
                lines.append(_dump_yaml(item, indent=indent + 1))
        return "".join(lines)

    if isinstance(obj, dict):
        if not obj:
            return "{}\n" if indent == 0 else "{}"
        lines = []
        for k in sorted(obj.keys(), key=str):
            key = str(k)
            val = obj[k]
            if _is_scalar(val):
                lines.append(f"{pad}{key}: {_dump_yaml(val, indent=indent + 1).rstrip()}\n")
            else:
                lines.append(f"{pad}{key}:\n")
                lines.append(_dump_yaml(val, indent=indent + 1))
        return "".join(lines)

    raise TypeError(f"unsupported type for YAML manifest: {type(obj)!r}")


def _is_scalar(x: Any) -> bool:
    return x is None or isinstance(x, (str, int, float, bool))


def _quote_yaml_str(s: str) -> str:
    # Conservative: always single-quote, escape single quotes by doubling.
    return "'" + s.replace("'", "''") + "'"


@dataclass(frozen=True, slots=True)
class ClockSnapshot:
    daemon_wall_ns: int
    daemon_mono_ns: int
    monotonic_wall_anchor_wall0_ns: int
    monotonic_wall_anchor_mono0_ns: int


def build_manifest_start(
    *,
    run_id: str,
    agent_id: str,
    host: str,
    adapter_port: int,
    autonomy_port: int,
    protocol_version: int,
    schema_version: int,
    start_wall_ns: int,
    scenario: str | None,
    seed: int | None,
    clock: ClockSnapshot,
) -> dict[str, Any]:
    return {
        "run_id": str(run_id),
        "agent_id": str(agent_id),
        "protocol_version": int(protocol_version),
        "schema_version": int(schema_version),
        "transport": "framed_protobuf",
        "start_wall_ns": int(start_wall_ns),
        "start_wall": wall_ns_to_iso8601(start_wall_ns),
        "end_wall_ns": None,
        "end_wall": None,
        "host": str(host),
        "adapter_port": int(adapter_port),
        "autonomy_port": int(autonomy_port),
        "ports": {"adapter": int(adapter_port), "autonomy": int(autonomy_port)},
        "scenario": str(scenario) if scenario is not None else None,
        "seed": int(seed) if seed is not None else None,
        "clock": {
            "daemon_wall_ns": int(clock.daemon_wall_ns),
            "daemon_mono_ns": int(clock.daemon_mono_ns),
            "monotonic_wall_anchor_wall0_ns": int(clock.monotonic_wall_anchor_wall0_ns),
            "monotonic_wall_anchor_mono0_ns": int(clock.monotonic_wall_anchor_mono0_ns),
        },
        "clock_health": None,
    }


def finalize_manifest(
    manifest: Mapping[str, Any],
    *,
    end_wall_ns: int,
    clock_health: Mapping[str, Any] | None,
) -> dict[str, Any]:
    out = dict(manifest)
    out["end_wall_ns"] = int(end_wall_ns)
    out["end_wall"] = wall_ns_to_iso8601(end_wall_ns)
    out["clock_health"] = dict(clock_health) if clock_health is not None else None
    return out
