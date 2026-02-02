from __future__ import annotations

import json
import logging
import socket
import threading
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, MutableMapping, cast

from lca_stack.ipc import MonotonicWallClock, make_header, now_mono_ns, now_wall_ns, try_extract_header_from_obj
from lca_stack.ipc.jsonl import JsonlSocket
from lca_stack.ipc.protocol import PROTOCOL_VERSION, dumps_json, make_handshake, make_run_event
from lca_stack.log.mcap_logger import (
    TOPIC_ACTUATION,
    TOPIC_ACTUATION_REQUEST,
    TOPIC_OBSERVATION,
    TOPIC_RUN_EVENT,
    McapLogger,
)
from lca_stack.run import ClockSnapshot, build_manifest_start, finalize_manifest, write_manifest

from .server import Listeners, accept_one, listen_pair

logger = logging.getLogger(__name__)


_CLOCK_SPREAD_WARN_NS = 50_000_000  # 50 ms
_MONO_JUMP_WARN_NS = 5_000_000_000  # 5 s


@dataclass(slots=True)
class _OffsetStats:
    count: int = 0
    min_offset_ns: int = 0
    max_offset_ns: int = 0
    last_offset_ns: int = 0

    def update(self, offset_ns: int) -> None:
        if self.count == 0:
            self.min_offset_ns = int(offset_ns)
            self.max_offset_ns = int(offset_ns)
        else:
            self.min_offset_ns = int(min(self.min_offset_ns, int(offset_ns)))
            self.max_offset_ns = int(max(self.max_offset_ns, int(offset_ns)))
        self.last_offset_ns = int(offset_ns)
        self.count += 1

    def to_dict(self) -> dict[str, Any]:
        return {
            "count": int(self.count),
            "min_offset_ns": int(self.min_offset_ns),
            "max_offset_ns": int(self.max_offset_ns),
            "spread_ns": int(self.max_offset_ns - self.min_offset_ns) if self.count else None,
            "last_offset_ns": int(self.last_offset_ns) if self.count else None,
        }


class _TimeSanityChecker:
    def __init__(self) -> None:
        self._last_mono_ns: dict[tuple[str, str], int] = {}
        self._warned_backward: set[tuple[str, str]] = set()
        self._warned_jump: set[tuple[str, str]] = set()

    def check(self, *, agent_id: str, topic: str, t_mono_ns: int, emit: callable) -> None:
        key = (str(agent_id), str(topic))
        prev = self._last_mono_ns.get(key)
        if prev is not None:
            if int(t_mono_ns) < int(prev) and key not in self._warned_backward:
                self._warned_backward.add(key)
                emit(
                    level="WARNING",
                    event="time_monotonic_backward",
                    message="origin monotonic time went backward (non-decreasing expected)",
                    fields={
                        "agent_id": str(agent_id),
                        "topic": str(topic),
                        "prev_t_mono_ns": int(prev),
                        "t_mono_ns": int(t_mono_ns),
                    },
                )
            if int(t_mono_ns) - int(prev) > _MONO_JUMP_WARN_NS and key not in self._warned_jump:
                self._warned_jump.add(key)
                emit(
                    level="WARNING",
                    event="time_monotonic_jump",
                    message="origin monotonic time jumped forward (large gap)",
                    fields={
                        "agent_id": str(agent_id),
                        "topic": str(topic),
                        "prev_t_mono_ns": int(prev),
                        "t_mono_ns": int(t_mono_ns),
                        "delta_ns": int(t_mono_ns) - int(prev),
                    },
                )
        self._last_mono_ns[key] = int(t_mono_ns)


@dataclass(frozen=True, slots=True)
class _Link:
    role: str
    sock: socket.socket
    addr: tuple[str, int]
    link: JsonlSocket


def run_bridge(
    *,
    agent_id: str,
    host: str,
    adapter_port: int,
    autonomy_port: int,
    runs_dir: Path,
    scenario: str | None,
    seed: int | None,
) -> None:
    # Daemon invocation == one run. The daemon owns the run_id and communicates it
    # to Adapter and Autonomy via an immediate handshake.
    run_id = str(uuid.uuid4())

    wall_clock = MonotonicWallClock()
    mcap = McapLogger.create(runs_dir=runs_dir, run_id=run_id, agent_id=agent_id)
    logger.info("run_id=%s | writing mcap=%s", run_id, mcap.path)

    listeners: Listeners = listen_pair(host, adapter_port, autonomy_port)
    try:
        links = _accept_links_with_handshake(
            listeners=listeners,
            run_id=run_id,
            agent_id=agent_id,
            adapter_port=adapter_port,
            autonomy_port=autonomy_port,
            scenario=scenario,
            seed=seed,
            wall_clock=wall_clock,
            mcap=mcap,
        )
    finally:
        try:
            listeners.adapter_listener.close()
        finally:
            listeners.autonomy_listener.close()

    manifest_path = Path(runs_dir) / run_id / "manifest.yaml"

    start_wall_ns = int(now_wall_ns())
    clock_snapshot = ClockSnapshot(
        daemon_wall_ns=int(start_wall_ns),
        daemon_mono_ns=int(now_mono_ns()),
        monotonic_wall_anchor_wall0_ns=int(wall_clock.wall0_ns),
        monotonic_wall_anchor_mono0_ns=int(wall_clock.mono0_ns),
    )
    manifest = build_manifest_start(
        run_id=run_id,
        agent_id=agent_id,
        host=host,
        adapter_port=adapter_port,
        autonomy_port=autonomy_port,
        protocol_version=int(PROTOCOL_VERSION),
        start_wall_ns=start_wall_ns,
        scenario=scenario,
        seed=seed,
        clock=clock_snapshot,
    )
    write_manifest(manifest_path, manifest)
    logger.info("run_id=%s | wrote manifest=%s", run_id, manifest_path)

    bridge = _AgentBridge(
        run_id=run_id,
        agent_id=agent_id,
        host=host,
        adapter_port=adapter_port,
        autonomy_port=autonomy_port,
        scenario=scenario,
        seed=seed,
        wall_clock=wall_clock,
        mcap=mcap,
        adapter_link=links["adapter"],
        autonomy_link=links["autonomy"],
    )

    try:
        bridge.emit_run_start()
        bridge.run()
    finally:
        try:
            bridge.emit_run_stop()
        finally:
            end_wall_ns = int(now_wall_ns())
            final = finalize_manifest(manifest, end_wall_ns=end_wall_ns, clock_health=bridge.clock_health_snapshot())
            write_manifest(manifest_path, final)
            bridge.close()


def _accept_links_with_handshake(
    *,
    listeners: Listeners,
    run_id: str,
    agent_id: str,
    adapter_port: int,
    autonomy_port: int,
    scenario: str | None,
    seed: int | None,
    wall_clock: MonotonicWallClock,
    mcap: McapLogger,
) -> dict[str, _Link]:
    out: dict[str, _Link] = {}
    lock = threading.Lock()

    def _accept(role: str, listener: socket.socket) -> None:
        sock, addr = accept_one(listener)
        link = JsonlSocket(sock)

        hs = make_handshake(
            run_id=run_id,
            agent_id=agent_id,
            role=role,
            adapter_port=adapter_port,
            autonomy_port=autonomy_port,
            scenario=scenario,
            seed=seed,
        )
        _ensure_origin_fields(hs, origin_agent_id=agent_id, origin_seq=int(hs["header"]["seq"]), origin_topic=TOPIC_RUN_EVENT)
        hs_line = dumps_json(hs)
        link.write_line(hs_line)
        _log_event_payload(mcap=mcap, wall_clock=wall_clock, payload_line=hs_line, publish_time_ns=int(hs["header"]["t_wall_ns"]))
        logger.info("accepted %s from %s:%s", role, addr[0], addr[1])
        with lock:
            out[role] = _Link(role=role, sock=sock, addr=addr, link=link)

    t_adapter = threading.Thread(target=_accept, name="accept-adapter", args=("adapter", listeners.adapter_listener), daemon=True)
    t_autonomy = threading.Thread(target=_accept, name="accept-autonomy", args=("autonomy", listeners.autonomy_listener), daemon=True)
    t_adapter.start()
    t_autonomy.start()
    t_adapter.join()
    t_autonomy.join()
    return out


def _log_event_payload(*, mcap: McapLogger, wall_clock: MonotonicWallClock, payload_line: str, publish_time_ns: int) -> None:
    daemon_time_ns = int(wall_clock.now_wall_ns())
    mcap.log_run_event(daemon_time_ns, int(publish_time_ns), payload_line.encode("utf-8"))


class _AgentBridge:
    def __init__(
        self,
        *,
        run_id: str,
        agent_id: str,
        host: str,
        adapter_port: int,
        autonomy_port: int,
        scenario: str | None,
        seed: int | None,
        wall_clock: MonotonicWallClock,
        mcap: McapLogger,
        adapter_link: _Link,
        autonomy_link: _Link,
    ) -> None:
        self._run_id = str(run_id)
        self._agent_id = str(agent_id)
        self._host = str(host)
        self._adapter_port = int(adapter_port)
        self._autonomy_port = int(autonomy_port)
        self._scenario = scenario
        self._seed = seed

        self._wall_clock = wall_clock
        self._mcap = mcap
        self._adapter = adapter_link
        self._autonomy = autonomy_link
        self._send_lock_adapter = threading.Lock()
        self._send_lock_autonomy = threading.Lock()
        self._stop = threading.Event()

        self._time_check = _TimeSanityChecker()
        self._offset_stats: dict[str, _OffsetStats] = {
            TOPIC_OBSERVATION: _OffsetStats(),
            TOPIC_ACTUATION_REQUEST: _OffsetStats(),
        }
        self._clock_spread_warned = False
        self._stop_reason: str | None = None

        # Per-channel sequence numbers for daemon-injected headers.
        self._inject_seq: dict[str, int] = {
            TOPIC_OBSERVATION: 0,
            TOPIC_ACTUATION_REQUEST: 0,
            TOPIC_RUN_EVENT: 0,
        }

    def emit_run_start(self) -> None:
        ev = make_run_event(
            run_id=self._run_id,
            agent_id=self._agent_id,
            event="run_start",
            level="INFO",
            fields={
                "host": self._host,
                "ports": {"adapter": self._adapter_port, "autonomy": self._autonomy_port},
                "scenario": self._scenario,
                "seed": self._seed,
                "protocol_version": int(PROTOCOL_VERSION),
            },
        )
        self._broadcast_event(ev)

    def emit_run_stop(self) -> None:
        fields: dict[str, Any] = {
            "host": self._host,
            "ports": {"adapter": self._adapter_port, "autonomy": self._autonomy_port},
        }
        if self._stop_reason is not None:
            fields["reason"] = self._stop_reason

        ev = make_run_event(
            run_id=self._run_id,
            agent_id=self._agent_id,
            event="run_stop",
            level="INFO",
            fields=fields,
        )
        self._broadcast_event(ev)

    def run(self) -> None:
        t_obs = threading.Thread(target=self._relay_observation, name="relay-observation", daemon=True)
        t_act = threading.Thread(target=self._relay_actuation, name="relay-actuation", daemon=True)

        t_obs.start()
        t_act.start()

        t_obs.join()
        t_act.join()

    def close(self) -> None:
        for link in (self._adapter, self._autonomy):
            try:
                link.sock.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            try:
                link.sock.close()
            except Exception:
                pass
        try:
            self._mcap.close()
        except Exception:
            pass

    def clock_health_snapshot(self) -> dict[str, Any]:
        # Snapshot is intended for quick health debugging, not precise calibration.
        offsets = {topic: stats.to_dict() for topic, stats in self._offset_stats.items()}

        last_offsets = [
            stats.last_offset_ns
            for topic, stats in self._offset_stats.items()
            if stats.count and topic in (TOPIC_OBSERVATION, TOPIC_ACTUATION_REQUEST)
        ]
        spread_ns: int | None = None
        if last_offsets:
            spread_ns = int(max(last_offsets) - min(last_offsets))

        return {
            "local_wall_ns": int(now_wall_ns()),
            "local_mono_ns": int(now_mono_ns()),
            "monotonic_wall_anchor_wall0_ns": int(self._wall_clock.wall0_ns),
            "monotonic_wall_anchor_mono0_ns": int(self._wall_clock.mono0_ns),
            "offsets": offsets,
            "last_spread_ns": spread_ns,
        }

    def _relay_observation(self) -> None:
        while not self._stop.is_set():
            try:
                line = self._adapter.link.read_line()
            except Exception as e:
                self._set_stop("adapter_disconnect")
                self._emit_warning(
                    "disconnect",
                    "adapter link error; treating as disconnect",
                    fields={"error": repr(e)},
                )
                break
            if line is None:
                self._set_stop("adapter_disconnect")
                self._emit_warning("disconnect", "adapter disconnected")
                break

            msg = self._parse_message(line, inferred_topic=TOPIC_OBSERVATION, source_role="adapter")
            if msg is None:
                continue

            # Log and forward to autonomy.
            payload_line = dumps_json(msg)
            header = cast(Mapping[str, object], msg["header"])  # ensured by _parse_message
            self._mcap.log_observation(
                int(self._wall_clock.now_wall_ns()),
                int(cast(int, header.get("t_wall_ns"))),
                payload_line.encode("utf-8"),
            )
            with self._send_lock_autonomy:
                self._autonomy.link.write_line(payload_line)

    def _relay_actuation(self) -> None:
        while not self._stop.is_set():
            try:
                line = self._autonomy.link.read_line()
            except Exception as e:
                self._set_stop("autonomy_disconnect")
                self._emit_warning(
                    "disconnect",
                    "autonomy link error; treating as disconnect",
                    fields={"error": repr(e)},
                )
                break
            if line is None:
                self._set_stop("autonomy_disconnect")
                self._emit_warning("disconnect", "autonomy disconnected")
                break

            req = self._parse_message(line, inferred_topic=TOPIC_ACTUATION_REQUEST, source_role="autonomy")
            if req is None:
                continue

            # Log the request as-sent by autonomy.
            req_line = dumps_json(req)
            header = cast(Mapping[str, object], req["header"])  # ensured by _parse_message
            self._mcap.log_actuation_request(
                int(self._wall_clock.now_wall_ns()),
                int(cast(int, header.get("t_wall_ns"))),
                req_line.encode("utf-8"),
            )

            # Apply safety later. For now, pass through but re-topic the message to reflect
            # what the adapter actually receives.
            act = dict(req)
            act["topic"] = TOPIC_ACTUATION
            act.setdefault("lca_meta", {})
            if isinstance(act["lca_meta"], dict):
                act["lca_meta"].setdefault("safety", {"applied": False})

            _ensure_origin_fields(
                act,
                origin_agent_id=str(act.get("origin_agent_id") or cast(str, cast(Mapping[str, object], act["header"]).get("agent_id"))),
                origin_seq=int(act.get("origin_seq") or cast(int, cast(Mapping[str, object], act["header"]).get("seq"))),
                origin_topic=str(act.get("origin_topic") or TOPIC_ACTUATION_REQUEST),
            )

            act_line = dumps_json(act)
            self._mcap.log_actuation(
                int(self._wall_clock.now_wall_ns()),
                int(cast(int, header.get("t_wall_ns"))),
                act_line.encode("utf-8"),
            )
            with self._send_lock_adapter:
                self._adapter.link.write_line(act_line)

    def _parse_message(self, line: str, *, inferred_topic: str, source_role: str) -> dict[str, Any] | None:
        try:
            parsed = json.loads(line)
        except Exception as e:
            self._emit_warning(
                "invalid_json",
                f"{source_role} sent invalid JSON; dropping",
                fields={"error": repr(e)},
            )
            return None

        if not isinstance(parsed, dict):
            self._emit_warning(
                "invalid_message",
                f"{source_role} sent non-object JSON; dropping",
                fields={"type": type(parsed).__name__},
            )
            return None

        msg = cast(dict[str, Any], parsed)

        # Standardize topic and provide an origin key for log alignment across agents.
        self._ensure_topic(msg, inferred_topic=inferred_topic, source_role=source_role)
        header_injected = self._ensure_header(msg, source_role=source_role, inferred_topic=inferred_topic)
        header = cast(Mapping[str, object], msg["header"])

        _ensure_origin_fields(
            msg,
            origin_agent_id=str(header.get("agent_id")),
            origin_seq=int(cast(int, header.get("seq"))),
            origin_topic=str(msg.get("topic")),
        )

        # Sanity checks / diagnostics.
        self._time_check.check(
            agent_id=str(header.get("agent_id")),
            topic=str(msg.get("topic")),
            t_mono_ns=int(cast(int, header.get("t_mono_ns"))),
            emit=self._emit_event,
        )
        self._update_clock_offsets(
            inferred_topic=str(msg.get("topic")),
            t_wall_ns=int(cast(int, header.get("t_wall_ns"))),
        )
        if header_injected:
            self._emit_warning(
                "header_injected",
                f"{source_role} message missing/invalid header; daemon injected a header",
                fields={"topic": str(msg.get("topic"))},
            )

        return msg

    def _ensure_topic(self, msg: MutableMapping[str, Any], *, inferred_topic: str, source_role: str) -> None:
        existing = msg.get("topic")
        if isinstance(existing, str) and existing:
            if existing != inferred_topic:
                # Keep a breadcrumb and normalize.
                meta = msg.setdefault("lca_meta", {})
                if isinstance(meta, dict):
                    meta.setdefault("original_topic", existing)
                    meta.setdefault("topic_normalized_by_daemon", True)
                msg["topic"] = inferred_topic
                self._emit_warning(
                    "topic_mismatch",
                    f"{source_role} message topic did not match expected; normalized",
                    fields={"original": existing, "expected": inferred_topic},
                )
            return
        msg["topic"] = inferred_topic

    def _ensure_header(self, msg: MutableMapping[str, Any], *, source_role: str, inferred_topic: str) -> bool:
        header = try_extract_header_from_obj(cast(Mapping[str, object], msg))
        if header is None:
            # Inject a daemon header so logs remain usable.
            self._inject_seq[inferred_topic] = int(self._inject_seq.get(inferred_topic, 0)) + 1
            hdr = make_header(self._run_id, self._agent_id, seq=int(self._inject_seq[inferred_topic]))
            msg["header"] = hdr
            meta = msg.setdefault("lca_meta", {})
            if isinstance(meta, dict):
                meta["header_injected"] = True
                meta["header_reason"] = "missing_or_invalid"
            return True

        # Normalize run_id + agent_id to this daemon's configuration.
        changed = False
        header_obj = cast(dict[str, Any], cast(dict[str, Any], msg.get("header")))
        if header.run_id != self._run_id:
            header_obj["run_id"] = self._run_id
            changed = True
        if header.agent_id != self._agent_id:
            header_obj["agent_id"] = self._agent_id
            changed = True
        if changed:
            meta = msg.setdefault("lca_meta", {})
            if isinstance(meta, dict):
                meta.setdefault("header_normalized_by_daemon", True)
        return False

    def _update_clock_offsets(self, *, inferred_topic: str, t_wall_ns: int) -> None:
        daemon_recv_wall_ns = int(self._wall_clock.now_wall_ns())
        offset_ns = int(daemon_recv_wall_ns - int(t_wall_ns))
        stats = self._offset_stats.get(inferred_topic)
        if stats is None:
            stats = _OffsetStats()
            self._offset_stats[inferred_topic] = stats
        stats.update(offset_ns)

        obs = self._offset_stats[TOPIC_OBSERVATION]
        act = self._offset_stats[TOPIC_ACTUATION_REQUEST]
        if obs.count and act.count and not self._clock_spread_warned:
            spread = int(max(obs.last_offset_ns, act.last_offset_ns) - min(obs.last_offset_ns, act.last_offset_ns))
            if spread > _CLOCK_SPREAD_WARN_NS:
                self._clock_spread_warned = True
                self._emit_warning(
                    "clock_spread",
                    "observed wall-clock spread between local processes exceeds threshold",
                    fields={
                        "spread_ns": spread,
                        "threshold_ns": _CLOCK_SPREAD_WARN_NS,
                        "offsets_ns": {
                            TOPIC_OBSERVATION: obs.last_offset_ns,
                            TOPIC_ACTUATION_REQUEST: act.last_offset_ns,
                        },
                    },
                )

    def _emit_warning(self, event: str, message: str, *, fields: dict[str, Any] | None = None) -> None:
        self._emit_event(level="WARNING", event=event, message=message, fields=fields)

    def _emit_event(self, *, level: str, event: str, message: str, fields: dict[str, Any] | None = None) -> None:
        ev = make_run_event(
            run_id=self._run_id,
            agent_id=self._agent_id,
            event=event,
            level=level,
            message=message,
            fields=fields,
        )
        self._broadcast_event(ev)

    def _broadcast_event(self, ev: dict[str, Any]) -> None:
        # Always normalize and ensure origin fields for events.
        ev["topic"] = TOPIC_RUN_EVENT

        header_obj = ev.get("header")
        if not isinstance(header_obj, dict):
            self._inject_seq[TOPIC_RUN_EVENT] = int(self._inject_seq.get(TOPIC_RUN_EVENT, 0)) + 1
            ev["header"] = make_header(self._run_id, self._agent_id, seq=int(self._inject_seq[TOPIC_RUN_EVENT]))
        header_obj = cast(Mapping[str, object], cast(dict[str, Any], ev["header"]))
        _ensure_origin_fields(
            ev,
            origin_agent_id=str(header_obj.get("agent_id")),
            origin_seq=int(cast(int, header_obj.get("seq"))),
            origin_topic=TOPIC_RUN_EVENT,
        )

        line = dumps_json(ev)
        _log_event_payload(
            mcap=self._mcap,
            wall_clock=self._wall_clock,
            payload_line=line,
            publish_time_ns=int(cast(int, header_obj.get("t_wall_ns"))),
        )

        with self._send_lock_adapter:
            try:
                self._adapter.link.write_line(line)
            except Exception:
                pass
        with self._send_lock_autonomy:
            try:
                self._autonomy.link.write_line(line)
            except Exception:
                pass

    def _set_stop(self, reason: str) -> None:
        if self._stop_reason is None:
            self._stop_reason = str(reason)
        self._stop.set()

        # Unblock any threads stuck in read_line() by shutting down both sockets.
        for link in (self._adapter, self._autonomy):
            try:
                link.sock.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass


def _ensure_origin_fields(
    msg: MutableMapping[str, Any],
    *,
    origin_agent_id: str,
    origin_seq: int,
    origin_topic: str,
) -> None:
    if not isinstance(msg.get("origin_agent_id"), str) or not msg.get("origin_agent_id"):
        msg["origin_agent_id"] = str(origin_agent_id)
    if not isinstance(msg.get("origin_seq"), int):
        msg["origin_seq"] = int(origin_seq)
    if not isinstance(msg.get("origin_topic"), str) or not msg.get("origin_topic"):
        msg["origin_topic"] = str(origin_topic)
