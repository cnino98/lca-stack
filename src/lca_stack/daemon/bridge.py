from __future__ import annotations

from collections.abc import Callable

import logging
import socket
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any, MutableMapping

from lca_stack.daemon.safety import SafetyGuard
from lca_stack.ipc import MonotonicWallClock, now_mono_ns, now_wall_ns
from lca_stack.ipc.framing import FramedSocket
from lca_stack.ipc.protocol import (
    PROTOCOL_VERSION,
    HandshakeResult,
    daemon_handshake,
    make_actuation_envelope,
    make_event_envelope,
    make_status_envelope,
)
from lca_stack.ipc.validate import ValidationError, validate_envelope
from lca_stack.log.mcap_logger import (
    TOPIC_ACTUATION,
    TOPIC_ACTUATION_REQUEST,
    TOPIC_OBSERVATION,
    TOPIC_RUN_EVENT,
    McapLogger,
    topic_status,
)
from lca_stack.proto import lca_stack_pb2 as pb
from lca_stack.run import ClockSnapshot, build_manifest_start, finalize_manifest, write_manifest

from .server import Listeners, accept_one, listen_pair

logger = logging.getLogger(__name__)


_CLOCK_SPREAD_WARN_NS = 50_000_000  # 50 ms
_MONO_JUMP_WARN_NS = 5_000_000_000  # 5 s
_STATUS_PERIOD_S = 0.2


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

    def check(self, *, sender: str, topic: str, t_mono_ns: int, emit_event: Callable[..., Any]) -> None:
        key = (str(sender), str(topic))
        prev = self._last_mono_ns.get(key)
        if prev is not None:
            if int(t_mono_ns) < int(prev) and key not in self._warned_backward:
                self._warned_backward.add(key)
                emit_event(
                    "time_monotonic_backward",
                    severity=pb.SEVERITY_WARNING,
                    message="origin monotonic time went backward (non-decreasing expected)",
                    data={
                        "sender": str(sender),
                        "topic": str(topic),
                        "prev_t_mono_ns": int(prev),
                        "t_mono_ns": int(t_mono_ns),
                    },
                )
            if int(t_mono_ns) - int(prev) > _MONO_JUMP_WARN_NS and key not in self._warned_jump:
                self._warned_jump.add(key)
                emit_event(
                    "time_monotonic_jump",
                    severity=pb.SEVERITY_WARNING,
                    message="origin monotonic time jumped forward (large gap)",
                    data={
                        "sender": str(sender),
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
    link: FramedSocket
    handshake: HandshakeResult


def run_bridge(
    *,
    agent_id: str,
    host: str,
    adapter_port: int,
    autonomy_port: int,
    runs_dir: Path,
    scenario: str | None,
    seed: int | None,
    run_id: str | None = None,
    enable_dds: bool = True,
    dds_domain_id: int = 0,
    subscribe_status_agents: list[str] | None = None,
    subscribe_all_status: bool = False,
    join_run: bool = False,
    join_timeout_s: float = 2.0,
) -> None:
    """Run the agent daemon bridge.

    This function owns the run lifecycle:
    - selects/creates a run_id
    - performs handshakes
    - starts local bridging + DDS pub/sub
    - writes run artifacts
    """

    # Determine run identity.
    run_id_mode = "generated"
    selected_run_id: str | None = str(run_id) if run_id not in (None, "") else None

    if selected_run_id is None and join_run and enable_dds:
        try:
            from lca_stack.dds.bus import probe_run_start  # local import to keep DDS optional

            joined = probe_run_start(domain_id=int(dds_domain_id), timeout_s=float(join_timeout_s))
            if joined:
                selected_run_id = str(joined)
                run_id_mode = "joined"
        except Exception as e:
            # If DDS is misconfigured, do not prevent local-only runs.
            logger.warning("failed to probe run_start via DDS: %s", e)

    if selected_run_id is None:
        selected_run_id = str(uuid.uuid4())
        run_id_mode = "generated"
    else:
        run_id_mode = "explicit" if run_id is not None else run_id_mode

    run_id_str = str(selected_run_id)

    wall_clock = MonotonicWallClock()
    mcap = McapLogger.create(runs_dir=runs_dir, run_id=run_id_str, agent_id=agent_id)
    logger.info("run_id=%s | writing mcap=%s", run_id_str, mcap.path)

    listeners: Listeners = listen_pair(host, adapter_port, autonomy_port)
    try:
        links = _accept_links_with_handshake(
            listeners=listeners,
            run_id=run_id_str,
            agent_id=agent_id,
            adapter_port=adapter_port,
            autonomy_port=autonomy_port,
            scenario=scenario,
            seed=seed,
        )
    finally:
        try:
            listeners.adapter_listener.close()
        finally:
            listeners.autonomy_listener.close()

    # Both local links must agree on schema version.
    schema_version = int(links["adapter"].handshake.schema_version)
    if int(links["autonomy"].handshake.schema_version) != int(schema_version):
        raise RuntimeError("schema version mismatch between adapter and autonomy")

    # Configure DDS bus.
    dds_bus = None
    dds_cfg: dict[str, Any] | None = None
    if enable_dds:
        try:
            from lca_stack.dds.bus import DdsBus, DdsBusConfig

            dds_bus = DdsBus(
                config=DdsBusConfig(
                    domain_id=int(dds_domain_id),
                    subscribe_status_agents=list(subscribe_status_agents or []),
                    subscribe_all_status=bool(subscribe_all_status),
                )
            )
            dds_cfg = {
                "enabled": True,
                "domain_id": int(dds_domain_id),
                "subscribe_status_agents": list(subscribe_status_agents or []),
                "subscribe_all_status": bool(subscribe_all_status),
                "join_run_requested": bool(join_run),
                "join_timeout_s": float(join_timeout_s),
            }
        except Exception as e:
            # DDS is part of the reference design, but allow local-only runs
            # if the DDS backend is unavailable.
            logger.error("DDS enabled but could not be initialized: %s", e)
            dds_bus = None
            dds_cfg = {
                "enabled": False,
                "error": str(e),
                "domain_id": int(dds_domain_id),
                "subscribe_status_agents": list(subscribe_status_agents or []),
                "subscribe_all_status": bool(subscribe_all_status),
                "join_run_requested": bool(join_run),
                "join_timeout_s": float(join_timeout_s),
            }

    manifest_path = Path(runs_dir) / run_id_str / "manifest.yaml"

    start_wall_ns = int(now_wall_ns())
    clock_snapshot = ClockSnapshot(
        daemon_wall_ns=int(start_wall_ns),
        daemon_mono_ns=int(now_mono_ns()),
        monotonic_wall_anchor_wall0_ns=int(wall_clock.wall0_ns),
        monotonic_wall_anchor_mono0_ns=int(wall_clock.mono0_ns),
    )

    config_snapshot: dict[str, Any] = {
        "agent_id": str(agent_id),
        "host": str(host),
        "ports": {"adapter": int(adapter_port), "autonomy": int(autonomy_port)},
        "scenario": str(scenario) if scenario is not None else None,
        "seed": int(seed) if seed is not None else None,
        "enable_dds_requested": bool(enable_dds),
        "dds_domain_id": int(dds_domain_id),
        "subscribe_status_agents": list(subscribe_status_agents or []),
        "subscribe_all_status": bool(subscribe_all_status),
        "join_run_requested": bool(join_run),
        "join_timeout_s": float(join_timeout_s),
    }

    manifest = build_manifest_start(
        run_id=run_id_str,
        agent_id=str(agent_id),
        host=str(host),
        adapter_port=int(adapter_port),
        autonomy_port=int(autonomy_port),
        protocol_version=int(PROTOCOL_VERSION),
        schema_version=int(schema_version),
        start_wall_ns=int(start_wall_ns),
        scenario=scenario,
        seed=seed,
        clock=clock_snapshot,
        run_id_mode=str(run_id_mode),
        dds=dds_cfg,
        config=config_snapshot,
    )
    write_manifest(manifest_path, manifest)
    logger.info("run_id=%s | wrote manifest=%s", run_id_str, manifest_path)

    bridge = _AgentBridge(
        run_id=run_id_str,
        agent_id=str(agent_id),
        schema_version=int(schema_version),
        wall_clock=wall_clock,
        mcap=mcap,
        adapter_link=links["adapter"],
        autonomy_link=links["autonomy"],
        dds_bus=dds_bus,
    )

    try:
        bridge.start_team_bus()

        bridge.emit_event(
            "handshake_complete",
            data={
                "role": "adapter",
                "protocol_version": int(links["adapter"].handshake.protocol_version),
                "schema_version": int(links["adapter"].handshake.schema_version),
            },
        )
        bridge.emit_event(
            "handshake_complete",
            data={
                "role": "autonomy",
                "protocol_version": int(links["autonomy"].handshake.protocol_version),
                "schema_version": int(links["autonomy"].handshake.schema_version),
            },
        )

        bridge.emit_event(
            "run_start",
            data={
                "run_id": str(run_id_str),
                "agent_id": str(agent_id),
                "scenario": str(scenario) if scenario is not None else "",
                "seed": int(seed) if seed is not None else 0,
            },
        )
        bridge.run()
    finally:
        try:
            bridge.emit_event("run_stop")
        finally:
            end_wall_ns = int(now_wall_ns())
            final = finalize_manifest(
                manifest,
                end_wall_ns=end_wall_ns,
                clock_health=bridge.clock_health_snapshot(),
                participants=bridge.participants_snapshot(),
            )
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
) -> dict[str, _Link]:
    out: dict[str, _Link] = {}

    def _accept(role: str, listener: socket.socket) -> _Link:
        sock, addr = accept_one(listener)
        link = FramedSocket(sock)
        hs = daemon_handshake(
            link=link,
            role=str(role),
            run_id=str(run_id),
            agent_id=str(agent_id),
            adapter_port=int(adapter_port),
            autonomy_port=int(autonomy_port),
            scenario=scenario,
            seed=seed,
        )
        return _Link(role=str(role), sock=sock, addr=addr, link=link, handshake=hs)

    with ThreadPoolExecutor(max_workers=2, thread_name_prefix="accept") as pool:
        fut_adapter = pool.submit(_accept, "adapter", listeners.adapter_listener)
        fut_autonomy = pool.submit(_accept, "autonomy", listeners.autonomy_listener)
        adapter_link = fut_adapter.result()
        autonomy_link = fut_autonomy.result()

    out["adapter"] = adapter_link
    out["autonomy"] = autonomy_link
    return out


class _AgentBridge:
    def __init__(
        self,
        *,
        run_id: str,
        agent_id: str,
        schema_version: int,
        wall_clock: MonotonicWallClock,
        mcap: McapLogger,
        adapter_link: _Link,
        autonomy_link: _Link,
        dds_bus: object | None,
    ) -> None:
        self._run_id = str(run_id)
        self._agent_id = str(agent_id)
        self._schema_version = int(schema_version)
        self._wall_clock = wall_clock
        self._mcap = mcap

        self._adapter = adapter_link
        self._autonomy = autonomy_link

        self._dds_bus = dds_bus
        self._dds_started = False
        self._dds_tx_lock = threading.Lock()

        self._stop = threading.Event()
        self._adapter_tx_lock = threading.Lock()
        self._autonomy_tx_lock = threading.Lock()

        self._event_seq = 0
        self._status_seq = 0
        self._actuation_seq = 0

        self._mode = pb.MODE_INIT
        self._estop = False
        self._faults: list[str] = []

        self._last_adapter_wall_ns: int = 0
        self._last_autonomy_wall_ns: int = 0
        self._last_team_rx_wall_ns: int = 0
        self._last_team_tx_wall_ns: int = 0

        self._offset_stats: MutableMapping[str, _OffsetStats] = {
            "adapter": _OffsetStats(),
            "autonomy": _OffsetStats(),
        }
        self._team_offset_stats: MutableMapping[str, _OffsetStats] = {}
        self._clock_spread_warned = False
        self._time_sanity = _TimeSanityChecker()

        self._safety = SafetyGuard(agent_id=self._agent_id)

        self._injected_seq: dict[str, int] = {"adapter": 0, "autonomy": 0}
        self._header_injection_warn_count: dict[str, int] = {"adapter": 0, "autonomy": 0}

        self._participants_lock = threading.Lock()
        self._participants: set[str] = {str(self._agent_id)}

        self._threads: list[threading.Thread] = []

    def start_team_bus(self) -> None:
        if self._dds_bus is None or self._dds_started:
            return
        try:
            from lca_stack.dds.bus import DdsBus

            bus = self._dds_bus
            if not isinstance(bus, DdsBus):
                return

            bus.start(on_sample=self._on_dds_sample)
            self._dds_started = True
            self.emit_event(
                "dds_started",
                data={"domain_id": int(bus._cfg.domain_id)},  # type: ignore[attr-defined]
            )
        except Exception as e:
            logger.exception("DDS start failed (team bus will be disabled): %s", e)
            self.emit_event(
                "dds_start_failed",
                severity=pb.SEVERITY_WARNING,
                message=str(e),
            )

    def close(self) -> None:
        self._stop.set()

        try:
            self._adapter.link.close()
        except Exception:
            pass
        try:
            self._autonomy.link.close()
        except Exception:
            pass

        if self._dds_bus is not None and self._dds_started:
            try:
                from lca_stack.dds.bus import DdsBus

                bus = self._dds_bus
                if isinstance(bus, DdsBus):
                    bus.stop()
            except Exception:
                pass

        for t in self._threads:
            try:
                t.join(timeout=1.0)
            except Exception:
                pass

        self._mcap.close()

    def run(self) -> None:
        self._mode = pb.MODE_RUNNING

        self.emit_event(
            "link_connected",
            data={"role": "adapter", "addr": f"{self._adapter.addr[0]}:{self._adapter.addr[1]}"},
        )
        self.emit_event(
            "link_connected",
            data={"role": "autonomy", "addr": f"{self._autonomy.addr[0]}:{self._autonomy.addr[1]}"},
        )

        t1 = threading.Thread(target=self._adapter_rx_loop, name="adapter-rx", daemon=True)
        t2 = threading.Thread(target=self._autonomy_rx_loop, name="autonomy-rx", daemon=True)
        t3 = threading.Thread(target=self._status_loop, name="status", daemon=True)

        self._threads = [t1, t2, t3]
        for t in self._threads:
            t.start()

        self._stop.wait()
        self._mode = pb.MODE_STOPPED

    def _status_loop(self) -> None:
        import time

        next_t = time.monotonic()
        heartbeat_seq = 0

        while not self._stop.is_set():
            now = time.monotonic()
            if now < next_t:
                time.sleep(min(0.05, max(0.0, next_t - now)))
                continue
            next_t = now + _STATUS_PERIOD_S

            heartbeat_seq += 1
            status = pb.Status(
                mode=self._mode,
                faults=list(self._faults),
                estop=bool(self._estop),
                heartbeat_seq=int(heartbeat_seq),
                daemon_wall_ns=int(now_wall_ns()),
                last_adapter_wall_ns=int(self._last_adapter_wall_ns),
                last_autonomy_wall_ns=int(self._last_autonomy_wall_ns),
                last_team_rx_wall_ns=int(self._last_team_rx_wall_ns),
                last_team_tx_wall_ns=int(self._last_team_tx_wall_ns),
            )

            self._status_seq += 1
            env = make_status_envelope(
                run_id=self._run_id,
                agent_id=self._agent_id,
                seq=int(self._status_seq),
                status=status,
            )

            self._send_to_autonomy(env)

            now_log = self._wall_clock.now_wall_ns()
            self._mcap.log_status(self._agent_id, now_log, int(env.header.t_wall_ns), env.SerializeToString())

            self._dds_publish(topic=str(env.topic), env=env, log_time_ns=int(now_log))

    def emit_event(
        self,
        event_type: str,
        *,
        severity: pb.Severity = pb.SEVERITY_INFO,
        message: str | None = None,
        data: dict[str, object] | None = None,
    ) -> None:
        self._event_seq += 1
        env = make_event_envelope(
            run_id=self._run_id,
            agent_id=self._agent_id,
            seq=int(self._event_seq),
            event_type=str(event_type),
            severity=severity,
            message=message,
            data=data,
        )

        self._send_to_autonomy(env)

        now_log = self._wall_clock.now_wall_ns()
        self._mcap.log_run_event(now_log, int(env.header.t_wall_ns), env.SerializeToString())

        self._dds_publish(topic=TOPIC_RUN_EVENT, env=env, log_time_ns=int(now_log))

    def _adapter_rx_loop(self) -> None:
        try:
            while not self._stop.is_set():
                env = pb.Envelope()
                self._adapter.link.read_message(env)
                self._handle_inbound_local(env, sender="adapter")
        except EOFError:
            if not self._stop.is_set():
                self._fault("adapter_disconnected")
                self.emit_event(
                    "link_disconnected",
                    severity=pb.SEVERITY_WARNING,
                    data={"role": "adapter"},
                )
                self._stop.set()
        except Exception as e:
            if not self._stop.is_set():
                logger.exception("adapter rx failed")
                self._fault("adapter_rx_error")
                self.emit_event(
                    "link_error",
                    severity=pb.SEVERITY_ERROR,
                    message=str(e),
                    data={"role": "adapter"},
                )
                self._stop.set()

    def _autonomy_rx_loop(self) -> None:
        try:
            while not self._stop.is_set():
                env = pb.Envelope()
                self._autonomy.link.read_message(env)
                self._handle_inbound_local(env, sender="autonomy")
        except EOFError:
            if not self._stop.is_set():
                self._fault("autonomy_disconnected")
                self.emit_event(
                    "link_disconnected",
                    severity=pb.SEVERITY_WARNING,
                    data={"role": "autonomy"},
                )
                self._stop.set()
        except Exception as e:
            if not self._stop.is_set():
                logger.exception("autonomy rx failed")
                self._fault("autonomy_rx_error")
                self.emit_event(
                    "link_error",
                    severity=pb.SEVERITY_ERROR,
                    message=str(e),
                    data={"role": "autonomy"},
                )
                self._stop.set()

    def _handle_inbound_local(self, env: pb.Envelope, *, sender: str) -> None:
        """Handle inbound message from a local link (adapter/autonomy)."""
        try:
            validate_envelope(
                env,
                expected_schema_version=int(self._schema_version),
                allow_missing_header=True,
                allow_missing_topic=True,
                allow_missing_kind=True,
            )
        except ValidationError as e:
            self.emit_event(
                "invalid_message",
                severity=pb.SEVERITY_WARNING,
                message=str(e),
                data={"sender": str(sender)},
            )
            return

        normalized = self._normalize_local_env(env, sender=str(sender))
        if normalized.get("header_injected_missing"):
            self._warn_header_injection(sender=str(sender), env=env)

        self._update_clock_health_local(env, sender=str(sender))

        if env.header and int(env.header.t_mono_ns) != 0:
            self._time_sanity.check(
                sender=str(sender),
                topic=str(env.topic),
                t_mono_ns=int(env.header.t_mono_ns),
                emit_event=self.emit_event,
            )

        payload = env.WhichOneof("payload")
        if sender == "adapter":
            if payload == "observation":
                self._last_adapter_wall_ns = int(env.header.t_wall_ns)
                now_log = self._wall_clock.now_wall_ns()
                self._mcap.log_observation(now_log, int(env.header.t_wall_ns), env.SerializeToString())
                self._send_to_autonomy(env)
            elif payload == "event":
                now_log = self._wall_clock.now_wall_ns()
                self._mcap.log_run_event(now_log, int(env.header.t_wall_ns), env.SerializeToString())
                self._send_to_autonomy(env)
                self._dds_publish(topic=TOPIC_RUN_EVENT, env=env, log_time_ns=int(now_log))
            else:
                return

        elif sender == "autonomy":
            if payload == "actuation_request":
                self._last_autonomy_wall_ns = int(env.header.t_wall_ns)

                now_log = self._wall_clock.now_wall_ns()
                self._mcap.log_actuation_request(now_log, int(env.header.t_wall_ns), env.SerializeToString())

                decision = self._safety.evaluate(mode=self._mode, estop=self._estop, req_env=env)

                if decision.event_type is not None:
                    self.emit_event(
                        decision.event_type,
                        severity=pb.SEVERITY_WARNING,
                        data=dict(decision.event_data or {}),
                    )

                self._actuation_seq += 1
                act_env = make_actuation_envelope(
                    run_id=self._run_id,
                    agent_id=self._agent_id,
                    seq=int(self._actuation_seq),
                    kind=str(env.kind),
                    data=dict(decision.final_data),
                    safety_applied=bool(decision.safety_applied),
                    safety_reason=str(decision.safety_reason),
                    origin_agent_id=str(env.origin_agent_id),
                    origin_seq=int(env.origin_seq),
                    origin_topic=str(env.origin_topic),
                )

                self._send_to_adapter(act_env)
                now_log2 = self._wall_clock.now_wall_ns()
                self._mcap.log_actuation(now_log2, int(act_env.header.t_wall_ns), act_env.SerializeToString())

            elif payload == "command":
                now_log = self._wall_clock.now_wall_ns()
                self._dds_publish(topic="team/command", env=env, log_time_ns=int(now_log))
                self._apply_command_side_effects(env)

            elif payload == "team_message":
                now_log = self._wall_clock.now_wall_ns()
                self._dds_publish(topic="team/message", env=env, log_time_ns=int(now_log))

            elif payload == "event":
                now_log = self._wall_clock.now_wall_ns()
                self._mcap.log_run_event(now_log, int(env.header.t_wall_ns), env.SerializeToString())
                self._dds_publish(topic=TOPIC_RUN_EVENT, env=env, log_time_ns=int(now_log))
            else:
                return

    def _send_to_adapter(self, env: pb.Envelope) -> None:
        with self._adapter_tx_lock:
            try:
                self._adapter.link.write_message(env)
            except Exception:
                pass

    def _send_to_autonomy(self, env: pb.Envelope) -> None:
        with self._autonomy_tx_lock:
            try:
                self._autonomy.link.write_message(env)
            except Exception:
                pass

    def _fault(self, fault: str) -> None:
        if str(fault) not in self._faults:
            self._faults.append(str(fault))

    def _warn_header_injection(self, *, sender: str, env: pb.Envelope) -> None:
        cnt = int(self._header_injection_warn_count.get(sender, 0))
        if cnt >= 10:
            return
        self._header_injection_warn_count[sender] = int(cnt) + 1
        self.emit_event(
            "header_injected",
            severity=pb.SEVERITY_WARNING,
            message="inbound message was missing header; daemon injected one",
            data={
                "sender": str(sender),
                "reason": str(env.header_injected_reason),
                "topic": str(env.topic),
            },
        )

    def _normalize_local_env(self, env: pb.Envelope, *, sender: str) -> dict[str, bool]:
        """Normalize inbound envelopes from local IPC.

        - Inject missing header
        - Inject missing topic/kind for known payloads
        - Force run_id/agent_id
        - Standardize topics
        - Ensure origin identity is present
        """

        changes: dict[str, bool] = {
            "header_injected_missing": False,
            "header_overrode": False,
            "topic_normalized": False,
            "kind_injected": False,
            "origin_injected": False,
        }

        payload = env.WhichOneof("payload")

        expected_topic: str | None = None
        expected_kind: str | None = None
        if payload == "observation":
            expected_topic = TOPIC_OBSERVATION
            expected_kind = "lca/observation_v1"
        elif payload == "actuation_request":
            expected_topic = TOPIC_ACTUATION_REQUEST
            expected_kind = "lca/actuation_request_v1"
        elif payload == "actuation":
            expected_topic = TOPIC_ACTUATION
            expected_kind = "lca/actuation_v1"
        elif payload == "status":
            expected_topic = topic_status(self._agent_id)
            expected_kind = "lca/status_v1"
        elif payload == "event":
            expected_topic = TOPIC_RUN_EVENT
            expected_kind = "lca/event_v1"
        elif payload == "command":
            expected_topic = "team/command"
            expected_kind = "lca/command_v1"
        elif payload == "team_message":
            expected_topic = "team/message"
            expected_kind = "lca/team_message_v1"

        if not env.topic and expected_topic is not None:
            env.topic = str(expected_topic)
            env.topic_normalized = True
            env.topic_original = ""
            changes["topic_normalized"] = True
        elif expected_topic is not None and str(env.topic) != str(expected_topic):
            if not env.topic_normalized:
                env.topic_original = str(env.topic)
            env.topic = str(expected_topic)
            env.topic_normalized = True
            changes["topic_normalized"] = True

        if not env.kind and expected_kind is not None:
            env.kind = str(expected_kind)
            changes["kind_injected"] = True

        if (
            not env.header.run_id
            and not env.header.agent_id
            and int(env.header.t_wall_ns) == 0
            and int(env.header.t_mono_ns) == 0
        ):
            self._injected_seq[sender] = int(self._injected_seq.get(sender, 0)) + 1
            injected = pb.Header(
                run_id=str(self._run_id),
                agent_id=str(self._agent_id),
                seq=int(self._injected_seq[sender]),
                t_mono_ns=int(now_mono_ns()),
                t_wall_ns=int(now_wall_ns()),
            )
            env.header.CopyFrom(injected)
            env.header_injected = True
            env.header_injected_reason = "missing_header"
            changes["header_injected_missing"] = True

        if str(env.header.run_id) and str(env.header.run_id) != str(self._run_id):
            env.header_injected = True
            env.header_injected_reason = (
                env.header_injected_reason + ";" if env.header_injected_reason else ""
            ) + "run_id_override"
            env.header.run_id = str(self._run_id)
            changes["header_overrode"] = True

        if str(env.header.agent_id) and str(env.header.agent_id) != str(self._agent_id):
            env.header_injected = True
            env.header_injected_reason = (
                env.header_injected_reason + ";" if env.header_injected_reason else ""
            ) + "agent_id_override"
            env.header.agent_id = str(self._agent_id)
            changes["header_overrode"] = True

        if not env.origin_agent_id:
            env.origin_agent_id = str(env.header.agent_id)
            changes["origin_injected"] = True
        if int(env.origin_seq) == 0:
            env.origin_seq = int(env.header.seq)
            changes["origin_injected"] = True
        if not env.origin_topic:
            env.origin_topic = str(env.topic)
            changes["origin_injected"] = True

        return changes

    def _apply_command_side_effects(self, env: pb.Envelope) -> None:
        if not env.HasField("command"):
            return
        cmd = env.command
        cmd_type = str(cmd.command_type).lower()

        targeted = False
        if str(cmd.target_agent_id):
            targeted = str(cmd.target_agent_id) == str(self._agent_id)
        elif str(cmd.target_group):
            targeted = str(cmd.target_group).lower() in ("all", "team", "*", "broadcast")

        if cmd_type in ("estop", "team_stop", "stop") and targeted:
            if not self._estop:
                self._estop = True
                self.emit_event("estop_latched", severity=pb.SEVERITY_ERROR)

    def _update_clock_health_local(self, env: pb.Envelope, *, sender: str) -> None:
        if int(env.header.t_wall_ns) == 0:
            return
        offset_ns = int(now_wall_ns()) - int(env.header.t_wall_ns)
        stats = self._offset_stats.get(sender)
        if stats is not None:
            stats.update(int(offset_ns))

        offsets: list[int] = []
        for s in ("adapter", "autonomy"):
            st = self._offset_stats.get(s)
            if st is not None and st.count:
                offsets.append(int(st.last_offset_ns))
        if len(offsets) >= 2:
            spread = int(max(offsets) - min(offsets))
            if spread > _CLOCK_SPREAD_WARN_NS and not self._clock_spread_warned:
                self._clock_spread_warned = True
                self.emit_event(
                    "clock_spread_warning",
                    severity=pb.SEVERITY_WARNING,
                    message="observed wall-clock offset spread across local processes",
                    data={
                        "spread_ns": int(spread),
                        "threshold_ns": int(_CLOCK_SPREAD_WARN_NS),
                        "offsets": {k: v.to_dict() for k, v in self._offset_stats.items()},
                    },
                )

    def _note_participant(self, agent_id: str) -> None:
        if not agent_id:
            return
        with self._participants_lock:
            self._participants.add(str(agent_id))

    def participants_snapshot(self) -> list[str]:
        with self._participants_lock:
            return sorted(self._participants)

    def _dds_publish(self, *, topic: str, env: pb.Envelope, log_time_ns: int) -> None:
        if self._dds_bus is None or not self._dds_started:
            return

        if str(env.topic) != str(topic):
            if not env.topic_normalized:
                env.topic_original = str(env.topic)
            env.topic = str(topic)
            env.topic_normalized = True

        payload = env.SerializeToString()

        try:
            from lca_stack.dds.bus import DdsBus

            bus = self._dds_bus
            if not isinstance(bus, DdsBus):
                return

            with self._dds_tx_lock:
                bus.publish(str(topic), env)

            self._last_team_tx_wall_ns = int(now_wall_ns())
            self._note_participant(str(env.header.agent_id))

            self._mcap.log_dds_tx(
                dds_topic=str(topic),
                log_time_ns=int(log_time_ns),
                publish_time_ns=int(env.header.t_wall_ns),
                payload=payload,
            )
        except Exception as e:
            self.emit_event(
                "dds_publish_failed",
                severity=pb.SEVERITY_WARNING,
                message=str(e),
                data={"topic": str(topic)},
            )

    def _on_dds_sample(self, topic_name: str, env: pb.Envelope) -> None:
        try:
            validate_envelope(
                env,
                expected_schema_version=int(self._schema_version),
                allow_missing_header=False,
                allow_missing_topic=False,
                allow_missing_kind=False,
            )
        except ValidationError as e:
            self.emit_event(
                "invalid_dds_message",
                severity=pb.SEVERITY_WARNING,
                message=str(e),
                data={"topic": str(topic_name)},
            )
            return

        if str(env.topic) != str(topic_name):
            if not env.topic_normalized:
                env.topic_original = str(env.topic)
            env.topic = str(topic_name)
            env.topic_normalized = True

        self._last_team_rx_wall_ns = int(now_wall_ns())

        self._note_participant(str(env.header.agent_id))
        if int(env.header.t_wall_ns) != 0:
            off = int(now_wall_ns()) - int(env.header.t_wall_ns)
            st = self._team_offset_stats.get(str(env.header.agent_id))
            if st is None:
                st = _OffsetStats()
                self._team_offset_stats[str(env.header.agent_id)] = st
            st.update(int(off))

        if str(env.header.run_id) and str(env.header.run_id) != str(self._run_id):
            self.emit_event(
                "run_id_mismatch",
                severity=pb.SEVERITY_WARNING,
                message="received DDS message with a different run_id",
                data={
                    "local_run_id": str(self._run_id),
                    "remote_run_id": str(env.header.run_id),
                    "remote_agent_id": str(env.header.agent_id),
                    "topic": str(topic_name),
                },
            )

        now_log = self._wall_clock.now_wall_ns()
        self._mcap.log_dds_rx(
            dds_topic=str(topic_name),
            log_time_ns=int(now_log),
            publish_time_ns=int(env.header.t_wall_ns),
            payload=env.SerializeToString(),
        )

        if env.HasField("command"):
            self._apply_command_side_effects(env)

        self._send_to_autonomy(env)

    def clock_health_snapshot(self) -> dict[str, Any]:
        offsets = {k: v.to_dict() for k, v in self._offset_stats.items()}
        team_offsets = {k: v.to_dict() for k, v in self._team_offset_stats.items()}
        spreads = [v.to_dict().get("spread_ns") for v in self._offset_stats.values() if v.count]
        spread_ns = int(max(int(s or 0) for s in spreads)) if spreads else None
        return {
            "snapshot_wall_ns": int(now_wall_ns()),
            "snapshot_mono_ns": int(now_mono_ns()),
            "offsets": offsets,
            "team_offsets": team_offsets,
            "max_observed_spread_ns": spread_ns,
            "topic": {
                "observation": TOPIC_OBSERVATION,
                "actuation_request": TOPIC_ACTUATION_REQUEST,
                "actuation": TOPIC_ACTUATION,
                "run_event": TOPIC_RUN_EVENT,
                "status": topic_status(self._agent_id),
            },
        }