from __future__ import annotations

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
from lca_stack.run import ClockSnapshot, build_manifest_start, finalize_manifest, write_manifest
from lca_stack.proto import lca_stack_pb2 as pb

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

    def check(self, *, sender: str, topic: str, t_mono_ns: int, emit_event: callable) -> None:
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
) -> None:
    # Daemon invocation == one run. The daemon owns the run_id.
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
        schema_version=int(schema_version),
        start_wall_ns=int(start_wall_ns),
        scenario=scenario,
        seed=seed,
        clock=clock_snapshot,
    )
    write_manifest(manifest_path, manifest)
    logger.info("run_id=%s | wrote manifest=%s", run_id, manifest_path)

    bridge = _AgentBridge(
        run_id=run_id,
        agent_id=agent_id,
        schema_version=int(schema_version),
        wall_clock=wall_clock,
        mcap=mcap,
        adapter_link=links["adapter"],
        autonomy_link=links["autonomy"],
    )

    try:
        bridge.emit_event("run_start")
        bridge.run()
    finally:
        try:
            bridge.emit_event("run_stop")
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
    ) -> None:
        self._run_id = str(run_id)
        self._agent_id = str(agent_id)
        self._schema_version = int(schema_version)
        self._wall_clock = wall_clock
        self._mcap = mcap

        self._adapter = adapter_link
        self._autonomy = autonomy_link

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
        self._clock_spread_warned = False
        self._time_sanity = _TimeSanityChecker()

        self._safety = SafetyGuard(agent_id=self._agent_id)

        self._injected_seq: dict[str, int] = {"adapter": 0, "autonomy": 0}

        # Threads
        self._threads: list[threading.Thread] = []

    def close(self) -> None:
        # Stop first, then close sockets to unblock any blocking recv() loops.
        self._stop.set()

        try:
            self._adapter.link.close()
        except Exception:
            pass
        try:
            self._autonomy.link.close()
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
            env = make_status_envelope(run_id=self._run_id, agent_id=self._agent_id, seq=int(self._status_seq), status=status)

            # Send locally to Autonomy (the adapter does not require daemon status).
            self._send_to_autonomy(env)

            self._mcap.log_status(
                self._wall_clock.now_wall_ns(),
                int(env.header.t_wall_ns),
                env.SerializeToString(),
            )

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

        # Best-effort local broadcast.
        self._send_to_autonomy(env)

        self._mcap.log_run_event(
            self._wall_clock.now_wall_ns(),
            int(env.header.t_wall_ns),
            env.SerializeToString(),
        )

    def _adapter_rx_loop(self) -> None:
        try:
            while not self._stop.is_set():
                env = pb.Envelope()
                self._adapter.link.read_message(env)
                self._handle_inbound(env, sender="adapter")
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
            # If we're shutting down, socket close/shutdown can surface as errors.
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
                self._handle_inbound(env, sender="autonomy")
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

    def _handle_inbound(self, env: pb.Envelope, *, sender: str) -> None:
        try:
            validate_envelope(env, expected_schema_version=int(self._schema_version))
        except ValidationError as e:
            self.emit_event(
                "invalid_message",
                severity=pb.SEVERITY_WARNING,
                message=str(e),
                data={"sender": str(sender)},
            )
            return

        self._normalize_env(env, sender=str(sender))

        # Update local clocks/health.
        self._update_clock_health(env, sender=str(sender))

        # Sanity-check monotonic time per sender+topic.
        self._time_sanity.check(sender=str(sender), topic=str(env.topic), t_mono_ns=int(env.header.t_mono_ns), emit_event=self.emit_event)

        payload = env.WhichOneof("payload")
        if sender == "adapter":
            if payload == "observation":
                self._last_adapter_wall_ns = int(env.header.t_wall_ns)
                self._mcap.log_observation(
                    self._wall_clock.now_wall_ns(),
                    int(env.header.t_wall_ns),
                    env.SerializeToString(),
                )
                # Forward observations to autonomy.
                self._send_to_autonomy(env)
            elif payload == "event":
                # Adapter-side events are forwarded + logged.
                self._mcap.log_run_event(self._wall_clock.now_wall_ns(), int(env.header.t_wall_ns), env.SerializeToString())
                self._send_to_autonomy(env)
            else:
                # Ignore unexpected payloads from adapter for now.
                return

        elif sender == "autonomy":
            if payload == "actuation_request":
                self._last_autonomy_wall_ns = int(env.header.t_wall_ns)

                # Always log the request.
                self._mcap.log_actuation_request(
                    self._wall_clock.now_wall_ns(),
                    int(env.header.t_wall_ns),
                    env.SerializeToString(),
                )

                decision = self._safety.evaluate(mode=self._mode, estop=self._estop, req_env=env)

                # Emit event(s) for interventions.
                if decision.event_type is not None:
                    self.emit_event(
                        decision.event_type,
                        severity=pb.SEVERITY_WARNING,
                        data=dict(decision.event_data or {}),
                    )

                # Produce final actuation envelope (preserving origin identity for alignment).
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
                    publish_time_ns=int(now_wall_ns()),
                )

                # Send to adapter and log final actuation.
                self._send_to_adapter(act_env)
                self._mcap.log_actuation(
                    self._wall_clock.now_wall_ns(),
                    int(act_env.header.t_wall_ns),
                    act_env.SerializeToString(),
                )

            elif payload == "command":
                # Local team-level commands are reflected as events for now.
                cmd = env.command
                self.emit_event(
                    "command_received",
                    data={
                        "command_type": str(cmd.command_type),
                        "target_agent_id": str(cmd.target_agent_id),
                        "target_group": str(cmd.target_group),
                    },
                )

                if str(cmd.command_type).lower() in ("estop", "team_stop"):
                    self._estop = True
                    self.emit_event("estop_latched", severity=pb.SEVERITY_ERROR)

            elif payload == "event":
                # Forward autonomy-generated events to adapter (optional) + log.
                self._mcap.log_run_event(self._wall_clock.now_wall_ns(), int(env.header.t_wall_ns), env.SerializeToString())
                # For now, keep daemon->adapter traffic minimal.
            else:
                return

    def _send_to_adapter(self, env: pb.Envelope) -> None:
        with self._adapter_tx_lock:
            try:
                self._adapter.link.write_message(env)
            except Exception:
                # Best-effort.
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

    def _normalize_env(self, env: pb.Envelope, *, sender: str) -> None:
        """Normalize inbound envelopes for logging + routing.

        - Inject missing header
        - Force run_id/agent_id
        - Standardize topics for known payloads
        - Ensure origin identity is present
        """

        # Standardize topic based on payload.
        payload = env.WhichOneof("payload")
        expected_topic = None
        if payload == "observation":
            expected_topic = TOPIC_OBSERVATION
        elif payload == "actuation_request":
            expected_topic = TOPIC_ACTUATION_REQUEST
        elif payload == "actuation":
            expected_topic = TOPIC_ACTUATION
        elif payload == "status":
            expected_topic = topic_status(self._agent_id)
        elif payload == "event":
            expected_topic = TOPIC_RUN_EVENT

        if expected_topic is not None and str(env.topic) != str(expected_topic):
            if not env.topic_normalized:
                env.topic_original = str(env.topic)
            env.topic = str(expected_topic)
            env.topic_normalized = True

        # Inject header if missing.
        if not env.header.run_id and not env.header.agent_id and int(env.header.t_wall_ns) == 0 and int(env.header.t_mono_ns) == 0:
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

        # Force run_id/agent_id to the daemon's run/agent.
        if str(env.header.run_id) and str(env.header.run_id) != str(self._run_id):
            env.header_injected = True
            env.header_injected_reason = (env.header_injected_reason + ";" if env.header_injected_reason else "") + "run_id_override"
            env.header.run_id = str(self._run_id)

        if str(env.header.agent_id) and str(env.header.agent_id) != str(self._agent_id):
            env.header_injected = True
            env.header_injected_reason = (env.header_injected_reason + ";" if env.header_injected_reason else "") + "agent_id_override"
            env.header.agent_id = str(self._agent_id)

        # Ensure origin identity exists.
        if not env.origin_agent_id:
            env.origin_agent_id = str(env.header.agent_id)
        if int(env.origin_seq) == 0:
            env.origin_seq = int(env.header.seq)
        if not env.origin_topic:
            env.origin_topic = str(env.topic)

    def _update_clock_health(self, env: pb.Envelope, *, sender: str) -> None:
        if int(env.header.t_wall_ns) == 0:
            return
        offset_ns = int(now_wall_ns()) - int(env.header.t_wall_ns)
        stats = self._offset_stats.get(sender)
        if stats is not None:
            stats.update(int(offset_ns))

        # Spread across senders (adapter vs autonomy). This is a diagnostic proxy until multi-agent exists.
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

    def clock_health_snapshot(self) -> dict[str, Any]:
        offsets = {k: v.to_dict() for k, v in self._offset_stats.items()}
        spreads = [v.to_dict().get("spread_ns") for v in self._offset_stats.values() if v.count]
        spread_ns = int(max(int(s or 0) for s in spreads)) if spreads else None
        return {
            "snapshot_wall_ns": int(now_wall_ns()),
            "snapshot_mono_ns": int(now_mono_ns()),
            "offsets": offsets,
            "max_observed_spread_ns": spread_ns,
            "topic": {
                "observation": TOPIC_OBSERVATION,
                "actuation_request": TOPIC_ACTUATION_REQUEST,
                "actuation": TOPIC_ACTUATION,
                "run_event": TOPIC_RUN_EVENT,
                "status": topic_status(self._agent_id),
            },
        }
