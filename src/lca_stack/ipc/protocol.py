from __future__ import annotations

from dataclasses import dataclass

from lca_stack.proto import lca_stack_pb2 as pb

from .clock import now_mono_ns
from .framing import FramedSocket
from .header import make_header
from .structs import dict_to_struct

# Wire protocol (framed protobuf) version.
PROTOCOL_VERSION: int = 2

# Envelope schema version.
SCHEMA_VERSION: int = 1

# Handshake stages.
STAGE_DAEMON_HELLO = "daemon_hello"
STAGE_CLIENT_HELLO = "client_hello"
STAGE_DAEMON_CONFIRM = "daemon_confirm"


@dataclass(frozen=True, slots=True)
class HandshakeResult:
    run_id: str
    agent_id: str
    role: str
    protocol_version: int
    schema_version: int
    adapter_port: int
    autonomy_port: int
    scenario: str | None
    seed: int | None


def _supported_schema_versions() -> list[int]:
    return [SCHEMA_VERSION]


def daemon_handshake(
    *,
    link: FramedSocket,
    role: str,
    run_id: str,
    agent_id: str,
    adapter_port: int,
    autonomy_port: int,
    scenario: str | None,
    seed: int | None,
) -> HandshakeResult:
    """Perform daemon-side handshake negotiation.

    The daemon sends a hello with supported schema versions, receives the client selection,
    then confirms.
    """
    hello = pb.Handshake(
        protocol_version=int(PROTOCOL_VERSION),
        supported_schema_versions=[int(v) for v in _supported_schema_versions()],
        selected_schema_version=0,
        role=str(role),
        stage=STAGE_DAEMON_HELLO,
        run_id=str(run_id),
        agent_id=str(agent_id),
        adapter_port=int(adapter_port),
        autonomy_port=int(autonomy_port),
        scenario=str(scenario) if scenario is not None else "",
        seed=int(seed) if seed is not None else 0,
        message="",
    )
    link.write_message(hello)

    resp = pb.Handshake()
    link.read_message(resp)
    if resp.stage != STAGE_CLIENT_HELLO:
        raise ValueError("expected client_hello handshake")
    if int(resp.protocol_version) != int(PROTOCOL_VERSION):
        raise ValueError("protocol_version mismatch")

    selected = int(resp.selected_schema_version)
    if selected not in _supported_schema_versions():
        raise ValueError("unsupported schema_version selected")

    confirm = pb.Handshake(
        protocol_version=int(PROTOCOL_VERSION),
        supported_schema_versions=[int(v) for v in _supported_schema_versions()],
        selected_schema_version=int(selected),
        role=str(role),
        stage=STAGE_DAEMON_CONFIRM,
        run_id=str(run_id),
        agent_id=str(agent_id),
        adapter_port=int(adapter_port),
        autonomy_port=int(autonomy_port),
        scenario=str(scenario) if scenario is not None else "",
        seed=int(seed) if seed is not None else 0,
        message="",
    )
    link.write_message(confirm)

    return HandshakeResult(
        run_id=str(run_id),
        agent_id=str(agent_id),
        role=str(role),
        protocol_version=int(PROTOCOL_VERSION),
        schema_version=int(selected),
        adapter_port=int(adapter_port),
        autonomy_port=int(autonomy_port),
        scenario=str(scenario) if scenario is not None else None,
        seed=int(seed) if seed is not None else None,
    )


def client_handshake(*, link: FramedSocket, role: str) -> HandshakeResult:
    """Perform client-side handshake negotiation.

    Reads the daemon hello, chooses a schema version, replies, and reads confirm.
    """
    hello = pb.Handshake()
    link.read_message(hello)
    if hello.stage != STAGE_DAEMON_HELLO:
        raise ValueError("expected daemon_hello handshake")
    if int(hello.protocol_version) != int(PROTOCOL_VERSION):
        raise ValueError("protocol_version mismatch")

    supported: set[int] = set(int(v) for v in hello.supported_schema_versions)
    if not supported:
        raise ValueError("daemon did not advertise supported schema versions")

    selected = max(v for v in supported if v <= SCHEMA_VERSION)

    resp = pb.Handshake(
        protocol_version=int(PROTOCOL_VERSION),
        supported_schema_versions=[int(SCHEMA_VERSION)],
        selected_schema_version=int(selected),
        role=str(role),
        stage=STAGE_CLIENT_HELLO,
        run_id=str(hello.run_id),
        agent_id=str(hello.agent_id),
        adapter_port=int(hello.adapter_port),
        autonomy_port=int(hello.autonomy_port),
        scenario=str(hello.scenario),
        seed=int(hello.seed),
        message="",
    )
    link.write_message(resp)

    confirm = pb.Handshake()
    link.read_message(confirm)
    if confirm.stage != STAGE_DAEMON_CONFIRM:
        raise ValueError("expected daemon_confirm handshake")
    if int(confirm.selected_schema_version) != int(selected):
        raise ValueError("schema_version confirm mismatch")

    scenario = str(confirm.scenario) if str(confirm.scenario) else None
    seed = int(confirm.seed) if int(confirm.seed) != 0 else None

    return HandshakeResult(
        run_id=str(confirm.run_id),
        agent_id=str(confirm.agent_id),
        role=str(role),
        protocol_version=int(confirm.protocol_version),
        schema_version=int(selected),
        adapter_port=int(confirm.adapter_port),
        autonomy_port=int(confirm.autonomy_port),
        scenario=scenario,
        seed=seed,
    )


def make_event_envelope(
    *,
    run_id: str,
    agent_id: str,
    seq: int,
    event_type: str,
    severity: pb.Severity = pb.Severity.SEVERITY_INFO,
    message: str | None = None,
    data: dict[str, object] | None = None,
) -> pb.Envelope:
    ev = pb.Event(
        event_type=str(event_type),
        severity=severity,
        message=str(message) if message is not None else "",
    )
    if data:
        ev.data.CopyFrom(dict_to_struct(data))

    header = make_header(run_id, agent_id, seq)
    env = pb.Envelope(
        schema_version=int(SCHEMA_VERSION),
        header=header,
        topic="run/event",
        kind="lca/event_v1",
        origin_agent_id=str(header.agent_id),
        origin_seq=int(header.seq),
        origin_topic="run/event",
        event=ev,
    )
    return env


def make_status_envelope(
    *,
    run_id: str,
    agent_id: str,
    seq: int,
    status: pb.Status,
) -> pb.Envelope:
    header = make_header(run_id, agent_id, seq)
    topic = f"agent/{agent_id}/status"
    env = pb.Envelope(
        schema_version=int(SCHEMA_VERSION),
        header=header,
        topic=topic,
        kind="lca/status_v1",
        origin_agent_id=str(header.agent_id),
        origin_seq=int(header.seq),
        origin_topic=topic,
        status=status,
    )
    return env


def make_observation_envelope(
    *,
    run_id: str,
    agent_id: str,
    seq: int,
    kind: str,
    data: dict[str, object],
) -> pb.Envelope:
    header = make_header(run_id, agent_id, seq)
    topic = "local/adapter/observation"
    obs = pb.LocalObservation(data=dict_to_struct(data))
    return pb.Envelope(
        schema_version=int(SCHEMA_VERSION),
        header=header,
        topic=topic,
        kind=str(kind),
        origin_agent_id=str(header.agent_id),
        origin_seq=int(header.seq),
        origin_topic=topic,
        observation=obs,
    )


def make_actuation_request_envelope(
    *,
    run_id: str,
    agent_id: str,
    seq: int,
    kind: str,
    data: dict[str, object],
    target_agent_id: str | None = None,
    expires_wall_ns: int | None = None,
) -> pb.Envelope:
    header = make_header(run_id, agent_id, seq)
    topic = "local/autonomy/actuation_request"
    req = pb.ActuationRequest(
        target_agent_id=str(target_agent_id) if target_agent_id else "",
        expires_wall_ns=int(expires_wall_ns) if expires_wall_ns is not None else 0,
        data=dict_to_struct(data),
    )
    return pb.Envelope(
        schema_version=int(SCHEMA_VERSION),
        header=header,
        topic=topic,
        kind=str(kind),
        origin_agent_id=str(header.agent_id),
        origin_seq=int(header.seq),
        origin_topic=topic,
        actuation_request=req,
    )


def make_actuation_envelope(
    *,
    run_id: str,
    agent_id: str,
    seq: int,
    kind: str,
    data: dict[str, object],
    safety_applied: bool,
    safety_reason: str,
    origin_agent_id: str,
    origin_seq: int,
    origin_topic: str,
    publish_time_ns: int,
) -> pb.Envelope:
    # Actuation uses header times from daemon at emission (publish_time_ns), but preserves
    # origin identity for cross-log matching.
    header = pb.Header(
        run_id=str(run_id),
        agent_id=str(agent_id),
        seq=int(seq),
        t_mono_ns=int(now_mono_ns()),
        t_wall_ns=int(publish_time_ns),
    )
    topic = "local/adapter/actuation"
    act = pb.Actuation(safety_applied=bool(safety_applied), safety_reason=str(safety_reason), data=dict_to_struct(data))
    return pb.Envelope(
        schema_version=int(SCHEMA_VERSION),
        header=header,
        topic=topic,
        kind=str(kind),
        origin_agent_id=str(origin_agent_id),
        origin_seq=int(origin_seq),
        origin_topic=str(origin_topic),
        actuation=act,
    )
