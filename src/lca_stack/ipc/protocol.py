from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from lca_stack.proto import lca_stack_pb2 as pb

from .clock import now_mono_ns, now_wall_ns
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


def _select_schema_version(advertised: Iterable[int]) -> int:
    supported = [int(v) for v in advertised]
    candidates = [v for v in supported if v <= int(SCHEMA_VERSION)]
    if not candidates:
        raise ValueError(
            f"no compatible schema_version (daemon supports {sorted(set(supported))}, "
            f"client supports <= {int(SCHEMA_VERSION)})"
        )
    return int(max(candidates))


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
    hello = pb.Handshake()
    link.read_message(hello)
    if hello.stage != STAGE_DAEMON_HELLO:
        raise ValueError("expected daemon_hello handshake")
    if int(hello.protocol_version) != int(PROTOCOL_VERSION):
        raise ValueError("protocol_version mismatch")

    if not hello.supported_schema_versions:
        raise ValueError("daemon did not advertise supported schema versions")

    selected = _select_schema_version(int(v) for v in hello.supported_schema_versions)

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
    topic = "run/event"
    return pb.Envelope(
        schema_version=int(SCHEMA_VERSION),
        header=header,
        topic=topic,
        kind="lca/event_v1",
        origin_agent_id=str(header.agent_id),
        origin_seq=int(header.seq),
        origin_topic=topic,
        event=ev,
    )


def make_status_envelope(*, run_id: str, agent_id: str, seq: int, status: pb.Status) -> pb.Envelope:
    header = make_header(run_id, agent_id, seq)
    topic = f"agent/{agent_id}/status"
    return pb.Envelope(
        schema_version=int(SCHEMA_VERSION),
        header=header,
        topic=topic,
        kind="lca/status_v1",
        origin_agent_id=str(header.agent_id),
        origin_seq=int(header.seq),
        origin_topic=topic,
        status=status,
    )


def make_observation_envelope(*, run_id: str, agent_id: str, seq: int, kind: str, data: dict[str, object]) -> pb.Envelope:
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
    target = target_agent_id if target_agent_id is not None else agent_id
    req = pb.ActuationRequest(
        target_agent_id=str(target) if target else "",
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
    publish_time_ns: int | None = None,
) -> pb.Envelope:
    """Create an Actuation envelope (daemon -> adapter).

    `publish_time_ns` is optional for backwards compatibility; when not provided,
    it defaults to the current wall clock.
    """
    publish_wall_ns = int(now_wall_ns() if publish_time_ns is None else publish_time_ns)
    header = pb.Header(
        run_id=str(run_id),
        agent_id=str(agent_id),
        seq=int(seq),
        t_mono_ns=int(now_mono_ns()),
        t_wall_ns=int(publish_wall_ns),
    )
    topic = "local/adapter/actuation"
    act = pb.Actuation(
        safety_applied=bool(safety_applied),
        safety_reason=str(safety_reason),
        data=dict_to_struct(data),
    )
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


def make_team_message_envelope(
    *,
    run_id: str,
    agent_id: str,
    seq: int,
    message_type: str,
    data: dict[str, object],
) -> pb.Envelope:
    header = make_header(run_id, agent_id, seq)
    topic = "team/message"
    msg = pb.TeamMessage(message_type=str(message_type), data=dict_to_struct(data))
    return pb.Envelope(
        schema_version=int(SCHEMA_VERSION),
        header=header,
        topic=topic,
        kind=str(message_type),
        origin_agent_id=str(header.agent_id),
        origin_seq=int(header.seq),
        origin_topic=topic,
        team_message=msg,
    )


def make_command_envelope(
    *,
    run_id: str,
    agent_id: str,
    seq: int,
    command_type: str,
    target_agent_id: str | None = None,
    target_group: str | None = None,
    params: dict[str, object] | None = None,
) -> pb.Envelope:
    header = make_header(run_id, agent_id, seq)
    topic = "team/command"
    cmd = pb.Command(
        command_type=str(command_type),
        target_agent_id=str(target_agent_id) if target_agent_id else "",
        target_group=str(target_group) if target_group else "",
    )
    if params:
        cmd.params.CopyFrom(dict_to_struct(params))

    return pb.Envelope(
        schema_version=int(SCHEMA_VERSION),
        header=header,
        topic=topic,
        kind=str(command_type),
        origin_agent_id=str(header.agent_id),
        origin_seq=int(header.seq),
        origin_topic=topic,
        command=cmd,
    )
