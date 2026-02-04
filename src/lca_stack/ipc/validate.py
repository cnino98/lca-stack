from __future__ import annotations

from dataclasses import dataclass

from lca_stack.proto import lca_stack_pb2 as pb


@dataclass(frozen=True, slots=True)
class ValidationError(Exception):
    message: str

    def __str__(self) -> str:
        return str(self.message)


def _has_any_header_fields(h: pb.Header) -> bool:
    return bool(
        str(h.run_id)
        or str(h.agent_id)
        or int(h.seq) != 0
        or int(h.t_wall_ns) != 0
        or int(h.t_mono_ns) != 0
    )


def validate_envelope(
    env: pb.Envelope,
    *,
    expected_schema_version: int,
    allow_missing_header: bool = True,
    allow_missing_topic: bool = False,
    allow_missing_kind: bool = False,
) -> None:
    """Validate an Envelope.

    The daemon uses a "soft" mode for local IPC where topic/kind/header may be
    missing and later injected/normalized. DDS messages are expected to be fully
    populated.
    """

    if int(env.schema_version) != int(expected_schema_version):
        raise ValidationError(
            f"schema_version mismatch: {int(env.schema_version)} != {int(expected_schema_version)}"
        )

    if not env.topic and not allow_missing_topic:
        raise ValidationError("missing topic")
    if not env.kind and not allow_missing_kind:
        raise ValidationError("missing kind")

    payload = env.WhichOneof("payload")
    if payload is None:
        raise ValidationError("missing payload")

    if allow_missing_header:
        # Header may be absent (daemon injects). If any header fields are set, require a complete header.
        if _has_any_header_fields(env.header):
            if not env.header.run_id or not env.header.agent_id:
                raise ValidationError("partial header: run_id/agent_id must both be set")
            if int(env.header.seq) == 0:
                raise ValidationError("header.seq must be set")
            if int(env.header.t_wall_ns) == 0 or int(env.header.t_mono_ns) == 0:
                raise ValidationError("header timestamps must be set")
    else:
        if not env.header.run_id or not env.header.agent_id:
            raise ValidationError("missing header")
        if int(env.header.seq) == 0:
            raise ValidationError("header.seq must be set")
        if int(env.header.t_wall_ns) == 0 or int(env.header.t_mono_ns) == 0:
            raise ValidationError("header timestamps must be set")

    if payload == "event":
        if not env.event.event_type:
            raise ValidationError("missing event.event_type")
    elif payload == "command":
        if not env.command.command_type:
            raise ValidationError("missing command.command_type")
        if not env.command.target_agent_id and not env.command.target_group:
            raise ValidationError("command must specify target_agent_id or target_group")
    elif payload == "team_message":
        if not env.team_message.message_type:
            raise ValidationError("missing team_message.message_type")
    elif payload == "actuation_request":
        if not env.actuation_request.target_agent_id:
            raise ValidationError("missing actuation_request.target_agent_id")