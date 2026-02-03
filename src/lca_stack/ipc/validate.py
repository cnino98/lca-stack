from __future__ import annotations

from dataclasses import dataclass

from lca_stack.proto import lca_stack_pb2 as pb


@dataclass(frozen=True, slots=True)
class ValidationError(Exception):
    message: str

    def __str__(self) -> str:
        return str(self.message)


def validate_envelope(env: pb.Envelope, *, expected_schema_version: int) -> None:
    if int(env.schema_version) != int(expected_schema_version):
        raise ValidationError(f"schema_version mismatch: {env.schema_version} != {expected_schema_version}")

    if not env.topic:
        raise ValidationError("missing topic")
    if not env.kind:
        raise ValidationError("missing kind")

    # Exactly one payload should be set.
    if env.WhichOneof("payload") is None:
        raise ValidationError("missing payload")

    # Header may be missing on inbound client messages; the daemon can inject.
    # If present, require timestamps (helps keep logs useful).
    if env.header.run_id or env.header.agent_id:
        if not env.header.run_id or not env.header.agent_id:
            raise ValidationError("partial header: run_id/agent_id must both be set")
        if int(env.header.t_wall_ns) == 0 or int(env.header.t_mono_ns) == 0:
            raise ValidationError("header timestamps must be set")

    # Type-specific minimal checks.
    payload = env.WhichOneof("payload")
    if payload == "actuation_request":
        # data should exist, but Struct may be empty.
        pass
    elif payload == "observation":
        pass
    elif payload == "event":
        if not env.event.event_type:
            raise ValidationError("missing event.event_type")
    elif payload == "status":
        # heartbeat_seq is optional; mode may be unset.
        pass
