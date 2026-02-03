from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, Protocol, Self, cast

from google.protobuf import descriptor_pb2

from lca_stack.proto import lca_stack_pb2 as pb

TOPIC_OBSERVATION = "local/adapter/observation"
TOPIC_ACTUATION_REQUEST = "local/autonomy/actuation_request"
TOPIC_ACTUATION = "local/adapter/actuation"
TOPIC_RUN_EVENT = "run/event"


def topic_status(agent_id: str) -> str:
    return f"agent/{agent_id}/status"


_SCHEMA_ROOT_MESSAGE = "lca_stack.Envelope"
# Foxglove uses `Schema.name` as the Protobuf root message type.
_SCHEMA_NAME = _SCHEMA_ROOT_MESSAGE


def _protobuf_file_descriptor_set() -> bytes:
    """Return a FileDescriptorSet suitable for MCAP Protobuf schemas.

    MCAP expects Protobuf schemas as serialized FileDescriptorSet bytes.
    """
    fds = descriptor_pb2.FileDescriptorSet()

    # Our schema.
    fdp = descriptor_pb2.FileDescriptorProto.FromString(pb.DESCRIPTOR.serialized_pb)
    fds.file.append(fdp)

    # Include Struct dependency to make logs self-contained for simple tooling.
    import google.protobuf.struct_pb2 as struct_pb2

    struct_fdp = descriptor_pb2.FileDescriptorProto.FromString(struct_pb2.DESCRIPTOR.serialized_pb)
    fds.file.append(struct_fdp)

    return fds.SerializeToString()


class _McapWriter(Protocol):
    def start(self) -> None: ...
    def finish(self) -> None: ...

    def register_schema(self, *, name: str, encoding: object, data: bytes) -> int: ...

    def register_channel(
        self,
        *,
        topic: str,
        message_encoding: object,
        schema_id: int,
        metadata: dict[str, str],
    ) -> int: ...

    def add_message(
        self,
        channel_id: int,
        *,
        log_time: int,
        publish_time: int,
        data: bytes,
        sequence: int,
    ) -> None: ...


@dataclass(frozen=True, slots=True)
class _ChannelIds:
    observation: int
    actuation_request: int
    actuation: int
    run_event: int
    status: int


class McapLogger:
    _path: Path
    _writer: _McapWriter
    _file: BinaryIO
    _channels: _ChannelIds
    _observation_sequence: int
    _actuation_sequence: int
    _actuation_final_sequence: int
    _event_sequence: int
    _status_sequence: int
    _closed: bool

    def __init__(self, *, path: Path, writer: _McapWriter, file: BinaryIO, channels: _ChannelIds) -> None:
        self._path = path
        self._writer = writer
        self._file = file
        self._channels = channels
        self._observation_sequence = 0
        self._actuation_sequence = 0
        self._actuation_final_sequence = 0
        self._event_sequence = 0
        self._status_sequence = 0
        self._closed = False

    @property
    def path(self) -> Path:
        return self._path

    @classmethod
    def create(cls, *, runs_dir: str | Path, run_id: str, agent_id: str) -> Self:
        # mcap does not ship with type hints today; keep the boundary here and cast to a local Protocol.
        from mcap.writer import Writer  # type: ignore[import-untyped]
        from mcap.well_known import MessageEncoding, SchemaEncoding  # type: ignore[import-untyped]

        runs_path = Path(runs_dir)
        logs_dir = runs_path / run_id / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)

        path = logs_dir / f"{agent_id}.mcap"
        file = open(path, "wb")

        writer_any = Writer(file)
        writer: _McapWriter = cast(_McapWriter, writer_any)
        writer.start()

        schema_id = writer.register_schema(
            name=_SCHEMA_NAME,
            encoding=SchemaEncoding.Protobuf,
            data=_protobuf_file_descriptor_set(),
        )

        metadata = {"protobuf_root_message": _SCHEMA_ROOT_MESSAGE}

        observation_channel_id = writer.register_channel(
            topic=TOPIC_OBSERVATION,
            message_encoding=MessageEncoding.Protobuf,
            schema_id=schema_id,
            metadata=metadata,
        )

        actuation_channel_id = writer.register_channel(
            topic=TOPIC_ACTUATION_REQUEST,
            message_encoding=MessageEncoding.Protobuf,
            schema_id=schema_id,
            metadata=metadata,
        )

        actuation_final_channel_id = writer.register_channel(
            topic=TOPIC_ACTUATION,
            message_encoding=MessageEncoding.Protobuf,
            schema_id=schema_id,
            metadata=metadata,
        )

        run_event_channel_id = writer.register_channel(
            topic=TOPIC_RUN_EVENT,
            message_encoding=MessageEncoding.Protobuf,
            schema_id=schema_id,
            metadata=metadata,
        )

        status_channel_id = writer.register_channel(
            topic=topic_status(agent_id),
            message_encoding=MessageEncoding.Protobuf,
            schema_id=schema_id,
            metadata=metadata,
        )

        channels = _ChannelIds(
            observation=observation_channel_id,
            actuation_request=actuation_channel_id,
            actuation=actuation_final_channel_id,
            run_event=run_event_channel_id,
            status=status_channel_id,
        )
        return cls(path=path, writer=writer, file=file, channels=channels)

    def log_observation(self, log_time_ns: int, publish_time_ns: int, payload: bytes) -> None:
        self._observation_sequence = (self._observation_sequence + 1) & 0xFFFFFFFF
        self._writer.add_message(
            self._channels.observation,
            log_time=int(log_time_ns),
            publish_time=int(publish_time_ns),
            data=payload,
            sequence=int(self._observation_sequence),
        )

    def log_actuation_request(self, log_time_ns: int, publish_time_ns: int, payload: bytes) -> None:
        self._actuation_sequence = (self._actuation_sequence + 1) & 0xFFFFFFFF
        self._writer.add_message(
            self._channels.actuation_request,
            log_time=int(log_time_ns),
            publish_time=int(publish_time_ns),
            data=payload,
            sequence=int(self._actuation_sequence),
        )

    def log_actuation(self, log_time_ns: int, publish_time_ns: int, payload: bytes) -> None:
        self._actuation_final_sequence = (self._actuation_final_sequence + 1) & 0xFFFFFFFF
        self._writer.add_message(
            self._channels.actuation,
            log_time=int(log_time_ns),
            publish_time=int(publish_time_ns),
            data=payload,
            sequence=int(self._actuation_final_sequence),
        )

    def log_run_event(self, log_time_ns: int, publish_time_ns: int, payload: bytes) -> None:
        self._event_sequence = (self._event_sequence + 1) & 0xFFFFFFFF
        self._writer.add_message(
            self._channels.run_event,
            log_time=int(log_time_ns),
            publish_time=int(publish_time_ns),
            data=payload,
            sequence=int(self._event_sequence),
        )

    def log_status(self, log_time_ns: int, publish_time_ns: int, payload: bytes) -> None:
        self._status_sequence = (self._status_sequence + 1) & 0xFFFFFFFF
        self._writer.add_message(
            self._channels.status,
            log_time=int(log_time_ns),
            publish_time=int(publish_time_ns),
            data=payload,
            sequence=int(self._status_sequence),
        )

    def close(self) -> None:
        if self._closed:
            return
        try:
            self._writer.finish()
        finally:
            try:
                self._file.close()
            finally:
                self._closed = True

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass
