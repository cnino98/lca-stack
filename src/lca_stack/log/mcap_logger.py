from __future__ import annotations

from pathlib import Path
from typing import BinaryIO, Protocol, cast

from google.protobuf import descriptor_pb2

from lca_stack.proto import lca_stack_pb2 as pb

TOPIC_OBSERVATION = "local/adapter/observation"
TOPIC_ACTUATION_REQUEST = "local/autonomy/actuation_request"
TOPIC_ACTUATION = "local/adapter/actuation"
TOPIC_RUN_EVENT = "run/event"

_DDS_IN_PREFIX = "dds/in/"
_DDS_OUT_PREFIX = "dds/out/"


def topic_status(agent_id: str) -> str:
    return f"agent/{agent_id}/status"


def topic_dds_in(dds_topic: str) -> str:
    return f"{_DDS_IN_PREFIX}{dds_topic}"


def topic_dds_out(dds_topic: str) -> str:
    return f"{_DDS_OUT_PREFIX}{dds_topic}"


_SCHEMA_ROOT_MESSAGE = "lca_stack.Envelope"
# Foxglove uses `Schema.name` as the Protobuf root message type.
_SCHEMA_NAME = _SCHEMA_ROOT_MESSAGE


def _protobuf_file_descriptor_set() -> bytes:
    """Return FileDescriptorSet bytes suitable for MCAP Protobuf schemas."""
    fds = descriptor_pb2.FileDescriptorSet()

    fdp = descriptor_pb2.FileDescriptorProto.FromString(pb.DESCRIPTOR.serialized_pb)
    fds.file.append(fdp)

    # Ensure Struct dependency is included for logs to be self-contained.
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


class McapLogger:
    """MCAP logger for Envelope streams.

    All recorded messages are serialized Protobuf `Envelope` bytes.
    """

    def __init__(self, *, path: Path, writer: _McapWriter, file: BinaryIO, schema_id: int) -> None:
        self._path = path
        self._writer = writer
        self._file = file
        self._schema_id = int(schema_id)
        self._metadata = {"protobuf_root_message": _SCHEMA_ROOT_MESSAGE}

        self._channels: dict[str, int] = {}
        self._seq: dict[str, int] = {}
        self._closed = False

    @property
    def path(self) -> Path:
        return self._path

    @classmethod
    def create(cls, *, runs_dir: str | Path, run_id: str, agent_id: str) -> "McapLogger":
        # mcap does not ship with type hints today; keep the boundary here.
        from mcap.writer import Writer  # type: ignore[import-untyped]
        from mcap.well_known import SchemaEncoding  # type: ignore[import-untyped]

        runs_path = Path(runs_dir)
        logs_dir = runs_path / run_id / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)

        path = logs_dir / f"{agent_id}.mcap"
        file = open(path, "wb")
        try:
            writer_any = Writer(file)
            writer: _McapWriter = cast(_McapWriter, writer_any)
            writer.start()

            schema_id = writer.register_schema(
                name=_SCHEMA_NAME,
                encoding=SchemaEncoding.Protobuf,
                data=_protobuf_file_descriptor_set(),
            )

            self = cls(path=path, writer=writer, file=file, schema_id=schema_id)

            # Register the default channels upfront.
            self._ensure_channel(TOPIC_OBSERVATION)
            self._ensure_channel(TOPIC_ACTUATION_REQUEST)
            self._ensure_channel(TOPIC_ACTUATION)
            self._ensure_channel(TOPIC_RUN_EVENT)
            self._ensure_channel(topic_status(agent_id))

            return self
        except Exception:
            try:
                file.close()
            finally:
                raise

    def _ensure_channel(self, topic: str) -> int:
        from mcap.well_known import MessageEncoding  # type: ignore[import-untyped]

        topic = str(topic)
        cid = self._channels.get(topic)
        if cid is not None:
            return int(cid)

        cid = self._writer.register_channel(
            topic=topic,
            message_encoding=MessageEncoding.Protobuf,
            schema_id=int(self._schema_id),
            metadata=dict(self._metadata),
        )
        self._channels[topic] = int(cid)
        self._seq[topic] = 0
        return int(cid)

    def log(self, topic: str, *, log_time_ns: int, publish_time_ns: int, payload: bytes) -> None:
        topic = str(topic)
        channel_id = self._ensure_channel(topic)
        seq = (int(self._seq.get(topic, 0)) + 1) & 0xFFFFFFFF
        self._seq[topic] = int(seq)
        self._writer.add_message(
            int(channel_id),
            log_time=int(log_time_ns),
            publish_time=int(publish_time_ns),
            data=payload,
            sequence=int(seq),
        )

    # Convenience wrappers for fixed channels.

    def log_observation(self, log_time_ns: int, publish_time_ns: int, payload: bytes) -> None:
        self.log(
            TOPIC_OBSERVATION,
            log_time_ns=int(log_time_ns),
            publish_time_ns=int(publish_time_ns),
            payload=payload,
        )

    def log_actuation_request(self, log_time_ns: int, publish_time_ns: int, payload: bytes) -> None:
        self.log(
            TOPIC_ACTUATION_REQUEST,
            log_time_ns=int(log_time_ns),
            publish_time_ns=int(publish_time_ns),
            payload=payload,
        )

    def log_actuation(self, log_time_ns: int, publish_time_ns: int, payload: bytes) -> None:
        self.log(
            TOPIC_ACTUATION,
            log_time_ns=int(log_time_ns),
            publish_time_ns=int(publish_time_ns),
            payload=payload,
        )

    def log_run_event(self, log_time_ns: int, publish_time_ns: int, payload: bytes) -> None:
        self.log(
            TOPIC_RUN_EVENT,
            log_time_ns=int(log_time_ns),
            publish_time_ns=int(publish_time_ns),
            payload=payload,
        )

    def log_status(self, agent_id: str, log_time_ns: int, publish_time_ns: int, payload: bytes) -> None:
        self.log(
            topic_status(str(agent_id)),
            log_time_ns=int(log_time_ns),
            publish_time_ns=int(publish_time_ns),
            payload=payload,
        )

    # DDS logging.

    def log_dds_rx(self, *, dds_topic: str, log_time_ns: int, publish_time_ns: int, payload: bytes) -> None:
        self.log(
            topic_dds_in(str(dds_topic)),
            log_time_ns=int(log_time_ns),
            publish_time_ns=int(publish_time_ns),
            payload=payload,
        )

    def log_dds_tx(self, *, dds_topic: str, log_time_ns: int, publish_time_ns: int, payload: bytes) -> None:
        self.log(
            topic_dds_out(str(dds_topic)),
            log_time_ns=int(log_time_ns),
            publish_time_ns=int(publish_time_ns),
            payload=payload,
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