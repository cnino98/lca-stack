from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, Protocol, Self, cast

TOPIC_OBSERVATION = "local/adapter/observation"
TOPIC_ACTUATION_REQUEST = "local/autonomy/actuation_request"

_SCHEMA_NAME = "lca/json_object"
_SCHEMA_JSON = b'{"type":"object"}'


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


class McapLogger:
    _path: Path
    _writer: _McapWriter
    _file: BinaryIO
    _channels: _ChannelIds
    _observation_sequence: int
    _actuation_sequence: int
    _closed: bool

    def __init__(self, *, path: Path, writer: _McapWriter, file: BinaryIO, channels: _ChannelIds) -> None:
        self._path = path
        self._writer = writer
        self._file = file
        self._channels = channels
        self._observation_sequence = 0
        self._actuation_sequence = 0
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
            encoding=SchemaEncoding.JSONSchema,
            data=_SCHEMA_JSON,
        )

        observation_channel_id = writer.register_channel(
            topic=TOPIC_OBSERVATION,
            message_encoding=MessageEncoding.JSON,
            schema_id=schema_id,
            metadata={},
        )

        actuation_channel_id = writer.register_channel(
            topic=TOPIC_ACTUATION_REQUEST,
            message_encoding=MessageEncoding.JSON,
            schema_id=schema_id,
            metadata={},
        )

        channels = _ChannelIds(observation=observation_channel_id, actuation_request=actuation_channel_id)
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
