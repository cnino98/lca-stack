from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Callable

from cyclonedds.domain import DomainParticipant  # type: ignore[import-not-found]
from cyclonedds.pub import DataWriter  # type: ignore[import-not-found]
from cyclonedds.sub import DataReader  # type: ignore[import-not-found]
from cyclonedds.topic import Topic  # type: ignore[import-not-found]

from lca_stack.proto import lca_stack_pb2 as pb

from .qos import qos_for_topic
from .types import EnvelopeBytes, pack_envelope_bytes, unpack_envelope_bytes


DdsRxCallback = Callable[[str, pb.Envelope], None]


@dataclass
class DdsBusConfig:
    domain_id: int = 0
    # Subscribe to specific agent status topics: agent/<id>/status
    subscribe_status_agents: list[str] = field(default_factory=list)
    # If true, attempt to subscribe to all status topics discovered at runtime.
    subscribe_all_status: bool = False


class DdsBus:
    """A minimal DDS pub/sub wrapper for LCA Stack.

    LCA transports serialized Protobuf `Envelope` bytes via DDS.
    """

    def __init__(self, *, config: DdsBusConfig) -> None:
        self._cfg = config
        self._participant: DomainParticipant | None = None
        self._writers: dict[str, DataWriter[EnvelopeBytes]] = {}
        self._readers: dict[str, DataReader[EnvelopeBytes]] = {}
        self._threads: list[threading.Thread] = []
        self._stop = threading.Event()
        self._status_lock = threading.Lock()

    def start(self, *, on_sample: DdsRxCallback) -> None:
        if self._participant is not None:
            raise RuntimeError("DDS bus already started")

        self._stop.clear()
        self._participant = DomainParticipant(self._cfg.domain_id)

        # Core subscriptions.
        for topic_name in ["team/command", "team/message", "run/event"]:
            self._create_reader(topic_name, on_sample)

        # Optional status subscriptions.
        for agent_id in self._cfg.subscribe_status_agents:
            self._create_reader(f"agent/{agent_id}/status", on_sample)

        if self._cfg.subscribe_all_status:
            th = threading.Thread(
                target=self._status_discovery_loop,
                name="dds-status-discovery",
                args=(on_sample,),
                daemon=True,
            )
            th.start()
            self._threads.append(th)

    def stop(self) -> None:
        self._stop.set()
        for th in self._threads:
            th.join(timeout=1.5)

        # Let DDS entities be GC'd; CycloneDDS cleans up in __del__.
        self._threads.clear()
        self._writers.clear()
        self._readers.clear()
        self._participant = None

    def publish(self, topic_name: str, env: pb.Envelope) -> None:
        if self._participant is None:
            raise RuntimeError("DDS bus not started")

        writer = self._writers.get(topic_name)
        if writer is None:
            writer = self._create_writer(topic_name)
            self._writers[topic_name] = writer

        payload = env.SerializeToString()
        writer.write(pack_envelope_bytes(payload))

    def _create_writer(self, topic_name: str) -> DataWriter[EnvelopeBytes]:
        assert self._participant is not None
        topic = Topic(self._participant, topic_name, EnvelopeBytes)
        qos = qos_for_topic(topic_name).writer
        return DataWriter(self._participant, topic, qos=qos)

    def _create_reader(self, topic_name: str, on_sample: DdsRxCallback) -> None:
        if self._participant is None:
            raise RuntimeError("DDS bus not started")
        if topic_name in self._readers:
            return

        topic = Topic(self._participant, topic_name, EnvelopeBytes)
        qos = qos_for_topic(topic_name).reader
        reader: DataReader[EnvelopeBytes] = DataReader(self._participant, topic, qos=qos)
        self._readers[topic_name] = reader

        th = threading.Thread(
            target=self._reader_loop,
            name=f"dds-rx-{topic_name}",
            args=(topic_name, reader, on_sample),
            daemon=True,
        )
        th.start()
        self._threads.append(th)

    def _reader_loop(
        self,
        topic_name: str,
        reader: DataReader[EnvelopeBytes],
        on_sample: DdsRxCallback,
    ) -> None:
        # `take_iter` stops once timeout expires; each received sample resets it.
        while not self._stop.is_set():
            try:
                for sample in reader.take_iter(timeout=1):  # type: ignore[attr-defined]
                    try:
                        env = pb.Envelope()
                        env.ParseFromString(unpack_envelope_bytes(sample))
                    except Exception:
                        # Drop malformed payloads.
                        continue
                    on_sample(topic_name, env)
            except Exception:
                # If DDS errors, back off a bit.
                time.sleep(0.2)

    def _status_discovery_loop(self, on_sample: DdsRxCallback) -> None:
        """Subscribe to agent/<id>/status topics as they are discovered.

        DDS does not support wildcards; this loop polls discovered topic names.
        """
        assert self._participant is not None

        # Import builtins lazily; it is only needed if dynamic discovery is used.
        from cyclonedds.builtin import BuiltinTopicDcpsTopic  # type: ignore[import-not-found]
        from cyclonedds.sub import DataReader as BuiltinReader  # type: ignore[import-not-found]

        builtin_topic = Topic(self._participant, "DCPSTopic", BuiltinTopicDcpsTopic)
        builtin_reader: BuiltinReader[BuiltinTopicDcpsTopic] = BuiltinReader(
            self._participant, builtin_topic
        )

        known: set[str] = set(self._readers.keys())
        while not self._stop.is_set():
            try:
                for t in builtin_reader.take_iter(timeout=1):  # type: ignore[attr-defined]
                    try:
                        name = str(t.name)
                    except Exception:
                        continue
                    if not (name.startswith("agent/") and name.endswith("/status")):
                        continue
                    with self._status_lock:
                        if name in known:
                            continue
                        known.add(name)
                    self._create_reader(name, on_sample)
            except Exception:
                time.sleep(0.5)


def probe_run_start(
    *, domain_id: int, timeout_s: float = 2.0
) -> str | None:
    """Listen for a `run_start` event and return its run_id, if observed."""
    participant = DomainParticipant(domain_id)
    topic = Topic(participant, "run/event", EnvelopeBytes)
    qos = qos_for_topic("run/event").reader
    reader: DataReader[EnvelopeBytes] = DataReader(participant, topic, qos=qos)

    deadline = time.time() + timeout_s
    while time.time() < deadline:
        remaining = max(0.0, deadline - time.time())
        try:
            for sample in reader.take_iter(timeout=remaining):  # type: ignore[attr-defined]
                env = pb.Envelope()
                try:
                    env.ParseFromString(unpack_envelope_bytes(sample))
                except Exception:
                    continue
                if env.HasField("event") and str(env.event.event_type) == "run_start":
                    if env.header and str(env.header.run_id):
                        return str(env.header.run_id)
        except Exception:
            time.sleep(0.05)
    return None
