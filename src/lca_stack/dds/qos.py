from __future__ import annotations

from dataclasses import dataclass

from cyclonedds.core import Policy, Qos  # type: ignore[import-not-found]
from cyclonedds.util import duration  # type: ignore[import-not-found]


@dataclass(frozen=True)
class TopicQos:
    writer: Qos
    reader: Qos


def _writer_qos_reliable(*, keep_last: int, max_block_s: float, transient_local: bool = False) -> Qos:
    policies: list[object] = [
        Policy.Reliability.Reliable(max_blocking_time=duration(seconds=max_block_s)),
        Policy.History.KeepLast(depth=int(keep_last)),
    ]
    if transient_local:
        # TransientLocal allows late-joiners to receive recent samples (e.g., run_start).
        dur = getattr(getattr(Policy, "Durability", object()), "TransientLocal", None)
        if dur is not None:
            policies.append(dur)
    return Qos(*policies)


def _writer_qos_best_effort(*, keep_last: int) -> Qos:
    return Qos(
        Policy.Reliability.BestEffort,
        Policy.History.KeepLast(depth=int(keep_last)),
    )


def _reader_qos_from_writer(writer_qos: Qos) -> Qos:
    """Derive a reader QoS from the writer QoS.

    Some CycloneDDS versions expose IgnoreLocal policies; others don't.
    Treat IgnoreLocal as an optional optimization, not a hard requirement.
    """
    ignore_local = getattr(getattr(Policy, "IgnoreLocal", None), "Participant", None)
    if ignore_local is None:
        return Qos(base=writer_qos)
    return Qos(ignore_local, base=writer_qos)


RELIABLE_TOPICS: set[str] = {
    "team/command",
    "team/message",
    "run/event",
}

TRANSIENT_TOPICS: set[str] = {
    # Used for run identity coordination.
    "run/event",
}


def qos_for_topic(
    topic_name: str,
    *,
    keep_last: int = 64,
    reliable_max_block_s: float = 1.0,
) -> TopicQos:
    """Return default writer/reader QoS for an LCA DDS topic."""
    if topic_name in RELIABLE_TOPICS:
        writer = _writer_qos_reliable(
            keep_last=keep_last,
            max_block_s=reliable_max_block_s,
            transient_local=topic_name in TRANSIENT_TOPICS,
        )
    else:
        # Status is best-effort by default; other non-critical topics follow.
        writer = _writer_qos_best_effort(keep_last=keep_last)

    reader = _reader_qos_from_writer(writer)
    return TopicQos(writer=writer, reader=reader)