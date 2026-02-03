from __future__ import annotations

from dataclasses import dataclass

from lca_stack.proto import lca_stack_pb2 as pb

from .clock import now_mono_ns, now_wall_ns


Header = pb.Header


@dataclass(frozen=True, slots=True)
class HeaderLite:
    run_id: str
    agent_id: str
    t_wall_ns: int


def make_header(run_id: str, agent_id: str, seq: int) -> Header:
    return Header(
        run_id=str(run_id),
        agent_id=str(agent_id),
        seq=int(seq),
        t_mono_ns=int(now_mono_ns()),
        t_wall_ns=int(now_wall_ns()),
    )


def header_is_populated(h: Header) -> bool:
    # seq may legitimately be 0 (daemon lifecycle messages). Treat run_id/agent_id and
    # timestamps as the presence check.
    return bool(h.run_id) and bool(h.agent_id) and int(h.t_wall_ns) != 0 and int(h.t_mono_ns) != 0


def header_lite(h: Header) -> HeaderLite:
    return HeaderLite(run_id=str(h.run_id), agent_id=str(h.agent_id), t_wall_ns=int(h.t_wall_ns))
