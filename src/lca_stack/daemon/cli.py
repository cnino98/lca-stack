from __future__ import annotations

import argparse
import logging
import os
import uuid
from pathlib import Path

from .bridge import run_bridge

_LOG_LEVELS: dict[str, int] = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
}


def _resolve_run_id_from_file(path: Path) -> str:
    """Read or create a shared run_id.

    This is a convenience for local multi-process runs (multiple daemons on one
    machine) where you want all agents to share the same run_id without relying
    on DDS discovery/join.
    """
    path = Path(path)
    if path.exists():
        run_id = path.read_text(encoding="utf-8").strip()
        if not run_id:
            raise ValueError(f"run-id-file is empty: {path}")
        uuid.UUID(run_id)  # validate format
        return run_id

    run_id = str(uuid.uuid4())
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(run_id + "\n", encoding="utf-8")
    os.replace(tmp, path)
    return run_id


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="lca-daemon")
    parser.add_argument("--agent-id", required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--adapter-port", type=int, required=True)
    parser.add_argument("--autonomy-port", type=int, required=True)

    parser.add_argument(
        "--run-id",
        default=None,
        help="Explicit run UUID to use (otherwise generated).",
    )
    parser.add_argument(
        "--run-id-file",
        default=None,
        help=(
            "Path to a text file containing a run UUID. If the file does not exist, "
            "a new run UUID is generated and written. Useful for starting multiple daemons "
            "with the same run_id without relying on DDS join."
        ),
    )

    parser.add_argument(
        "--no-dds",
        action="store_true",
        help="Disable DDS team bus (local loop + logging only).",
    )
    parser.add_argument(
        "--enable-dds",
        action="store_true",
        help="Explicitly enable DDS team bus (default unless --no-dds).",
    )
    parser.add_argument(
        "--dds-domain-id",
        type=int,
        default=0,
        help="DDS domain id (CycloneDDS).",
    )

    parser.add_argument(
        "--subscribe-status-agent",
        action="append",
        default=[],
        help="Subscribe to agent/<id>/status for this agent id (repeatable).",
    )
    parser.add_argument(
        "--subscribe-all-status",
        action="store_true",
        help="Dynamically subscribe to all discovered agent/<id>/status topics.",
    )

    parser.add_argument(
        "--join-run",
        action="store_true",
        help="Attempt to join an existing run_id by listening for run_start on DDS.",
    )
    parser.add_argument(
        "--join-timeout",
        type=float,
        default=2.0,
        help="How long to wait (seconds) for run_start when --join-run is set.",
    )

    parser.add_argument("--scenario", default=None, help="Scenario name/version (recorded in manifest)")
    parser.add_argument("--seed", type=int, default=None, help="Random seed (recorded in manifest)")
    parser.add_argument("--runs-dir", default="runs")
    parser.add_argument("--log-level", default="INFO", choices=sorted(_LOG_LEVELS.keys()))
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=_LOG_LEVELS[str(args.log_level)],
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    run_id: str | None = str(args.run_id) if args.run_id not in (None, "") else None
    run_id_file: Path | None = Path(str(args.run_id_file)) if args.run_id_file not in (None, "") else None
    if run_id is None and run_id_file is not None:
        run_id = _resolve_run_id_from_file(run_id_file)
        logging.getLogger(__name__).info("using run-id-file=%s run_id=%s", run_id_file, run_id)

    enable_dds: bool = bool(args.enable_dds) or (not bool(args.no_dds))

    run_bridge(
        agent_id=str(args.agent_id),
        host=str(args.host),
        adapter_port=int(args.adapter_port),
        autonomy_port=int(args.autonomy_port),
        runs_dir=Path(str(args.runs_dir)),
        scenario=str(args.scenario) if args.scenario not in (None, "") else None,
        seed=int(args.seed) if args.seed is not None else None,
        run_id=run_id,
        enable_dds=enable_dds,
        dds_domain_id=int(args.dds_domain_id),
        subscribe_status_agents=[str(x) for x in (args.subscribe_status_agent or [])],
        subscribe_all_status=bool(args.subscribe_all_status),
        join_run=bool(args.join_run),
        join_timeout_s=float(args.join_timeout),
    )


if __name__ == "__main__":
    main()