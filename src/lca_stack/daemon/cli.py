from __future__ import annotations

import argparse
import logging
from pathlib import Path

from .bridge import run_bridge

_LOG_LEVELS: dict[str, int] = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
}


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="lca-daemon")
    parser.add_argument("--agent-id", required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--adapter-port", type=int, required=True)
    parser.add_argument("--autonomy-port", type=int, required=True)
    parser.add_argument("--scenario", default=None, help="Scenario name/version (recorded in manifest)")
    parser.add_argument("--seed", type=int, default=None, help="Random seed (recorded in manifest)")
    parser.add_argument("--runs-dir", default="runs")
    parser.add_argument("--log-level", default="INFO", choices=sorted(_LOG_LEVELS.keys()))
    args = parser.parse_args(argv)

    agent_id: str = str(args.agent_id)
    host: str = str(args.host)
    adapter_port: int = int(args.adapter_port)
    autonomy_port: int = int(args.autonomy_port)
    scenario: str | None = str(args.scenario) if args.scenario not in (None, "") else None
    seed: int | None = int(args.seed) if args.seed is not None else None
    runs_dir: Path = Path(str(args.runs_dir))
    log_level_name: str = str(args.log_level)

    logging.basicConfig(
        level=_LOG_LEVELS[log_level_name],
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    run_bridge(
        agent_id=agent_id,
        host=host,
        adapter_port=adapter_port,
        autonomy_port=autonomy_port,
        runs_dir=runs_dir,
        scenario=scenario,
        seed=seed,
    )


if __name__ == "__main__":
    main()
