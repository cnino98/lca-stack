from __future__ import annotations

import argparse
import curses
import time
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    from lca_stack.ipc import JsonlClient, make_header
except ModuleNotFoundError:
    REPO_ROOT = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(REPO_ROOT / "src"))
    from lca_stack.ipc import JsonlClient, make_header


@dataclass(frozen=True)
class TeleopLimits:
    max_vx: float = 0.45
    max_vy: float = 0.25
    max_wz: float = 0.90
    key_hold_timeout_s: float = 0.22


class KeyboardTeleop:
    """Curses-based replica of the Webots teleop mapping (velocity-level)."""

    def __init__(self, limits: TeleopLimits) -> None:
        self._limits = limits
        self._active_until: dict[str, float] = {
            "forward": 0.0,
            "back": 0.0,
            "left": 0.0,
            "right": 0.0,
            "yaw_left": 0.0,
            "yaw_right": 0.0,
        }

    def on_key(self, key: int, now_s: float) -> None:
        timeout = self._limits.key_hold_timeout_s

        if key == curses.KEY_UP:
            self._active_until["forward"] = now_s + timeout
        elif key == curses.KEY_DOWN:
            self._active_until["back"] = now_s + timeout
        elif key == curses.KEY_LEFT:
            self._active_until["yaw_left"] = now_s + timeout
        elif key == curses.KEY_RIGHT:
            self._active_until["yaw_right"] = now_s + timeout
        else:
            self._refresh_char(key, now_s, timeout)

    def poll(self, now_s: float) -> tuple[float, float, float]:
        vx_unit = (1.0 if now_s < self._active_until["forward"] else 0.0) - (1.0 if now_s < self._active_until["back"] else 0.0)
        vy_unit = (1.0 if now_s < self._active_until["left"] else 0.0) - (1.0 if now_s < self._active_until["right"] else 0.0)
        wz_unit = (1.0 if now_s < self._active_until["yaw_left"] else 0.0) - (1.0 if now_s < self._active_until["yaw_right"] else 0.0)

        return (
            float(vx_unit) * self._limits.max_vx,
            float(vy_unit) * self._limits.max_vy,
            float(wz_unit) * self._limits.max_wz,
        )

    def _refresh_char(self, key: int, now_s: float, timeout: float) -> None:
        if key == -1:
            return

        if key in (ord("W"), ord("w")):
            self._active_until["forward"] = now_s + timeout
        elif key in (ord("S"), ord("s")):
            self._active_until["back"] = now_s + timeout
        elif key in (ord("A"), ord("a")):
            self._active_until["left"] = now_s + timeout
        elif key in (ord("D"), ord("d")):
            self._active_until["right"] = now_s + timeout
        elif key in (ord("Q"), ord("q"), ord("J"), ord("j")):
            self._active_until["yaw_left"] = now_s + timeout
        elif key in (ord("E"), ord("e"), ord("L"), ord("l")):
            self._active_until["yaw_right"] = now_s + timeout
        elif key == ord(" "):
            for k in self._active_until:
                self._active_until[k] = now_s


HELP = [
    "Controls (matches Webots mapping):",
    "  ↑ / W : forward",
    "  ↓ / S : back",
    "  A     : left strafe",
    "  D     : right strafe",
    "  ← / Q / J : yaw left",
    "  → / E / L : yaw right",
    "  space : stop",
    "  q     : quit",
]


def _extract_run_id(msg: Any) -> str | None:
    if not isinstance(msg, dict):
        return None
    header = msg.get("header")
    if not isinstance(header, dict):
        return None
    run_id = header.get("run_id")
    if isinstance(run_id, str) and run_id:
        return run_id
    return None


def _read_one_message(client: JsonlClient, timeout_s: float) -> Any | None:
    client.set_timeout(timeout_s)
    try:
        return client.read_json()
    finally:
        client.set_timeout(None)


def _teleop_loop(stdscr: Any, *, host: str, port: int, agent_id: str) -> None:
    stdscr.nodelay(True)
    curses.curs_set(0)

    teleop = KeyboardTeleop(TeleopLimits())
    run_id: str | None = None
    seq = 0

    client: JsonlClient | None = None
    last_connect_attempt_s = 0.0
    reconnect_period_s = 0.5

    last_send_s = time.monotonic()
    send_period_s = 0.02  # 50 Hz

    while True:
        now_s = time.monotonic()
        ch = stdscr.getch()
        if ch in (ord("q"), ord("Q")):
            break

        if ch != -1:
            teleop.on_key(ch, now_s)

        # Ensure we have a live client connection.
        if client is None and (now_s - last_connect_attempt_s) >= reconnect_period_s:
            last_connect_attempt_s = now_s
            try:
                client = JsonlClient(host, port)
            except Exception:
                client = None

        # If we’re connected but still don't know run_id, keep reading until we do.
        if client is not None and run_id is None:
            try:
                msg = _read_one_message(client, timeout_s=0.2)
                if msg is None:
                    # EOF: the daemon closed the connection. Treat as disconnected.
                    client.close()
                    client = None
                else:
                    maybe_run_id = _extract_run_id(msg)
                    if maybe_run_id is not None:
                        run_id = maybe_run_id
            except Exception:
                # Timeout or transient read issue: keep waiting.
                pass

        # Once run_id exists, send commands periodically.
        if client is not None and run_id is not None and (now_s - last_send_s) >= send_period_s:
            vx, vy, wz = teleop.poll(now_s)
            seq += 1
            cmd_msg = {
                "header": make_header(run_id, agent_id, seq),
                "kind": "spot_cmd_vel_v1",
                "vx_mps": vx,
                "vy_mps": vy,
                "wz_rps": wz,
            }
            try:
                client.write_json(cmd_msg)
            except Exception:
                # If the daemon went away, drop back to reconnect + run_id wait.
                try:
                    client.close()
                finally:
                    client = None
                    run_id = None
                    seq = 0
            last_send_s = now_s

        # UI
        stdscr.erase()
        stdscr.addstr(0, 0, "LCA Stack Spot Teleop (Autonomy)")
        stdscr.addstr(2, 0, f"agent_id: {agent_id}")

        if client is None:
            stdscr.addstr(4, 0, f"Daemon: DISCONNECTED (retrying {host}:{port})")
            stdscr.addstr(6, 0, "Waiting for daemon connection... (press q to quit)")
        elif run_id is None:
            stdscr.addstr(4, 0, f"Daemon: CONNECTED ({host}:{port})")
            stdscr.addstr(6, 0, "Waiting for first observation to learn run_id... (press q to quit)")
        else:
            vx, vy, wz = teleop.poll(now_s)
            stdscr.addstr(1, 0, f"run_id:  {run_id}")
            stdscr.addstr(4, 0, f"desired vx={vx:+.2f} m/s  vy={vy:+.2f} m/s  wz={wz:+.2f} rad/s")
            stdscr.addstr(6, 0, "Help:")
            for i, line in enumerate(HELP):
                stdscr.addstr(7 + i, 2, line)

        stdscr.refresh()
        time.sleep(0.005)

    if client is not None:
        client.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True, help="Daemon autonomy port")
    parser.add_argument("--agent-id", default="cf1")
    args = parser.parse_args()

    curses.wrapper(_teleop_loop, host=str(args.host), port=int(args.port), agent_id=str(args.agent_id))


if __name__ == "__main__":
    main()
