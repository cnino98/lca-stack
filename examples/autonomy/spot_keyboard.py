from __future__ import annotations

import argparse
import curses
import socket
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path

try:
    from lca_stack.ipc import FramedClient, client_handshake, make_actuation_request_envelope
    from lca_stack.ipc.clock import now_wall_ns
    from lca_stack.proto import lca_stack_pb2 as pb
except ModuleNotFoundError:
    REPO_ROOT = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(REPO_ROOT / "src"))
    from lca_stack.ipc import FramedClient, client_handshake, make_actuation_request_envelope
    from lca_stack.ipc.clock import now_wall_ns
    from lca_stack.proto import lca_stack_pb2 as pb


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


class _RxDrain(threading.Thread):
    """Read and discard daemon->autonomy traffic to avoid socket backpressure."""

    def __init__(self, link: object) -> None:
        super().__init__(name="daemon-rx", daemon=True)
        self._link = link
        self._stop = threading.Event()

    def stop(self) -> None:
        self._stop.set()

    def run(self) -> None:
        # Link is a FramedSocket (no explicit typing here to keep example lightweight).
        try:
            getattr(self._link, "set_timeout")(0.25)
        except Exception:
            pass

        while not self._stop.is_set():
            env = pb.Envelope()
            try:
                getattr(self._link, "read_message")(env)
            except socket.timeout:
                continue
            except EOFError:
                return
            except Exception:
                return


def _curses_main(stdscr: "curses._CursesWindow", *, host: str, port: int, requested_agent_id: str | None) -> None:
    stdscr.nodelay(True)
    stdscr.keypad(True)

    client = FramedClient(host=str(host), port=int(port))
    link = client.connect()

    hs = client_handshake(link=link, role="autonomy")
    run_id = hs.run_id
    agent_id = hs.agent_id

    if requested_agent_id is not None and requested_agent_id != agent_id:
        stdscr.addstr(0, 0, f"WARNING: requested agent_id={requested_agent_id} but daemon reports agent_id={agent_id}\n")

    rx = _RxDrain(link)
    rx.start()

    teleop = KeyboardTeleop(TeleopLimits())
    seq = 0

    last_send_s = 0.0

    try:
        while True:
            now_s = time.monotonic()
            key = stdscr.getch()
            if key in (ord("x"), ord("X")):
                break

            teleop.on_key(key, now_s)
            vx, vy, wz = teleop.poll(now_s)

            # ~30 Hz send.
            if now_s - last_send_s >= (1.0 / 30.0):
                last_send_s = now_s
                seq += 1
                env = make_actuation_request_envelope(
                    run_id=run_id,
                    agent_id=agent_id,
                    seq=seq,
                    kind="spot_cmd_vel_v1",
                    data={
                        "vx_mps": float(vx),
                        "vy_mps": float(vy),
                        "wz_rps": float(wz),
                        "source": "keyboard",
                    },
                    target_agent_id=agent_id,
                    expires_wall_ns=int(now_wall_ns() + 400_000_000),
                )
                link.write_message(env)

            stdscr.erase()
            stdscr.addstr(0, 0, "Spot keyboard teleop (Protobuf IPC)\n")
            stdscr.addstr(1, 0, f"run_id:  {run_id}\n")
            stdscr.addstr(2, 0, f"agent_id: {agent_id}\n")
            stdscr.addstr(4, 0, "Controls: arrows or WASD, Q/E yaw, X to exit\n")
            stdscr.addstr(6, 0, f"vx: {vx:+.2f} m/s   vy: {vy:+.2f} m/s   wz: {wz:+.2f} rad/s\n")
            stdscr.refresh()

            time.sleep(0.01)
    finally:
        # Try to send a final stop.
        try:
            seq += 1
            env = make_actuation_request_envelope(
                run_id=run_id,
                agent_id=agent_id,
                seq=seq,
                kind="spot_cmd_vel_v1",
                data={"vx_mps": 0.0, "vy_mps": 0.0, "wz_rps": 0.0, "source": "keyboard"},
                target_agent_id=agent_id,
                expires_wall_ns=int(now_wall_ns() + 400_000_000),
            )
            link.write_message(env)
        except Exception:
            pass

        rx.stop()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--agent-id", default=None, help="Optional cross-check against daemon-reported agent_id")
    args = parser.parse_args(argv)

    curses.wrapper(_curses_main, host=str(args.host), port=int(args.port), requested_agent_id=str(args.agent_id) if args.agent_id else None)


if __name__ == "__main__":
    main()
