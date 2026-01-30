from __future__ import annotations

import logging
import socket
import threading
from pathlib import Path

from lca_stack.ipc.clock import MonotonicWallClock
from lca_stack.ipc.header import Header, extract_header
from lca_stack.ipc.jsonl import JsonlSocket
from lca_stack.log.mcap_logger import McapLogger

from .server import Connections, accept_two

log = logging.getLogger("lca_stack.daemon")


def _safe_shutdown(sock: socket.socket) -> None:
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except Exception:
        pass
    try:
        sock.close()
    except Exception:
        pass


class _RunLoggerManager:
    """Owns the per-agent MCAP logger for the current run.

    The current stack creates a single MCAP file per daemon process (agent). If messages
    later carry a different run_id, we keep logging to the original file and warn.
    """

    _lock: threading.Lock
    _logger: McapLogger | None
    _run_id: str | None

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._logger = None
        self._run_id = None

    def ensure(self, *, runs_dir: Path, run_id: str, agent_id: str) -> McapLogger:
        with self._lock:
            if self._logger is None:
                self._logger = McapLogger.create(runs_dir=runs_dir, run_id=run_id, agent_id=agent_id)
                self._run_id = run_id
                log.info("logging to %s", self._logger.path)
                return self._logger

            if self._run_id is not None and self._run_id != run_id:
                log.warning("received run_id=%s after logger initialized for run_id=%s", run_id, self._run_id)

            return self._logger

    def get(self) -> McapLogger | None:
        with self._lock:
            return self._logger

    def close(self) -> None:
        with self._lock:
            if self._logger is not None:
                self._logger.close()
                self._logger = None
                self._run_id = None


class _AgentBridge:
    """Bidirectional JSONL forwarder between Adapter and Autonomy, with MCAP logging."""

    _agent_id: str
    _runs_dir: Path
    _connections: Connections

    _clock: MonotonicWallClock
    _logger_manager: _RunLoggerManager

    _adapter_link: JsonlSocket
    _autonomy_link: JsonlSocket

    _stop: threading.Event
    _write_lock_adapter: threading.Lock
    _write_lock_autonomy: threading.Lock

    def __init__(self, *, agent_id: str, runs_dir: Path, connections: Connections) -> None:
        self._agent_id = agent_id
        self._runs_dir = runs_dir
        self._connections = connections

        self._clock = MonotonicWallClock()
        self._logger_manager = _RunLoggerManager()

        self._adapter_link = JsonlSocket(connections.adapter)
        self._autonomy_link = JsonlSocket(connections.autonomy)

        self._stop = threading.Event()
        self._write_lock_adapter = threading.Lock()
        self._write_lock_autonomy = threading.Lock()

    def run(self) -> None:
        adapter_thread = threading.Thread(
            target=self._forward_adapter_to_autonomy,
            daemon=True,
            name="adapter->autonomy",
        )
        autonomy_thread = threading.Thread(
            target=self._forward_autonomy_to_adapter,
            daemon=True,
            name="autonomy->adapter",
        )

        adapter_thread.start()
        autonomy_thread.start()

        try:
            adapter_thread.join()
            autonomy_thread.join()
        finally:
            self.close()

    def close(self) -> None:
        self._stop.set()
        self._logger_manager.close()
        self._adapter_link.close()
        self._autonomy_link.close()

    def _forward_adapter_to_autonomy(self) -> None:
        try:
            while not self._stop.is_set():
                line = self._adapter_link.read_line()
                if line is None:
                    log.info("adapter disconnected; shutting down")
                    _safe_shutdown(self._connections.autonomy)
                    self._stop.set()
                    return

                header = self._try_parse_header(line, source_label="observation")
                if header is not None and header.agent_id != self._agent_id:
                    log.warning(
                        "observation header.agent_id=%s != daemon agent_id=%s",
                        header.agent_id,
                        self._agent_id,
                    )

                self._log_observation(line=line, header=header)
                with self._write_lock_autonomy:
                    self._autonomy_link.write_line(line)

        except (ConnectionResetError, BrokenPipeError):
            log.info("adapter connection reset; shutting down")
            self._stop.set()
            _safe_shutdown(self._connections.autonomy)
        except Exception:
            log.exception("adapter->autonomy loop crashed; shutting down")
            self._stop.set()
            _safe_shutdown(self._connections.autonomy)

    def _forward_autonomy_to_adapter(self) -> None:
        try:
            while not self._stop.is_set():
                line = self._autonomy_link.read_line()
                if line is None:
                    log.info("autonomy disconnected; shutting down")
                    _safe_shutdown(self._connections.adapter)
                    self._stop.set()
                    return

                header = self._try_parse_header(line, source_label="actuation_request")
                if header is not None and header.agent_id != self._agent_id:
                    log.warning(
                        "actuation header.agent_id=%s != daemon agent_id=%s",
                        header.agent_id,
                        self._agent_id,
                    )

                self._log_actuation_request(line=line, header=header)
                with self._write_lock_adapter:
                    self._adapter_link.write_line(line)

        except (ConnectionResetError, BrokenPipeError):
            log.info("autonomy connection reset; shutting down")
            self._stop.set()
            _safe_shutdown(self._connections.adapter)
        except Exception:
            log.exception("autonomy->adapter loop crashed; shutting down")
            self._stop.set()
            _safe_shutdown(self._connections.adapter)

    def _try_parse_header(self, line: str, *, source_label: str) -> Header | None:
        try:
            return extract_header(line)
        except Exception:
            log.warning("failed header parse for %s message: %r", source_label, line[:200])
            return None

    def _log_observation(self, *, line: str, header: Header | None) -> None:
        daemon_time_ns = self._clock.now_wall_ns()
        publish_time_ns = header.t_wall_ns if header is not None else daemon_time_ns

        if header is not None:
            logger = self._logger_manager.ensure(runs_dir=self._runs_dir, run_id=header.run_id, agent_id=self._agent_id)
            logger.log_observation(daemon_time_ns, publish_time_ns, line.encode("utf-8"))
            return

        logger = self._logger_manager.get()
        if logger is not None:
            logger.log_observation(daemon_time_ns, publish_time_ns, line.encode("utf-8"))

    def _log_actuation_request(self, *, line: str, header: Header | None) -> None:
        daemon_time_ns = self._clock.now_wall_ns()
        publish_time_ns = header.t_wall_ns if header is not None else daemon_time_ns

        if header is not None:
            logger = self._logger_manager.ensure(runs_dir=self._runs_dir, run_id=header.run_id, agent_id=self._agent_id)
            logger.log_actuation_request(daemon_time_ns, publish_time_ns, line.encode("utf-8"))
            return

        logger = self._logger_manager.get()
        if logger is not None:
            logger.log_actuation_request(daemon_time_ns, publish_time_ns, line.encode("utf-8"))


def run_bridge(*, agent_id: str, host: str, adapter_port: int, autonomy_port: int, runs_dir: str | Path) -> None:
    runs_path = Path(runs_dir)
    log.info("[%s] listening: adapter=%d autonomy=%d", agent_id, adapter_port, autonomy_port)

    connections = accept_two(host, adapter_port, autonomy_port)
    log.info("[%s] adapter connected from %s", agent_id, connections.adapter_addr)
    log.info("[%s] autonomy connected from %s", agent_id, connections.autonomy_addr)

    bridge = _AgentBridge(agent_id=agent_id, runs_dir=runs_path, connections=connections)
    try:
        bridge.run()
    finally:
        log.info("[%s] exiting", agent_id)
