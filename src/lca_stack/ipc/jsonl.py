from __future__ import annotations

import json
import socket
from typing import Any


class JsonlSocket:
    """Newline-delimited JSON (JSONL) over TCP.

    IMPORTANT: We intentionally avoid socket.makefile() here.

    On some platforms / Python versions, using makefile()+readline() together with
    socket timeouts can leave the file object in a broken state after a timeout,
    causing later reads to fail even when data arrives. Implementing framing via
    recv() avoids that class of bugs and makes startup order robust.
    """

    sock: socket.socket
    _recv_buffer: bytearray

    def __init__(self, sock: socket.socket) -> None:
        self.sock = sock
        self._recv_buffer = bytearray()

    def set_timeout(self, timeout_s: float | None) -> None:
        """Set the underlying socket timeout.

        None => blocking, 0.0 => non-blocking.
        """
        self.sock.settimeout(timeout_s)

    def close(self) -> None:
        try:
            self.sock.close()
        except Exception:
            pass

    def read_line(self) -> str | None:
        """Read one newline-delimited UTF-8 line.

        Returns:
          - str if a line is read
          - None if the socket is closed and no buffered data remains

        Raises:
          - TimeoutError / OSError on timeout or socket errors
        """
        while True:
            newline_index = self._recv_buffer.find(b"\n")
            if newline_index != -1:
                line_bytes = bytes(self._recv_buffer[:newline_index])
                del self._recv_buffer[: newline_index + 1]

                if line_bytes.endswith(b"\r"):
                    line_bytes = line_bytes[:-1]

                return line_bytes.decode("utf-8")

            chunk = self.sock.recv(4096)
            if not chunk:
                # Peer closed. If we have leftover bytes without a newline, return them once.
                if self._recv_buffer:
                    line_bytes = bytes(self._recv_buffer)
                    self._recv_buffer.clear()
                    if line_bytes.endswith(b"\r"):
                        line_bytes = line_bytes[:-1]
                    return line_bytes.decode("utf-8")
                return None

            self._recv_buffer.extend(chunk)

    def write_line(self, line: str) -> None:
        data = (line + "\n").encode("utf-8")
        self.sock.sendall(data)

    def read_json(self) -> Any | None:
        line = self.read_line()
        if line is None:
            return None
        return json.loads(line)

    def write_json(self, obj: Any) -> None:
        self.write_line(json.dumps(obj))


class JsonlClient(JsonlSocket):
    def __init__(self, host: str, port: int) -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, int(port)))
        super().__init__(sock)
