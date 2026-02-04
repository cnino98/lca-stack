from __future__ import annotations

import errno
import socket
import struct
from dataclasses import dataclass
from typing import Any, Protocol, TypeVar

TProto = TypeVar("TProto", bound="ProtobufMessage")


class ProtobufMessage(Protocol):
    def ParseFromString(self, data: bytes) -> Any: ...
    def SerializeToString(self) -> bytes: ...


class FramedSocket:
    """Length-prefixed framing for Protobuf messages over TCP.

    Frame format:
      - 4-byte big-endian unsigned length (N)
      - N bytes payload
    """

    __slots__ = ("sock", "_recv_buffer")

    def __init__(self, sock: socket.socket) -> None:
        self.sock = sock
        self._recv_buffer = bytearray()

    def set_timeout(self, timeout_s: float | None) -> None:
        self.sock.settimeout(timeout_s)

    def close(self) -> None:
        try:
            try:
                # Unblock any waiting recv() in other threads.
                self.sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self.sock.close()
        except Exception:
            pass

    def _recv_into_buffer(self) -> None:
        try:
            chunk = self.sock.recv(4096)
        except OSError as e:
            # Treat common shutdown/close conditions as a clean EOF.
            if e.errno in (errno.EBADF, errno.ECONNRESET, errno.ENOTCONN, errno.EPIPE):
                raise EOFError("socket closed") from e
            raise
        if not chunk:
            raise EOFError("socket closed")
        self._recv_buffer.extend(chunk)

    def read_frame(self) -> bytes:
        # Read length prefix.
        while len(self._recv_buffer) < 4:
            self._recv_into_buffer()

        length = int(struct.unpack_from(">I", self._recv_buffer, 0)[0])
        del self._recv_buffer[:4]

        # Read payload.
        while len(self._recv_buffer) < length:
            self._recv_into_buffer()

        payload = bytes(self._recv_buffer[:length])
        del self._recv_buffer[:length]
        return payload

    def write_frame(self, payload: bytes) -> None:
        header = struct.pack(">I", int(len(payload)))
        self.sock.sendall(header + payload)

    def read_message(self, msg: TProto) -> TProto:
        msg.ParseFromString(self.read_frame())
        return msg

    def write_message(self, msg: ProtobufMessage) -> None:
        self.write_frame(msg.SerializeToString())


@dataclass(slots=True)
class FramedClient:
    host: str
    port: int

    def connect(self) -> FramedSocket:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((self.host, int(self.port)))
        return FramedSocket(sock)