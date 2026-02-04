from __future__ import annotations

import socket
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

SocketAddress = tuple[str, int]


@dataclass(frozen=True, slots=True)
class Connections:
    adapter: socket.socket
    adapter_addr: SocketAddress
    autonomy: socket.socket
    autonomy_addr: SocketAddress


@dataclass(frozen=True, slots=True)
class Listeners:
    adapter_listener: socket.socket
    autonomy_listener: socket.socket


def _listen(host: str, port: int) -> socket.socket:
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind((host, int(port)))
    listener.listen(1)
    return listener


def listen_pair(host: str, adapter_port: int, autonomy_port: int) -> Listeners:
    """Create two TCP listeners.

    The daemon uses separate ports for Adapter and Autonomy local links.
    """
    return Listeners(
        adapter_listener=_listen(host, adapter_port),
        autonomy_listener=_listen(host, autonomy_port),
    )


def accept_one(listener: socket.socket) -> tuple[socket.socket, SocketAddress]:
    conn, addr = listener.accept()
    conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    return conn, addr


def accept_two(host: str, adapter_port: int, autonomy_port: int) -> Connections:
    listeners = listen_pair(host, adapter_port, autonomy_port)
    adapter_listener = listeners.adapter_listener
    autonomy_listener = listeners.autonomy_listener

    try:
        with ThreadPoolExecutor(max_workers=2, thread_name_prefix="accept") as pool:
            adapter_future = pool.submit(adapter_listener.accept)
            autonomy_future = pool.submit(autonomy_listener.accept)

            adapter_conn, adapter_addr = adapter_future.result()
            autonomy_conn, autonomy_addr = autonomy_future.result()
    finally:
        adapter_listener.close()
        autonomy_listener.close()

    adapter_conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    autonomy_conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    return Connections(
        adapter=adapter_conn,
        adapter_addr=adapter_addr,
        autonomy=autonomy_conn,
        autonomy_addr=autonomy_addr,
    )