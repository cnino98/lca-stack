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


def _listen(host: str, port: int) -> socket.socket:
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind((host, int(port)))
    listener.listen(1)
    return listener


def accept_two(host: str, adapter_port: int, autonomy_port: int) -> Connections:
    adapter_listener = _listen(host, adapter_port)
    autonomy_listener = _listen(host, autonomy_port)

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
