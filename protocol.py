"""Mensagens JSON delimitadas por quebra de linha sobre TCP, usadas pelo banco e pelos nos."""

from __future__ import annotations

import json
import socket
from datetime import datetime
from pathlib import Path
from typing import Any

MSG_TOKEN = "TOKEN"
MSG_BANK_REQUEST = "BANK_REQUEST"
MSG_BANK_RESPONSE = "BANK_RESPONSE"
MSG_SHUTDOWN = "SHUTDOWN"

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent / "config.json"


def timestamp() -> str:
    """Horario atual (HH:MM:SS.mmm) para logs."""
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


def load_config(config_path: str | Path = DEFAULT_CONFIG_PATH) -> dict[str, Any]:
    path = Path(config_path)
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def get_process_by_id(config: dict[str, Any], process_id: int) -> dict[str, Any]:
    for proc in config["ring"]["processes"]:
        if proc["id"] == process_id:
            return proc
    valid_ids = [p["id"] for p in config["ring"]["processes"]]
    raise ValueError(f"Processo id={process_id} nao encontrado. IDs validos: {valid_ids}")


def get_successor(config: dict[str, Any], process_id: int) -> dict[str, Any]:
    """Sucessor no anel: proximo da lista de processos do config.json, com volta ao inicio."""
    processes = config["ring"]["processes"]
    for idx, proc in enumerate(processes):
        if proc["id"] == process_id:
            return processes[(idx + 1) % len(processes)]
    raise ValueError(f"Processo id={process_id} nao encontrado na configuracao do anel.")


def send_json(conn: socket.socket, message: dict[str, Any]) -> None:
    payload = (json.dumps(message, ensure_ascii=False) + "\n").encode("utf-8")
    conn.sendall(payload)


def receive_json(conn: socket.socket) -> dict[str, Any] | None:
    """Le uma linha JSON do socket. Retorna None se a conexao fechar sem dados."""
    with conn.makefile("r", encoding="utf-8") as reader:
        line = reader.readline()
    if not line:
        return None
    return json.loads(line)


def send_message(
    host: str,
    port: int,
    message: dict[str, Any],
    expect_reply: bool = False,
    timeout: float = 5.0,
) -> dict[str, Any] | None:
    """Abre uma conexao, envia a mensagem e, se pedido, devolve a resposta."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(timeout)
        sock.connect((host, port))
        send_json(sock, message)
        if expect_reply:
            return receive_json(sock)
    return None
