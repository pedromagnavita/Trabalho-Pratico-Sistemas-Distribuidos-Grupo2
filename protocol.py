"""
Protocolo de comunicacao TCP com mensagens JSON delimitadas por quebra de linha.

Utilizado tanto pelo Servidor Bancario (recurso compartilhado / secao critica)
quanto pelos Nos do Anel (algoritmo de exclusao mutua distribuida baseado em anel).
"""

from __future__ import annotations

import json
import socket
from datetime import datetime
from pathlib import Path
from typing import Any

# Tipos de mensagens trocadas no sistema distribuido
MSG_TOKEN = "TOKEN"
MSG_BANK_REQUEST = "BANK_REQUEST"
MSG_BANK_RESPONSE = "BANK_RESPONSE"
MSG_SHUTDOWN = "SHUTDOWN"

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent / "config.json"


def timestamp() -> str:
    """Retorna horario atual formatado para logs."""
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


def load_config(config_path: str | Path = DEFAULT_CONFIG_PATH) -> dict[str, Any]:
    """Carrega a configuracao do servidor bancario e dos processos do anel."""
    path = Path(config_path)
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def get_process_by_id(config: dict[str, Any], process_id: int) -> dict[str, Any]:
    """Localiza a definicao (id, nome, host, porta) de um processo pelo seu numero."""
    for proc in config["ring"]["processes"]:
        if proc["id"] == process_id:
            return proc
    valid_ids = [p["id"] for p in config["ring"]["processes"]]
    raise ValueError(f"Processo id={process_id} nao encontrado. IDs validos: {valid_ids}")


def get_successor(config: dict[str, Any], process_id: int) -> dict[str, Any]:
    """
    Retorna o processo sucessor no anel logico: P_{(indice + 1) mod N}.
    A topologia e derivada da lista ordenada de processos em config.json.
    """
    processes = config["ring"]["processes"]
    for idx, proc in enumerate(processes):
        if proc["id"] == process_id:
            return processes[(idx + 1) % len(processes)]
    raise ValueError(f"Processo id={process_id} nao encontrado na configuracao do anel.")


def send_json(conn: socket.socket, message: dict[str, Any]) -> None:
    """Serializa um dicionario em JSON UTF-8 finalizado com '\\n' e envia via socket TCP."""
    payload = (json.dumps(message, ensure_ascii=False) + "\n").encode("utf-8")
    conn.sendall(payload)


def receive_json(conn: socket.socket) -> dict[str, Any] | None:
    """
    Recebe uma mensagem JSON delimitada por quebra de linha ('\\n') de um socket TCP.
    Retorna None caso a conexao seja encerrada antes de receber dados.
    """
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
    """
    Abre uma conexao TCP com (host, port), envia a mensagem JSON e, opcionalmente,
    aguarda uma resposta JSON antes de encerrar o socket.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(timeout)
        sock.connect((host, port))
        send_json(sock, message)
        if expect_reply:
            return receive_json(sock)
    return None
