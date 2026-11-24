"""
Servico 2: No do Anel (Algoritmo de Exclusao Mutua Distribuido baseado em Anel + Cliente Bancario).

Cada processo P_i:
1. Escuta mensagens TCP de seu antecessor no anel logico para receber o TOKEN;
2. Mantem uma fila local de requisicoes bancarias (deposito, saque, transferencia, saldo);
3. Quando recebe o TOKEN:
   - Se ha uma requisicao pendente, retem o TOKEN, entra na Secao Critica atuando como
     cliente TCP do Servidor Bancario, aguarda a resposta e sai da Secao Critica;
   - Em seguida (ou imediatamente, caso nao queira usar a Secao Critica), encaminha
     o TOKEN ao seu sucessor P_{(i + 1) mod N}.
"""

from __future__ import annotations

import argparse
import queue
import socket
import threading
from dataclasses import dataclass, field
from typing import Any

from protocol import (
    DEFAULT_CONFIG_PATH,
    MSG_BANK_REQUEST,
    MSG_SHUTDOWN,
    MSG_TOKEN,
    get_process_by_id,
    get_successor,
    load_config,
    receive_json,
    send_message,
    timestamp,
)


@dataclass
class PendingRequest:
    """Representa uma requisicao bancaria enfileirada aguardando a chegada do TOKEN."""

    payload: dict[str, Any]
    done_event: threading.Event = field(default_factory=threading.Event)
    response: dict[str, Any] | None = None


class RingNode:
    """Implementa um processo P_i no algoritmo de exclusao mutua baseado em anel."""

    def __init__(
        self,
        process_id: int,
        config: dict[str, Any],
        initial_token: bool = False,
        token_pass_delay: float | None = None,
        retry_interval: float | None = None,
        verbose_token: bool = True,
        quiet: bool = False,
    ) -> None:
        self.config = config
        self.proc_info = get_process_by_id(config, process_id)
        self.successor = get_successor(config, process_id)
        self.bank_cfg = config["bank_server"]

        # Definicao explicita do numero/ID e endereco do processo
        self.process_id: int = self.proc_info["id"]
        self.name: str = self.proc_info["name"]
        self.host: str = self.proc_info["host"]
        self.port: int = self.proc_info["port"]

        self.initial_token = initial_token
        self.token_pass_delay = (
            float(config["ring"].get("token_pass_delay", 0.8))
            if token_pass_delay is None
            else float(token_pass_delay)
        )
        self.retry_interval = (
            float(config["ring"].get("retry_interval", 1.0))
            if retry_interval is None
            else float(retry_interval)
        )
        self.verbose_token = verbose_token
        self.quiet = quiet

        # Estado local do algoritmo de anel
        self.pending_requests: queue.Queue[PendingRequest] = queue.Queue()
        self._token_queue: queue.Queue[dict[str, Any]] = queue.Queue()
        self.has_token = False
        self.in_critical_section = False
        self.tokens_received = 0
        self.cs_entries = 0

        self._stop_event = threading.Event()
        self._listener_ready = threading.Event()
        self._server_socket: socket.socket | None = None
        self._listener_thread: threading.Thread | None = None
        self._worker_thread: threading.Thread | None = None

    def _log(self, text: str) -> None:
        if not self.quiet:
            print(f"[{timestamp()}] [{self.name}] {text}", flush=True)

    def start(self) -> None:
        """Inicia o servidor TCP do anel e a thread responsavel pelo processamento do TOKEN."""
        self._listener_thread = threading.Thread(
            target=self._listen_ring, name=f"RingListener-{self.name}", daemon=True
        )
        self._worker_thread = threading.Thread(
            target=self._token_worker, name=f"TokenWorker-{self.name}", daemon=True
        )
        self._listener_thread.start()
        self._listener_ready.wait(timeout=5.0)
        self._worker_thread.start()

        self._log(
            f"Processo iniciado (ID={self.process_id}) em {self.host}:{self.port} | "
            f"Sucessor: {self.successor['name']} ({self.successor['host']}:{self.successor['port']}) | "
            f"Banco: {self.bank_cfg['host']}:{self.bank_cfg['port']}"
        )

        if self.initial_token:
            self._log("Gerando TOKEN inicial no anel...")
            self._token_queue.put(
                {
                    "type": MSG_TOKEN,
                    "token_id": "RING-TOKEN-1",
                    "seq": 1,
                    "sender_id": self.process_id,
                    "sender_name": self.name,
                    "timestamp": timestamp(),
                }
            )

    def stop(self) -> None:
        """Encerra as threads e libera a porta TCP do processo."""
        self._stop_event.set()
        if self._server_socket is not None:
            try:
                self._server_socket.close()
            except OSError:
                pass
        if self._listener_thread is not None and self._listener_thread.is_alive():
            self._listener_thread.join(timeout=2.0)
        if self._worker_thread is not None and self._worker_thread.is_alive():
            self._worker_thread.join(timeout=2.0)

    def enqueue_request(
        self,
        operation: str,
        account: str = "",
        target_account: str = "",
        amount: float | None = None,
    ) -> PendingRequest:
        """
        Enfileira uma operacao bancaria para execucao assim que este processo
        receber o TOKEN do anel.
        """
        payload: dict[str, Any] = {
            "type": MSG_BANK_REQUEST,
            "process_id": self.process_id,
            "process_name": self.name,
            "operation": operation.upper(),
            "account": account.upper(),
            "target_account": target_account.upper(),
            "amount": amount,
        }
        pending = PendingRequest(payload=payload)
        self.pending_requests.put(pending)
        self._log(
            f"Requisicao enfileirada: {self._describe_request(payload)} "
            f"(aguardando chegada do TOKEN...)"
        )
        return pending

    def _describe_request(self, payload: dict[str, Any]) -> str:
        op = payload["operation"]
        acc = payload.get("account") or "ALL"
        dst = payload.get("target_account")
        amt = payload.get("amount")
        if op == "TRANSFER":
            return f"TRANSFER R$ {float(amt):.2f} ({acc} -> {dst})"
        if op in ("DEPOSIT", "WITHDRAW"):
            return f"{op} R$ {float(amt):.2f} (conta {acc})"
        if op == "BALANCE":
            return f"BALANCE (conta {acc})"
        return op

    def _listen_ring(self) -> None:
        """Recebe mensagens TCP vindas do antecessor no anel."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_sock:
            server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_sock.bind((self.host, self.port))
            server_sock.listen(8)
            server_sock.settimeout(0.5)
            self._server_socket = server_sock
            self._listener_ready.set()

            while not self._stop_event.is_set():
                try:
                    conn, _addr = server_sock.accept()
                except socket.timeout:
                    continue
                except OSError:
                    break

                with conn:
                    try:
                        message = receive_json(conn)
                    except Exception as exc:
                        self._log(f"Erro ao receber mensagem no anel: {exc}")
                        continue

                if not message:
                    continue

                msg_type = message.get("type")
                if msg_type == MSG_TOKEN:
                    self._token_queue.put(message)
                elif msg_type == MSG_SHUTDOWN:
                    self._log("Mensagem SHUTDOWN recebida. Encerrando no...")
                    self._stop_event.set()

    def _token_worker(self) -> None:
        """Processa o TOKEN recebido: entra na Secao Critica (se desejado) e repassa ao sucessor."""
        while not self._stop_event.is_set():
            try:
                token_msg = self._token_queue.get(timeout=0.2)
            except queue.Empty:
                continue

            self.has_token = True
            self.tokens_received += 1
            token_seq = token_msg.get("seq", 0)
            sender_name = token_msg.get("sender_name", f"P{token_msg.get('sender_id', '?')}")

            # Verifica se ha requisicao pendente (atende 1 por rodada para garantir justica/sem starvation)
            pending: PendingRequest | None = None
            try:
                pending = self.pending_requests.get_nowait()
            except queue.Empty:
                pending = None

            if pending is not None:
                self._enter_critical_section(pending, token_seq, sender_name)
            else:
                if self.verbose_token:
                    self._log(
                        f"TOKEN #{token_seq} recebido de {sender_name} -> "
                        f"sem requisicao pendente -> repassando para {self.successor['name']}"
                    )
                if self.token_pass_delay > 0:
                    self._stop_event.wait(self.token_pass_delay)

            self.has_token = False

            if self._stop_event.is_set():
                break

            # Prepara e encaminha o TOKEN ao proximo processo do anel
            next_token = {
                "type": MSG_TOKEN,
                "token_id": token_msg.get("token_id", "RING-TOKEN-1"),
                "seq": token_seq + 1,
                "sender_id": self.process_id,
                "sender_name": self.name,
                "timestamp": timestamp(),
            }
            self._forward_token(next_token)

    def _enter_critical_section(
        self, pending: PendingRequest, token_seq: int, sender_name: str
    ) -> None:
        """Executa a operacao bancaria no servidor enquanto retem o TOKEN."""
        self.in_critical_section = True
        self.cs_entries += 1
        request_payload = dict(pending.payload)
        request_payload["token_seq"] = token_seq

        self._log(
            f"[SECAO CRITICA] TOKEN #{token_seq} recebido de {sender_name} -> RETIDO! "
            f"Executando {self._describe_request(request_payload)} no Servidor Bancario..."
        )

        try:
            response = send_message(
                host=self.bank_cfg["host"],
                port=self.bank_cfg["port"],
                message=request_payload,
                expect_reply=True,
                timeout=10.0,
            )
            pending.response = response
            if response:
                status = response.get("status", "UNKNOWN")
                msg = response.get("message", "")
                self._log(f"[SECAO CRITICA] Resposta do Banco [{status}]: {msg}")
            else:
                self._log("[SECAO CRITICA] Sem resposta do Servidor Bancario.")
        except OSError as exc:
            pending.response = {
                "status": "ERROR",
                "message": f"Falha ao conectar ao Servidor Bancario: {exc}",
            }
            self._log(f"[SECAO CRITICA] Erro de comunicacao com o Banco: {exc}")
        finally:
            self.in_critical_section = False
            pending.done_event.set()
            self._log(
                f"[SECAO CRITICA] Saindo da secao critica e liberando TOKEN #{token_seq + 1} "
                f"para {self.successor['name']}."
            )

    def _forward_token(self, token_msg: dict[str, Any]) -> None:
        """Envia o TOKEN via TCP ao processo sucessor, tentando novamente caso ele ainda esteja iniciando."""
        warned = False
        while not self._stop_event.is_set():
            try:
                send_message(
                    host=self.successor["host"],
                    port=self.successor["port"],
                    message=token_msg,
                    expect_reply=False,
                    timeout=3.0,
                )
                return
            except OSError:
                if not warned:
                    self._log(
                        f"Aguardando sucessor {self.successor['name']} "
                        f"({self.successor['host']}:{self.successor['port']}) ficar disponivel..."
                    )
                    warned = True
                self._stop_event.wait(self.retry_interval)

    def print_help(self) -> None:
        print(
            "\nComandos disponiveis neste no:\n"
            "  saldo [conta]                      -> Consulta saldo (ex: saldo A ou saldo)\n"
            "  depositar <conta> <valor>          -> Deposita valor (ex: depositar A 250)\n"
            "  sacar <conta> <valor>              -> Saca valor (ex: sacar B 100)\n"
            "  transferir <orig> <dest> <valor>   -> Transfere valor (ex: transferir A B 150)\n"
            "  historico                          -> Exibe historico e auditoria do banco\n"
            "  status                             -> Exibe estado local deste processo no anel\n"
            "  ajuda                              -> Mostra esta lista de comandos\n"
            "  sair                               -> Encerra este processo\n",
            flush=True,
        )

    def run_interactive(self) -> None:
        """Interface interativa de terminal para enfileirar operacoes bancarias."""
        self.print_help()
        while not self._stop_event.is_set():
            try:
                raw = input().strip()
            except (EOFError, KeyboardInterrupt):
                break

            if not raw:
                continue

            parts = raw.split()
            cmd = parts[0].lower()

            if cmd in ("sair", "exit", "quit"):
                break
            elif cmd in ("ajuda", "help"):
                self.print_help()
            elif cmd == "status":
                self._log(
                    f"STATUS -> ID={self.process_id} ({self.name}) | "
                    f"Sucessor={self.successor['name']} | "
                    f"Com TOKEN={self.has_token} | Em SC={self.in_critical_section} | "
                    f"Fila pendente={self.pending_requests.qsize()} | "
                    f"Tokens recebidos={self.tokens_received} | Entradas em SC={self.cs_entries}"
                )
            elif cmd == "saldo":
                account = parts[1].upper() if len(parts) >= 2 else "ALL"
                self.enqueue_request("BALANCE", account=account)
            elif cmd == "depositar" and len(parts) == 3:
                try:
                    self.enqueue_request("DEPOSIT", account=parts[1], amount=float(parts[2]))
                except ValueError:
                    print("Valor invalido. Uso: depositar <conta> <valor>", flush=True)
            elif cmd == "sacar" and len(parts) == 3:
                try:
                    self.enqueue_request("WITHDRAW", account=parts[1], amount=float(parts[2]))
                except ValueError:
                    print("Valor invalido. Uso: sacar <conta> <valor>", flush=True)
            elif cmd == "transferir" and len(parts) == 4:
                try:
                    self.enqueue_request(
                        "TRANSFER",
                        account=parts[1],
                        target_account=parts[2],
                        amount=float(parts[3]),
                    )
                except ValueError:
                    print("Valor invalido. Uso: transferir <origem> <destino> <valor>", flush=True)
            elif cmd == "historico":
                try:
                    resp = send_message(
                        host=self.bank_cfg["host"],
                        port=self.bank_cfg["port"],
                        message={
                            "type": MSG_BANK_REQUEST,
                            "process_id": self.process_id,
                            "process_name": self.name,
                            "operation": "HISTORY",
                        },
                        expect_reply=True,
                        timeout=5.0,
                    )
                    if resp and resp.get("status") == "OK":
                        print("\n--- HISTORICO E AUDITORIA DO BANCO ---", flush=True)
                        print(f"Contas: {resp.get('accounts')}", flush=True)
                        print(
                            f"Concorrencia maxima na SC: {resp.get('max_concurrent_cs')} | "
                            f"Violacoes de exclusao mutua: {resp.get('violations')}",
                            flush=True,
                        )
                        for item in resp.get("history", []):
                            print(f"  {item}", flush=True)
                        print("--------------------------------------\n", flush=True)
                except OSError as exc:
                    self._log(f"Erro ao consultar historico: {exc}")
            else:
                print("Comando invalido. Digite 'ajuda' para ver os comandos.", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="No participante do Algoritmo de Exclusao Mutua baseado em Anel"
    )
    parser.add_argument(
        "--id",
        type=int,
        required=True,
        help="Numero/ID do processo definido no config.json (ex: 0, 1, 2)",
    )
    parser.add_argument(
        "--config",
        default=str(DEFAULT_CONFIG_PATH),
        help="Caminho para o arquivo config.json",
    )
    parser.add_argument(
        "--initial-token",
        action="store_true",
        help="Define que este processo inicia o anel com o TOKEN",
    )
    parser.add_argument(
        "--quiet-token",
        action="store_true",
        help="Oculta logs de repasse de TOKEN quando nao ha requisicao pendente",
    )
    parser.add_argument(
        "--token-delay",
        type=float,
        default=None,
        help="Sobrescreve o intervalo de passagem do TOKEN (segundos)",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    node = RingNode(
        process_id=args.id,
        config=config,
        initial_token=args.initial_token,
        token_pass_delay=args.token_delay,
        verbose_token=not args.quiet_token,
    )

    node.start()
    try:
        node.run_interactive()
    except KeyboardInterrupt:
        pass
    finally:
        print(f"\nEncerrando processo {node.name}...", flush=True)
        node.stop()


if __name__ == "__main__":
    main()
