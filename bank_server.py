"""
Servidor bancario: recurso compartilhado acessado dentro da secao critica.

Nao ha lock global durante o processamento das operacoes, de modo que a exclusao
mutua depende exclusivamente do anel. O servidor apenas conta acessos simultaneos
e registra cada um como violacao.
"""

from __future__ import annotations

import argparse
import copy
import socket
import threading
import time
from typing import Any

from protocol import (
    DEFAULT_CONFIG_PATH,
    MSG_BANK_REQUEST,
    MSG_BANK_RESPONSE,
    load_config,
    receive_json,
    send_json,
    timestamp,
)


class BankServer:
    def __init__(
        self,
        host: str,
        port: int,
        initial_accounts: dict[str, float],
        processing_delay: float = 0.4,
        quiet: bool = False,
    ) -> None:
        self.host = host
        self.port = port
        self.accounts: dict[str, float] = {k: float(v) for k, v in initial_accounts.items()}
        self.processing_delay = float(processing_delay)
        self.quiet = quiet

        self.history: list[dict[str, Any]] = []
        self._metrics_lock = threading.Lock()
        self.active_in_cs = 0
        self.max_concurrent_cs = 0
        self.violations = 0
        self.total_requests = 0

        self._stop_event = threading.Event()
        self._server_socket: socket.socket | None = None
        self._server_thread: threading.Thread | None = None
        self._ready_event = threading.Event()

    def _log(self, text: str) -> None:
        if not self.quiet:
            print(f"[{timestamp()}] [BANCO] {text}", flush=True)

    def start(self, blocking: bool = True) -> None:
        if blocking:
            self._serve_forever()
        else:
            self._server_thread = threading.Thread(
                target=self._serve_forever, name="BankServerThread", daemon=True
            )
            self._server_thread.start()
            self._ready_event.wait(timeout=5.0)

    def stop(self) -> None:
        self._stop_event.set()
        if self._server_socket is not None:
            try:
                self._server_socket.close()
            except OSError:
                pass
        if self._server_thread is not None and self._server_thread.is_alive():
            self._server_thread.join(timeout=3.0)

    def _serve_forever(self) -> None:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_sock:
            server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_sock.bind((self.host, self.port))
            server_sock.listen(16)
            server_sock.settimeout(0.5)
            self._server_socket = server_sock
            self._ready_event.set()

            self._log(
                f"Servidor Bancario escutando em {self.host}:{self.port} | "
                f"Contas iniciais: {self._format_accounts()}"
            )

            while not self._stop_event.is_set():
                try:
                    conn, addr = server_sock.accept()
                except socket.timeout:
                    continue
                except OSError:
                    break

                client_thread = threading.Thread(
                    target=self._handle_client,
                    args=(conn, addr),
                    daemon=True,
                )
                client_thread.start()

        self._log("Servidor Bancario encerrado.")

    def _format_accounts(self) -> str:
        return ", ".join(f"{acc}: R$ {bal:.2f}" for acc, bal in sorted(self.accounts.items()))

    def _handle_client(self, conn: socket.socket, addr: tuple[str, int]) -> None:
        with conn:
            try:
                request = receive_json(conn)
                if not request:
                    return
                response = self._process_request(request)
                send_json(conn, response)
            except Exception as exc:
                self._log(f"Erro ao atender cliente {addr}: {exc}")
                try:
                    send_json(
                        conn,
                        {
                            "type": MSG_BANK_RESPONSE,
                            "status": "ERROR",
                            "message": f"Erro interno no servidor: {exc}",
                        },
                    )
                except OSError:
                    pass

    def _process_request(self, request: dict[str, Any]) -> dict[str, Any]:
        if request.get("type") != MSG_BANK_REQUEST:
            return {
                "type": MSG_BANK_RESPONSE,
                "status": "ERROR",
                "message": f"Tipo de mensagem invalido: {request.get('type')}",
            }

        operation = str(request.get("operation", "")).upper()
        process_id = request.get("process_id", "?")
        process_name = request.get("process_name", f"P{process_id}")
        token_seq = request.get("token_seq")

        # STATS e HISTORY sao somente leitura e nao contam como secao critica
        if operation == "STATS":
            with self._metrics_lock:
                return {
                    "type": MSG_BANK_RESPONSE,
                    "status": "OK",
                    "operation": "STATS",
                    "accounts": copy.deepcopy(self.accounts),
                    "total_requests": self.total_requests,
                    "max_concurrent_cs": self.max_concurrent_cs,
                    "violations": self.violations,
                    "history_count": len(self.history),
                }

        if operation == "HISTORY":
            with self._metrics_lock:
                return {
                    "type": MSG_BANK_RESPONSE,
                    "status": "OK",
                    "operation": "HISTORY",
                    "accounts": copy.deepcopy(self.accounts),
                    "history": copy.deepcopy(self.history[-20:]),
                    "violations": self.violations,
                    "max_concurrent_cs": self.max_concurrent_cs,
                }

        with self._metrics_lock:
            self.active_in_cs += 1
            self.total_requests += 1
            if self.active_in_cs > self.max_concurrent_cs:
                self.max_concurrent_cs = self.active_in_cs
            if self.active_in_cs > 1:
                self.violations += 1
                self._log(
                    f"[ALERTA] VIOLACAO DE EXCLUSAO MUTUA! {self.active_in_cs} processos "
                    f"simultaneos na secao critica (processo atual: {process_name})."
                )

        try:
            self._log(
                f"Secao Critica INICIADA por {process_name} (TOKEN #{token_seq}) -> "
                f"Operacao: {operation}"
            )
            result = self._execute_bank_operation(request, process_id, process_name, token_seq)
            self._log(
                f"Secao Critica FINALIZADA por {process_name} -> {result['message']} | "
                f"Saldos: {self._format_accounts()}"
            )
            return result
        finally:
            with self._metrics_lock:
                self.active_in_cs -= 1

    def _execute_bank_operation(
        self,
        request: dict[str, Any],
        process_id: Any,
        process_name: str,
        token_seq: Any,
    ) -> dict[str, Any]:
        operation = str(request.get("operation", "")).upper()
        account = str(request.get("account", "")).upper()
        target_account = str(request.get("target_account", "")).upper()
        amount = request.get("amount")

        if operation == "BALANCE":
            if self.processing_delay > 0:
                time.sleep(self.processing_delay * 0.5)
            if not account or account == "ALL":
                msg = f"Saldos atuais -> {self._format_accounts()}"
                return {
                    "type": MSG_BANK_RESPONSE,
                    "status": "OK",
                    "operation": operation,
                    "accounts": copy.deepcopy(self.accounts),
                    "message": msg,
                }
            if account not in self.accounts:
                return self._error_response(operation, f"Conta '{account}' inexistente.")
            bal = self.accounts[account]
            return {
                "type": MSG_BANK_RESPONSE,
                "status": "OK",
                "operation": operation,
                "account": account,
                "balance": bal,
                "accounts": copy.deepcopy(self.accounts),
                "message": f"Saldo da conta {account}: R$ {bal:.2f}",
            }

        try:
            value = round(float(amount), 2)
        except (TypeError, ValueError):
            return self._error_response(operation, "Valor monetario invalido.")

        if value <= 0:
            return self._error_response(operation, "O valor da operacao deve ser maior que zero.")

        if operation == "DEPOSIT":
            if account not in self.accounts:
                return self._error_response(operation, f"Conta '{account}' inexistente.")

            # le, espera e escreve: perde atualizacoes se houver concorrencia
            current_balance = self.accounts[account]
            if self.processing_delay > 0:
                time.sleep(self.processing_delay)
            new_balance = round(current_balance + value, 2)
            self.accounts[account] = new_balance

            msg = f"Deposito de R$ {value:.2f} na conta {account} concluido (novo saldo: R$ {new_balance:.2f})."
            self._record_history(process_id, process_name, token_seq, operation, account, None, value, "OK")
            return self._ok_response(operation, msg)

        if operation == "WITHDRAW":
            if account not in self.accounts:
                return self._error_response(operation, f"Conta '{account}' inexistente.")

            current_balance = self.accounts[account]
            if self.processing_delay > 0:
                time.sleep(self.processing_delay)
            if current_balance < value:
                msg = (
                    f"Saldo insuficiente na conta {account} para saque de R$ {value:.2f} "
                    f"(saldo atual: R$ {current_balance:.2f})."
                )
                self._record_history(
                    process_id, process_name, token_seq, operation, account, None, value, "DENIED"
                )
                return self._error_response(operation, msg)

            new_balance = round(current_balance - value, 2)
            self.accounts[account] = new_balance

            msg = f"Saque de R$ {value:.2f} na conta {account} concluido (novo saldo: R$ {new_balance:.2f})."
            self._record_history(process_id, process_name, token_seq, operation, account, None, value, "OK")
            return self._ok_response(operation, msg)

        if operation == "TRANSFER":
            if account not in self.accounts:
                return self._error_response(operation, f"Conta de origem '{account}' inexistente.")
            if target_account not in self.accounts:
                return self._error_response(operation, f"Conta de destino '{target_account}' inexistente.")
            if account == target_account:
                return self._error_response(operation, "Conta de origem e destino devem ser diferentes.")

            src_balance = self.accounts[account]
            dst_balance = self.accounts[target_account]
            if self.processing_delay > 0:
                time.sleep(self.processing_delay)
            if src_balance < value:
                msg = (
                    f"Saldo insuficiente na conta {account} para transferir R$ {value:.2f} "
                    f"(saldo atual: R$ {src_balance:.2f})."
                )
                self._record_history(
                    process_id, process_name, token_seq, operation, account, target_account, value, "DENIED"
                )
                return self._error_response(operation, msg)

            self.accounts[account] = round(src_balance - value, 2)
            self.accounts[target_account] = round(dst_balance + value, 2)

            msg = (
                f"Transferencia de R$ {value:.2f} de {account} para {target_account} concluida "
                f"({account}: R$ {self.accounts[account]:.2f} | {target_account}: R$ {self.accounts[target_account]:.2f})."
            )
            self._record_history(
                process_id, process_name, token_seq, operation, account, target_account, value, "OK"
            )
            return self._ok_response(operation, msg)

        return self._error_response(operation, f"Operacao desconhecida: '{operation}'.")

    def _record_history(
        self,
        process_id: Any,
        process_name: str,
        token_seq: Any,
        operation: str,
        account: str,
        target_account: str | None,
        amount: float,
        status: str,
    ) -> None:
        with self._metrics_lock:
            self.history.append(
                {
                    "timestamp": timestamp(),
                    "process_id": process_id,
                    "process_name": process_name,
                    "token_seq": token_seq,
                    "operation": operation,
                    "account": account,
                    "target_account": target_account,
                    "amount": amount,
                    "status": status,
                }
            )

    def _ok_response(self, operation: str, message: str) -> dict[str, Any]:
        return {
            "type": MSG_BANK_RESPONSE,
            "status": "OK",
            "operation": operation,
            "accounts": copy.deepcopy(self.accounts),
            "message": message,
        }

    def _error_response(self, operation: str, message: str) -> dict[str, Any]:
        return {
            "type": MSG_BANK_RESPONSE,
            "status": "ERROR",
            "operation": operation,
            "accounts": copy.deepcopy(self.accounts),
            "message": message,
        }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Servidor Bancario (Recurso Compartilhado / Secao Critica)"
    )
    parser.add_argument(
        "--config",
        default=str(DEFAULT_CONFIG_PATH),
        help="Caminho para o arquivo config.json",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=None,
        help="Sobrescreve o tempo simulado de processamento na secao critica (segundos)",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    bank_cfg = config["bank_server"]
    delay = bank_cfg.get("processing_delay", 0.4) if args.delay is None else args.delay

    server = BankServer(
        host=bank_cfg["host"],
        port=bank_cfg["port"],
        initial_accounts=bank_cfg["initial_accounts"],
        processing_delay=delay,
    )

    try:
        server.start(blocking=True)
    except KeyboardInterrupt:
        print("\nEncerrando Servidor Bancario...", flush=True)
        server.stop()


if __name__ == "__main__":
    main()
