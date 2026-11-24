"""
Demonstracao automatizada ponta-a-ponta do Algoritmo de Exclusao Mutua baseado em Anel.

Sobe o Servidor Bancario e todos os processos do anel definidos em config.json
usando sockets TCP reais, dispara operacoes concorrentes simultaneamente a partir
de todos os nos e verifica a consistencia dos saldos e a ausencia de violacoes
de exclusao mutua.
"""

from __future__ import annotations

import time

from bank_server import BankServer
from protocol import MSG_BANK_REQUEST, load_config, send_message
from ring_node import PendingRequest, RingNode


def run_demo() -> None:
    config = load_config()
    bank_cfg = config["bank_server"]

    print("=" * 72)
    print(" DEMONSTRACAO: EXCLUSAO MUTUA DISTRIBUIDA BASEADA EM ANEL (GRUPO 2)")
    print("=" * 72)

    server = BankServer(
        host=bank_cfg["host"],
        port=bank_cfg["port"],
        initial_accounts=bank_cfg["initial_accounts"],
        processing_delay=0.25,
    )
    server.start(blocking=False)

    nodes: list[RingNode] = []
    for idx, proc in enumerate(config["ring"]["processes"]):
        node = RingNode(
            process_id=proc["id"],
            config=config,
            initial_token=(idx == 0),
            token_pass_delay=0.15,
            retry_interval=0.2,
            verbose_token=True,
        )
        nodes.append(node)

    # Inicia todos os nos do anel (P0 inicia com o TOKEN)
    for node in nodes:
        node.start()

    time.sleep(0.5)

    print("\n--- Disparando operacoes bancarias simultaneas em P0, P1 e P2 ---\n", flush=True)
    pending: list[PendingRequest] = [
        # P0: Deposita 200 em A, transfere 150 de A para B
        nodes[0].enqueue_request("DEPOSIT", account="A", amount=200.0),
        nodes[0].enqueue_request("TRANSFER", account="A", target_account="B", amount=150.0),
        # P1: Saca 100 de B, transfere 250 de B para C
        nodes[1].enqueue_request("WITHDRAW", account="B", amount=100.0),
        nodes[1].enqueue_request("TRANSFER", account="B", target_account="C", amount=250.0),
        # P2: Transfere 300 de C para A, consulta saldo geral
        nodes[2].enqueue_request("TRANSFER", account="C", target_account="A", amount=300.0),
        nodes[2].enqueue_request("BALANCE", account="ALL"),
    ]

    for req in pending:
        finished = req.done_event.wait(timeout=20.0)
        if not finished:
            raise TimeoutError("Tempo excedido aguardando conclusao das operacoes no anel.")

    # Consulta estatisticas finais diretamente no Servidor Bancario
    stats = send_message(
        host=bank_cfg["host"],
        port=bank_cfg["port"],
        message={"type": MSG_BANK_REQUEST, "process_id": -1, "operation": "STATS"},
        expect_reply=True,
    )

    # Encerra nos e servidor
    for node in nodes:
        node.stop()
    server.stop()

    print("\n" + "=" * 72)
    print(" RELATORIO FINAL DA DEMONSTRACAO")
    print("=" * 72)
    # Calculo esperado:
    # A: 1000 + 200 - 150 + 300 = 1350.00
    # B: 1000 + 150 - 100 - 250 =  800.00
    # C: 1000 + 250 - 300       =  950.00
    expected = {"A": 1350.0, "B": 800.0, "C": 950.0}
    actual = stats["accounts"]
    print(f" Saldos esperados             : {expected}")
    print(f" Saldos obtidos no servidor   : {actual}")
    print(f" Total de requisicoes na SC   : {stats['total_requests']}")
    print(f" Concorrencia maxima na SC    : {stats['max_concurrent_cs']}")
    print(f" Violacoes de exclusao mutua  : {stats['violations']}")
    print(
        f" Consistencia verificada      : {'SUCESSO' if actual == expected and stats['violations'] == 0 else 'FALHA'}"
    )
    print("=" * 72)


if __name__ == "__main__":
    run_demo()
