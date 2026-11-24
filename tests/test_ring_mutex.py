"""Testes automatizados para validar a exclusao mutua distribuida baseada em anel."""

from __future__ import annotations

import copy
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from bank_server import BankServer
from protocol import MSG_BANK_REQUEST, load_config, send_message
from ring_node import RingNode


class TestRingMutualExclusion(unittest.TestCase):
    def test_concurrent_bank_operations_preserve_consistency_and_mutex(self) -> None:
        config = copy.deepcopy(load_config())
        # Usa portas dedicadas para teste para nao conflitar com instancias em execucao
        config["bank_server"]["port"] = 16000
        config["bank_server"]["processing_delay"] = 0.08
        for idx, proc in enumerate(config["ring"]["processes"]):
            proc["port"] = 15000 + idx

        bank_cfg = config["bank_server"]
        server = BankServer(
            host=bank_cfg["host"],
            port=bank_cfg["port"],
            initial_accounts=bank_cfg["initial_accounts"],
            processing_delay=bank_cfg["processing_delay"],
            quiet=True,
        )
        server.start(blocking=False)

        nodes = [
            RingNode(
                process_id=proc["id"],
                config=config,
                initial_token=(idx == 0),
                token_pass_delay=0.02,
                retry_interval=0.05,
                verbose_token=False,
                quiet=True,
            )
            for idx, proc in enumerate(config["ring"]["processes"])
        ]

        try:
            for node in nodes:
                node.start()

            # Dispara 9 operacoes concorrentes simultaneamente entre os 3 nos
            requests = [
                nodes[0].enqueue_request("DEPOSIT", account="A", amount=100.0),
                nodes[1].enqueue_request("DEPOSIT", account="A", amount=200.0),
                nodes[2].enqueue_request("WITHDRAW", account="A", amount=50.0),
                nodes[0].enqueue_request("TRANSFER", account="A", target_account="B", amount=150.0),
                nodes[1].enqueue_request("TRANSFER", account="B", target_account="C", amount=300.0),
                nodes[2].enqueue_request("TRANSFER", account="C", target_account="A", amount=100.0),
                nodes[0].enqueue_request("WITHDRAW", account="B", amount=50.0),
                nodes[1].enqueue_request("DEPOSIT", account="C", amount=250.0),
                nodes[2].enqueue_request("BALANCE", account="ALL"),
            ]

            for req in requests:
                self.assertTrue(
                    req.done_event.wait(timeout=15.0),
                    "Requisicao nao foi concluida dentro do tempo esperado.",
                )
                self.assertIsNotNone(req.response)
                self.assertEqual(req.response["status"], "OK")

            stats = send_message(
                host=bank_cfg["host"],
                port=bank_cfg["port"],
                message={"type": MSG_BANK_REQUEST, "process_id": -1, "operation": "STATS"},
                expect_reply=True,
            )
            self.assertIsNotNone(stats)
            self.assertEqual(stats["violations"], 0)
            self.assertEqual(stats["max_concurrent_cs"], 1)
            self.assertEqual(stats["total_requests"], 9)

            # Saldos esperados:
            # A: 1000 + 100 + 200 - 50 - 150 + 100 = 1200.0
            # B: 1000 + 150 - 300 - 50             =  800.0
            # C: 1000 + 300 - 100 + 250            = 1450.0
            self.assertEqual(stats["accounts"], {"A": 1200.0, "B": 800.0, "C": 1450.0})
        finally:
            for node in nodes:
                node.stop()
            server.stop()

    def test_insufficient_funds_is_rejected_without_corrupting_balance(self) -> None:
        config = copy.deepcopy(load_config())
        config["bank_server"]["port"] = 16010
        config["bank_server"]["processing_delay"] = 0.05
        for idx, proc in enumerate(config["ring"]["processes"]):
            proc["port"] = 15010 + idx

        bank_cfg = config["bank_server"]
        server = BankServer(
            host=bank_cfg["host"],
            port=bank_cfg["port"],
            initial_accounts={"A": 100.0, "B": 50.0},
            processing_delay=bank_cfg["processing_delay"],
            quiet=True,
        )
        server.start(blocking=False)

        nodes = [
            RingNode(
                process_id=proc["id"],
                config=config,
                initial_token=(idx == 0),
                token_pass_delay=0.02,
                retry_interval=0.05,
                verbose_token=False,
                quiet=True,
            )
            for idx, proc in enumerate(config["ring"]["processes"])
        ]

        try:
            for node in nodes:
                node.start()

            req = nodes[1].enqueue_request("TRANSFER", account="A", target_account="B", amount=500.0)
            self.assertTrue(req.done_event.wait(timeout=10.0))
            self.assertEqual(req.response["status"], "ERROR")
            self.assertEqual(req.response["accounts"], {"A": 100.0, "B": 50.0})
        finally:
            for node in nodes:
                node.stop()
            server.stop()


if __name__ == "__main__":
    unittest.main()
