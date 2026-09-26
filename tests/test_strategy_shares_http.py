import json
import tempfile
import threading
import unittest
from http.client import HTTPConnection
from http.server import HTTPServer
from pathlib import Path

from spx_collector import backtest_prod
class StrategySharesHttpTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)

        market_db = Path(self.temp_dir.name) / "market.db"
        market_db.touch()
        share_db_url = f"sqlite:///{Path(self.temp_dir.name) / 'shares.db'}"
        share_db = backtest_prod.ensure_strategy_share_db(share_db_url)

        backtest_prod.SqlUiHandler.db_path = market_db
        backtest_prod.SqlUiHandler.share_db_path = share_db

        self.server = HTTPServer(("127.0.0.1", 0), backtest_prod.SqlUiHandler)
        self.host, self.port = self.server.server_address
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self._shutdown_server)

    def _shutdown_server(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)

    def _request(self, method, path, body=None):
        conn = HTTPConnection(self.host, self.port, timeout=5)
        headers = {}
        payload = None
        if body is not None:
            payload = json.dumps(body)
            headers["Content-Type"] = "application/json"
        conn.request(method, path, body=payload, headers=headers)
        response = conn.getresponse()
        raw = response.read()
        content_type = response.getheader("Content-Type", "")
        conn.close()
        if "application/json" in content_type:
            return response.status, json.loads(raw.decode("utf-8"))
        return response.status, raw.decode("utf-8")

    def test_create_and_fetch_share_from_prod_handler(self):
        status, created = self._request(
            "POST",
            "/api/strategy-shares",
            {
                "strategy": {
                    "symbol": "SPX",
                    "snapshot_from_date": "2026-03-10",
                    "snapshot_to_date": "2026-03-20",
                    "hold_till_expiry": True,
                    "exit_days": 0,
                    "exit_time": "15:30",
                    "legs": [
                        {
                            "side": "BUY",
                            "quantity": 1,
                            "option_type": "PUT",
                            "target_delta": 35,
                            "target_dte": 1,
                            "entry_time": "10:30",
                            "snapshot_from_date": "2026-03-10",
                            "snapshot_to_date": "2026-03-20",
                            "isResolved": True,
                            "resolved_contracts": [{"streamer_symbol": ".SPXW260320P5000"}],
                        }
                    ],
                },
                "results": {
                    "rows": [
                        {
                            "trade_index": 1,
                            "snapshot_ts": "2026-03-20T14:30:00Z",
                            "strategy_price": 2.5,
                            "strategy_cost": 2.0,
                            "strategy_pnl": 0.5,
                            "strategy_indexed": 125.0,
                            "expiration_date": "2026-03-20",
                        }
                    ]
                },
                "meta": {"source": "backtest_prod", "share_version": 1},
            },
        )
        self.assertEqual(status, 201)
        self.assertTrue(created["share_token"])
        self.assertIn("/?share=", created["share_url"])

        status, fetched = self._request(
            "GET", f"/api/strategy-shares/{created['share_token']}"
        )
        self.assertEqual(status, 200)
        self.assertEqual(fetched["share_token"], created["share_token"])
        self.assertEqual(fetched["strategy"]["symbol"], "SPX")


if __name__ == "__main__":
    unittest.main()
