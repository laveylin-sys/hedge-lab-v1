from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from typing import Any

import psycopg
from psycopg.rows import dict_row

import hedge_lab as core


DATABASE_URL = os.getenv("DATABASE_URL", "").strip()


class PostgresDB:
    """PostgreSQL persistence adapter matching hedge_lab.DB's write API."""

    def __init__(self, _path: str, starting_capital: float):
        if not DATABASE_URL:
            raise RuntimeError("DATABASE_URL is not configured")
        self.starting_capital = float(starting_capital)
        self.con = psycopg.connect(DATABASE_URL, autocommit=True, row_factory=dict_row)
        self._init()

    def _init(self) -> None:
        with self.con.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS runs(
                    id BIGSERIAL PRIMARY KEY,
                    started_at TIMESTAMPTZ NOT NULL,
                    starting_capital DOUBLE PRECISION NOT NULL
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS trades(
                    id BIGSERIAL PRIMARY KEY,
                    ts_ms BIGINT NOT NULL,
                    pair_id BIGINT,
                    market_id TEXT NOT NULL,
                    interval TEXT NOT NULL,
                    outcome TEXT NOT NULL,
                    side TEXT NOT NULL,
                    price DOUBLE PRECISION NOT NULL,
                    qty DOUBLE PRECISION NOT NULL,
                    notional DOUBLE PRECISION NOT NULL,
                    liquidity TEXT NOT NULL,
                    reason TEXT NOT NULL
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS equity(
                    ts_ms BIGINT PRIMARY KEY,
                    cash DOUBLE PRECISION NOT NULL,
                    inventory_value DOUBLE PRECISION NOT NULL,
                    equity DOUBLE PRECISION NOT NULL,
                    realized_pnl DOUBLE PRECISION NOT NULL,
                    unrealized_pnl DOUBLE PRECISION NOT NULL,
                    unhedged_exposure DOUBLE PRECISION NOT NULL,
                    completed_pairs INTEGER NOT NULL,
                    incomplete_pairs INTEGER NOT NULL,
                    max_drawdown DOUBLE PRECISION NOT NULL
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS pair_stats(
                    pair_id BIGINT PRIMARY KEY,
                    market_id TEXT NOT NULL,
                    interval TEXT NOT NULL,
                    first_fill_ms BIGINT NOT NULL,
                    hedge_fill_ms BIGINT,
                    time_to_hedge_ms BIGINT,
                    complete INTEGER NOT NULL DEFAULT 0,
                    qty DOUBLE PRECISION NOT NULL,
                    total_cost DOUBLE PRECISION,
                    locked_edge DOUBLE PRECISION
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS engine_state(
                    id SMALLINT PRIMARY KEY CHECK (id = 1),
                    updated_at TIMESTAMPTZ NOT NULL,
                    state_json JSONB NOT NULL
                )
            """)
            cur.execute("SELECT COUNT(*) AS n FROM runs")
            if int(cur.fetchone()["n"]) == 0:
                cur.execute(
                    "INSERT INTO runs(started_at, starting_capital) VALUES(%s, %s)",
                    (datetime.now(timezone.utc), self.starting_capital),
                )

    def add_trade(self, pair_id: int, market: core.Market, leg: core.Leg, reason: str) -> None:
        with self.con.cursor() as cur:
            cur.execute("""
                INSERT INTO trades(
                    ts_ms,pair_id,market_id,interval,outcome,side,price,qty,notional,liquidity,reason
                ) VALUES(%s,%s,%s,%s,%s,'BUY',%s,%s,%s,%s,%s)
            """, (
                leg.ts_ms, pair_id, market.id, market.interval, leg.outcome,
                leg.price, leg.qty, leg.price * leg.qty, leg.liquidity, reason,
            ))

    def open_pair(self, pair: core.Pair) -> None:
        with self.con.cursor() as cur:
            cur.execute("""
                INSERT INTO pair_stats(pair_id,market_id,interval,first_fill_ms,complete,qty)
                VALUES(%s,%s,%s,%s,0,%s)
                ON CONFLICT (pair_id) DO UPDATE SET
                    market_id=EXCLUDED.market_id,
                    interval=EXCLUDED.interval,
                    first_fill_ms=EXCLUDED.first_fill_ms,
                    complete=0,
                    qty=EXCLUDED.qty
            """, (pair.pair_id, pair.market_id, pair.interval, pair.opened_ms, pair.first.qty))

    def close_pair(self, pair: core.Pair) -> None:
        assert pair.second is not None
        tth = pair.second.ts_ms - pair.first.ts_ms
        total_cost = pair.first.price + pair.second.price
        edge = 1.0 - total_cost
        with self.con.cursor() as cur:
            cur.execute("""
                UPDATE pair_stats
                SET hedge_fill_ms=%s,time_to_hedge_ms=%s,complete=1,total_cost=%s,locked_edge=%s
                WHERE pair_id=%s
            """, (pair.second.ts_ms, tth, total_cost, edge, pair.pair_id))

    def equity_row(self, **x: Any) -> None:
        with self.con.cursor() as cur:
            cur.execute("""
                INSERT INTO equity(
                    ts_ms,cash,inventory_value,equity,realized_pnl,unrealized_pnl,
                    unhedged_exposure,completed_pairs,incomplete_pairs,max_drawdown
                ) VALUES(
                    %(ts_ms)s,%(cash)s,%(inventory_value)s,%(equity)s,%(realized_pnl)s,%(unrealized_pnl)s,
                    %(unhedged_exposure)s,%(completed_pairs)s,%(incomplete_pairs)s,%(max_drawdown)s
                )
                ON CONFLICT (ts_ms) DO UPDATE SET
                    cash=EXCLUDED.cash,
                    inventory_value=EXCLUDED.inventory_value,
                    equity=EXCLUDED.equity,
                    realized_pnl=EXCLUDED.realized_pnl,
                    unrealized_pnl=EXCLUDED.unrealized_pnl,
                    unhedged_exposure=EXCLUDED.unhedged_exposure,
                    completed_pairs=EXCLUDED.completed_pairs,
                    incomplete_pairs=EXCLUDED.incomplete_pairs,
                    max_drawdown=EXCLUDED.max_drawdown
            """, x)

    def load_state(self) -> dict[str, Any] | None:
        with self.con.cursor() as cur:
            cur.execute("SELECT state_json FROM engine_state WHERE id=1")
            row = cur.fetchone()
        return dict(row["state_json"]) if row else None

    def save_state(self, state: dict[str, Any]) -> None:
        with self.con.cursor() as cur:
            cur.execute("""
                INSERT INTO engine_state(id,updated_at,state_json)
                VALUES(1,%s,%s::jsonb)
                ON CONFLICT (id) DO UPDATE SET
                    updated_at=EXCLUDED.updated_at,
                    state_json=EXCLUDED.state_json
            """, (datetime.now(timezone.utc), json.dumps(state, separators=(",", ":"))))


class PersistentEngine(core.Engine):
    def __init__(self, cfg: dict[str, Any], db: PostgresDB):
        super().__init__(cfg, db)
        self._restore_state()

    @staticmethod
    def _leg_to_dict(leg: core.Leg | None) -> dict[str, Any] | None:
        if leg is None:
            return None
        return {
            "token": leg.token,
            "outcome": leg.outcome,
            "qty": leg.qty,
            "price": leg.price,
            "ts_ms": leg.ts_ms,
            "liquidity": leg.liquidity,
        }

    @staticmethod
    def _leg_from_dict(x: dict[str, Any] | None) -> core.Leg | None:
        if not x:
            return None
        return core.Leg(
            token=str(x["token"]), outcome=str(x["outcome"]), qty=float(x["qty"]),
            price=float(x["price"]), ts_ms=int(x["ts_ms"]), liquidity=str(x["liquidity"]),
        )

    def _state_payload(self) -> dict[str, Any]:
        return {
            "version": 1,
            "cash": self.cash,
            "next_pair": self.next_pair,
            "peak_equity": self.peak_equity,
            "max_dd": self.max_dd,
            "realized": self.realized,
            "pairs": [
                {
                    "pair_id": p.pair_id,
                    "market_id": p.market_id,
                    "interval": p.interval,
                    "opened_ms": p.opened_ms,
                    "closed_ms": p.closed_ms,
                    "first": self._leg_to_dict(p.first),
                    "second": self._leg_to_dict(p.second),
                }
                for p in self.pairs.values()
            ],
        }

    def _restore_state(self) -> None:
        state = self.db.load_state()
        if not state:
            print("PERSISTENCE: PostgreSQL connected; starting fresh state", flush=True)
            return
        self.cash = float(state.get("cash", self.cash))
        self.next_pair = int(state.get("next_pair", self.next_pair))
        self.peak_equity = float(state.get("peak_equity", self.peak_equity))
        self.max_dd = float(state.get("max_dd", self.max_dd))
        self.realized = float(state.get("realized", self.realized))
        restored: dict[int, core.Pair] = {}
        for raw in state.get("pairs", []):
            first = self._leg_from_dict(raw.get("first"))
            if first is None:
                continue
            second = self._leg_from_dict(raw.get("second"))
            p = core.Pair(
                pair_id=int(raw["pair_id"]),
                market_id=str(raw["market_id"]),
                interval=str(raw["interval"]),
                opened_ms=int(raw["opened_ms"]),
                first=first,
                second=second,
                closed_ms=int(raw["closed_ms"]) if raw.get("closed_ms") is not None else None,
            )
            restored[p.pair_id] = p
        self.pairs = restored
        if restored:
            self.next_pair = max(self.next_pair, max(restored) + 1)
        print(
            f"PERSISTENCE: restored {len(self.pairs)} pairs | cash ${self.cash:,.2f} | realized {self.realized:+,.2f}",
            flush=True,
        )

    def _persist_state(self) -> None:
        self.db.save_state(self._state_payload())

    def _buy(self, *args: Any, **kwargs: Any) -> core.Leg | None:
        leg = super()._buy(*args, **kwargs)
        if leg is not None:
            self._persist_state()
        return leg

    def snapshot(self):
        eq = super().snapshot()
        self._persist_state()
        return eq


def postgres_report() -> None:
    db = PostgresDB("", 10000.0)
    with db.con.cursor() as cur:
        cur.execute("SELECT * FROM equity ORDER BY ts_ms DESC LIMIT 1")
        e = cur.fetchone()
        if not e:
            print("No equity snapshots yet.")
            return
        cur.execute("SELECT COUNT(*) AS n FROM pair_stats")
        total = int(cur.fetchone()["n"])
        cur.execute("SELECT COUNT(*) AS n FROM pair_stats WHERE complete=1")
        completed = int(cur.fetchone()["n"])
        cur.execute("SELECT AVG(time_to_hedge_ms) AS avg_ms FROM pair_stats WHERE complete=1")
        tth = cur.fetchone()["avg_ms"]
        cur.execute("SELECT COUNT(*) AS n FROM trades")
        trades = int(cur.fetchone()["n"])
    print(json.dumps({
        "equity": e["equity"],
        "pnl": float(e["equity"]) - 10000.0,
        "pair_completion_rate": completed / total if total else 0,
        "avg_time_to_hedge_sec": (float(tth) / 1000.0) if tth is not None else None,
        "unhedged_exposure": e["unhedged_exposure"],
        "max_drawdown": e["max_drawdown"],
        "trades": trades,
    }, indent=2))


def main() -> None:
    if not DATABASE_URL:
        print("PERSISTENCE: DATABASE_URL missing; falling back to original SQLite mode", flush=True)
        core.main()
        return

    if len(sys.argv) > 1 and sys.argv[1] == "report":
        postgres_report()
        return

    core.DB = PostgresDB
    core.Engine = PersistentEngine
    core.main()


if __name__ == "__main__":
    main()
