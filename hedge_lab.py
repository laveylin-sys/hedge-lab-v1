from __future__ import annotations

import argparse
import asyncio
import json
import math
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
import websockets

GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
MARKET_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


def now_ms() -> int:
    return int(time.time() * 1000)


def parse_jsonish(v: Any) -> Any:
    if isinstance(v, str):
        try:
            return json.loads(v)
        except Exception:
            return v
    return v


@dataclass
class Market:
    id: str
    condition_id: str
    slug: str
    question: str
    interval: str
    end_ts: float
    up_token: str
    down_token: str
    tick_size: float = 0.01
    taker_fee_rate: float = 0.07


@dataclass
class Book:
    token: str
    bid: float | None = None
    ask: float | None = None
    bid_size: float = 0.0
    ask_size: float = 0.0
    last_trade: float | None = None
    updated_ms: int = 0


@dataclass
class Leg:
    token: str
    outcome: str
    qty: float
    price: float
    ts_ms: int
    liquidity: str


@dataclass
class PendingQuote:
    market_id: str
    token: str
    outcome: str
    price: float
    usd: float
    posted_ms: int


@dataclass
class Pair:
    pair_id: int
    market_id: str
    interval: str
    opened_ms: int
    first: Leg
    second: Leg | None = None
    closed_ms: int | None = None

    @property
    def complete(self) -> bool:
        return self.second is not None

    def cost_per_pair(self) -> float | None:
        if not self.second:
            return None
        return self.first.price + self.second.price


class DB:
    def __init__(self, path: str, starting_capital: float):
        self.con = sqlite3.connect(path)
        self.con.row_factory = sqlite3.Row
        self.starting_capital = starting_capital
        self._init()

    def _init(self):
        self.con.executescript("""
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS runs(
          id INTEGER PRIMARY KEY, started_at TEXT NOT NULL, starting_capital REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS trades(
          id INTEGER PRIMARY KEY AUTOINCREMENT, ts_ms INTEGER NOT NULL, pair_id INTEGER,
          market_id TEXT NOT NULL, interval TEXT NOT NULL, outcome TEXT NOT NULL,
          side TEXT NOT NULL, price REAL NOT NULL, qty REAL NOT NULL,
          notional REAL NOT NULL, liquidity TEXT NOT NULL, reason TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS equity(
          ts_ms INTEGER PRIMARY KEY, cash REAL NOT NULL, inventory_value REAL NOT NULL,
          equity REAL NOT NULL, realized_pnl REAL NOT NULL, unrealized_pnl REAL NOT NULL,
          unhedged_exposure REAL NOT NULL, completed_pairs INTEGER NOT NULL,
          incomplete_pairs INTEGER NOT NULL, max_drawdown REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS pair_stats(
          pair_id INTEGER PRIMARY KEY, market_id TEXT NOT NULL, interval TEXT NOT NULL,
          first_fill_ms INTEGER NOT NULL, hedge_fill_ms INTEGER,
          time_to_hedge_ms INTEGER, complete INTEGER NOT NULL DEFAULT 0,
          qty REAL NOT NULL, total_cost REAL, locked_edge REAL
        );
        """)
        if self.con.execute("SELECT COUNT(*) FROM runs").fetchone()[0] == 0:
            self.con.execute(
                "INSERT INTO runs(id,started_at,starting_capital) VALUES(1,?,?)",
                (datetime.now(timezone.utc).isoformat(), self.starting_capital),
            )
            self.con.commit()

    def add_trade(self, pair_id: int, market: Market, leg: Leg, reason: str):
        self.con.execute("""
          INSERT INTO trades(ts_ms,pair_id,market_id,interval,outcome,side,price,qty,notional,liquidity,reason)
          VALUES(?,?,?,?,?,'BUY',?,?,?,?,?)
        """, (leg.ts_ms, pair_id, market.id, market.interval, leg.outcome, leg.price,
              leg.qty, leg.price*leg.qty, leg.liquidity, reason))
        self.con.commit()

    def open_pair(self, pair: Pair):
        self.con.execute("""
          INSERT OR REPLACE INTO pair_stats(pair_id,market_id,interval,first_fill_ms,complete,qty)
          VALUES(?,?,?,?,0,?)
        """, (pair.pair_id, pair.market_id, pair.interval, pair.opened_ms, pair.first.qty))
        self.con.commit()

    def close_pair(self, pair: Pair):
        assert pair.second is not None
        tth = pair.second.ts_ms - pair.first.ts_ms
        total_cost = pair.first.price + pair.second.price
        edge = 1.0 - total_cost
        self.con.execute("""
          UPDATE pair_stats SET hedge_fill_ms=?,time_to_hedge_ms=?,complete=1,total_cost=?,locked_edge=?
          WHERE pair_id=?
        """, (pair.second.ts_ms, tth, total_cost, edge, pair.pair_id))
        self.con.commit()

    def equity_row(self, **x: Any):
        self.con.execute("""
          INSERT OR REPLACE INTO equity(ts_ms,cash,inventory_value,equity,realized_pnl,unrealized_pnl,
          unhedged_exposure,completed_pairs,incomplete_pairs,max_drawdown)
          VALUES(:ts_ms,:cash,:inventory_value,:equity,:realized_pnl,:unrealized_pnl,
          :unhedged_exposure,:completed_pairs,:incomplete_pairs,:max_drawdown)
        """, x)
        self.con.commit()


class Discovery:
    def __init__(self):
        self.s = requests.Session()

    def active_btc(self) -> list[Market]:
        # Gamma's public list endpoint changes ordering over time. Pull a broad active slice,
        # then identify current BTC Up/Down markets by slug/question and token metadata.
        r = self.s.get(f"{GAMMA}/markets", params={"active": "true", "closed": "false", "limit": 500}, timeout=20)
        r.raise_for_status()
        out: list[Market] = []
        now = time.time()
        for m in r.json():
            slug = str(m.get("slug") or "")
            q = str(m.get("question") or "")
            low = (slug + " " + q).lower()
            if "btc" not in low and "bitcoin" not in low:
                continue
            interval = "5m" if "5m" in low or "5 min" in low or "5-minute" in low else (
                "15m" if "15m" in low or "15 min" in low or "15-minute" in low else "")
            if not interval or ("up" not in low or "down" not in low):
                continue
            tids = parse_jsonish(m.get("clobTokenIds") or m.get("clob_token_ids") or [])
            outcomes = parse_jsonish(m.get("outcomes") or [])
            if not isinstance(tids, list) or len(tids) != 2 or not isinstance(outcomes, list) or len(outcomes) != 2:
                continue
            mapping = {str(o).lower(): str(t) for o, t in zip(outcomes, tids)}
            up = mapping.get("up") or mapping.get("yes")
            down = mapping.get("down") or mapping.get("no")
            if not up or not down:
                continue
            end_raw = m.get("endDate") or m.get("end_date")
            try:
                end_ts = datetime.fromisoformat(str(end_raw).replace("Z", "+00:00")).timestamp()
            except Exception:
                end_ts = now + (300 if interval == "5m" else 900)
            if end_ts < now - 30:
                continue
            condition_id = str(m.get("conditionId") or "")
            tick = float(m.get("orderPriceMinTickSize") or 0.01)
            fee_rate = 0.07
            if condition_id:
                try:
                    info = self.s.get(f"{CLOB}/clob-markets/{condition_id}", timeout=10).json()
                    tick = float(info.get("mts") or tick)
                    fd = info.get("fd") or {}
                    if fd.get("r") is not None:
                        fee_rate = float(fd["r"])
                except Exception:
                    pass
            out.append(Market(
                id=str(m.get("id") or m.get("conditionId") or slug),
                condition_id=condition_id, slug=slug, question=q,
                interval=interval, end_ts=end_ts, up_token=up, down_token=down,
                tick_size=tick, taker_fee_rate=fee_rate,
            ))
        out.sort(key=lambda x: x.end_ts)
        # One nearest-expiry live market per interval is enough for baseline V1.
        picked: dict[str, Market] = {}
        for m in out:
            picked.setdefault(m.interval, m)
        return list(picked.values())

    def get_books(self, market: Market) -> dict[str, Book]:
        r = self.s.post(f"{CLOB}/books", json=[{"token_id": market.up_token}, {"token_id": market.down_token}], timeout=10)
        r.raise_for_status()
        books: dict[str, Book] = {}
        for raw in r.json():
            bids = raw.get("bids") or []
            asks = raw.get("asks") or []
            best_bid = max(bids, key=lambda x: float(x["price"])) if bids else None
            best_ask = min(asks, key=lambda x: float(x["price"])) if asks else None
            tok = str(raw.get("asset_id"))
            books[tok] = Book(
                token=tok,
                bid=float(best_bid["price"]) if best_bid else None,
                ask=float(best_ask["price"]) if best_ask else None,
                bid_size=float(best_bid["size"]) if best_bid else 0.0,
                ask_size=float(best_ask["size"]) if best_ask else 0.0,
                last_trade=float(raw["last_trade_price"]) if raw.get("last_trade_price") else None,
                updated_ms=now_ms(),
            )
        return books


class Engine:
    def __init__(self, cfg: dict[str, Any], db: DB):
        self.cfg, self.db = cfg, db
        self.cash = float(cfg["starting_capital"])
        self.markets: dict[str, Market] = {}
        self.books: dict[str, Book] = {}
        self.token_map: dict[str, tuple[str, str]] = {}
        self.pairs: dict[int, Pair] = {}
        self.next_pair = 1
        self.peak_equity = self.cash
        self.max_dd = 0.0
        self.realized = 0.0
        self.last_action_ms: dict[str, int] = {}
        self.pending_quotes: dict[str, PendingQuote] = {}

    def install_market(self, m: Market, books: dict[str, Book]):
        self.markets[m.id] = m
        self.books.update(books)
        self.token_map[m.up_token] = (m.id, "UP")
        self.token_map[m.down_token] = (m.id, "DOWN")

    def exposure(self) -> float:
        return sum(p.first.price*p.first.qty for p in self.pairs.values() if not p.complete)

    def completed(self) -> int:
        return sum(1 for p in self.pairs.values() if p.complete)

    def incomplete(self) -> int:
        return sum(1 for p in self.pairs.values() if not p.complete)

    def pair_completion_rate(self) -> float:
        n = len(self.pairs)
        return self.completed()/n if n else 0.0

    def avg_tth(self) -> float | None:
        vals = [(p.second.ts_ms-p.first.ts_ms)/1000 for p in self.pairs.values() if p.second]
        return sum(vals)/len(vals) if vals else None

    def mtm(self) -> tuple[float, float, float]:
        inv = 0.0
        # Complete pairs are conservatively valued at their guaranteed $1 payout per matched share.
        for p in self.pairs.values():
            if p.complete:
                q = min(p.first.qty, p.second.qty if p.second else 0)
                inv += q
            else:
                b = self.books.get(p.first.token)
                mark = (b.bid if b and b.bid is not None else p.first.price)
                inv += p.first.qty * mark
        eq = self.cash + inv
        unreal = eq - float(self.cfg["starting_capital"]) - self.realized
        return inv, eq, unreal

    def snapshot(self):
        inv, eq, unreal = self.mtm()
        self.peak_equity = max(self.peak_equity, eq)
        dd = (self.peak_equity - eq) / self.peak_equity if self.peak_equity else 0
        self.max_dd = max(self.max_dd, dd)
        self.db.equity_row(
            ts_ms=now_ms(), cash=self.cash, inventory_value=inv, equity=eq,
            realized_pnl=self.realized, unrealized_pnl=unreal,
            unhedged_exposure=self.exposure(), completed_pairs=self.completed(),
            incomplete_pairs=self.incomplete(), max_drawdown=self.max_dd,
        )
        return eq

    def taker_fee(self, market: Market, qty: float, price: float, liquidity: str) -> float:
        if liquidity != "TAKER":
            return 0.0
        # V2 fee formula: shares * rate * p * (1-p), rounded to 5 decimals.
        return round(qty * market.taker_fee_rate * price * (1.0-price), 5)

    def _buy(self, market: Market, outcome: str, price: float, usd: float, liquidity: str, reason: str, pair: Pair | None = None) -> Leg | None:
        qty = usd / price if price > 0 else 0
        fee = self.taker_fee(market, qty, price, liquidity)
        cost = qty * price + fee
        if cost > self.cash or cost <= 0:
            return None
        token = market.up_token if outcome == "UP" else market.down_token
        leg = Leg(token, outcome, qty, price, now_ms(), liquidity)
        self.cash -= cost
        if pair is None:
            pid = self.next_pair; self.next_pair += 1
            pair = Pair(pid, market.id, market.interval, leg.ts_ms, leg)
            self.pairs[pid] = pair
            self.db.open_pair(pair)
        else:
            pair.second = leg; pair.closed_ms = leg.ts_ms
        self.db.add_trade(pair.pair_id, market, leg, reason)
        if pair.complete:
            # Pair is a synthetic $1 claim. Realize locked edge immediately for comparable accounting.
            q = min(pair.first.qty, pair.second.qty)
            locked = q * (1 - pair.first.price - pair.second.price) - self.taker_fee(market, q, pair.first.price, pair.first.liquidity) - self.taker_fee(market, q, pair.second.price, pair.second.liquidity)
            self.realized += locked
            self.db.close_pair(pair)
        return leg

    def evaluate_market(self, market: Market):
        up, dn = self.books.get(market.up_token), self.books.get(market.down_token)
        if not up or not dn or up.ask is None or dn.ask is None:
            return
        t = now_ms()
        if t - min(up.updated_ms, dn.updated_ms) > int(self.cfg["stale_book_sec"]*1000):
            return
        if t - self.last_action_ms.get(market.id, 0) < 1000:
            return

        # 1) Complete existing one-leg inventory first. Hedge can cross only when pair still locks configured edge.
        for p in [x for x in self.pairs.values() if x.market_id == market.id and not x.complete]:
            other_outcome = "DOWN" if p.first.outcome == "UP" else "UP"
            b = dn if other_outcome == "DOWN" else up
            # Include taker fee in the final decision below; price cap is only a fast pre-filter.
            max_hedge = 1.0 - p.first.price - float(self.cfg["min_locked_edge"])
            age_s = (t - p.first.ts_ms)/1000
            emergency = age_s >= float(self.cfg["hedge_timeout_sec"])
            # Normal hedge requires locked edge. Emergency hedge may accept a small loss to cap exposure.
            cap = max_hedge if not emergency else min(0.999, max_hedge + 0.015)
            if b.ask is not None and b.ask <= cap:
                q = p.first.qty
                fee1 = self.taker_fee(market, q, p.first.price, p.first.liquidity)
                fee2 = self.taker_fee(market, q, b.ask, "TAKER")
                net_edge = q*(1.0-p.first.price-b.ask)-fee1-fee2
                min_edge_dollars = q*float(self.cfg["min_locked_edge"])
                if net_edge >= min_edge_dollars or emergency:
                    usd = q * b.ask
                    self._buy(market, other_outcome, b.ask, usd, "TAKER", "hedge_complete" if not emergency else "timeout_hedge", p)
                    self.last_action_ms[market.id] = t
                    return

        if self.exposure() >= float(self.cfg["max_unhedged_usd"]):
            return

        # 2) Atomic pair opportunity at displayed asks. This is the cleanest baseline edge.
        pair_sum = up.ask + dn.ask
        usd = float(self.cfg["pair_size_usd"])
        q = usd / up.ask if up.ask > 0 else 0
        pair_fees = self.taker_fee(market, q, up.ask, "TAKER") + self.taker_fee(market, q, dn.ask, "TAKER")
        net_edge_per_share = (1.0 - pair_sum) - (pair_fees/q if q else 0.0)
        if net_edge_per_share >= float(self.cfg["min_locked_edge"]):
            if self.cash >= 2*usd + pair_fees:
                first = self._buy(market, "UP", up.ask, usd, "TAKER", "atomic_pair_first")
                if first:
                    p = self.pairs[max(self.pairs)]
                    self._buy(market, "DOWN", dn.ask, p.first.qty*dn.ask, "TAKER", "atomic_pair_second", p)
                    self.last_action_ms[market.id] = t
            return

        # 3) Maker-first baseline. Bid one tick inside the best bid on the cheaper leg while preserving hedge room.
        candidates = []
        for outcome, b, other in [("UP", up, dn), ("DOWN", dn, up)]:
            if b.bid is None or other.ask is None:
                continue
            maker_px = min((b.bid + float(self.cfg["maker_improvement"])), b.ask - market.tick_size if b.ask else 0.99)
            maker_px = max(market.tick_size, math.floor(maker_px/market.tick_size)*market.tick_size)
            implied_edge = 1.0 - maker_px - other.ask
            candidates.append((implied_edge, outcome, maker_px, b))
        if not candidates:
            return
        edge, outcome, maker_px, b = max(candidates)
        if edge < float(self.cfg["min_locked_edge"]):
            return

        # Conservative paper fill: post a simulated maker quote now. It may only fill on a FUTURE
        # last_trade_price event at/below our bid; current/stale prints never count.
        existing = self.pending_quotes.get(market.id)
        token = market.up_token if outcome == "UP" else market.down_token
        if (existing is None or existing.token != token or abs(existing.price-maker_px) >= market.tick_size):
            self.pending_quotes[market.id] = PendingQuote(market.id, token, outcome, maker_px, float(self.cfg["pair_size_usd"]), t)

    def on_event(self, evt: dict[str, Any]):
        typ = evt.get("event_type")
        if typ == "book":
            tok = str(evt.get("asset_id")); b = self.books.setdefault(tok, Book(tok))
            bids, asks = evt.get("bids") or [], evt.get("asks") or []
            bb = max(bids, key=lambda x: float(x["price"])) if bids else None
            ba = min(asks, key=lambda x: float(x["price"])) if asks else None
            b.bid = float(bb["price"]) if bb else None; b.bid_size = float(bb["size"]) if bb else 0
            b.ask = float(ba["price"]) if ba else None; b.ask_size = float(ba["size"]) if ba else 0
            b.updated_ms = int(evt.get("timestamp") or now_ms())
        elif typ == "price_change":
            for ch in evt.get("price_changes") or []:
                tok = str(ch.get("asset_id")); b = self.books.setdefault(tok, Book(tok))
                if ch.get("best_bid") is not None: b.bid = float(ch["best_bid"])
                if ch.get("best_ask") is not None: b.ask = float(ch["best_ask"])
                b.updated_ms = int(evt.get("timestamp") or now_ms())
        elif typ == "last_trade_price":
            tok = str(evt.get("asset_id")); b = self.books.setdefault(tok, Book(tok))
            trade_ms = int(evt.get("timestamp") or now_ms())
            b.last_trade = float(evt["price"]); b.updated_ms = trade_ms
            if tok in self.token_map:
                market_id, _ = self.token_map[tok]
                q = self.pending_quotes.get(market_id)
                if q and q.token == tok and trade_ms > q.posted_ms and b.last_trade <= q.price:
                    market = self.markets[market_id]
                    if self.exposure() + q.usd <= float(self.cfg["max_unhedged_usd"]):
                        self._buy(market, q.outcome, q.price, q.usd, "MAKER", "trade_through_fill")
                        self.last_action_ms[market_id] = trade_ms
                    self.pending_quotes.pop(market_id, None)
        tok = str(evt.get("asset_id") or "")
        if tok in self.token_map:
            market_id, _ = self.token_map[tok]
            self.evaluate_market(self.markets[market_id])

    def status(self) -> str:
        eq = self.snapshot()
        pnl = eq - float(self.cfg["starting_capital"])
        tth = self.avg_tth()
        return (f"Equity ${eq:,.2f} | P&L {pnl:+,.2f} | Pairs {self.completed()}/{len(self.pairs)} "
                f"({self.pair_completion_rate()*100:.1f}%) | Avg TTH {tth:.2f}s" if tth is not None else
                f"Equity ${eq:,.2f} | P&L {pnl:+,.2f} | Pairs {self.completed()}/{len(self.pairs)} "
                f"({self.pair_completion_rate()*100:.1f}%) | Avg TTH --") + \
               f" | Unhedged ${self.exposure():,.2f} | MaxDD {self.max_dd*100:.2f}%"


async def run_live(cfg: dict[str, Any]):
    db = DB(cfg["db_path"], float(cfg["starting_capital"]))
    disc = Discovery(); engine = Engine(cfg, db)
    markets = disc.active_btc()
    if not markets:
        raise RuntimeError("No active BTC 5m/15m markets discovered from Gamma API")
    for m in markets:
        engine.install_market(m, disc.get_books(m))
        print(f"TRACK {m.interval}: {m.question} | {m.slug}")
    tokens = [t for m in markets for t in (m.up_token, m.down_token)]
    print("HEDGE-LAB V1 | PAPER ONLY | starting capital $10,000")
    print(engine.status())

    async def reporter():
        while True:
            await asyncio.sleep(5)
            print(datetime.now().strftime("%H:%M:%S"), engine.status(), flush=True)

    async def ws_loop():
        while True:
            try:
                async with websockets.connect(MARKET_WS, ping_interval=None, close_timeout=5) as ws:
                    await ws.send(json.dumps({"assets_ids": tokens, "type": "market"}))
                    last_ping = time.monotonic()
                    while True:
                        timeout = max(0.1, 10 - (time.monotonic() - last_ping))
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
                            if raw == "PONG":
                                continue
                            msg = json.loads(raw)
                            if isinstance(msg, list):
                                for e in msg: engine.on_event(e)
                            elif isinstance(msg, dict): engine.on_event(msg)
                        except asyncio.TimeoutError:
                            await ws.send("PING"); last_ping = time.monotonic()
            except Exception as e:
                print(f"WS reconnect: {type(e).__name__}: {e}", flush=True)
                await asyncio.sleep(2)

    await asyncio.gather(ws_loop(), reporter())


def report(db_path: str):
    con = sqlite3.connect(db_path); con.row_factory = sqlite3.Row
    try:
        e = con.execute("SELECT * FROM equity ORDER BY ts_ms DESC LIMIT 1").fetchone()
    except sqlite3.OperationalError:
        print("No HEDGE-LAB database initialized yet.")
        return
    if not e:
        print("No equity snapshots yet."); return
    total = con.execute("SELECT COUNT(*) FROM pair_stats").fetchone()[0]
    completed = con.execute("SELECT COUNT(*) FROM pair_stats WHERE complete=1").fetchone()[0]
    tth = con.execute("SELECT AVG(time_to_hedge_ms) FROM pair_stats WHERE complete=1").fetchone()[0]
    print(json.dumps({
        "equity": e["equity"], "pnl": e["equity"]-10000.0,
        "pair_completion_rate": completed/total if total else 0,
        "avg_time_to_hedge_sec": (tth/1000) if tth is not None else None,
        "unhedged_exposure": e["unhedged_exposure"], "max_drawdown": e["max_drawdown"],
        "trades": con.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
    }, indent=2))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("command", choices=["run", "report"], nargs="?", default="run")
    ap.add_argument("--config", default="config.json")
    args = ap.parse_args()
    cfg = json.loads(Path(args.config).read_text())
    if args.command == "run": asyncio.run(run_live(cfg))
    else: report(cfg["db_path"])


if __name__ == "__main__":
    main()
