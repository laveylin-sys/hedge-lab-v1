# HEDGE-LAB V1

Independent Polymarket BTC 5m/15m hedge / market-making paper-trading baseline.

## Hard constraints
- Starting virtual capital: **US$10,000**
- **Paper only**: no private key, API key, order signing, or live order endpoint
- Independent from Directional V4: separate process and state
- Live metrics: Equity, P&L, trade log, Pair Completion Rate, Time-to-Hedge, unhedged exposure, Max Drawdown

## Data path
1. Gamma REST discovers current BTC Up/Down 5m/15m markets and token IDs.
2. CLOB `/books` seeds both outcome order books.
3. Public Market WebSocket streams `book`, `price_change`, and `last_trade_price` events.
4. `hedge_lab.py` uses SQLite for the original local baseline.
5. `persistent_hedge_lab.py` uses PostgreSQL when Render provides `DATABASE_URL`, and restores engine state after worker restarts.

## Baseline execution logic
1. **Hedge-first:** incomplete pairs are always evaluated before opening new inventory.
2. **Atomic pair:** if displayed UP ask + DOWN ask <= `1 - min_locked_edge`, paper-buy both legs.
3. **Maker-first:** otherwise seek a maker entry that leaves enough room to buy the opposite ask and retain `min_locked_edge`.
4. **Conservative fill model:** a maker bid is not assumed filled simply because it is best bid. It fills only after an observed trade prints at/below the simulated bid (`trade_through`).
5. **Timeout hedge:** after `hedge_timeout_sec`, permit a small edge concession to control one-leg inventory.

This intentionally prioritizes honest baseline measurement over flattering paper P&L.

## Run original SQLite mode
```bash
python -m venv .venv
# Windows: .venv\\Scripts\\activate
# macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
python hedge_lab.py run
```

Get current SQLite summary:
```bash
python hedge_lab.py report
```

## Run persistent PostgreSQL mode
Set `DATABASE_URL` in the environment, then run:
```bash
python persistent_hedge_lab.py run
```

Get the persistent PostgreSQL summary:
```bash
python persistent_hedge_lab.py report
```

On Render, after `DATABASE_URL` is linked to the Background Worker, change the Start Command to:
```bash
python persistent_hedge_lab.py
```

**Important:** the first PostgreSQL activation starts a new persistent US$10,000 measurement baseline. The prior ephemeral SQLite run is not automatically migrated.

## Key config
- `pair_size_usd`: target dollars for first leg
- `min_locked_edge`: minimum pair edge per $1 payout before normal hedging
- `max_unhedged_usd`: cap on incomplete-leg cost basis
- `hedge_timeout_sec`: maximum preferred one-leg duration
- `paper_fill_model`: V1 uses `trade_through`

## V1 acceptance gate before HYBRID
Collect enough BTC 5m and 15m samples to compare independently against Directional V4. Do not merge strategy capital or databases. At minimum evaluate P&L, MaxDD, pair completion, TTH distribution, unhedged exposure-time, and results split by interval.
