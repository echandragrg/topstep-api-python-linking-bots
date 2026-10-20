"""
Liquidation-signal bot.

Signal source : Binance futures XAUUSDT liquidations + 1-minute candle colour
Trades        : gold futures (MGC / GC) on ProjectX / TopstepX

Rules:
  - BUY  liquidation + GREEN candle -> BUY
  - SELL liquidation + RED candle   -> SELL
  - Stop-loss = signal candle low (BUY) / high (SELL)
  - Targets   = 1:3, 1:6, 1:10 of the risk
  - Signals are confirmed on candle CLOSE: liquidations are recorded while the
    1-minute candle is open, and the trade is only taken when the candle closes
    with the matching colour (BUY liq + GREEN close, SELL liq + RED close).
    Entry, stop-loss and targets are then built from the CLOSED candle.
  - No cooldown: every confirmed signal is acted on
  - Opposite signal while in a trade: close the whole position, cancel the
    old stop/target orders, then open the new position (reversal)
  - Same-direction signal while in a trade: skipped (no stacking)

Needs signal_bot.py in the same folder (it provides the ProjectX client).

Setup:
    pip install requests websocket-client
    export PX_USERNAME=... PX_API_KEY=...
    export PX_SYMBOL="MGC"          # MGC = micro gold ($1/tick), GC = full gold ($10/tick)
    export DRY_RUN="true"           # "false" to send real orders
    python liquidation_bot.py

Optional env vars:
    ORDER_SIZE=1          contracts per order (per target leg if SPLIT_TARGETS)
    SPLIT_TARGETS=true    true = 3 separate orders, one per TP (1:3, 1:6, 1:10)
                          false = one order with a single TP at 1:3
    MAX_RISK_USD=100      skip any new entry whose stop-loss risk exceeds this
"""

import json
import logging
import os
import sys
import threading
import time
from datetime import datetime

import websocket  # pip install websocket-client

from signal_bot import (  # reuses the ProjectX client + your credentials/env
    ACCOUNT_NAME,
    API_KEY,
    BASE_URL,
    DRY_RUN,
    POS_LONG,
    SIDE_BUY,
    SIDE_SELL,
    USERNAME,
    ProjectXClient,
)

log = logging.getLogger("liquidation_bot")

# ----------------------------- configuration ---------------------------------
SOURCE_SYMBOL = "XAUUSDT"                         # Binance symbol (signal only)
TRADE_SYMBOL = os.getenv("PX_SYMBOL", "MGC")      # ProjectX contract search text
ORDER_SIZE = int(os.getenv("ORDER_SIZE", "1"))
SPLIT_TARGETS = os.getenv("SPLIT_TARGETS", "true").lower() == "true"
MAX_RISK_USD = float(os.getenv("MAX_RISK_USD", "100"))

R_MULTIPLES = (3, 6, 10)

URL = (
    "wss://fstream.binance.com/market/stream"
    f"?streams={SOURCE_SYMBOL.lower()}@forceOrder/"
    f"{SOURCE_SYMBOL.lower()}@kline_1m"
)

# ----------------------------- state -----------------------------------------
candle = {
    "open": None, "close": None, "high": None, "low": None,
    "volume": None, "closed": False, "color": None, "start": None,
}
seen = set()
pending: dict[int, set] = {}   # candle start (ms) -> liquidation sides seen during that candle

client: ProjectXClient | None = None
account: dict = {}
contract: dict = {}
order_lock = threading.Lock()  # one order routine at a time


# ----------------------------- strategy --------------------------------------
def calculate_trade_levels(signal, entry, high, low):
    """SL from the signal candle; TP1/TP2/TP3 at 1:3, 1:6, 1:10."""
    if signal == "BUY":
        stop_loss = low
        risk = entry - stop_loss
        if risk <= 0:
            return None
        tps = [entry + risk * m for m in R_MULTIPLES]
    else:
        stop_loss = high
        risk = stop_loss - entry
        if risk <= 0:
            return None
        tps = [entry - risk * m for m in R_MULTIPLES]

    return {
        "entry": entry, "stop_loss": stop_loss, "risk": risk,
        "tp1": tps[0], "tp2": tps[1], "tp3": tps[2],
    }


# ----------------------------- execution -------------------------------------
def execute_signal(signal: str, levels: dict):
    """Reverse if needed, then convert the signal's distances to ticks and place orders."""
    with order_lock:
        try:
            tick_size = float(contract["tickSize"])
            tick_value = float(contract["tickValue"])
            want_long = signal == "BUY"
            tag = "  [DRY RUN - no order sent]" if DRY_RUN else ""

            # 1) Existing position?
            pos = client.open_position(account["id"], contract["id"])
            if pos:
                is_long = pos["type"] == POS_LONG
                if is_long == want_long:
                    log.info("SKIPPED: already %s, same-direction %s signal ignored",
                             "long" if is_long else "short", signal)
                    return
                log.info("REVERSING: closing %s position (size %s) before %s%s",
                         "long" if is_long else "short", pos.get("size"), signal, tag)
                if not DRY_RUN:
                    client.close_position(account["id"], contract["id"])
                    n = client.cancel_all_orders(account["id"], contract["id"])
                    log.info("Cancelled %d leftover stop/target order(s)", n)
                    time.sleep(1)  # let the account update before entering

            # 2) Size the new entry
            stop_ticks = max(1, round(levels["risk"] / tick_size))
            legs = [(m, ORDER_SIZE) for m in R_MULTIPLES] if SPLIT_TARGETS \
                else [(R_MULTIPLES[0], ORDER_SIZE)]
            total_size = sum(size for _, size in legs)
            risk_usd = stop_ticks * tick_value * total_size

            if risk_usd > MAX_RISK_USD:
                log.warning("ENTRY SKIPPED: risk $%.2f exceeds MAX_RISK_USD $%.2f (%d ticks x %d contracts)",
                            risk_usd, MAX_RISK_USD, stop_ticks, total_size)
                return

            # 3) Enter
            side = SIDE_BUY if want_long else SIDE_SELL
            for mult, size in legs:
                target_ticks = max(1, round(levels["risk"] * mult / tick_size))
                log.info("%s %d x %s | stop %d ticks | target 1:%d (%d ticks) | risk ~$%.2f%s",
                         signal, size, contract["id"], stop_ticks, mult, target_ticks,
                         stop_ticks * tick_value * size, tag)
                if not DRY_RUN:
                    result = client.place_market(
                        account["id"], contract["id"], side, size, stop_ticks, target_ticks
                    )
                    log.info("Order placed: %s", result.get("orderId"))

            if not DRY_RUN:
                time.sleep(1)  # let the position register before the next signal is handled
        except Exception as exc:
            log.error("Execution error: %s", exc)


# ----------------------------- websocket handlers ----------------------------
def on_open(ws):
    print("=" * 60)
    print(f" {SOURCE_SYMBOL} LIQUIDATION SIGNALS -> {TRADE_SYMBOL} on ProjectX")
    print(f" DRY_RUN = {DRY_RUN}")
    print(" LONG LIQUIDATIONS (Binance forced SELL) + GREEN candle close = BUY")
    print(" SHORT LIQUIDATIONS (Binance forced BUY) + RED candle close = SELL")
    print(" Signals are confirmed when the 1-minute candle closes")
    print(" Opposite signal = close position, then reverse")
    print(f" Targets = {', '.join('1:%d' % m for m in R_MULTIPLES)} | no cooldown")
    print(f" Split targets = {SPLIT_TARGETS} | order size per TP = {ORDER_SIZE} | total contracts when split = {ORDER_SIZE * len(R_MULTIPLES) if SPLIT_TARGETS else ORDER_SIZE} | max risk = ${MAX_RISK_USD}")
    print("=" * 60)


def on_candle_close():
    """The 1-minute candle just closed: confirm (or drop) any liquidation seen during it."""
    start = candle["start"]
    sides = pending.pop(start, set())
    for old in [k for k in pending if k < start]:   # drop anything stale
        del pending[old]
    if not sides:
        return

    if candle["color"] == "GREEN" and "BUY" in sides:
        signal = "BUY"
    elif candle["color"] == "RED" and "SELL" in sides:
        signal = "SELL"
    else:
        signal = None

    when = datetime.fromtimestamp(start / 1000).strftime("%H:%M")
    log.info("Candle %s CLOSED %s O=%s H=%s L=%s C=%s | liquidations: %s | %s",
             when, candle["color"], candle["open"], candle["high"], candle["low"],
             candle["close"], "+".join(sorted(sides)), signal or "no signal")

    if signal:
        levels = calculate_trade_levels(signal, candle["close"], candle["high"], candle["low"])
        if levels is None:
            log.info("SIGNAL SKIPPED: invalid or zero risk.")
            return

        print(f"\n>>> {signal} SIGNAL (confirmed on close) <<<  {datetime.now():%H:%M:%S}")
        print(f"Entry {levels['entry']:.2f} | SL {levels['stop_loss']:.2f} | "
              f"risk {levels['risk']:.2f} | TP1 {levels['tp1']:.2f} | "
              f"TP2 {levels['tp2']:.2f} | TP3 {levels['tp3']:.2f}\n")

        # Run in a thread so the websocket keeps receiving while we call the API
        threading.Thread(target=execute_signal, args=(signal, levels), daemon=True).start()


def on_message(ws, message):
    try:
        packet = json.loads(message)
        stream = packet.get("stream", "")
        data = packet.get("data", {})

        # --- update the current 1-minute candle ---
        if "@kline_1m" in stream:
            k = data.get("k", {})
            candle["open"] = float(k["o"])
            candle["close"] = float(k["c"])
            candle["high"] = float(k["h"])
            candle["low"] = float(k["l"])
            candle["volume"] = float(k["v"])
            candle["closed"] = bool(k["x"])
            candle["start"] = int(k["t"])

            if candle["close"] > candle["open"]:
                candle["color"] = "GREEN"
            elif candle["close"] < candle["open"]:
                candle["color"] = "RED"
            else:
                candle["color"] = "DOJI"

            if candle["closed"]:
                on_candle_close()
            return

        # --- liquidation event: just record it, decide when the candle closes ---
        if "@forceOrder" in stream:
            order = data.get("o", {})
            if order.get("s") != SOURCE_SYMBOL:
                return

            # Binance reports the forced-order side, not the liquidated position side.
            # Forced SELL means a LONG was liquidated; forced BUY means a SHORT was liquidated.
            forced_side = order.get("S")
            if forced_side == "SELL":
                liquidation_signal = "BUY"   # long liquidation -> look for green candle
                liquidation_label = "LONG LIQUIDATION -> LIQUIDATIONS BUY"
            elif forced_side == "BUY":
                liquidation_signal = "SELL"  # short liquidation -> look for red candle
                liquidation_label = "SHORT LIQUIDATION -> LIQUIDATIONS SELL"
            else:
                return

            price = float(order.get("ap", 0))
            qty = float(order.get("l", 0))
            value = price * qty
            event_time = int(order.get("T", 0))

            key = (event_time, forced_side, order.get("ap"), order.get("l"), order.get("z"))
            if key in seen:
                return
            seen.add(key)
            if len(seen) > 5000:
                seen.clear()
                seen.add(key)

            event_candle = event_time // 60000 * 60000   # the minute this liquidation belongs to
            if candle["start"] is not None and event_candle < candle["start"]:
                log.info("%s @ %s arrived after its candle closed - ignored", liquidation_label, price)
                return

            pending.setdefault(event_candle, set()).add(liquidation_signal)
            log.info("%s @ %s (%.2f USDT) | candle now %s | waiting for the %s candle to close",
                     liquidation_label, price, value, candle["color"],
                     datetime.fromtimestamp(event_candle / 1000).strftime("%H:%M"))

    except (ValueError, KeyError, TypeError) as exc:
        log.error("Parsing error: %s", exc)


def on_error(ws, error):
    log.error("WebSocket error: %s", error)


def on_close(ws, status, message):
    log.warning("Connection closed: %s %s", status, message)


# ----------------------------- main ------------------------------------------
def main():
    global client, account, contract

    if not USERNAME or not API_KEY:
        sys.exit("Set PX_USERNAME and PX_API_KEY environment variables.")

    client = ProjectXClient(BASE_URL, USERNAME, API_KEY)
    client.login()
    account = client.get_account(ACCOUNT_NAME)
    contract = client.get_contract(TRADE_SYMBOL)
    log.info("Account: %s (id %s) | Contract: %s | tickSize=%s tickValue=%s",
             account["name"], account["id"], contract["id"],
             contract.get("tickSize"), contract.get("tickValue"))

    try:
        while True:  # reconnect automatically (Binance drops streams periodically)
            ws = websocket.WebSocketApp(
                URL, on_open=on_open, on_message=on_message,
                on_error=on_error, on_close=on_close,
            )
            ws.run_forever(ping_interval=20, ping_timeout=10)
            log.warning("Disconnected. Reconnecting in 5 seconds...")
            time.sleep(5)
    except KeyboardInterrupt:
        print("Bot stopped.")


if __name__ == "__main__":
    main()
