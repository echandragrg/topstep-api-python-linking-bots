"""
Binance XAUUSDT liquidations -> Telegram.

Sends a Telegram message for every liquidation on Binance futures XAUUSDT.
Nothing else: no trading, no candles.

Setup (Windows):
    pip install requests websocket-client

    PowerShell:
        $env:TG_BOT_TOKEN = "your_token"
        $env:TG_CHAT_ID   = "your_chat_id"
        python liquidation_telegram.py

    Command Prompt:
        set TG_BOT_TOKEN=your_token
        set TG_CHAT_ID=your_chat_id
        python liquidation_telegram.py

Optional:
    SYMBOL=XAUUSDT      Binance futures symbol to watch
    MIN_USD=0           only alert if liquidation value >= this (0 = every liquidation)
"""

import json
import logging
import os
import queue
import sys
import threading
import time
from datetime import datetime

import requests
import websocket  # pip install websocket-client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("liq_telegram")

# ----------------------------- configuration ---------------------------------
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")
SYMBOL = os.getenv("SYMBOL", "XAUUSDT").upper()
MIN_USD = float(os.getenv("MIN_USD", "0"))

URL = f"wss://fstream.binance.com/market/stream?streams={SYMBOL.lower()}@forceOrder"

# ----------------------------- state -----------------------------------------
seen = set()
tg_queue: "queue.Queue[str]" = queue.Queue()


# ----------------------------- telegram --------------------------------------
def tg_worker():
    """Sends Telegram messages in the background so the websocket never blocks."""
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    while True:
        text = tg_queue.get()
        try:
            r = requests.post(url, data={"chat_id": TG_CHAT_ID, "text": text}, timeout=10)
            if r.status_code == 429:  # rate limited: wait as Telegram asks, retry once
                wait = r.json().get("parameters", {}).get("retry_after", 5)
                time.sleep(wait)
                r = requests.post(url, data={"chat_id": TG_CHAT_ID, "text": text}, timeout=10)
            if not r.ok:
                log.error("Telegram error %s: %s", r.status_code, r.text)
        except Exception as exc:
            log.error("Telegram send failed: %s", exc)
        time.sleep(1)  # Telegram allows roughly 1 message/sec per chat


def notify(text: str):
    tg_queue.put(text)


# ----------------------------- websocket handlers ----------------------------
def on_open(ws):
    log.info("Connected. Watching %s liquidations (min value $%.0f)", SYMBOL, MIN_USD)


def on_message(ws, message):
    try:
        packet = json.loads(message)
        data = packet.get("data", {})
        order = data.get("o", {})
        if not order or order.get("s") != SYMBOL:
            return

        # Binance reports the forced-order side, not the liquidated position side.
        # Forced SELL = a LONG was liquidated; forced BUY = a SHORT was liquidated.
        forced_side = order.get("S")
        if forced_side == "SELL":
            pos_side = "LONG"
        elif forced_side == "BUY":
            pos_side = "SHORT"
        else:
            return

        price = float(order.get("ap", 0))
        qty = float(order.get("l", 0))
        value = price * qty
        event_time = int(order.get("T", 0))

        # skip duplicates
        key = (event_time, forced_side, order.get("ap"), order.get("l"), order.get("z"))
        if key in seen:
            return
        seen.add(key)
        if len(seen) > 5000:
            seen.clear()
            seen.add(key)

        if value < MIN_USD:
            return

        when = datetime.fromtimestamp(event_time / 1000).strftime("%H:%M:%S")
        icon = "🔴" if pos_side == "LONG" else "🟢"
        text = (
            f"{icon} {SYMBOL} {pos_side} LIQUIDATED\n"
            f"Price: {price:,.2f}\n"
            f"Qty: {qty}\n"
            f"Value: ${value:,.2f}\n"
            f"Time: {when}"
        )
        log.info("%s liquidated @ %s | %s | $%.2f", pos_side, price, qty, value)
        notify(text)

    except (ValueError, KeyError, TypeError) as exc:
        log.error("Parsing error: %s", exc)


def on_error(ws, error):
    log.error("WebSocket error: %s", error)


def on_close(ws, status, message):
    log.warning("Connection closed: %s %s", status, message)


# ----------------------------- main ------------------------------------------
def main():
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        sys.exit("Set TG_BOT_TOKEN and TG_CHAT_ID environment variables first.")

    threading.Thread(target=tg_worker, daemon=True).start()
    notify(f"✅ Liquidation alerts started for {SYMBOL}")

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
        print("Stopped.")


if __name__ == "__main__":
    main()
