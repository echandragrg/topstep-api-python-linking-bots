
"""
Simple buy/sell signal bot for ProjectX Gateway API (TopstepX).

Strategy:
  - BUY: fast SMA crosses above slow SMA AND price is above the trend EMA.
  - SELL: fast SMA crosses below slow SMA AND price is below the trend EMA.
  - Trend EMA is calculated using 5-minute bars by default.
  - Optional counter-trend handling can close an existing position and stay flat.

Windows:
  - Prevents automatic system sleep while the bot is running.
  - Restores normal sleep behaviour when the bot exits.

SAFETY:
  - DRY_RUN is ON by default. No orders are sent in dry-run mode.
  - Test on a Practice account first.
  - Verify stop/target behaviour and API settings before live trading.
"""

import ctypes
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone

import requests


# ----------------------------- configuration ---------------------------------

BASE_URL = os.getenv(
    "PX_BASE_URL", "https://api.topstepx.com"
).rstrip("/")

USERNAME = os.getenv("PX_USERNAME", "")
API_KEY = os.getenv("PX_API_KEY", "")
SYMBOL = os.getenv("PX_SYMBOL", "MNQ")
ACCOUNT_NAME = os.getenv("PX_ACCOUNT_NAME", "")

# Safety: trading orders are disabled unless explicitly set to false.
DRY_RUN = os.getenv("DRY_RUN", "true").lower() != "false"

FAST = int(os.getenv("FAST_SMA", "9"))
SLOW = int(os.getenv("SLOW_SMA", "21"))

BAR_MINUTES = int(os.getenv("BAR_MINUTES", "1"))
ORDER_SIZE = int(os.getenv("ORDER_SIZE", "1"))

STOP_TICKS = int(os.getenv("STOP_TICKS", "40"))
TARGET_TICKS = int(os.getenv("TARGET_TICKS", "80"))

POLL_SECONDS = int(os.getenv("POLL_SECONDS", "5"))

# Larger-timeframe trend filter.
TREND_FILTER = os.getenv("TREND_FILTER", "true").lower() != "false"
TREND_EMA = int(os.getenv("TREND_EMA", "200"))
TREND_BAR_MINUTES = int(os.getenv("TREND_BAR_MINUTES", "5"))

CLOSE_ON_COUNTER_TREND = (
    os.getenv("CLOSE_ON_COUNTER_TREND", "false").lower() == "true"
)

# ProjectX API enum values.
SIDE_BUY, SIDE_SELL = 0, 1
ORDER_MARKET, ORDER_LIMIT, ORDER_STOP = 2, 1, 4
POS_LONG, POS_SHORT = 1, 2
UNIT_MINUTE = 2

# Windows sleep-prevention constants.
ES_CONTINUOUS = 0x80000000
ES_SYSTEM_REQUIRED = 0x00000001


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

log = logging.getLogger("signal_bot")


# -------------------------- Windows sleep prevention -------------------------


def prevent_sleep():
    """Prevent Windows from automatically sleeping while this bot runs."""
    if sys.platform != "win32":
        return

    try:
        result = ctypes.windll.kernel32.SetThreadExecutionState(
            ES_CONTINUOUS | ES_SYSTEM_REQUIRED
        )

        if result:
            log.info("Windows automatic sleep prevention enabled.")
        else:
            log.warning("Windows sleep prevention could not be enabled.")

    except Exception:
        log.exception("Failed to enable Windows sleep prevention.")


def allow_sleep():
    """Restore normal Windows sleep behaviour."""
    if sys.platform != "win32":
        return

    try:
        result = ctypes.windll.kernel32.SetThreadExecutionState(
            ES_CONTINUOUS
        )

        if result:
            log.info("Normal Windows sleep behaviour restored.")
        else:
            log.warning("Could not restore Windows sleep behaviour.")

    except Exception:
        log.exception("Failed to restore Windows sleep behaviour.")


# ----------------------------- API client ------------------------------------


class ProjectXClient:
    def __init__(self, base_url: str, username: str, api_key: str):
        self.base_url = base_url
        self.username = username
        self.api_key = api_key
        self.session = requests.Session()
        self.token_time = 0.0

    def login(self):
        response = self.session.post(
            f"{self.base_url}/api/Auth/loginKey",
            json={
                "userName": self.username,
                "apiKey": self.api_key,
            },
            timeout=15,
        )

        response.raise_for_status()
        data = response.json()

        if not data.get("success"):
            raise RuntimeError(
                f"Login failed: {data.get('errorMessage')}"
            )

        self.session.headers["Authorization"] = (
            f"Bearer {data['token']}"
        )

        self.token_time = time.time()
        log.info("Successfully logged in to ProjectX.")

    def post(self, path: str, payload: dict) -> dict:
        # Refresh the token proactively every 20 hours.
        if time.time() - self.token_time > 20 * 3600:
            self.login()

        for attempt in range(3):
            response = self.session.post(
                f"{self.base_url}{path}",
                json=payload,
                timeout=15,
            )

            if response.status_code == 401:
                self.login()
                continue

            if response.status_code == 429:
                time.sleep(2 * (attempt + 1))
                continue

            response.raise_for_status()
            data = response.json()

            if not data.get("success", True):
                raise RuntimeError(
                    f"{path} failed: {data.get('errorMessage')} "
                    f"(code {data.get('errorCode')})"
                )

            return data

        raise RuntimeError(f"{path} failed after retries")

    def get_account(self, name: str = "") -> dict:
        data = self.post(
            "/api/Account/search",
            {"onlyActiveAccounts": True},
        )

        accounts = [
            account
            for account in data["accounts"]
            if account.get("canTrade")
        ]

        if name:
            accounts = [
                account
                for account in accounts
                if account["name"] == name
            ]

        if not accounts:
            raise RuntimeError("No tradable account found.")

        return accounts[0]

    def get_contract(self, symbol: str) -> dict:
        data = self.post(
            "/api/Contract/search",
            {
                "live": False,
                "searchText": symbol,
            },
        )

        contracts = data["contracts"]
        active = [
            contract
            for contract in contracts
            if contract.get("activeContract")
        ]

        if not (active or contracts):
            raise RuntimeError(
                f"No contract found for symbol {symbol}."
            )

        return (active or contracts)[0]

    def get_bars(
        self,
        contract_id: str,
        minutes: int,
        count: int,
        span_factor: float = 1.0,
    ) -> list[dict]:
        """Retrieve closed bars, ordered from oldest to newest."""

        end = datetime.now(timezone.utc)

        window = minutes * (count + 5) * span_factor
        start = end - timedelta(minutes=window)

        data = self.post(
            "/api/History/retrieveBars",
            {
                "contractId": contract_id,
                "live": False,
                "startTime": start.isoformat(),
                "endTime": end.isoformat(),
                "unit": UNIT_MINUTE,
                "unitNumber": minutes,
                "limit": int(window / minutes) + 5,
                "includePartialBar": False,
            },
        )

        bars = sorted(
            data.get("bars", []),
            key=lambda bar: bar["t"],
        )

        return bars[-count:]

    def open_position(
        self,
        account_id: int,
        contract_id: str,
    ):
        data = self.post(
            "/api/Position/searchOpen",
            {"accountId": account_id},
        )

        for position in data["positions"]:
            if position["contractId"] == contract_id:
                return position

        return None

    def close_position(
        self,
        account_id: int,
        contract_id: str,
    ):
        return self.post(
            "/api/Position/closeContract",
            {
                "accountId": account_id,
                "contractId": contract_id,
            },
        )

    def open_orders(
        self,
        account_id: int,
        contract_id: str | None = None,
    ) -> list[dict]:
        data = self.post(
            "/api/Order/searchOpen",
            {"accountId": account_id},
        )

        orders = data.get("orders", [])

        if contract_id:
            orders = [
                order
                for order in orders
                if order.get("contractId") == contract_id
            ]

        return orders

    def cancel_all_orders(
        self,
        account_id: int,
        contract_id: str,
    ) -> int:
        """Cancel open orders for the specified contract."""

        orders = self.open_orders(account_id, contract_id)

        for order in orders:
            self.post(
                "/api/Order/cancel",
                {
                    "accountId": account_id,
                    "orderId": order["id"],
                },
            )

        return len(orders)

    def place_market(
        self,
        account_id: int,
        contract_id: str,
        side: int,
        size: int,
        stop_ticks: int = 0,
        target_ticks: int = 0,
    ) -> dict:
        payload = {
            "accountId": account_id,
            "contractId": contract_id,
            "type": ORDER_MARKET,
            "side": side,
            "size": size,
        }

        # IMPORTANT: verify bracket tick direction and API schema
        # against the current ProjectX documentation and in sim.
        sign = 1 if side == SIDE_BUY else -1

        if stop_ticks:
            payload["stopLossBracket"] = {
                "ticks": -sign * stop_ticks,
                "type": ORDER_STOP,
            }

        if target_ticks:
            payload["takeProfitBracket"] = {
                "ticks": sign * target_ticks,
                "type": ORDER_LIMIT,
            }

        return self.post("/api/Order/place", payload)


# ----------------------------- strategy --------------------------------------


def sma(values: list[float], n: int) -> float:
    return sum(values[-n:]) / n


def ema(values: list[float], n: int) -> float:
    """EMA seeded with the SMA of the first n values."""

    if len(values) < n:
        raise ValueError("Not enough values to calculate EMA.")

    multiplier = 2 / (n + 1)
    current = sum(values[:n]) / n

    for value in values[n:]:
        current = (
            value * multiplier
            + current * (1 - multiplier)
        )

    return current


def trend_state(
    client: "ProjectXClient",
    contract_id: str,
    price: float,
):
    """Return trend direction and EMA value."""

    bars = client.get_bars(
        contract_id,
        TREND_BAR_MINUTES,
        TREND_EMA * 3,
        span_factor=2.5,
    )

    closes = [bar["c"] for bar in bars]

    if len(closes) < TREND_EMA:
        return None, None

    ema_value = ema(closes, TREND_EMA)

    if price > ema_value:
        return "UP", ema_value

    if price < ema_value:
        return "DOWN", ema_value

    return "FLAT", ema_value


def get_signal(closes: list[float]) -> str | None:
    """Return BUY or SELL when a closed-bar SMA crossover occurs."""

    if len(closes) < SLOW + 1:
        return None

    fast_now = sma(closes, FAST)
    slow_now = sma(closes, SLOW)

    fast_prev = sma(closes[:-1], FAST)
    slow_prev = sma(closes[:-1], SLOW)

    if fast_prev <= slow_prev and fast_now > slow_now:
        return "BUY"

    if fast_prev >= slow_prev and fast_now < slow_now:
        return "SELL"

    return None


# ----------------------------- execution -------------------------------------


def execute(
    client: ProjectXClient,
    account_id: int,
    contract_id: str,
    signal: str,
    entry_allowed: bool = True,
):
    position = client.open_position(account_id, contract_id)
    want_long = signal == "BUY"

    dry_run_tag = (
        " [DRY RUN - no order sent]"
        if DRY_RUN
        else ""
    )

    if position:
        is_long = position["type"] == POS_LONG

        if is_long == want_long:
            log.info(
                "Signal %s but already %s; skipping.",
                signal,
                "long" if is_long else "short",
            )
            return

        log.info(
            "Closing existing %s position%s.",
            "long" if is_long else "short",
            dry_run_tag,
        )

        if not DRY_RUN:
            client.close_position(account_id, contract_id)

            # Give the close request a moment before checking orders.
            time.sleep(1)

            cancelled = client.cancel_all_orders(
                account_id,
                contract_id,
            )

            log.info(
                "Cancelled %d leftover order(s).",
                cancelled,
            )

            # Verify that the old position has actually closed.
            remaining = client.open_position(
                account_id,
                contract_id,
            )

            if remaining:
                log.error(
                    "Position still appears open. "
                    "Skipping new entry for safety."
                )
                return

    if not entry_allowed:
        log.info(
            "Counter-trend %s: no new entry; staying flat.",
            signal,
        )
        return

    side = SIDE_BUY if want_long else SIDE_SELL

    log.info(
        "%s %d x %s (SL=%d ticks, TP=%d ticks)%s",
        signal,
        ORDER_SIZE,
        contract_id,
        STOP_TICKS,
        TARGET_TICKS,
        dry_run_tag,
    )

    if not DRY_RUN:
        result = client.place_market(
            account_id,
            contract_id,
            side,
            ORDER_SIZE,
            STOP_TICKS,
            TARGET_TICKS,
        )

        log.info("Order response: %s", result)


def handle_signal(
    client: ProjectXClient,
    account_id: int,
    contract_id: str,
    signal: str,
    price: float,
):
    """Apply the trend filter before executing a signal."""

    entry_allowed = True

    if TREND_FILTER:
        trend, ema_value = trend_state(
            client,
            contract_id,
            price,
        )

        if trend is None:
            log.warning(
                "Trend unavailable: need at least %d %d-minute bars. "
                "Skipping %s signal.",
                TREND_EMA,
                TREND_BAR_MINUTES,
                signal,
            )
            return

        entry_allowed = (
            (signal == "BUY" and trend == "UP")
            or (signal == "SELL" and trend == "DOWN")
        )

        relation = {
            "UP": "above",
            "DOWN": "below",
            "FLAT": "at",
        }[trend]

        log.info(
            "Trend: price %.2f is %s EMA%d (%dm) %.2f; "
            "%s is %s.",
            price,
            relation,
            TREND_EMA,
            TREND_BAR_MINUTES,
            ema_value,
            signal,
            "allowed" if entry_allowed else "counter-trend",
        )

        if not entry_allowed and not CLOSE_ON_COUNTER_TREND:
            log.info(
                "%s signal ignored because it conflicts with the trend.",
                signal,
            )
            return

    execute(
        client,
        account_id,
        contract_id,
        signal,
        entry_allowed,
    )


# ----------------------------- main loop -------------------------------------


def main():
    if not USERNAME or not API_KEY:
        sys.exit(
            "Set PX_USERNAME and PX_API_KEY environment variables."
        )

    if FAST < 1 or SLOW < 1 or FAST >= SLOW:
        sys.exit(
            "Invalid SMA settings: FAST_SMA must be positive "
            "and smaller than SLOW_SMA."
        )

    if ORDER_SIZE < 1:
        sys.exit("ORDER_SIZE must be at least 1.")

    if POLL_SECONDS < 1:
        sys.exit("POLL_SECONDS must be at least 1.")

    client = ProjectXClient(
        BASE_URL,
        USERNAME,
        API_KEY,
    )

    client.login()

    account = client.get_account(ACCOUNT_NAME)
    contract = client.get_contract(SYMBOL)

    log.info(
        "Account: %s (id %s) | Contract: %s | DRY_RUN=%s | "
        "Trend filter: %s",
        account["name"],
        account["id"],
        contract["id"],
        DRY_RUN,
        (
            f"EMA{TREND_EMA} on {TREND_BAR_MINUTES}-minute bars"
            if TREND_FILTER
            else "off"
        ),
    )

    if DRY_RUN:
        log.warning(
            "DRY RUN is enabled. No trading orders will be sent."
        )

    prevent_sleep()

    try:
        last_bar_time = None

        while True:
            try:
                bars = client.get_bars(
                    contract["id"],
                    BAR_MINUTES,
                    count=SLOW + 20,
                )

                if bars:
                    newest_bar_time = bars[-1]["t"]

                    # Process each newly observed closed bar once.
                    if newest_bar_time != last_bar_time:
                        first_run = last_bar_time is None
                        last_bar_time = newest_bar_time

                        closes = [
                            bar["c"]
                            for bar in bars
                        ]

                        signal = get_signal(closes)
                        latest_close = closes[-1]

                        log.info(
                            "New %d-minute bar %s | close=%s | signal=%s",
                            BAR_MINUTES,
                            newest_bar_time,
                            latest_close,
                            signal,
                        )

                        # Avoid acting on a crossover from an old bar
                        # when the bot first starts.
                        if signal and not first_run:
                            handle_signal(
                                client,
                                account["id"],
                                contract["id"],
                                signal,
                                latest_close,
                            )

                else:
                    log.warning("No bars returned by the API.")

            except KeyboardInterrupt:
                log.info("Bot stopped by user.")
                break

            except Exception:
                log.exception("Error in trading loop.")

            time.sleep(POLL_SECONDS)

    finally:
        # Restore normal Windows sleep settings on exit.
        allow_sleep()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("Bot stopped.")
