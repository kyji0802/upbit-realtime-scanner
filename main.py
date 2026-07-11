import asyncio
import json
import logging
import os
import signal
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import aiohttp
import websockets

# ============================================================
# 업비트 실시간 터틀 BUY 스캐너
#
# BUY1: 완성된 15분봉 종가가 이전 20봉 최고가 돌파
# BUY2: BUY1 가격 + BUY1 당시 ATR × 0.5 도달
# BUY3: BUY2 가격 + BUY1 당시 ATR × 0.5 도달
# BUY4: BUY3 가격 + BUY1 당시 ATR × 0.5 도달
# 리셋: 완성봉 종가가 이전 10봉 최저가 아래로 내려감
#
# 중요:
# - BUY 번호는 BUY4까지만
# - 한 코인은 한 개의 15분봉에서 최대 한 단계만 증가
# - 같은 보고서에 같은 코인이 여러 BUY 단계로 중복되지 않음
# - 준비/SELL/시작/초기화 메시지는 보내지 않음
# - Railway 재시작 시 BUY 상태는 초기화됨
# ============================================================

UPBIT_REST_URL = "https://api.upbit.com/v1"
UPBIT_WEBSOCKET_URL = "wss://api.upbit.com/websocket/v1"
KST = ZoneInfo("Asia/Seoul")

CANDLE_MINUTES = int(os.getenv("CANDLE_MINUTES", "15"))
BREAKOUT_LENGTH = int(os.getenv("BREAKOUT_LENGTH", "20"))
EXIT_LENGTH = int(os.getenv("EXIT_LENGTH", "10"))
ATR_LENGTH = int(os.getenv("ATR_LENGTH", "20"))
ADD_ATR_MULTIPLIER = float(os.getenv("ADD_ATR_MULTIPLIER", "0.5"))
MAX_BUY_LEVEL = int(os.getenv("MAX_BUY_LEVEL", "4"))

HISTORY_COUNT = int(os.getenv("HISTORY_COUNT", "80"))
REST_DELAY_SECONDS = float(os.getenv("REST_DELAY_SECONDS", "0.12"))
CLOSE_WAIT_SECONDS = int(os.getenv("CLOSE_WAIT_SECONDS", "12"))

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

SUPPORTED_MINUTES = {1, 3, 5, 10, 15, 30, 60, 240}

if CANDLE_MINUTES not in SUPPORTED_MINUTES:
    raise ValueError(
        f"CANDLE_MINUTES는 {sorted(SUPPORTED_MINUTES)} 중 하나여야 합니다."
    )

if MAX_BUY_LEVEL < 1:
    raise ValueError("MAX_BUY_LEVEL은 1 이상이어야 합니다.")

if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    raise RuntimeError(
        "TELEGRAM_BOT_TOKEN과 TELEGRAM_CHAT_ID 환경변수가 필요합니다."
    )

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("upbit-buy-scanner")


@dataclass
class Candle:
    start: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float

    @classmethod
    def from_rest(cls, item: dict[str, Any]) -> "Candle":
        return cls(
            start=parse_kst(item["candle_date_time_kst"]),
            open=float(item["opening_price"]),
            high=float(item["high_price"]),
            low=float(item["low_price"]),
            close=float(item["trade_price"]),
            volume=float(item["candle_acc_trade_volume"]),
        )

    @classmethod
    def from_websocket(cls, item: dict[str, Any]) -> "Candle":
        return cls(
            start=parse_kst(item["candle_date_time_kst"]),
            open=float(item["opening_price"]),
            high=float(item["high_price"]),
            low=float(item["low_price"]),
            close=float(item["trade_price"]),
            volume=float(item["candle_acc_trade_volume"]),
        )


@dataclass
class BuyState:
    active: bool = False
    level: int = 0
    last_buy_price: float = 0.0
    entry_atr: float = 0.0
    last_processed_candle: datetime | None = None


def parse_kst(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=KST)
    return parsed.astimezone(KST)


def candle_bucket_start(value: datetime) -> datetime:
    total_minutes = value.hour * 60 + value.minute
    bucket_minutes = (total_minutes // CANDLE_MINUTES) * CANDLE_MINUTES

    return value.replace(
        hour=bucket_minutes // 60,
        minute=bucket_minutes % 60,
        second=0,
        microsecond=0,
    )


def next_candle_boundary(now: datetime | None = None) -> datetime:
    now = now or datetime.now(KST)
    return candle_bucket_start(now) + timedelta(minutes=CANDLE_MINUTES)


def format_krw(price: float) -> str:
    if price >= 100:
        return f"₩{price:,.0f}"

    if price >= 1:
        return f"₩{price:,.2f}".rstrip("0").rstrip(".")

    return f"₩{price:,.8f}".rstrip("0").rstrip(".")


def calculate_true_range(current: Candle, previous_close: float) -> float:
    return max(
        current.high - current.low,
        abs(current.high - previous_close),
        abs(current.low - previous_close),
    )


def calculate_atr(candles: list[Candle], length: int) -> float | None:
    if len(candles) < length + 1:
        return None

    selected = candles[-(length + 1):]
    true_ranges: list[float] = []

    for index in range(1, len(selected)):
        true_ranges.append(
            calculate_true_range(
                selected[index],
                selected[index - 1].close,
            )
        )

    if not true_ranges:
        return None

    return sum(true_ranges) / len(true_ranges)


class UpbitBuyScanner:
    def __init__(self) -> None:
        history_size = max(
            HISTORY_COUNT,
            BREAKOUT_LENGTH + 5,
            EXIT_LENGTH + 5,
            ATR_LENGTH + 5,
        )

        self.http: aiohttp.ClientSession | None = None
        self.stop_event = asyncio.Event()
        self.process_lock = asyncio.Lock()

        self.names: dict[str, str] = {}
        self.histories: dict[str, deque[Candle]] = defaultdict(
            lambda: deque(maxlen=history_size)
        )
        self.current_candles: dict[str, Candle] = {}
        self.states: dict[str, BuyState] = defaultdict(BuyState)

    async def send_telegram(self, message: str) -> None:
        assert self.http is not None

        url = (
            f"https://api.telegram.org/"
            f"bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        )
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "disable_web_page_preview": True,
        }

        try:
            async with self.http.post(
                url,
                json=payload,
                timeout=20,
            ) as response:
                body = await response.text()

                if response.status != 200:
                    log.error(
                        "텔레그램 전송 실패 status=%s body=%s",
                        response.status,
                        body,
                    )
        except Exception:
            log.exception("텔레그램 전송 중 오류")

    async def fetch_markets(self) -> list[str]:
        assert self.http is not None

        async with self.http.get(
            f"{UPBIT_REST_URL}/market/all",
            params={"is_details": "true"},
            timeout=20,
        ) as response:
            response.raise_for_status()
            market_items = await response.json()

        markets: list[str] = []

        for item in market_items:
            market = item["market"]

            if not market.startswith("KRW-"):
                continue

            markets.append(market)
            self.names[market] = (
                item.get("korean_name")
                or market.split("-", 1)[1]
            )

        return sorted(markets)

    async def fetch_recent_candles(self, market: str) -> list[Candle]:
        assert self.http is not None

        url = (
            f"{UPBIT_REST_URL}/candles/minutes/"
            f"{CANDLE_MINUTES}"
        )
        params = {
            "market": market,
            "count": min(HISTORY_COUNT, 200),
        }

        for attempt in range(6):
            try:
                async with self.http.get(
                    url,
                    params=params,
                    timeout=25,
                ) as response:
                    if response.status == 429:
                        await asyncio.sleep(1.5 * (attempt + 1))
                        continue

                    response.raise_for_status()
                    items = await response.json()

                return [
                    Candle.from_rest(item)
                    for item in reversed(items)
                ]

            except Exception:
                if attempt == 5:
                    raise

                await asyncio.sleep(1.5 * (attempt + 1))

        return []

    async def initialize_market_data(
        self,
        markets: list[str],
    ) -> None:
        active_bucket = candle_bucket_start(datetime.now(KST))

        for index, market in enumerate(markets, start=1):
            try:
                candles = await self.fetch_recent_candles(market)

                completed = [
                    candle
                    for candle in candles
                    if candle.start < active_bucket
                ]
                ongoing = [
                    candle
                    for candle in candles
                    if candle.start == active_bucket
                ]

                self.histories[market].extend(completed)

                if ongoing:
                    self.current_candles[market] = ongoing[-1]

                # 재시작 시 BUY 상태는 반드시 초기화한다.
                self.states[market] = BuyState()

            except Exception as error:
                log.warning(
                    "%s 초기 데이터 조회 실패: %s",
                    market,
                    error,
                )

            if index % 20 == 0 or index == len(markets):
                log.info(
                    "초기화 %s/%s",
                    index,
                    len(markets),
                )

            await asyncio.sleep(REST_DELAY_SECONDS)

    def evaluate_completed_candle(
        self,
        market: str,
        candle: Candle,
    ) -> tuple[int, float] | None:
        history = self.histories[market]
        state = self.states[market]

        # 동일한 완성봉을 두 번 처리하지 않는다.
        if (
            state.last_processed_candle is not None
            and candle.start <= state.last_processed_candle
        ):
            return None

        minimum_history = max(
            BREAKOUT_LENGTH,
            EXIT_LENGTH,
            ATR_LENGTH + 1,
        )

        if len(history) < minimum_history:
            history.append(candle)
            state.last_processed_candle = candle.start
            return None

        previous_candles = list(history)

        breakout_high = max(
            item.high
            for item in previous_candles[-BREAKOUT_LENGTH:]
        )
        exit_low = min(
            item.low
            for item in previous_candles[-EXIT_LENGTH:]
        )
        atr = calculate_atr(
            previous_candles,
            ATR_LENGTH,
        )

        signal: tuple[int, float] | None = None

        if atr is not None and atr > 0:
            if not state.active:
                if candle.close > breakout_high:
                    state.active = True
                    state.level = 1
                    state.last_buy_price = candle.close
                    state.entry_atr = atr
                    signal = (1, candle.close)

            else:
                if candle.close < exit_low:
                    # SELL 알림은 보내지 않고 상태만 리셋한다.
                    state.active = False
                    state.level = 0
                    state.last_buy_price = 0.0
                    state.entry_atr = 0.0

                elif state.level < MAX_BUY_LEVEL:
                    next_buy_price = (
                        state.last_buy_price
                        + state.entry_atr * ADD_ATR_MULTIPLIER
                    )

                    if candle.close >= next_buy_price:
                        # 한 개의 완성봉에서는 정확히 한 단계만 증가한다.
                        state.level += 1
                        state.last_buy_price = candle.close
                        signal = (state.level, candle.close)

        state.last_processed_candle = candle.start
        history.append(candle)

        return signal

    def build_report(
        self,
        report_time: datetime,
        grouped_signals: dict[int, list[tuple[str, float]]],
    ) -> str | None:
        valid_levels = [
            level
            for level in sorted(grouped_signals)
            if 1 <= level <= MAX_BUY_LEVEL
        ]

        if not valid_levels:
            return None

        lines = [
            "🪙 업비트 실시간 코인 스캐너",
            report_time.strftime("%Y-%m-%d %H:%M"),
            "",
        ]

        for level in valid_levels:
            items = sorted(
                grouped_signals[level],
                key=lambda item: item[0],
            )

            lines.append(f"🚨 BUY{level} ({len(items)})")
            lines.append("")

            for market, price in items:
                symbol = market.split("-", 1)[1]
                korean_name = self.names.get(market, symbol)

                lines.append(f"{korean_name} ({symbol})")
                lines.append(format_krw(price))
                lines.append("")

        return "\n".join(lines).rstrip()

    async def finalize_interval(
        self,
        boundary: datetime,
    ) -> None:
        async with self.process_lock:
            target_start = (
                boundary
                - timedelta(minutes=CANDLE_MINUTES)
            )

            grouped_signals: dict[
                int,
                list[tuple[str, float]],
            ] = defaultdict(list)

            # market별로 정확히 한 번만 처리한다.
            for market, candle in list(
                self.current_candles.items()
            ):
                if candle.start != target_start:
                    continue

                signal = self.evaluate_completed_candle(
                    market,
                    candle,
                )

                if signal is None:
                    continue

                level, price = signal
                grouped_signals[level].append(
                    (market, price)
                )

            report = self.build_report(
                boundary,
                grouped_signals,
            )

            if report:
                await self.send_telegram(report)
                log.info(
                    "종합 BUY 알림 전송: %s",
                    {
                        level: len(items)
                        for level, items
                        in grouped_signals.items()
                    },
                )
            else:
                log.info(
                    "%s 마감: 신규 BUY 신호 없음",
                    boundary.strftime("%H:%M"),
                )

    async def boundary_loop(self) -> None:
        while not self.stop_event.is_set():
            boundary = next_candle_boundary()
            run_at = (
                boundary
                + timedelta(seconds=CLOSE_WAIT_SECONDS)
            )
            delay = max(
                0.0,
                (
                    run_at
                    - datetime.now(KST)
                ).total_seconds(),
            )

            try:
                await asyncio.wait_for(
                    self.stop_event.wait(),
                    timeout=delay,
                )
                return
            except asyncio.TimeoutError:
                pass

            await self.finalize_interval(boundary)

    async def handle_websocket_message(
        self,
        item: dict[str, Any],
    ) -> None:
        if "error" in item:
            raise RuntimeError(
                f"업비트 WebSocket 오류: {item['error']}"
            )

        if not str(
            item.get("type", "")
        ).startswith("candle."):
            return

        market = item.get("code")

        if not market:
            return

        self.current_candles[market] = (
            Candle.from_websocket(item)
        )

    async def websocket_loop(
        self,
        markets: list[str],
    ) -> None:
        request_payload = [
            {"ticket": str(uuid.uuid4())},
            {
                "type": f"candle.{CANDLE_MINUTES}m",
                "codes": markets,
                "is_only_realtime": True,
            },
            {"format": "DEFAULT"},
        ]

        reconnect_delay = 2

        while not self.stop_event.is_set():
            try:
                async with websockets.connect(
                    UPBIT_WEBSOCKET_URL,
                    ping_interval=30,
                    ping_timeout=20,
                    close_timeout=10,
                    max_size=None,
                ) as websocket:
                    await websocket.send(
                        json.dumps(request_payload)
                    )

                    log.info(
                        "WebSocket 연결 완료: %s개 마켓",
                        len(markets),
                    )

                    reconnect_delay = 2

                    async for raw_message in websocket:
                        if self.stop_event.is_set():
                            return

                        if isinstance(raw_message, bytes):
                            raw_message = raw_message.decode(
                                "utf-8"
                            )

                        await self.handle_websocket_message(
                            json.loads(raw_message)
                        )

            except asyncio.CancelledError:
                raise

            except Exception as error:
                log.warning(
                    "WebSocket 연결 오류: %s",
                    error,
                )

                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(
                    reconnect_delay * 2,
                    60,
                )

    async def run(self) -> None:
        timeout = aiohttp.ClientTimeout(total=30)
        headers = {
            "User-Agent": "upbit-buy-scanner/3.0"
        }

        async with aiohttp.ClientSession(
            timeout=timeout,
            headers=headers,
        ) as session:
            self.http = session

            markets = await self.fetch_markets()
            log.info(
                "KRW 마켓 %s개 확인",
                len(markets),
            )

            await self.initialize_market_data(markets)

            log.info(
                "초기화 완료. BUY1~BUY%s 신호만 종합 전송합니다.",
                MAX_BUY_LEVEL,
            )

            websocket_task = asyncio.create_task(
                self.websocket_loop(markets)
            )
            boundary_task = asyncio.create_task(
                self.boundary_loop()
            )

            done, pending = await asyncio.wait(
                {
                    websocket_task,
                    boundary_task,
                },
                return_when=asyncio.FIRST_EXCEPTION,
            )

            for task in pending:
                task.cancel()

            for task in done:
                exception = task.exception()

                if exception is not None:
                    raise exception


async def main() -> None:
    scanner = UpbitBuyScanner()
    loop = asyncio.get_running_loop()

    for sig in (
        signal.SIGINT,
        signal.SIGTERM,
    ):
        try:
            loop.add_signal_handler(
                sig,
                scanner.stop_event.set,
            )
        except NotImplementedError:
            pass

    await scanner.run()


if __name__ == "__main__":
    asyncio.run(main())
