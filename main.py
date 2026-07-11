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

# =========================================================
# 실전 설정
# BUY1: 완성된 15분봉 종가가 이전 20봉 최고가 돌파
# BUY2+: 직전 BUY 가격에서 0.5 ATR씩 상승
# 리셋: 완성된 15분봉 종가가 이전 10봉 최저가 이탈
# 텔레그램: 15분봉 마감 후 BUY 신호만 한 번에 묶어서 전송
# =========================================================

UPBIT_REST = "https://api.upbit.com/v1"
UPBIT_WS = "wss://api.upbit.com/websocket/v1"
KST = ZoneInfo("Asia/Seoul")

CANDLE_MINUTES = int(os.getenv("CANDLE_MINUTES", "15"))
BREAKOUT_LENGTH = int(os.getenv("BREAKOUT_LENGTH", "20"))
EXIT_LENGTH = int(os.getenv("EXIT_LENGTH", "10"))
ATR_LENGTH = int(os.getenv("ATR_LENGTH", "20"))
ADD_ATR_MULTIPLIER = float(os.getenv("ADD_ATR_MULTIPLIER", "0.5"))
HISTORY_COUNT = int(os.getenv("HISTORY_COUNT", "200"))
REST_DELAY_SECONDS = float(os.getenv("REST_DELAY_SECONDS", "0.12"))
CLOSE_WAIT_SECONDS = int(os.getenv("CLOSE_WAIT_SECONDS", "12"))

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

SUPPORTED_MINUTES = {1, 3, 5, 10, 15, 30, 60, 240}
if CANDLE_MINUTES not in SUPPORTED_MINUTES:
    raise ValueError(f"CANDLE_MINUTES는 {sorted(SUPPORTED_MINUTES)} 중 하나여야 합니다.")
if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    raise RuntimeError("TELEGRAM_BOT_TOKEN과 TELEGRAM_CHAT_ID 환경변수가 필요합니다.")

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("upbit-turtle-scanner")


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
    def from_ws(cls, item: dict[str, Any]) -> "Candle":
        return cls(
            start=parse_kst(item["candle_date_time_kst"]),
            open=float(item["opening_price"]),
            high=float(item["high_price"]),
            low=float(item["low_price"]),
            close=float(item["trade_price"]),
            volume=float(item["candle_acc_trade_volume"]),
        )


@dataclass
class PositionState:
    active: bool = False
    buy_number: int = 0
    last_buy_price: float = 0.0
    unit_atr: float = 0.0
    last_processed_start: datetime | None = None


def parse_kst(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    return dt.replace(tzinfo=KST) if dt.tzinfo is None else dt.astimezone(KST)


def bucket_start(dt: datetime) -> datetime:
    total = dt.hour * 60 + dt.minute
    rounded = (total // CANDLE_MINUTES) * CANDLE_MINUTES
    return dt.replace(
        hour=rounded // 60,
        minute=rounded % 60,
        second=0,
        microsecond=0,
    )


def next_boundary(now: datetime | None = None) -> datetime:
    now = now or datetime.now(KST)
    current = bucket_start(now)
    return current + timedelta(minutes=CANDLE_MINUTES)


def format_krw(price: float) -> str:
    if price >= 100:
        return f"₩{price:,.0f}"
    if price >= 1:
        return f"₩{price:,.2f}".rstrip("0").rstrip(".")
    return f"₩{price:,.8f}".rstrip("0").rstrip(".")


def true_range(current: Candle, previous_close: float) -> float:
    return max(
        current.high - current.low,
        abs(current.high - previous_close),
        abs(current.low - previous_close),
    )


def calculate_atr(candles: list[Candle], length: int) -> float | None:
    if len(candles) < length + 1:
        return None
    selected = candles[-(length + 1):]
    values = [
        true_range(selected[i], selected[i - 1].close)
        for i in range(1, len(selected))
    ]
    return sum(values) / len(values) if values else None


class Scanner:
    def __init__(self) -> None:
        max_history = max(HISTORY_COUNT, BREAKOUT_LENGTH + ATR_LENGTH + 20)
        self.names: dict[str, str] = {}
        self.histories: dict[str, deque[Candle]] = defaultdict(
            lambda: deque(maxlen=max_history)
        )
        self.current: dict[str, Candle] = {}
        self.states: dict[str, PositionState] = defaultdict(PositionState)
        self.http: aiohttp.ClientSession | None = None
        self.stop_event = asyncio.Event()
        self.process_lock = asyncio.Lock()

    async def telegram(self, text: str) -> None:
        assert self.http is not None
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "disable_web_page_preview": True,
        }
        try:
            async with self.http.post(url, json=payload, timeout=20) as response:
                body = await response.text()
                if response.status != 200:
                    log.error("텔레그램 전송 실패 %s: %s", response.status, body)
        except Exception:
            log.exception("텔레그램 전송 오류")

    async def fetch_markets(self) -> list[str]:
        assert self.http is not None
        async with self.http.get(
            f"{UPBIT_REST}/market/all",
            params={"is_details": "true"},
            timeout=20,
        ) as response:
            response.raise_for_status()
            items = await response.json()

        markets: list[str] = []
        for item in items:
            market = item["market"]
            if market.startswith("KRW-"):
                markets.append(market)
                self.names[market] = item.get("korean_name") or market.split("-", 1)[1]
        return sorted(markets)

    async def fetch_history(self, market: str) -> list[Candle]:
        assert self.http is not None
        url = f"{UPBIT_REST}/candles/minutes/{CANDLE_MINUTES}"
        params = {"market": market, "count": min(HISTORY_COUNT, 200)}

        for attempt in range(6):
            try:
                async with self.http.get(url, params=params, timeout=25) as response:
                    if response.status == 429:
                        await asyncio.sleep(1.5 * (attempt + 1))
                        continue
                    response.raise_for_status()
                    items = await response.json()
                return [Candle.from_rest(x) for x in reversed(items)]
            except Exception:
                if attempt == 5:
                    raise
                await asyncio.sleep(1.5 * (attempt + 1))
        return []

    def simulate_state(self, candles: list[Candle]) -> PositionState:
        """재시작 후에도 최근 200봉을 재생해 BUY 번호 상태를 복원한다."""
        state = PositionState()
        processed: list[Candle] = []

        for candle in candles:
            needed = max(BREAKOUT_LENGTH, EXIT_LENGTH, ATR_LENGTH + 1)
            if len(processed) < needed:
                processed.append(candle)
                continue

            previous_breakout_high = max(
                c.high for c in processed[-BREAKOUT_LENGTH:]
            )
            previous_exit_low = min(c.low for c in processed[-EXIT_LENGTH:])
            atr = calculate_atr(processed, ATR_LENGTH)

            if atr is None or atr <= 0:
                processed.append(candle)
                continue

            if not state.active:
                if candle.close > previous_breakout_high:
                    state.active = True
                    state.buy_number = 1
                    state.last_buy_price = candle.close
                    state.unit_atr = atr
            else:
                if candle.close < previous_exit_low:
                    state = PositionState()
                else:
                    step = state.unit_atr * ADD_ATR_MULTIPLIER
                    if step > 0 and candle.close >= state.last_buy_price + step:
                        # 한 개의 15분봉에서는 BUY 번호를 최대 1단계만 증가시킨다.
                        state.buy_number += 1
                        state.last_buy_price += step

            state.last_processed_start = candle.start
            processed.append(candle)

        return state

    async def initialize(self, markets: list[str]) -> None:
        active_start = bucket_start(datetime.now(KST))

        for index, market in enumerate(markets, start=1):
            try:
                candles = await self.fetch_history(market)
                completed = [c for c in candles if c.start < active_start]
                ongoing = [c for c in candles if c.start == active_start]

                self.histories[market].extend(completed)
                self.states[market] = self.simulate_state(completed)

                if ongoing:
                    self.current[market] = ongoing[-1]
            except Exception as error:
                log.warning("%s 초기화 실패: %s", market, error)

            if index % 20 == 0 or index == len(markets):
                log.info("초기화 %s/%s", index, len(markets))
            await asyncio.sleep(REST_DELAY_SECONDS)

    def process_completed_candle(
        self,
        market: str,
        candle: Candle,
    ) -> list[tuple[int, float]]:
        history = self.histories[market]
        state = self.states[market]

        if state.last_processed_start and candle.start <= state.last_processed_start:
            return []

        needed = max(BREAKOUT_LENGTH, EXIT_LENGTH, ATR_LENGTH + 1)
        if len(history) < needed:
            history.append(candle)
            state.last_processed_start = candle.start
            return []

        previous = list(history)
        breakout_high = max(c.high for c in previous[-BREAKOUT_LENGTH:])
        exit_low = min(c.low for c in previous[-EXIT_LENGTH:])
        atr = calculate_atr(previous, ATR_LENGTH)
        signals: list[tuple[int, float]] = []

        if atr and atr > 0:
            if not state.active:
                if candle.close > breakout_high:
                    state.active = True
                    state.buy_number = 1
                    state.last_buy_price = candle.close
                    state.unit_atr = atr
                    signals.append((1, candle.close))
            else:
                if candle.close < exit_low:
                    # SELL 메시지는 보내지 않고 BUY 번호 상태만 초기화
                    self.states[market] = PositionState(
                        last_processed_start=candle.start
                    )
                    history.append(candle)
                    return []
                else:
                    step = state.unit_atr * ADD_ATR_MULTIPLIER
                    if step > 0 and candle.close >= state.last_buy_price + step:
                        # 한 개의 15분봉에서는 BUY 번호를 최대 1단계만 증가시킨다.
                        state.buy_number += 1
                        state.last_buy_price += step
                        signals.append((state.buy_number, candle.close))

        state.last_processed_start = candle.start
        history.append(candle)
        return signals

    def build_report(
        self,
        report_time: datetime,
        grouped: dict[int, list[tuple[str, float]]],
    ) -> str | None:
        if not grouped:
            return None

        lines = [
            "🪙 업비트 실시간 코인 스캐너",
            report_time.strftime("%Y-%m-%d %H:%M"),
            "",
        ]

        for buy_number in sorted(grouped):
            items = sorted(
                grouped[buy_number],
                key=lambda item: item[0],
            )
            lines.append(f"🚨 BUY{buy_number} ({len(items)})")
            lines.append("")
            for market, price in items:
                symbol = market.split("-", 1)[1]
                name = self.names.get(market, symbol)
                lines.append(f"{name} ({symbol})")
                lines.append(format_krw(price))
                lines.append("")

        return "\n".join(lines).rstrip()

    async def finalize_interval(self, boundary: datetime) -> None:
        async with self.process_lock:
            target_start = boundary - timedelta(minutes=CANDLE_MINUTES)
            grouped: dict[int, list[tuple[str, float]]] = defaultdict(list)

            for market, candle in list(self.current.items()):
                if candle.start != target_start:
                    continue

                signals = self.process_completed_candle(market, candle)
                for buy_number, price in signals:
                    grouped[buy_number].append((market, price))

            report = self.build_report(boundary, grouped)
            if report:
                await self.telegram(report)
                log.info(
                    "종합 BUY 알림 전송: %s",
                    {k: len(v) for k, v in grouped.items()},
                )
            else:
                log.info("%s 마감: 신규 BUY 신호 없음", boundary.strftime("%H:%M"))

    async def boundary_loop(self) -> None:
        while not self.stop_event.is_set():
            boundary = next_boundary()
            run_at = boundary + timedelta(seconds=CLOSE_WAIT_SECONDS)
            delay = max(0.0, (run_at - datetime.now(KST)).total_seconds())

            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=delay)
                return
            except asyncio.TimeoutError:
                pass

            await self.finalize_interval(boundary)

    async def handle_ws(self, item: dict[str, Any]) -> None:
        if "error" in item:
            raise RuntimeError(f"업비트 WebSocket 오류: {item['error']}")
        if not str(item.get("type", "")).startswith("candle."):
            return

        market = item.get("code")
        if not market:
            return

        candle = Candle.from_ws(item)
        self.current[market] = candle

    async def websocket_loop(self, markets: list[str]) -> None:
        request = [
            {"ticket": str(uuid.uuid4())},
            {
                "type": f"candle.{CANDLE_MINUTES}m",
                "codes": markets,
                "is_only_realtime": True,
            },
            {"format": "DEFAULT"},
        ]

        backoff = 2
        while not self.stop_event.is_set():
            try:
                async with websockets.connect(
                    UPBIT_WS,
                    ping_interval=30,
                    ping_timeout=20,
                    close_timeout=10,
                    max_size=None,
                ) as websocket:
                    await websocket.send(json.dumps(request))
                    log.info("WebSocket 연결 완료: %s개 마켓", len(markets))
                    backoff = 2

                    async for raw in websocket:
                        if self.stop_event.is_set():
                            return
                        if isinstance(raw, bytes):
                            raw = raw.decode("utf-8")
                        await self.handle_ws(json.loads(raw))

            except asyncio.CancelledError:
                raise
            except Exception as error:
                log.warning("WebSocket 재연결 대기: %s", error)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    async def run(self) -> None:
        timeout = aiohttp.ClientTimeout(total=30)
        headers = {"User-Agent": "upbit-turtle-scanner/2.0"}

        async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
            self.http = session
            markets = await self.fetch_markets()
            log.info("KRW 마켓 %s개 확인", len(markets))

            # 텔레그램 시작/초기화 메시지는 보내지 않는다.
            await self.initialize(markets)
            log.info("초기화 완료. BUY 신호만 종합 전송합니다.")

            ws_task = asyncio.create_task(self.websocket_loop(markets))
            boundary_task = asyncio.create_task(self.boundary_loop())

            done, pending = await asyncio.wait(
                {ws_task, boundary_task},
                return_when=asyncio.FIRST_EXCEPTION,
            )

            for task in pending:
                task.cancel()
            for task in done:
                exception = task.exception()
                if exception:
                    raise exception


async def main() -> None:
    scanner = Scanner()
    loop = asyncio.get_running_loop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, scanner.stop_event.set)
        except NotImplementedError:
            pass

    await scanner.run()


if __name__ == "__main__":
    asyncio.run(main())
