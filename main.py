import asyncio
import json
import logging
import os
import signal
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from statistics import mean
from typing import Any
from zoneinfo import ZoneInfo

import aiohttp
import websockets

# =========================
# 기본 설정
# =========================
UPBIT_REST = "https://api.upbit.com/v1"
UPBIT_WS = "wss://api.upbit.com/websocket/v1"
KST = ZoneInfo("Asia/Seoul")

CANDLE_MINUTES = int(os.getenv("CANDLE_MINUTES", "15"))
BREAKOUT_LENGTH = int(os.getenv("BREAKOUT_LENGTH", "20"))
BUY_VOLUME_RATIO = float(os.getenv("BUY_VOLUME_RATIO", "1.5"))
READY_DISTANCE = float(os.getenv("READY_DISTANCE", "0.01"))      # 1%
READY_VOLUME_RATIO = float(os.getenv("READY_VOLUME_RATIO", "1.2"))
REST_DELAY_SECONDS = float(os.getenv("REST_DELAY_SECONDS", "0.12"))

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

SUPPORTED_MINUTES = {1, 3, 5, 10, 15, 30, 60, 240}
if CANDLE_MINUTES not in SUPPORTED_MINUTES:
    raise ValueError(
        f"CANDLE_MINUTES는 {sorted(SUPPORTED_MINUTES)} 중 하나여야 합니다."
    )
if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    raise RuntimeError(
        "TELEGRAM_BOT_TOKEN과 TELEGRAM_CHAT_ID 환경변수를 입력하세요."
    )

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("upbit-scanner")


@dataclass
class Candle:
    start: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    trade_value: float

    @classmethod
    def from_rest(cls, item: dict[str, Any]) -> "Candle":
        return cls(
            start=parse_kst(item["candle_date_time_kst"]),
            open=float(item["opening_price"]),
            high=float(item["high_price"]),
            low=float(item["low_price"]),
            close=float(item["trade_price"]),
            volume=float(item["candle_acc_trade_volume"]),
            trade_value=float(item.get("candle_acc_trade_price", 0)),
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
            trade_value=float(item.get("candle_acc_trade_price", 0)),
        )


def parse_kst(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    return dt.replace(tzinfo=KST) if dt.tzinfo is None else dt.astimezone(KST)


def current_bucket_start(now: datetime | None = None) -> datetime:
    now = now or datetime.now(KST)
    total_minutes = now.hour * 60 + now.minute
    bucket_minutes = (total_minutes // CANDLE_MINUTES) * CANDLE_MINUTES
    return now.replace(
        hour=bucket_minutes // 60,
        minute=bucket_minutes % 60,
        second=0,
        microsecond=0,
    )


def format_krw(price: float) -> str:
    if price >= 100:
        return f"₩{price:,.0f}"
    if price >= 1:
        return f"₩{price:,.2f}".rstrip("0").rstrip(".")
    return f"₩{price:,.6f}".rstrip("0").rstrip(".")


class Scanner:
    def __init__(self) -> None:
        self.names: dict[str, str] = {}
        self.histories: dict[str, deque[Candle]] = defaultdict(
            lambda: deque(maxlen=max(60, BREAKOUT_LENGTH + 10))
        )
        self.current: dict[str, Candle] = {}
        self.buy_active: dict[str, bool] = defaultdict(bool)
        self.ready_sent_for_candle: set[tuple[str, datetime]] = set()
        self.stop_event = asyncio.Event()
        self.http: aiohttp.ClientSession | None = None

    async def send_telegram(self, text: str) -> None:
        assert self.http is not None
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "disable_web_page_preview": True,
        }
        try:
            async with self.http.post(url, json=payload, timeout=15) as response:
                body = await response.text()
                if response.status != 200:
                    log.error("텔레그램 전송 실패 %s: %s", response.status, body)
        except Exception:
            log.exception("텔레그램 전송 중 오류")

    async def fetch_markets(self) -> list[str]:
        assert self.http is not None
        url = f"{UPBIT_REST}/market/all"
        params = {"is_details": "true"}
        async with self.http.get(url, params=params, timeout=20) as response:
            response.raise_for_status()
            items = await response.json()

        markets = []
        for item in items:
            market = item["market"]
            if not market.startswith("KRW-"):
                continue
            # 거래지원 주의 종목도 시세 감시는 가능하므로 기본 포함
            markets.append(market)
            self.names[market] = item.get("korean_name") or market.split("-", 1)[1]

        markets.sort()
        return markets

    async def fetch_history(self, market: str) -> None:
        assert self.http is not None
        url = f"{UPBIT_REST}/candles/minutes/{CANDLE_MINUTES}"
        params = {"market": market, "count": max(30, BREAKOUT_LENGTH + 5)}

        for attempt in range(5):
            try:
                async with self.http.get(url, params=params, timeout=20) as response:
                    if response.status == 429:
                        await asyncio.sleep(1.5 * (attempt + 1))
                        continue
                    response.raise_for_status()
                    items = await response.json()
                break
            except Exception:
                if attempt == 4:
                    raise
                await asyncio.sleep(1.5 * (attempt + 1))
        else:
            return

        candles = [Candle.from_rest(item) for item in reversed(items)]
        active_start = current_bucket_start()

        # 현재 진행 중인 봉은 과거 완성봉 목록에서 제외
        completed = [c for c in candles if c.start < active_start]
        self.histories[market].extend(completed)

    async def initialize_histories(self, markets: list[str]) -> None:
        total = len(markets)
        for index, market in enumerate(markets, start=1):
            try:
                await self.fetch_history(market)
            except Exception as error:
                log.warning("%s 초기 캔들 조회 실패: %s", market, error)

            if index % 20 == 0 or index == total:
                log.info("초기 캔들 준비 %s/%s", index, total)

            # 공개 Candle REST 요청 제한을 넘지 않도록 간격 유지
            await asyncio.sleep(REST_DELAY_SECONDS)

    def previous_stats(self, market: str) -> tuple[float, float] | None:
        history = self.histories[market]
        if len(history) < BREAKOUT_LENGTH:
            return None

        previous = list(history)[-BREAKOUT_LENGTH:]
        breakout_high = max(c.high for c in previous)
        average_volume = mean(c.volume for c in previous)
        return breakout_high, average_volume

    async def check_ready(self, market: str, candle: Candle) -> None:
        stats = self.previous_stats(market)
        if stats is None:
            return

        breakout_high, average_volume = stats
        if breakout_high <= 0 or average_volume <= 0:
            return

        # 아직 돌파 전이면서 최고가 1% 이내
        distance = (breakout_high - candle.close) / breakout_high
        if not (0 <= distance <= READY_DISTANCE):
            return

        now = datetime.now(KST)
        elapsed = max(1.0, (now - candle.start).total_seconds())
        full_seconds = CANDLE_MINUTES * 60
        progress = min(1.0, elapsed / full_seconds)

        # 진행률을 고려한 예상 거래량
        projected_volume = candle.volume / max(progress, 0.10)
        projected_ratio = projected_volume / average_volume

        if projected_ratio < READY_VOLUME_RATIO:
            return

        key = (market, candle.start)
        if key in self.ready_sent_for_candle:
            return
        self.ready_sent_for_candle.add(key)

        # 메모리 증가 방지
        if len(self.ready_sent_for_candle) > 3000:
            cutoff = datetime.now(KST) - timedelta(days=2)
            self.ready_sent_for_candle = {
                item for item in self.ready_sent_for_candle if item[1] >= cutoff
            }

        symbol = market.split("-", 1)[1]
        name = self.names.get(market, symbol)
        message = (
            f"👀 실시간 준비\n\n"
            f"{name} ({symbol})\n"
            f"{format_krw(candle.close)}\n\n"
            f"돌파까지: {distance * 100:.2f}%\n"
            f"예상 거래량: 평균 대비 {projected_ratio:.2f}배\n"
            f"기준: {CANDLE_MINUTES}분봉"
        )
        await self.send_telegram(message)
        log.info("READY %s distance=%.3f%% volume=%.2fx", market, distance * 100, projected_ratio)

    async def finalize_candle(self, market: str, candle: Candle) -> None:
        stats = self.previous_stats(market)
        if stats is None:
            self.histories[market].append(candle)
            return

        breakout_high, average_volume = stats
        history = self.histories[market]
        previous_close = history[-1].close if history else 0
        volume_ratio = candle.volume / average_volume if average_volume > 0 else 0

        buy_condition = (
            candle.close > breakout_high
            and previous_close <= breakout_high
            and volume_ratio >= BUY_VOLUME_RATIO
        )

        if buy_condition and not self.buy_active[market]:
            self.buy_active[market] = True
            symbol = market.split("-", 1)[1]
            name = self.names.get(market, symbol)
            message = (
                f"🚨 BUY1 확정\n\n"
                f"{name} ({symbol})\n"
                f"{format_krw(candle.close)}\n\n"
                f"{BREAKOUT_LENGTH}봉 최고가 돌파\n"
                f"거래량: 평균 대비 {volume_ratio:.2f}배\n"
                f"{CANDLE_MINUTES}분봉 마감 확정\n"
                f"{candle.start:%Y-%m-%d %H:%M} KST"
            )
            await self.send_telegram(message)
            log.info("BUY1 %s close=%s volume=%.2fx", market, candle.close, volume_ratio)
        elif not buy_condition:
            # 조건이 해제되면 다음 돌파를 다시 알릴 수 있게 재무장
            self.buy_active[market] = False

        self.histories[market].append(candle)

    async def handle_message(self, item: dict[str, Any]) -> None:
        if "error" in item:
            raise RuntimeError(f"업비트 WebSocket 오류: {item['error']}")

        if not str(item.get("type", "")).startswith("candle."):
            return

        market = item.get("code")
        if not market:
            return

        incoming = Candle.from_ws(item)
        previous = self.current.get(market)

        if previous is None:
            self.current[market] = incoming
            await self.check_ready(market, incoming)
            return

        if incoming.start == previous.start:
            self.current[market] = incoming
            await self.check_ready(market, incoming)
            return

        if incoming.start > previous.start:
            # 새 봉이 처음 도착한 순간 직전 봉을 완성봉으로 처리
            await self.finalize_candle(market, previous)
            self.current[market] = incoming
            await self.check_ready(market, incoming)

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

        retry_seconds = 2
        while not self.stop_event.is_set():
            try:
                log.info("업비트 WebSocket 연결 중...")
                async with websockets.connect(
                    UPBIT_WS,
                    ping_interval=30,
                    ping_timeout=20,
                    close_timeout=10,
                    max_size=None,
                ) as websocket:
                    await websocket.send(json.dumps(request))
                    log.info("WebSocket 연결 완료: KRW 마켓 %s개", len(markets))
                    retry_seconds = 2

                    async for raw in websocket:
                        if self.stop_event.is_set():
                            break
                        if isinstance(raw, bytes):
                            raw = raw.decode("utf-8")
                        item = json.loads(raw)
                        await self.handle_message(item)

            except asyncio.CancelledError:
                raise
            except Exception as error:
                log.warning("WebSocket 연결 오류: %s", error)
                await asyncio.sleep(retry_seconds)
                retry_seconds = min(retry_seconds * 2, 60)

    async def run(self) -> None:
        timeout = aiohttp.ClientTimeout(total=30)
        headers = {"User-Agent": "upbit-realtime-scanner/1.0"}

        async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
            self.http = session
            markets = await self.fetch_markets()
            log.info("업비트 KRW 마켓 %s개 확인", len(markets))

            await self.send_telegram(
                "🟡 코인 스캐너 초기화 중\n\n"
                f"감시 대상: 업비트 KRW {len(markets)}개\n"
                f"기준 봉: {CANDLE_MINUTES}분봉\n"
                "과거 캔들을 불러오는 중입니다."
            )

            await self.initialize_histories(markets)

            await self.send_telegram(
                "🟢 업비트 실시간 코인 스캐너 시작\n\n"
                f"감시 대상: KRW 마켓 {len(markets)}개\n"
                f"기준 봉: {CANDLE_MINUTES}분봉\n"
                f"준비: {BREAKOUT_LENGTH}봉 최고가 {READY_DISTANCE * 100:.1f}% 이내\n"
                f"BUY1: 최고가 돌파 + 거래량 {BUY_VOLUME_RATIO:.1f}배"
            )

            await self.websocket_loop(markets)


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
