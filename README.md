# 업비트 실시간 코인 스캐너

업비트 KRW 마켓의 15분봉을 WebSocket으로 실시간 수신하여 텔레그램으로 신호를 보냅니다.

## 기본 신호

- 준비: 현재 가격이 이전 20개 완성봉 최고가의 1% 이내이며 예상 거래량이 평균 1.2배 이상
- BUY1: 완성된 15분봉 종가가 이전 20개 봉 최고가를 돌파하고 거래량이 평균 1.5배 이상

## Railway 환경변수

필수:

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

선택:

- `CANDLE_MINUTES=15`
- `BREAKOUT_LENGTH=20`
- `BUY_VOLUME_RATIO=1.5`
- `READY_DISTANCE=0.01`
- `READY_VOLUME_RATIO=1.2`
- `LOG_LEVEL=INFO`

## Railway 실행 명령

```bash
python coin_scanner.py
```

Cron Schedule은 설정하지 않습니다. 서비스가 계속 실행되는 형태로 사용합니다.
