# 업비트 실시간 코인 스캐너

## 동작

- 업비트 KRW 전체 마켓 감시
- 15분봉 기준
- BUY1: 이전 20봉 최고가 종가 돌파
- BUY2~BUY4: 직전 BUY 가격에서 BUY1 당시 ATR의 0.5배 상승
- 한 개의 15분봉에서 한 단계만 증가
- BUY4까지만 전송
- 이전 10봉 최저가 종가 이탈 시 내부 상태 초기화
- 준비, SELL, 점수, 시작 및 초기화 텔레그램 메시지 없음
- BUY 신호가 있을 때만 15분봉 마감 후 한 번에 종합 전송
- Railway 재시작 시 BUY 상태는 초기화

## Railway 필수 환경변수

- TELEGRAM_BOT_TOKEN
- TELEGRAM_CHAT_ID

## 실행 파일

main.py