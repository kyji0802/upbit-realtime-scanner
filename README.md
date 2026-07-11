# 업비트 실시간 터틀 BUY 스캐너

- 업비트 KRW 전체
- 15분봉
- BUY1: 이전 20봉 최고가 종가 돌파
- BUY2 이상: 직전 BUY 가격에서 0.5 ATR 상승할 때마다 증가
- 이전 10봉 최저가 종가 이탈 시 번호만 초기화
- SELL, 준비, 점수, 시작 및 초기화 메시지 없음
- 봉 마감 후 BUY 신호만 한 번에 묶어서 텔레그램 전송

Railway 필수 환경변수:

- TELEGRAM_BOT_TOKEN
- TELEGRAM_CHAT_ID

실행 파일은 main.py이므로 Railway가 자동으로 `python main.py`를 감지합니다.
