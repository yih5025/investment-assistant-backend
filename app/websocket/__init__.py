# app/websocket/__init__.py
"""
WebSocket 실시간 데이터 스트리밍 모듈

이 모듈은 투자 관련 실시간 데이터를 WebSocket을 통해 클라이언트에게 전송하는 
기능을 제공합니다.

주요 컴포넌트:
- WebSocketManager: WebSocket 연결 관리
- RedisStreamer: Redis에서 실시간 데이터 스트리밍  
- handlers: WebSocket 메시지 처리
- RealtimeService: 실시간 데이터 처리 로직

지원하는 엔드포인트:
- /ws/stocks/topgainers - 모든 상승 주식 실시간 데이터
- /ws/stocks/topgainers/{symbol} - 특정 심볼 실시간 데이터
"""

from .manager import WebSocketManager
from .redis_streamer import RedisStreamer
from .handlers import WebSocketHandlers

__all__ = [
    "WebSocketManager",
    "RedisStreamer", 
    "WebSocketHandlers"
]

# 버전 정보
__version__ = "1.0.0"

# 설정 상수
DEFAULT_POLLING_INTERVAL = 0.5  # 500ms
DEFAULT_HEARTBEAT_INTERVAL = 30  # 30초
MAX_CLIENTS_PER_ENDPOINT = 100   # 엔드포인트당 최대 클라이언트 수