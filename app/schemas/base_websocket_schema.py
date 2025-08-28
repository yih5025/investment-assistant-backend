from pydantic import BaseModel, Field
from typing import List, Optional, Any, Dict, Union
from datetime import datetime
import pytz
from enum import Enum
import json

class WebSocketMessageType(str, Enum):
    """WebSocket 메시지 타입 정의"""
    # 상태 메시지
    STATUS = "status"
    ERROR = "error"
    HEARTBEAT = "heartbeat"
    
    # 데이터 업데이트 메시지
    TOPGAINERS_UPDATE = "topgainers_update"
    CRYPTO_UPDATE = "crypto_update"
    SP500_UPDATE = "sp500_update"
    SYMBOL_UPDATE = "symbol_update"
    DASHBOARD_UPDATE = "dashboard_update"

class SubscriptionType(str, Enum):
    """구독 타입 정의"""
    ALL_TOPGAINERS = "all_topgainers"
    ALL_CRYPTO = "all_crypto"
    ALL_SP500 = "all_sp500"
    SINGLE_SYMBOL = "single_symbol"
    DASHBOARD = "dashboard"

class BaseWebSocketMessage(BaseModel):
    """기본 WebSocket 메시지"""
    type: str
    timestamp: str = Field(default_factory=lambda: datetime.now(pytz.UTC).isoformat())
    
    class Config:
        from_attributes = True

class BaseStatusMessage(BaseWebSocketMessage):
    """기본 상태 메시지"""
    status: str = Field(..., description="connected, disconnected, error")
    connected_clients: Optional[int] = None

class BaseErrorMessage(BaseWebSocketMessage):
    """기본 에러 메시지"""
    error_code: str
    message: str
    details: Optional[Dict[str, Any]] = None

class BaseHeartbeatMessage(BaseWebSocketMessage):
    """기본 하트비트 메시지"""
    server_time: Optional[str] = None

# =========================
# 통합 메시지 모델들
# =========================

class SymbolUpdateMessage(BaseModel):
    """특정 심볼 업데이트 메시지"""
    type: str = "symbol_update"
    symbol: str
    data_type: str = Field(..., description="topgainers, crypto, sp500")
    data: Dict[str, Any]  # Union 대신 Dict 사용으로 유연성 증대
    timestamp: str = Field(default_factory=lambda: datetime.now(pytz.UTC).isoformat())
    
    class Config:
        from_attributes = True

class DashboardUpdateMessage(BaseModel):
    """대시보드 통합 업데이트 메시지"""
    type: str = "dashboard_update"
    timestamp: str = Field(default_factory=lambda: datetime.now(pytz.UTC).isoformat())
    
    # 각 데이터 소스별 요약 데이터
    top_gainers: Optional[List[Dict[str, Any]]] = None
    top_crypto: Optional[List[Dict[str, Any]]] = None
    sp500_highlights: Optional[List[Dict[str, Any]]] = None
    
    # 요약 통계
    summary: Optional[Dict[str, Any]] = None
    
    class Config:
        from_attributes = True

# =========================
# 유틸리티 함수들
# =========================

def validate_websocket_message(message_data: str) -> tuple[bool, Optional[Dict[str, Any]], Optional[str]]:
    """WebSocket 메시지 유효성 검증"""
    try:
        data = json.loads(message_data)
        
        if not isinstance(data, dict):
            return False, None, "메시지는 JSON 객체여야 합니다"
        
        # 필수 필드 검증
        if "type" not in data:
            return False, None, "메시지 타입이 필요합니다"
        
        return True, data, None
        
    except json.JSONDecodeError as e:
        return False, None, f"JSON 파싱 오류: {str(e)}"
    except Exception as e:
        return False, None, f"메시지 검증 오류: {str(e)}"

def create_symbol_update_message(symbol: str, data_type: str, data: Dict[str, Any]) -> SymbolUpdateMessage:
    """특정 심볼 업데이트 메시지 생성"""
    return SymbolUpdateMessage(
        symbol=symbol,
        data_type=data_type,
        data=data
    )

def create_dashboard_update_message(
    top_gainers: List[Dict[str, Any]] = None,
    top_crypto: List[Dict[str, Any]] = None,
    sp500_highlights: List[Dict[str, Any]] = None,
    summary: Dict[str, Any] = None
) -> DashboardUpdateMessage:
    """대시보드 업데이트 메시지 생성"""
    return DashboardUpdateMessage(
        top_gainers=top_gainers or [],
        top_crypto=top_crypto or [],
        sp500_highlights=sp500_highlights or [],
        summary=summary or {}
    )

def standardize_timestamp(dt: Union[datetime, str, None]) -> str:
    """타임스탬프 표준화"""
    if dt is None:
        return datetime.now(pytz.UTC).isoformat()
    elif isinstance(dt, str):
        return dt
    else:
        return dt.isoformat()