# app/schemas/websocket_schema.py
from pydantic import BaseModel, Field
from typing import List, Optional, Any, Dict, Union
from datetime import datetime
from enum import Enum

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

# =========================
# 기본 데이터 모델들
# =========================

class TopGainerData(BaseModel):
    """Top Gainers 개별 데이터 모델"""
    batch_id: int
    symbol: str
    category: str = Field(..., description="top_gainers, top_losers, most_actively_traded")
    last_updated: str
    rank_position: Optional[int] = None
    price: Optional[float] = None
    change_amount: Optional[float] = None
    change_percentage: Optional[str] = None
    volume: Optional[int] = None
    created_at: Optional[str] = None
    
    class Config:
        from_attributes = True

class CryptoData(BaseModel):
    """암호화폐 데이터 모델"""
    id: Optional[int] = None
    market: str = Field(..., description="마켓 코드 (예: KRW-BTC)")
    trade_price: Optional[float] = None
    signed_change_rate: Optional[float] = None
    signed_change_price: Optional[float] = None
    trade_volume: Optional[float] = None
    acc_trade_volume_24h: Optional[float] = None
    high_price: Optional[float] = None
    low_price: Optional[float] = None
    opening_price: Optional[float] = None
    prev_closing_price: Optional[float] = None
    change: Optional[str] = None  # RISE, FALL, EVEN
    timestamp_field: Optional[int] = None
    source: str = "bithumb"
    
    class Config:
        from_attributes = True

class SP500Data(BaseModel):
    """SP500 거래 데이터 모델"""
    id: Optional[int] = None
    symbol: str
    price: Optional[float] = None
    volume: Optional[int] = None
    timestamp_ms: Optional[int] = None
    trade_conditions: Optional[List[str]] = None
    category: Optional[str] = None  # top_gainers, most_actively_traded, top_losers
    source: str = "finnhub_websocket"
    created_at: Optional[str] = None
    
    class Config:
        from_attributes = True

# =========================
# 메시지 모델들
# =========================

class StatusMessage(BaseModel):
    """상태 메시지"""
    type: WebSocketMessageType = WebSocketMessageType.STATUS
    status: str = Field(..., description="connected, disconnected, error")
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    connected_clients: Optional[int] = None
    subscription_info: Optional[Dict[str, Any]] = None
    server_info: Optional[Dict[str, Any]] = None

class ErrorMessage(BaseModel):
    """에러 메시지"""
    type: WebSocketMessageType = WebSocketMessageType.ERROR
    error_code: str
    message: str
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    details: Optional[Dict[str, Any]] = None

class HeartbeatMessage(BaseModel):
    """하트비트 메시지"""
    type: WebSocketMessageType = WebSocketMessageType.HEARTBEAT
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    server_time: Optional[str] = None
    data_type: Optional[str] = None
    symbol: Optional[str] = None

# =========================
# 업데이트 메시지들
# =========================

class TopGainersUpdateMessage(BaseModel):
    """Top Gainers 업데이트 메시지"""
    type: WebSocketMessageType = WebSocketMessageType.TOPGAINERS_UPDATE
    data: List[TopGainerData]
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    batch_id: Optional[int] = None
    data_count: int = Field(..., description="전송된 데이터 개수")
    categories: Optional[List[str]] = None  # ["top_gainers", "top_losers"]
    
    class Config:
        from_attributes = True

class CryptoUpdateMessage(BaseModel):
    """암호화폐 업데이트 메시지"""
    type: WebSocketMessageType = WebSocketMessageType.CRYPTO_UPDATE
    data: List[CryptoData]
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    data_count: int = Field(..., description="전송된 데이터 개수")
    exchange: str = "bithumb"
    market_types: Optional[List[str]] = None  # ["KRW-BTC", "KRW-ETH"]
    
    class Config:
        from_attributes = True

class SP500UpdateMessage(BaseModel):
    """SP500 업데이트 메시지"""
    type: WebSocketMessageType = WebSocketMessageType.SP500_UPDATE
    data: List[SP500Data]
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    data_count: int = Field(..., description="전송된 데이터 개수")
    categories: Optional[List[str]] = None  # 카테고리 목록
    symbols: Optional[List[str]] = None     # 포함된 심볼들
    
    class Config:
        from_attributes = True

class SymbolUpdateMessage(BaseModel):
    """특정 심볼 업데이트 메시지"""
    type: WebSocketMessageType = WebSocketMessageType.SYMBOL_UPDATE
    symbol: str
    data_type: str = Field(..., description="topgainers, crypto, sp500")
    data: Union[TopGainerData, CryptoData, SP500Data]
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    
    class Config:
        from_attributes = True

class DashboardUpdateMessage(BaseModel):
    """대시보드 통합 업데이트 메시지"""
    type: WebSocketMessageType = WebSocketMessageType.DASHBOARD_UPDATE
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    
    # 각 데이터 소스별 요약 데이터
    top_gainers: Optional[List[TopGainerData]] = None
    top_crypto: Optional[List[CryptoData]] = None
    sp500_highlights: Optional[List[SP500Data]] = None
    
    # 요약 통계
    summary: Optional[Dict[str, Any]] = None
    
    class Config:
        from_attributes = True

# =========================
# 헬퍼 함수들
# =========================

def create_error_message(error_code: str, message: str, details: Dict[str, Any] = None) -> ErrorMessage:
    """에러 메시지 생성 헬퍼"""
    return ErrorMessage(
        error_code=error_code,
        message=message,
        details=details or {}
    )

def create_topgainers_update_message(data: List[TopGainerData], batch_id: int = None) -> TopGainersUpdateMessage:
    """TopGainers 업데이트 메시지 생성 헬퍼"""
    categories = list(set(item.category for item in data if item.category))
    
    return TopGainersUpdateMessage(
        data=data,
        batch_id=batch_id,
        data_count=len(data),
        categories=categories
    )

def create_crypto_update_message(data: List[CryptoData]) -> CryptoUpdateMessage:
    """암호화폐 업데이트 메시지 생성 헬퍼"""
    market_types = list(set(item.market for item in data if item.market))
    
    return CryptoUpdateMessage(
        data=data,
        data_count=len(data),
        market_types=market_types
    )

def create_sp500_update_message(data: List[SP500Data]) -> SP500UpdateMessage:
    """SP500 업데이트 메시지 생성 헬퍼"""
    categories = list(set(item.category for item in data if item.category))
    symbols = list(set(item.symbol for item in data if item.symbol))
    
    return SP500UpdateMessage(
        data=data,
        data_count=len(data),
        categories=categories,
        symbols=symbols
    )

def create_symbol_update_message(symbol: str, data_type: str, data: Union[TopGainerData, CryptoData, SP500Data]) -> SymbolUpdateMessage:
    """특정 심볼 업데이트 메시지 생성 헬퍼"""
    return SymbolUpdateMessage(
        symbol=symbol,
        data_type=data_type,
        data=data
    )

def create_dashboard_update_message(
    top_gainers: List[TopGainerData] = None,
    top_crypto: List[CryptoData] = None,
    sp500_highlights: List[SP500Data] = None,
    summary: Dict[str, Any] = None
) -> DashboardUpdateMessage:
    """대시보드 업데이트 메시지 생성 헬퍼"""
    return DashboardUpdateMessage(
        top_gainers=top_gainers or [],
        top_crypto=top_crypto or [],
        sp500_highlights=sp500_highlights or [],
        summary=summary or {}
    )

# =========================
# 유효성 검증 함수들
# =========================

def validate_symbol(symbol: str, data_type: str = "stock") -> bool:
    """심볼 유효성 검증"""
    if not symbol or not isinstance(symbol, str):
        return False
    
    symbol = symbol.strip().upper()
    
    if data_type == "crypto":
        # 암호화폐 마켓 코드 검증 (KRW-BTC 형태)
        return len(symbol) <= 15 and "-" in symbol
    else:
        # 주식 심볼 검증
        return 1 <= len(symbol) <= 10 and symbol.isalpha()

def validate_websocket_message(message_data: str) -> tuple[bool, Optional[Dict[str, Any]], Optional[str]]:
    """WebSocket 메시지 유효성 검증"""
    try:
        import json
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

# =========================
# 데이터 변환 유틸리티들
# =========================

def db_to_topgainer_data(db_obj) -> TopGainerData:
    """데이터베이스 객체를 TopGainerData로 변환"""
    return TopGainerData(
        batch_id=db_obj.batch_id,
        symbol=db_obj.symbol,
        category=db_obj.category,
        last_updated=db_obj.last_updated.isoformat() if db_obj.last_updated else None,
        rank_position=db_obj.rank_position,
        price=float(db_obj.price) if db_obj.price else None,
        change_amount=float(db_obj.change_amount) if db_obj.change_amount else None,
        change_percentage=db_obj.change_percentage,
        volume=db_obj.volume,
        created_at=db_obj.created_at.isoformat() if db_obj.created_at else None
    )

def db_to_crypto_data(db_obj) -> CryptoData:
    """데이터베이스 객체를 CryptoData로 변환"""
    return CryptoData(
        id=db_obj.id,
        market=db_obj.market,
        trade_price=float(db_obj.trade_price) if db_obj.trade_price else None,
        signed_change_rate=float(db_obj.signed_change_rate) if db_obj.signed_change_rate else None,
        signed_change_price=float(db_obj.signed_change_price) if db_obj.signed_change_price else None,
        trade_volume=float(db_obj.trade_volume) if db_obj.trade_volume else None,
        acc_trade_volume_24h=float(db_obj.acc_trade_volume_24h) if db_obj.acc_trade_volume_24h else None,
        high_price=float(db_obj.high_price) if db_obj.high_price else None,
        low_price=float(db_obj.low_price) if db_obj.low_price else None,
        opening_price=float(db_obj.opening_price) if db_obj.opening_price else None,
        prev_closing_price=float(db_obj.prev_closing_price) if db_obj.prev_closing_price else None,
        change=db_obj.change,
        timestamp_field=db_obj.timestamp_field,
        source=db_obj.source or "bithumb"
    )

def db_to_sp500_data(db_obj) -> SP500Data:
    """데이터베이스 객체를 SP500Data로 변환"""
    return SP500Data(
        id=db_obj.id,
        symbol=db_obj.symbol,
        price=float(db_obj.price) if db_obj.price else None,
        volume=db_obj.volume,
        timestamp_ms=db_obj.timestamp_ms,
        trade_conditions=db_obj.trade_conditions,
        category=db_obj.category,
        source=db_obj.source or "finnhub_websocket",
        created_at=db_obj.created_at.isoformat() if db_obj.created_at else None
    )