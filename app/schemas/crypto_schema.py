# app/schemas/websocket/crypto_schema.py
from pydantic import BaseModel, Field
from typing import List, Optional, Any, Dict
from datetime import datetime
import pytz
from enum import Enum

class CryptoChangeType(str, Enum):
    """암호화폐 변동 타입"""
    RISE = "RISE"
    FALL = "FALL"
    EVEN = "EVEN"

class CryptoExchange(str, Enum):
    """지원하는 암호화폐 거래소"""
    BITHUMB = "bithumb"

class CryptoData(BaseModel):
    """암호화폐 데이터 모델"""
    id: Optional[int] = None
    market_code: str = Field(..., description="마켓 코드 (예: KRW-BTC)")
    symbol: str = Field(..., description="암호화폐 심볼 (예: BTC)")
    korean_name: Optional[str] = Field(None, description="한국 이름 (예: 비트코인)")
    english_name: Optional[str] = Field(None, description="영어 이름 (예: Bitcoin)")
    price: Optional[float] = Field(None, description="현재 거래가")
    change_24h: Optional[float] = Field(None, description="24시간 변동가격")
    change_rate_24h: Optional[str] = Field(None, description="24시간 변동률 (예: 2.56%)")
    volume: Optional[float] = Field(None, description="24시간 거래량")
    acc_trade_value_24h: Optional[float] = Field(None, description="24시간 누적 거래대금")
    timestamp: Optional[int] = Field(None, description="타임스탬프")
    source: CryptoExchange = CryptoExchange.BITHUMB
    
    # 기존 호환성을 위한 필드들 (deprecated)
    market: Optional[str] = Field(None, description="마켓 코드 (deprecated, use market_code)")
    trade_price: Optional[float] = Field(None, description="현재 거래가 (deprecated, use price)")
    signed_change_rate: Optional[float] = None
    signed_change_price: Optional[float] = None
    trade_volume: Optional[float] = None
    acc_trade_volume_24h: Optional[float] = None
    high_price: Optional[float] = None
    low_price: Optional[float] = None
    opening_price: Optional[float] = None
    prev_closing_price: Optional[float] = None
    change: Optional[CryptoChangeType] = None
    timestamp_field: Optional[int] = None
    crypto_name: Optional[str] = None
    
    class Config:
        from_attributes = True

class CryptoUpdateMessage(BaseModel):
    """암호화폐 업데이트 메시지"""
    type: str = "crypto_update"
    data: List[CryptoData]
    timestamp: str = Field(default_factory=lambda: datetime.now(pytz.UTC).isoformat())
    data_count: int = Field(..., description="전송된 데이터 개수")
    exchange: CryptoExchange = CryptoExchange.BITHUMB
    market_types: Optional[List[str]] = None
    connection_info: Optional[Dict[str, Any]] = None
    
    class Config:
        from_attributes = True

class CryptoStatusMessage(BaseModel):
    """암호화폐 WebSocket 상태 메시지"""
    type: str = "crypto_status"
    status: str = Field(..., description="connected, disconnected, error, reconnecting")
    timestamp: str = Field(default_factory=lambda: datetime.now(pytz.UTC).isoformat())
    connected_clients: Optional[int] = None
    exchange: CryptoExchange = CryptoExchange.BITHUMB
    websocket_url: Optional[str] = None
    subscribed_markets: Optional[List[str]] = None
    connection_attempts: Optional[int] = None

class CryptoErrorMessage(BaseModel):
    """암호화폐 WebSocket 에러 메시지"""
    type: str = "crypto_error"
    error_code: str
    message: str
    timestamp: str = Field(default_factory=lambda: datetime.now(pytz.UTC).isoformat())
    exchange: Optional[CryptoExchange] = None
    market: Optional[str] = None
    details: Optional[Dict[str, Any]] = None

class CryptoHeartbeatMessage(BaseModel):
    """암호화폐 WebSocket 하트비트 메시지"""
    type: str = "crypto_heartbeat"
    timestamp: str = Field(default_factory=lambda: datetime.now(pytz.UTC).isoformat())
    server_time: Optional[str] = None
    exchange: CryptoExchange = CryptoExchange.BITHUMB
    active_connections: Optional[int] = None

# =========================
# 헬퍼 함수들
# =========================

def create_crypto_update_message(data: List[CryptoData], exchange: CryptoExchange = CryptoExchange.BITHUMB) -> CryptoUpdateMessage:
    """암호화폐 업데이트 메시지 생성"""
    market_types = list(set(item.market for item in data if item.market))
    
    return CryptoUpdateMessage(
        data=data,
        data_count=len(data),
        exchange=exchange,
        market_types=market_types
    )

def create_crypto_error_message(error_code: str, message: str, exchange: CryptoExchange = None, market: str = None, details: Dict[str, Any] = None) -> CryptoErrorMessage:
    """암호화폐 에러 메시지 생성"""
    return CryptoErrorMessage(
        error_code=error_code,
        message=message,
        exchange=exchange,
        market=market,
        details=details or {}
    )

def db_to_crypto_data(db_obj, crypto_name: str = None) -> CryptoData:
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
        change=CryptoChangeType(db_obj.change) if db_obj.change else None,
        timestamp_field=db_obj.timestamp_field,
        source=CryptoExchange(db_obj.source) if db_obj.source else CryptoExchange.BITHUMB,
        crypto_name=crypto_name or db_obj.crypto_name
    )

def validate_crypto_market(market: str) -> bool:
    """암호화폐 마켓 코드 유효성 검증"""
    if not market or not isinstance(market, str):
        return False
    
    market = market.strip().upper()
    
    # KRW-BTC, USDT-ETH 등의 형태 검증
    if "-" not in market:
        return False
    
    parts = market.split("-")
    if len(parts) != 2:
        return False
    
    base, quote = parts
    return (len(base) >= 2 and len(base) <= 10 and 
            len(quote) >= 2 and len(quote) <= 10 and
            base.isalpha() and quote.isalpha())

def extract_crypto_symbol(market: str) -> str:
    """마켓 코드에서 암호화폐 심볼 추출 (KRW-BTC -> BTC)"""
    if not market or "-" not in market:
        return market
    
    return market.split("-")[-1]

def format_crypto_price(price: float, currency: str = "KRW") -> str:
    """암호화폐 가격 포맷팅"""
    if not price:
        return "0"
    
    if currency == "KRW":
        if price >= 1000000:
            return f"₩{price/1000000:.1f}M"
        elif price >= 1000:
            return f"₩{price:,.0f}"
        else:
            return f"₩{price:.2f}"
    else:
        return f"${price:.8f}"