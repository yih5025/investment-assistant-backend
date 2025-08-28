# app/schemas/websocket/topgainers_schema.py
from pydantic import BaseModel, Field
from typing import List, Optional, Any, Dict
from datetime import datetime
import pytz
from enum import Enum

class TopGainersCategory(str, Enum):
    """Top Gainers 카테고리 정의"""
    TOP_GAINERS = "top_gainers"
    TOP_LOSERS = "top_losers"
    MOST_ACTIVELY_TRADED = "most_actively_traded"

class TopGainerData(BaseModel):
    """Top Gainers 개별 데이터 모델"""
    batch_id: int
    symbol: str
    category: TopGainersCategory = Field(..., description="카테고리")
    last_updated: str
    rank_position: Optional[int] = None
    price: Optional[float] = None
    change_amount: Optional[float] = None
    change_percentage: Optional[str] = None
    volume: Optional[int] = None
    created_at: Optional[str] = None
    company_name: Optional[str] = None
    
    class Config:
        from_attributes = True

# =========================
# 🎯 Polling API 응답 스키마들
# =========================

class TopGainersListResponse(BaseModel):
    """TopGainers 리스트 응답 (Polling API용)"""
    data: List[TopGainerData] = Field(..., description="TopGainers 데이터 리스트")
    total_count: int = Field(..., description="총 데이터 개수")
    batch_id: Optional[int] = Field(None, description="배치 ID")
    categories: Optional[List[str]] = Field(None, description="포함된 카테고리들")
    last_updated: str = Field(..., description="최종 업데이트 시간")
    data_source: str = Field(default="redis_api", description="데이터 소스")
    message: Optional[str] = Field(None, description="응답 메시지")
    
    class Config:
        from_attributes = True

class TopGainersCategoryResponse(BaseModel):
    """TopGainers 카테고리별 응답 (Polling API용)"""
    category: TopGainersCategory = Field(..., description="요청된 카테고리")
    data: List[TopGainerData] = Field(..., description="카테고리별 데이터")
    total_count: int = Field(..., description="해당 카테고리 데이터 개수")
    batch_id: Optional[int] = Field(None, description="배치 ID")
    last_updated: str = Field(..., description="최종 업데이트 시간")
    data_source: str = Field(default="redis_api", description="데이터 소스")
    message: Optional[str] = Field(None, description="응답 메시지")
    
    class Config:
        from_attributes = True

class TopGainersSymbolResponse(BaseModel):
    """TopGainers 특정 심볼 응답 (Polling API용)"""
    symbol: str = Field(..., description="심볼")
    data: Optional[TopGainerData] = Field(None, description="심볼 데이터")
    found: bool = Field(..., description="데이터 발견 여부")
    batch_id: Optional[int] = Field(None, description="배치 ID")
    last_updated: Optional[str] = Field(None, description="최종 업데이트 시간")
    message: Optional[str] = Field(None, description="응답 메시지")
    
    class Config:
        from_attributes = True

# =========================
# 🎯 WebSocket 메시지 스키마들  
# =========================

class TopGainersUpdateMessage(BaseModel):
    """Top Gainers 업데이트 메시지"""
    type: str = "topgainers_update"
    data: List[TopGainerData]
    timestamp: str = Field(default_factory=lambda: datetime.now(pytz.UTC).isoformat())
    batch_id: Optional[int] = None
    data_count: int = Field(..., description="전송된 데이터 개수")
    categories: Optional[List[str]] = None
    market_status: Optional[Dict[str, Any]] = None
    
    class Config:
        from_attributes = True

class TopGainersStatusMessage(BaseModel):
    """Top Gainers 상태 메시지"""
    type: str = "topgainers_status"
    status: str = Field(..., description="connected, disconnected, error, fetching")
    timestamp: str = Field(default_factory=lambda: datetime.now(pytz.UTC).isoformat())
    connected_clients: Optional[int] = None
    data_source: str = "redis_api"
    polling_interval: Optional[int] = None
    last_data_update: Optional[str] = None

class TopGainersErrorMessage(BaseModel):
    """Top Gainers 에러 메시지"""
    type: str = "topgainers_error"
    error_code: str
    message: str
    timestamp: str = Field(default_factory=lambda: datetime.now(pytz.UTC).isoformat())
    category: Optional[str] = None
    details: Optional[Dict[str, Any]] = None

# =========================
# 🎯 Polling API 헬퍼 함수들
# =========================

def create_topgainers_list_response(
    data: List[TopGainerData], 
    batch_id: int = None,
    data_source: str = "redis_api",
    message: str = None
) -> TopGainersListResponse:
    """TopGainers 리스트 응답 생성"""
    categories = list(set(item.category.value for item in data if item.category))
    
    return TopGainersListResponse(
        data=data,
        total_count=len(data),
        batch_id=batch_id,
        categories=categories,
        last_updated=datetime.now(pytz.UTC).isoformat(),
        data_source=data_source,
        message=message
    )

def create_topgainers_category_response(
    category: TopGainersCategory,
    data: List[TopGainerData],
    batch_id: int = None,
    data_source: str = "redis_api",
    message: str = None
) -> TopGainersCategoryResponse:
    """TopGainers 카테고리별 응답 생성"""
    return TopGainersCategoryResponse(
        category=category,
        data=data,
        total_count=len(data),
        batch_id=batch_id,
        last_updated=datetime.now(pytz.UTC).isoformat(),
        data_source=data_source,
        message=message
    )

def create_topgainers_symbol_response(
    symbol: str,
    data: TopGainerData = None,
    batch_id: int = None,
    message: str = None
) -> TopGainersSymbolResponse:
    """TopGainers 심볼별 응답 생성"""
    return TopGainersSymbolResponse(
        symbol=symbol.upper(),
        data=data,
        found=data is not None,
        batch_id=batch_id,
        last_updated=data.last_updated if data else None,
        message=message or ("데이터를 찾았습니다." if data else "데이터를 찾을 수 없습니다.")
    )

# =========================
# 🎯 WebSocket 헬퍼 함수들
# =========================

def create_topgainers_update_message(data: List[TopGainerData], batch_id: int = None) -> TopGainersUpdateMessage:
    """TopGainers 업데이트 메시지 생성"""
    categories = list(set(item.category.value for item in data if item.category))
    
    return TopGainersUpdateMessage(
        data=data,
        batch_id=batch_id,
        data_count=len(data),
        categories=categories
    )

def create_topgainers_error_message(error_code: str, message: str, category: str = None, details: Dict[str, Any] = None) -> TopGainersErrorMessage:
    """TopGainers 에러 메시지 생성"""
    return TopGainersErrorMessage(
        error_code=error_code,
        message=message,
        category=category,
        details=details or {}
    )

def db_to_topgainer_data(db_obj, company_name: str = None) -> TopGainerData:
    """데이터베이스 객체를 TopGainerData로 변환"""
    return TopGainerData(
        batch_id=db_obj.batch_id,
        symbol=db_obj.symbol,
        category=TopGainersCategory(db_obj.category),
        last_updated=db_obj.last_updated.isoformat() if db_obj.last_updated else datetime.now(pytz.UTC).isoformat(),
        rank_position=db_obj.rank_position,
        price=float(db_obj.price) if db_obj.price else None,
        change_amount=float(db_obj.change_amount) if db_obj.change_amount else None,
        change_percentage=db_obj.change_percentage,
        volume=db_obj.volume,
        created_at=db_obj.created_at.isoformat() if db_obj.created_at else None,
        company_name=company_name or f"{db_obj.symbol} Inc."
    )

def validate_topgainers_symbol(symbol: str) -> bool:
    """TopGainers 심볼 유효성 검증"""
    if not symbol or not isinstance(symbol, str):
        return False
    
    symbol = symbol.strip().upper()
    return 1 <= len(symbol) <= 10 and symbol.replace('.', '').isalnum()