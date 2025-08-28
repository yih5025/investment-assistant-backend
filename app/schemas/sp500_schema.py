# app/schemas/sp500_schema.py
from pydantic import BaseModel, Field, field_validator
from typing import List, Optional, Dict, Any, Union
from datetime import datetime
import pytz
from enum import Enum

class TimeframeEnum(str, Enum):
    """차트 시간대 열거형"""
    ONE_MINUTE = "1M"
    FIVE_MINUTES = "5M"
    ONE_HOUR = "1H"
    ONE_DAY = "1D"
    ONE_WEEK = "1W"
    ONE_MONTH = "1MO"

class MarketStatus(BaseModel):
    """시장 상태 정보"""
    is_open: bool = Field(..., description="시장 개장 여부")
    current_time_et: str = Field(..., description="현재 미국 동부 시간")
    current_time_utc: str = Field(..., description="현재 UTC 시간")
    status: str = Field(..., description="시장 상태 (OPEN/CLOSED)")
    timezone: str = Field(default="US/Eastern", description="시간대")

class ChartDataPoint(BaseModel):
    """차트 데이터 포인트"""
    timestamp: int = Field(..., description="타임스탬프 (밀리초)")
    price: float = Field(..., description="거래 가격")
    volume: Optional[int] = Field(None, description="거래량")
    datetime: str = Field(..., description="날짜시간 (ISO 형식)")

class StockInfo(BaseModel):
    """기본 주식 정보"""
    symbol: str = Field(..., description="주식 심볼")
    company_name: str = Field(..., description="회사명")
    current_price: Optional[float] = Field(None, description="현재가")
    change_amount: Optional[float] = Field(None, description="변동 금액")
    change_percentage: Optional[float] = Field(None, description="변동률 (%)")
    volume: Optional[int] = Field(None, description="거래량")
    last_updated: Optional[str] = Field(None, description="최종 업데이트 시간")
    is_positive: Optional[bool] = Field(None, description="상승 여부")

    @field_validator('change_amount', 'change_percentage')
    @classmethod
    def round_changes(cls, v):
        """변동 수치 반올림"""
        return round(v, 2) if v is not None else None

class StockDetail(StockInfo):
    """주식 상세 정보 (차트 데이터 포함)"""
    previous_close: Optional[float] = Field(None, description="전일 종가")
    chart_data: List[ChartDataPoint] = Field(default=[], description="차트 데이터")
    timeframe: str = Field(default="1D", description="차트 시간대")
    market_status: MarketStatus = Field(..., description="시장 상태")
    
    # 추가 회사 정보
    # sub_industry: Optional[str] = Field(None, description="세부 업종")
    # headquarters: Optional[str] = Field(None, description="본사 위치")

class StockListResponse(BaseModel):
    """주식 리스트 응답"""
    stocks: List[StockInfo] = Field(..., description="주식 리스트")
    total_count: int = Field(..., description="총 주식 개수")
    market_status: MarketStatus = Field(..., description="시장 상태")
    last_updated: str = Field(..., description="최종 업데이트 시간")
    message: Optional[str] = Field(None, description="메시지")

class CategoryStockResponse(BaseModel):
    """카테고리별 주식 응답"""
    category: str = Field(..., description="카테고리 (top_gainers, top_losers, most_active)")
    stocks: List[StockInfo] = Field(..., description="주식 리스트")
    total_count: int = Field(..., description="총 주식 개수")
    market_status: MarketStatus = Field(..., description="시장 상태")
    message: Optional[str] = Field(None, description="메시지")

class SearchResponse(BaseModel):
    """검색 결과 응답"""
    query: str = Field(..., description="검색어")
    results: List[StockInfo] = Field(..., description="검색 결과")
    total_count: int = Field(..., description="총 결과 개수")
    message: Optional[str] = Field(None, description="메시지")


class MarketSummary(BaseModel):
    """시장 요약 정보"""
    total_symbols: int = Field(..., description="총 심볼 개수")
    total_trades: int = Field(..., description="총 거래 개수")
    average_price: float = Field(..., description="평균 가격")
    highest_price: float = Field(..., description="최고 가격")
    lowest_price: float = Field(..., description="최저 가격")
    total_volume: int = Field(..., description="총 거래량")
    last_updated: Optional[str] = Field(None, description="최종 업데이트 시간")

class MarketHighlights(BaseModel):
    """시장 하이라이트"""
    top_gainers: List[StockInfo] = Field(..., description="상위 상승 종목")
    top_losers: List[StockInfo] = Field(..., description="상위 하락 종목")
    most_active: List[StockInfo] = Field(..., description="가장 활발한 종목")

class MarketOverviewResponse(BaseModel):
    """시장 개요 응답"""
    market_summary: MarketSummary = Field(..., description="시장 요약")
    market_status: MarketStatus = Field(..., description="시장 상태")
    highlights: MarketHighlights = Field(..., description="시장 하이라이트")
    last_updated: str = Field(..., description="최종 업데이트 시간")

class ServiceStats(BaseModel):
    """서비스 통계"""
    service_name: str = Field(..., description="서비스명")
    stats: Dict[str, Any] = Field(..., description="통계 정보")
    market_status: MarketStatus = Field(..., description="시장 상태")
    uptime: str = Field(..., description="가동 시간")

class HealthCheckResponse(BaseModel):
    """헬스 체크 응답"""
    status: str = Field(..., description="서비스 상태")
    database: str = Field(..., description="데이터베이스 상태")
    data_freshness: str = Field(..., description="데이터 신선도")
    market_status: MarketStatus = Field(..., description="시장 상태")
    last_check: str = Field(..., description="마지막 체크 시간")
    error: Optional[str] = Field(None, description="에러 메시지")

# =========================
# 🎯 요청 스키마들
# =========================

class StockDetailRequest(BaseModel):
    """주식 상세 정보 요청"""
    symbol: str = Field(..., description="주식 심볼", min_length=1, max_length=10)
    timeframe: TimeframeEnum = Field(default=TimeframeEnum.ONE_DAY, description="차트 시간대")

    @field_validator('symbol')
    @classmethod
    def symbol_uppercase(cls, v):
        """심볼을 대문자로 변환"""
        return v.upper().strip()

class SearchRequest(BaseModel):
    """검색 요청"""
    query: str = Field(..., description="검색어", min_length=1, max_length=50)
    limit: int = Field(default=20, description="최대 결과 개수", ge=1, le=100)

    @field_validator('query')
    @classmethod
    def query_clean(cls, v):
        """검색어 정리"""
        return v.strip()

class CategoryRequest(BaseModel):
    """카테고리별 요청"""
    category: str = Field(..., description="카테고리")
    limit: int = Field(default=20, description="최대 결과 개수", ge=1, le=100)

    @field_validator('category')
    @classmethod
    def category_lowercase(cls, v):
        """카테고리를 소문자로 변환"""
        return v.lower().strip()

# =========================
# 🎯 에러 응답 스키마들
# =========================

class ErrorDetail(BaseModel):
    """에러 상세 정보"""
    type: str = Field(..., description="에러 타입")
    message: str = Field(..., description="에러 메시지")
    code: Optional[str] = Field(None, description="에러 코드")

class ErrorResponse(BaseModel):
    """에러 응답"""
    error: ErrorDetail = Field(..., description="에러 정보")
    timestamp: str = Field(default_factory=lambda: datetime.now(pytz.UTC).isoformat(), description="에러 발생 시간")
    path: Optional[str] = Field(None, description="요청 경로")

# =========================
# 🎯 유틸리티 함수들
# =========================

# =========================
# 🎯 WebSocket 메시지 스키마들
# =========================

class SP500UpdateMessage(BaseModel):
    """SP500 업데이트 메시지"""
    type: str = "sp500_update"
    data: List[StockInfo]  # 기존 StockInfo 재사용
    timestamp: str = Field(default_factory=lambda: datetime.now(pytz.UTC).isoformat())
    data_count: int = Field(..., description="전송된 데이터 개수")
    market_status: MarketStatus = Field(..., description="시장 상태")
    data_source: str = Field(default="redis_api", description="데이터 소스")
    categories: Optional[List[str]] = None
    
    class Config:
        from_attributes = True

class SP500StatusMessage(BaseModel):
    """SP500 상태 메시지"""
    type: str = "sp500_status"
    status: str = Field(..., description="connected, disconnected, error, api_mode")
    timestamp: str = Field(default_factory=lambda: datetime.now(pytz.UTC).isoformat())
    connected_clients: Optional[int] = None
    market_status: MarketStatus = Field(..., description="시장 상태")
    data_source: str = Field(default="redis_api")
    last_data_update: Optional[str] = None

class SP500ErrorMessage(BaseModel):
    """SP500 에러 메시지"""
    type: str = "sp500_error"
    error_code: str
    message: str
    timestamp: str = Field(default_factory=lambda: datetime.now(pytz.UTC).isoformat())
    symbol: Optional[str] = None
    details: Optional[Dict[str, Any]] = None

# =========================
# 🎯 헬퍼 함수들
# =========================

def create_sp500_update_message(data: List[StockInfo], market_status: Optional[MarketStatus] = None) -> SP500UpdateMessage:
    """SP500 업데이트 메시지 생성"""
    # 기본 시장 상태 생성 (market_status가 없는 경우)
    if not market_status:
        current_time_utc = datetime.now(pytz.UTC)
        et_timezone = pytz.timezone('US/Eastern')
        current_time_et = current_time_utc.astimezone(et_timezone)
        
        market_status = MarketStatus(
            is_open=True,
            current_time_et=current_time_et.strftime("%Y-%m-%d %H:%M:%S"),
            current_time_utc=current_time_utc.strftime("%Y-%m-%d %H:%M:%S"),
            status="UNKNOWN",
            timezone="US/Eastern"
        )
    
    # 카테고리 추출 (상승/하락 등)
    categories = []
    for stock in data:
        if stock.is_positive is True:
            categories.append("gainers")
        elif stock.is_positive is False:
            categories.append("losers")
    
    return SP500UpdateMessage(
        data=data,
        data_count=len(data),
        market_status=market_status,
        categories=list(set(categories)) if categories else None
    )

def create_sp500_error_message(error_code: str, message: str, symbol: str = None, details: Dict[str, Any] = None) -> SP500ErrorMessage:
    """SP500 에러 메시지 생성"""
    return SP500ErrorMessage(
        error_code=error_code,
        message=message,
        symbol=symbol,
        details=details or {}
    )

def db_to_stock_info(trade_data: dict, change_info: dict, company_info: dict) -> StockInfo:
    """데이터베이스 데이터를 StockInfo로 변환"""
    return StockInfo(
        symbol=trade_data.get('symbol', ''),
        company_name=company_info.get('company_name', ''),
        current_price=change_info.get('current_price'),
        change_amount=change_info.get('change_amount'),
        change_percentage=change_info.get('change_percentage'),
        volume=change_info.get('volume'),
        last_updated=change_info.get('last_updated'),
        is_positive=change_info.get('change_amount', 0) > 0 if change_info.get('change_amount') else None
    )

def db_to_chart_data_point(trade) -> ChartDataPoint:
    """데이터베이스 거래 데이터를 ChartDataPoint로 변환"""
    return ChartDataPoint(
        timestamp=trade.timestamp_ms,
        price=float(trade.price),
        volume=trade.volume,
        datetime=trade.created_at.isoformat()
    )

def create_error_response(error_type: str, message: str, code: str = None, path: str = None) -> ErrorResponse:
    """에러 응답 생성 유틸리티"""
    return ErrorResponse(
        error=ErrorDetail(
            type=error_type,
            message=message,
            code=code
        ),
        path=path
    )

def market_status_to_dict(market_status: MarketStatus) -> Dict[str, Any]:
    """MarketStatus를 딕셔너리로 변환"""
    return {
        'is_open': market_status.is_open,
        'current_time_et': market_status.current_time_et,
        'current_time_utc': market_status.current_time_utc,
        'status': market_status.status,
        'timezone': market_status.timezone
    }

