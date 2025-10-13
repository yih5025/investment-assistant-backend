# app/schemas/etf_schema.py
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any, Union
from datetime import datetime
from enum import Enum

# =========================
# 기본 데이터 모델
# =========================

class MarketStatus(BaseModel):
    """시장 상태"""
    is_open: bool = Field(..., description="시장 개장 여부")
    status: str = Field(..., description="시장 상태 (OPEN/CLOSED/UNKNOWN)")
    current_time_et: Optional[str] = Field(None, description="동부 표준시 현재 시간")
    current_time_utc: Optional[str] = Field(None, description="UTC 현재 시간")
    timezone: str = Field(default="US/Eastern", description="시간대")

class SortOrderEnum(str, Enum):
    """정렬 순서"""
    asc = "asc"
    desc = "desc"

class TimeframeEnum(str, Enum):
    """차트 시간대"""
    ONE_DAY = "1D"
    ONE_WEEK = "1W"
    ONE_MONTH = "1M"

# =========================
# ETF 기본 정보 모델
# =========================

class ETFBasicInfo(BaseModel):
    """ETF 기본 정보"""
    symbol: str = Field(..., description="ETF 심볼")
    name: str = Field(..., description="ETF 명칭")

class ETFInfo(BaseModel):
    """ETF 상세 정보 (가격 포함)"""
    symbol: str = Field(..., description="ETF 심볼")
    name: str = Field(..., description="ETF 명칭")
    current_price: Optional[float] = Field(None, description="현재가")
    change_amount: Optional[float] = Field(None, description="전일 대비 변동액")
    change_percentage: Optional[float] = Field(None, description="전일 대비 변동률 (%)")
    volume: Optional[int] = Field(None, description="거래량")
    previous_close: Optional[float] = Field(None, description="전일 종가")
    is_positive: Optional[bool] = Field(None, description="상승/하락 여부")
    last_updated: Optional[str] = Field(None, description="최종 업데이트 시간")
    rank: Optional[int] = Field(None, description="순위")

# =========================
# ETF 프로필 및 보유종목 모델
# =========================

class SectorData(BaseModel):
    """섹터 구성 정보"""
    sector: str = Field(..., description="섹터명")
    weight: float = Field(..., description="구성 비중")
    color: Optional[str] = Field(None, description="차트 색상")

class HoldingData(BaseModel):
    """보유종목 정보"""
    symbol: str = Field(..., description="종목 심볼")
    description: str = Field(..., description="종목명")
    weight: float = Field(..., description="구성 비중")

class ETFProfile(BaseModel):
    """ETF 프로필 상세 정보"""
    symbol: str = Field(..., description="ETF 심볼")
    name: str = Field(..., description="ETF 명칭")
    net_assets: Optional[int] = Field(None, description="순자산 (달러)")
    net_expense_ratio: Optional[float] = Field(None, description="보수율")
    portfolio_turnover: Optional[float] = Field(None, description="포트폴리오 회전율")
    dividend_yield: Optional[float] = Field(None, description="배당수익률")
    inception_date: Optional[str] = Field(None, description="설정일")
    leveraged: Optional[str] = Field(None, description="레버리지 여부")
    sectors: Optional[List[SectorData]] = Field(None, description="섹터 구성")
    holdings: Optional[List[HoldingData]] = Field(None, description="주요 보유종목")

# =========================
# 차트 데이터 모델
# =========================

class ChartDataPoint(BaseModel):
    """차트 데이터 포인트"""
    timestamp: str = Field(..., description="시간")
    price: float = Field(..., description="가격")
    volume: Optional[int] = Field(None, description="거래량")
    datetime: str = Field(..., description="전체 날짜시간")
    raw_timestamp: Optional[int] = Field(None, description="원본 타임스탬프")

class ChartData(BaseModel):
    """차트 데이터"""
    symbol: str = Field(..., description="ETF 심볼")
    timeframe: str = Field(..., description="시간대")
    chart_data: List[ChartDataPoint] = Field(..., description="차트 데이터 포인트들")
    data_points: int = Field(..., description="데이터 포인트 개수")
    last_updated: str = Field(..., description="최종 업데이트 시간")

# =========================
# 섹터 및 보유종목 차트 데이터
# =========================

class SectorChartData(BaseModel):
    """섹터 파이차트 데이터"""
    name: str = Field(..., description="섹터명")
    value: float = Field(..., description="비중 (퍼센트)")
    color: str = Field(..., description="차트 색상")

class HoldingChartData(BaseModel):
    """보유종목 막대그래프 데이터"""
    symbol: str = Field(..., description="종목 심볼")
    name: str = Field(..., description="종목명")
    weight: float = Field(..., description="비중 (퍼센트)")

# =========================
# API 응답 모델
# =========================

class KeyMetrics(BaseModel):
    net_assets: str
    net_expense_ratio: str
    dividend_yield: str
    inception_year: str

class ETFDetailResponse(BaseModel):
    """(신규/수정) 개별 ETF 상세 페이지 최종 응답 스키마"""
    basic_info: Optional[ETFInfo] = None # 타입을 ETFInfo로 변경하여 더 많은 정보 포함
    profile: Optional[ETFProfile] = None # 타입을 ETFProfile로 수정
    sector_chart_data: Optional[List[SectorChartData]] = None
    holdings_chart_data: Optional[List[HoldingChartData]] = None
    key_metrics: Optional[KeyMetrics] = None
    last_updated: datetime

    class Config:
        from_attributes = True

class ETFListResponse(BaseModel):
    """ETF 리스트 응답"""
    etfs: List[ETFInfo] = Field(..., description="ETF 리스트")
    total_count: int = Field(..., description="총 ETF 개수")
    market_status: MarketStatus = Field(..., description="시장 상태")
    last_updated: str = Field(..., description="최종 업데이트 시간")
    message: Optional[str] = Field(None, description="메시지")

class ETFSearchResponse(BaseModel):
    """ETF 검색 결과 응답"""
    query: str = Field(..., description="검색어")
    results: List[ETFInfo] = Field(..., description="검색 결과")
    total_count: int = Field(..., description="총 결과 개수")
    message: Optional[str] = Field(None, description="메시지")

class MarketSummary(BaseModel):
    """시장 요약 정보"""
    total_etfs: int = Field(..., description="총 ETF 개수")
    total_trades: int = Field(..., description="총 거래 개수")
    average_price: float = Field(..., description="평균 가격")
    highest_price: float = Field(..., description="최고 가격")
    lowest_price: float = Field(..., description="최저 가격")
    total_volume: int = Field(..., description="총 거래량")
    last_updated: Optional[str] = Field(None, description="최종 업데이트 시간")

class ETFMarketOverviewResponse(BaseModel):
    """ETF 시장 개요 응답"""
    market_summary: MarketSummary = Field(..., description="시장 요약")
    market_status: MarketStatus = Field(..., description="시장 상태")
    last_updated: str = Field(..., description="최종 업데이트 시간")

# =========================
# ETF 상세 페이지 전용 모델
# =========================

class ETFKeyMetrics(BaseModel):
    """ETF 주요 지표 (포맷된 형태)"""
    net_assets: Optional[str] = Field(None, description="순자산 (포맷된)")
    net_expense_ratio: Optional[str] = Field(None, description="보수율 (포맷된)")
    dividend_yield: Optional[str] = Field(None, description="배당수익률 (포맷된)")
    inception_year: Optional[str] = Field(None, description="설정연도")

class ETFDetailCompleteResponse(BaseModel):
    """ETF 상세 정보 완전 응답"""
    # 기본 정보
    basic_info: ETFInfo = Field(..., description="기본 ETF 정보")
    
    # 프로필 정보
    profile: Optional[Dict[str, Any]] = Field(None, description="ETF 프로필")
    
    # 차트 데이터
    chart_data: List[ChartDataPoint] = Field(..., description="가격 차트 데이터")
    timeframe: str = Field(..., description="차트 시간대")
    
    # 섹터 구성 (파이차트용)
    sector_chart_data: Optional[List[SectorChartData]] = Field(None, description="섹터 파이차트 데이터")
    
    # 상위 보유종목 (막대그래프용)
    holdings_chart_data: Optional[List[HoldingChartData]] = Field(None, description="보유종목 막대그래프 데이터")
    
    # 주요 지표 (포맷된)
    key_metrics: Optional[ETFKeyMetrics] = Field(None, description="주요 지표")
    
    last_updated: str = Field(..., description="최종 업데이트 시간")

# =========================
# 폴링 API 전용 모델
# =========================

class ETFPollingMetadata(BaseModel):
    """폴링 API 메타데이터"""
    current_count: int = Field(..., description="현재 반환된 항목 수")
    total_available: int = Field(..., description="전체 사용 가능한 항목 수")
    has_more: bool = Field(..., description="더 많은 데이터 존재 여부")
    next_limit: int = Field(..., description="다음 요청 시 권장 limit")
    timestamp: str = Field(..., description="요청 타임스탬프")
    data_source: str = Field(..., description="데이터 소스")
    market_status: MarketStatus = Field(..., description="시장 상태")
    sort_info: Dict[str, str] = Field(..., description="정렬 정보")
    features: List[str] = Field(..., description="제공되는 기능 목록")

class ETFPollingResponse(BaseModel):
    """ETF 폴링 API 응답"""
    data: List[ETFInfo] = Field(..., description="ETF 데이터")
    metadata: ETFPollingMetadata = Field(..., description="메타데이터")

# =========================
# 개별 API 응답 모델
# =========================

class ETFBasicInfoResponse(BaseModel):
    """ETF 기본 정보 응답"""
    symbol: str = Field(..., description="ETF 심볼")
    name: str = Field(..., description="ETF 명칭")
    current_price: Optional[float] = Field(None, description="현재가")
    change_amount: Optional[float] = Field(None, description="변동액")
    change_percentage: Optional[float] = Field(None, description="변동률")
    volume: Optional[int] = Field(None, description="거래량")
    previous_close: Optional[float] = Field(None, description="전일 종가")
    is_positive: Optional[bool] = Field(None, description="상승/하락 여부")
    market_status: MarketStatus = Field(..., description="시장 상태")
    last_updated: Optional[str] = Field(None, description="최종 업데이트 시간")
    error: Optional[str] = Field(None, description="에러 메시지")

class ETFProfileResponse(BaseModel):
    """ETF 프로필 응답"""
    symbol: str = Field(..., description="ETF 심볼")
    profile: Optional[Dict[str, Any]] = Field(None, description="ETF 프로필 정보")
    error: Optional[str] = Field(None, description="에러 메시지")

class ETFChartResponse(BaseModel):
    """ETF 차트 응답"""
    symbol: str = Field(..., description="ETF 심볼")
    timeframe: str = Field(..., description="시간대")
    chart_data: List[ChartDataPoint] = Field(..., description="차트 데이터")
    data_points: int = Field(..., description="데이터 포인트 개수")
    market_status: MarketStatus = Field(..., description="시장 상태")
    last_updated: str = Field(..., description="최종 업데이트 시간")
    error: Optional[str] = Field(None, description="에러 메시지")

# =========================
# 서비스 상태 모델
# =========================

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

class ErrorResponse(BaseModel):
    """에러 응답"""
    error_type: str = Field(..., description="에러 타입")
    message: str = Field(..., description="에러 메시지")
    timestamp: str = Field(..., description="에러 발생 시간")
    path: str = Field(..., description="요청 경로")

# =========================
# 유틸리티 함수
# =========================

def create_error_response(error_type: str, message: str, path: str) -> ErrorResponse:
    """에러 응답 생성"""
    return ErrorResponse(
        error_type=error_type,
        message=message,
        timestamp=datetime.utcnow().isoformat(),
        path=path
    )

# =========================
# 데이터 변환 유틸리티 함수
# =========================

def format_currency(amount: Optional[float]) -> str:
    """통화 포맷팅"""
    if amount is None:
        return "N/A"
    
    if amount >= 1e12:
        return f"${amount/1e12:.2f}T"
    elif amount >= 1e9:
        return f"${amount/1e9:.2f}B"
    elif amount >= 1e6:
        return f"${amount/1e6:.2f}M"
    else:
        return f"${amount:,.0f}"

def format_percentage(ratio: Optional[float]) -> str:
    """퍼센트 포맷팅"""
    if ratio is None:
        return "N/A"
    return f"{ratio * 100:.2f}%"

def format_date(date_str: Optional[str]) -> str:
    """날짜 포맷팅"""
    if not date_str:
        return "N/A"
    try:
        return date_str[:4]  # 연도만 추출
    except:
        return "N/A"

# =========================
# 🆕 WebSocket 메시지 스키마
# =========================

class ETFUpdateMessage(BaseModel):
    """ETF WebSocket 업데이트 메시지"""
    type: str = "etf_update"
    data: List[ETFInfo]
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    data_count: int = Field(..., description="전송된 데이터 개수")
    market_status: MarketStatus = Field(..., description="시장 상태")
    data_source: str = Field(default="redis_push", description="데이터 소스")
    
    class Config:
        from_attributes = True

class ETFStatusMessage(BaseModel):
    """ETF WebSocket 상태 메시지"""
    type: str = "etf_status"
    status: str = Field(..., description="connected, disconnected, error")
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    connected_clients: Optional[int] = None
    market_status: MarketStatus = Field(..., description="시장 상태")
    data_source: str = Field(default="redis_push")

class ETFErrorMessage(BaseModel):
    """ETF WebSocket 에러 메시지"""
    type: str = "etf_error"
    error_code: str
    message: str
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    symbol: Optional[str] = None
    details: Optional[Dict[str, Any]] = None

# =========================
# 🆕 WebSocket 헬퍼 함수
# =========================

def create_etf_update_message(data: List[ETFInfo], market_status: Optional[MarketStatus] = None) -> ETFUpdateMessage:
    """ETF 업데이트 메시지 생성"""
    import pytz
    
    if not market_status:
        current_time_utc = datetime.now(pytz.UTC)
        et_timezone = pytz.timezone('US/Eastern')
        current_time_et = current_time_utc.astimezone(et_timezone)
        
        market_status = MarketStatus(
            is_open=True,
            status="UNKNOWN",
            current_time_et=current_time_et.strftime("%Y-%m-%d %H:%M:%S"),
            current_time_utc=current_time_utc.strftime("%Y-%m-%d %H:%M:%S"),
            timezone="US/Eastern"
        )
    
    return ETFUpdateMessage(
        data=data,
        data_count=len(data),
        market_status=market_status
    )

def create_etf_error_message(error_code: str, message: str, symbol: str = None, details: Dict[str, Any] = None) -> ETFErrorMessage:
    """ETF 에러 메시지 생성"""
    return ETFErrorMessage(
        error_code=error_code,
        message=message,
        symbol=symbol,
        details=details or {}
    )