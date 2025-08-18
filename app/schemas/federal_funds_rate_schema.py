from datetime import date
from typing import List, Optional
from pydantic import BaseModel, Field

class FederalFundsRateResponse(BaseModel):
    """연방기금금리 API 응답 스키마"""
    date: date
    rate: Optional[float] = None
    interval_type: str = "monthly"
    unit: str = "percent"
    name: str = "Effective Federal Funds Rate"

    class Config:
        from_attributes = True

class FederalFundsRateChartData(BaseModel):
    """차트용 간소화된 데이터"""
    year: int
    month: int
    rate: float
    date_str: str  # 'YYYY-MM' 형태

class FederalFundsRateListResponse(BaseModel):
    """금리 목록 응답"""
    total_count: int
    items: List[FederalFundsRateResponse]

class FederalFundsRateChartResponse(BaseModel):
    """차트용 응답 (프론트엔드 최적화)"""
    data: List[FederalFundsRateChartData]
    current_rate: Optional[float] = None  # 현재 금리
    rate_trend: Optional[str] = None      # 'rising', 'falling', 'stable'
    change_from_prev: Optional[float] = None  # 전월 대비 변화
    avg_rate_12m: Optional[float] = None  # 12개월 평균
    max_rate: Optional[float] = None
    min_rate: Optional[float] = None

class FederalFundsRateStatsResponse(BaseModel):
    """연방기금금리 통계 정보"""
    latest_date: str  # 'YYYY-MM-DD'
    latest_rate: float
    previous_rate: Optional[float] = None
    rate_change: Optional[float] = None  # 전월 대비 변화
    trend: str  # 'rising', 'falling', 'stable'
    avg_12_months: float
    max_12_months: float
    min_12_months: float
    total_months: int

class FederalFundsRateTrendResponse(BaseModel):
    """금리 변화 트렌드 분석"""
    recent_6m: List[FederalFundsRateChartData]  # 최근 6개월
    recent_12m: List[FederalFundsRateChartData] # 최근 12개월
    trend_direction: str  # 'increasing', 'decreasing', 'stable'
    volatility_score: float  # 0-10 (변동성 점수)
    rate_cycle_phase: str  # 'easing', 'tightening', 'neutral'