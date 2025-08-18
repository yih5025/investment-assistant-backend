from datetime import date
from typing import List, Optional
from pydantic import BaseModel

class CPIResponse(BaseModel):
    """소비자물가지수 API 응답 스키마"""
    date: date
    cpi_value: Optional[float] = None
    interval_type: str = "monthly"
    unit: str = "index 1982-1984=100"
    name: str = "Consumer Price Index for all Urban Consumers"

    class Config:
        from_attributes = True

class CPIChartData(BaseModel):
    """차트용 간소화된 데이터"""
    year: int
    month: int
    cpi_value: float
    date_str: str  # 'YYYY-MM' 형태
    inflation_rate: Optional[float] = None  # 전년 동월 대비 인플레이션율

class CPIListResponse(BaseModel):
    """CPI 목록 응답"""
    total_count: int
    items: List[CPIResponse]

class CPIChartResponse(BaseModel):
    """차트용 응답 (프론트엔드 최적화)"""
    data: List[CPIChartData]
    current_cpi: Optional[float] = None      # 현재 CPI 값
    previous_cpi: Optional[float] = None     # 전월 CPI 값
    monthly_change: Optional[float] = None   # 전월 대비 변화율
    annual_inflation: Optional[float] = None # 연간 인플레이션율
    avg_cpi_12m: Optional[float] = None      # 12개월 평균
    max_cpi: Optional[float] = None
    min_cpi: Optional[float] = None

class CPIStatsResponse(BaseModel):
    """CPI 통계 정보"""
    latest_date: str
    latest_cpi: float
    previous_month_cpi: Optional[float] = None
    monthly_change: Optional[float] = None           # 전월 대비 변화율 (%)
    monthly_change_value: Optional[float] = None     # 전월 대비 변화값
    annual_inflation: Optional[float] = None         # 연간 인플레이션율 (%)
    avg_12_months: float
    max_12_months: float
    min_12_months: float
    trend: str  # 'rising', 'falling', 'stable'
    total_months: int

class CPIInflationAnalysis(BaseModel):
    """인플레이션 분석"""
    recent_6m: List[CPIChartData]    # 최근 6개월 CPI
    recent_12m: List[CPIChartData]   # 최근 12개월 CPI
    avg_inflation_6m: float          # 6개월 평균 인플레이션
    avg_inflation_12m: float         # 12개월 평균 인플레이션
    inflation_trend: str             # 'accelerating', 'decelerating', 'stable'
    volatility_score: float          # 변동성 점수 (0-10)
    price_stability_status: str      # 'stable', 'moderate_inflation', 'high_inflation'

class CPIComparisonResponse(BaseModel):
    """CPI 기간별 비교"""
    start_date: str
    end_date: str
    start_cpi: float
    end_cpi: float
    total_change: float              # 절대 변화값
    total_change_percent: float      # 변화율 (%)
    average_monthly_change: float    # 월평균 변화율
    cumulative_inflation: float      # 누적 인플레이션율

class CPIMonthlyDetailResponse(BaseModel):
    """특정월 CPI 상세 정보"""
    date: date
    cpi_value: float
    
    # 인플레이션 분석
    year_over_year_inflation: Optional[float] = None      # 전년 동월 대비 인플레이션율
    month_over_month_change: Optional[float] = None       # 전월 대비 변화율
    month_over_month_value: Optional[float] = None        # 전월 대비 절대값 변화
    
    # 비교 데이터
    previous_month_cpi: Optional[float] = None            # 전월 CPI
    previous_year_cpi: Optional[float] = None             # 전년 동월 CPI
    
    # 메타 정보
    interval_type: str = "monthly"
    unit: str = "index 1982-1984=100"
    name: str = "Consumer Price Index for all Urban Consumers"
    
    # 추가 분석 (선택적)
    ranking_in_year: Optional[int] = None                 # 해당 연도 내 순위 (1=최고)
    is_yearly_high: bool = False                          # 연중 최고값 여부
    is_yearly_low: bool = False                           # 연중 최저값 여부