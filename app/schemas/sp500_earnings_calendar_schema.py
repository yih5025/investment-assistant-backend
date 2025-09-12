# app/schemas/sp500_earnings_calendar_schema.py
from pydantic import BaseModel, Field, field_validator
from typing import Optional, List
from datetime import date, datetime

class SP500EarningsCalendarBase(BaseModel):
    """S&P 500 실적 캘린더 기본 스키마"""
    symbol: str = Field(..., description="주식 심볼", example="AAPL")
    company_name: Optional[str] = Field(None, description="회사명", example="Apple Inc.")
    report_date: date = Field(..., description="실적 발표 예정일", example="2025-08-19")
    fiscal_date_ending: Optional[date] = Field(None, description="회계 연도 종료일", example="2025-07-31")
    estimate: Optional[float] = Field(None, description="예상 EPS", example=4.55)
    currency: Optional[str] = Field(None, description="통화", example="USD")
    
    # GICS 분류 정보
    gics_sector: Optional[str] = Field(None, description="GICS 섹터", example="Consumer Discretionary")
    gics_sub_industry: Optional[str] = Field(None, description="GICS 세부 산업", example="Home Improvement Retail")
    headquarters: Optional[str] = Field(None, description="본사 위치", example="Atlanta, Georgia")
    
    # 이벤트 정보
    event_type: Optional[str] = Field(None, description="이벤트 타입", example="earnings_report")
    event_title: Optional[str] = Field(None, description="이벤트 제목", example="HD 실적 발표 (예상 EPS: $4.55)")
    event_description: Optional[str] = Field(None, description="이벤트 설명")
    
    # 뉴스 카운트 정보
    total_news_count: Optional[int] = Field(None, description="총 뉴스 개수", example=50)
    forecast_news_count: Optional[int] = Field(None, description="예측 뉴스 개수", example=30)
    reaction_news_count: Optional[int] = Field(None, description="반응 뉴스 개수", example=20)

class SP500EarningsCalendarResponse(SP500EarningsCalendarBase):
    """S&P 500 실적 캘린더 응답 스키마"""
    id: int = Field(..., description="고유 ID", example=8)
    created_at: Optional[datetime] = Field(None, description="생성 시간")
    updated_at: Optional[datetime] = Field(None, description="수정 시간")
    
    # 추가 계산된 필드들
    has_estimate: Optional[bool] = Field(None, description="예상 수익 존재 여부")
    is_future_date: Optional[bool] = Field(None, description="미래 일정 여부")
    has_news: Optional[bool] = Field(None, description="관련 뉴스 존재 여부")
    
    model_config = {
        "from_attributes": True,
        "json_encoders": {
            datetime: lambda v: v.isoformat() if v else None,
            date: lambda v: v.isoformat() if v else None,
        }
    }
    
    @field_validator('estimate')
    @classmethod
    def round_estimate(cls, v):
        """예상 수익 반올림"""
        return round(v, 2) if v is not None else None

class SP500EarningsCalendarListResponse(BaseModel):
    """S&P 500 실적 캘린더 목록 응답 스키마"""
    items: List[SP500EarningsCalendarResponse] = Field(..., description="실적 발표 일정 목록")
    total_count: int = Field(..., description="전체 항목 수", example=150)
    message: Optional[str] = Field(None, description="메시지")

class SP500EarningsCalendarWeeklyResponse(BaseModel):
    """S&P 500 실적 캘린더 주간 응답 스키마"""
    week_start: date = Field(..., description="주 시작일 (월요일)", example="2025-08-18")
    week_end: date = Field(..., description="주 종료일 (일요일)", example="2025-08-24")
    events: List[SP500EarningsCalendarResponse] = Field(..., description="이번 주 실적 일정")
    total_count: int = Field(..., description="이번 주 실적 개수", example=15)
    message: Optional[str] = Field(None, description="메시지")

class SP500EarningsCalendarQueryParams(BaseModel):
    """S&P 500 실적 캘린더 쿼리 파라미터"""
    start_date: Optional[date] = Field(None, description="조회 시작일", example="2025-08-01")
    end_date: Optional[date] = Field(None, description="조회 종료일", example="2025-08-31")
    symbol: Optional[str] = Field(None, description="특정 주식 심볼", example="AAPL")
    sector: Optional[str] = Field(None, description="특정 섹터", example="Information Technology")
    has_estimate: Optional[bool] = Field(None, description="예상 수익 존재하는 것만", example=True)
    limit: int = Field(100, ge=1, le=1000, description="최대 조회 개수", example=50)
    offset: int = Field(0, ge=0, description="건너뛸 개수", example=0)

# 심볼별 실적 일정 응답용 (옵션)
class SP500EarningsCalendarBySymbolResponse(BaseModel):
    """특정 심볼의 실적 일정 응답"""
    symbol: str = Field(..., description="주식 심볼", example="AAPL")
    company_name: Optional[str] = Field(None, description="회사명", example="Apple Inc.")
    earnings: List[SP500EarningsCalendarResponse] = Field(..., description="실적 일정 목록")
    total_count: int = Field(..., description="해당 심볼의 총 실적 개수", example=4)
    message: Optional[str] = Field(None, description="메시지")

# 캘린더 통계 정보 (추가 기능)
class SP500EarningsCalendarStats(BaseModel):
    """S&P 500 실적 캘린더 통계"""
    total_companies: int = Field(..., description="총 회사 수", example=25)
    total_events: int = Field(..., description="총 실적 이벤트 수", example=156)
    events_with_estimates: int = Field(..., description="예상 수익이 있는 이벤트 수", example=120)
    events_with_news: int = Field(..., description="뉴스가 있는 이벤트 수", example=89)
    upcoming_events: int = Field(..., description="향후 예정된 이벤트 수", example=45)
    sectors_covered: List[str] = Field(..., description="포함된 섹터 목록")
    last_updated: Optional[datetime] = Field(None, description="마지막 업데이트 시간")