# app/schemas/ipo_calendar_schema.py
from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import date, datetime

class IPOCalendarBase(BaseModel):
    """IPO 캘린더 기본 스키마"""
    symbol: str = Field(..., description="기업 심볼", example="NP")
    company_name: str = Field(..., description="기업명", example="Neenah Paper Inc")
    ipo_date: date = Field(..., description="IPO 예정일", example="2025-10-01")
    price_range_low: Optional[float] = Field(None, description="공모가 하한선", example=18.0)
    price_range_high: Optional[float] = Field(None, description="공모가 상한선", example=20.0)
    currency: Optional[str] = Field("USD", description="통화", example="USD")
    exchange: Optional[str] = Field(None, description="거래소", example="NYSE")

class IPOCalendarResponse(IPOCalendarBase):
    """IPO 캘린더 응답 스키마"""
    id: int = Field(..., description="고유 ID", example=1)
    fetched_at: Optional[datetime] = Field(None, description="데이터 수집 시간")
    created_at: Optional[datetime] = Field(None, description="생성 시간")
    updated_at: Optional[datetime] = Field(None, description="수정 시간")
    
    model_config = {
        "from_attributes": True,
        "json_encoders": {
            datetime: lambda v: v.isoformat() if v else None,
            date: lambda v: v.isoformat() if v else None,
        }
    }

class IPOCalendarListResponse(BaseModel):
    """IPO 캘린더 목록 응답"""
    items: List[IPOCalendarResponse] = Field(..., description="IPO 일정 목록")
    total_count: int = Field(..., description="전체 항목 수", example=45)
    start_date: Optional[date] = Field(None, description="조회 시작일")
    end_date: Optional[date] = Field(None, description="조회 종료일")

class IPOCalendarMonthlyResponse(BaseModel):
    """이번 달 IPO 일정 응답"""
    month: str = Field(..., description="조회 월", example="2025-10")
    items: List[IPOCalendarResponse] = Field(..., description="IPO 일정 목록")
    total_count: int = Field(..., description="이번 달 IPO 개수", example=12)

class IPOCalendarStatistics(BaseModel):
    """IPO 통계 정보"""
    total_ipos: int = Field(..., description="전체 IPO 개수", example=45)
    this_month: int = Field(..., description="이번 달 IPO 개수", example=12)
    next_month: int = Field(..., description="다음 달 IPO 개수", example=8)
    by_exchange: dict = Field(..., description="거래소별 개수", example={"NYSE": 20, "NASDAQ": 25})
    avg_price_range: dict = Field(..., description="평균 공모가 범위", example={"low": 18.5, "high": 24.3})
    upcoming_7days: int = Field(..., description="향후 7일 내 IPO 개수", example=3)