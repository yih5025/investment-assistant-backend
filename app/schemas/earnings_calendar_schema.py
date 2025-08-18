from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import date, datetime

class EarningsCalendarBase(BaseModel):
    """실적 캘린더 기본 스키마"""
    symbol: str = Field(..., description="주식 심볼", example="AAPL")
    company_name: Optional[str] = Field(None, description="회사명", example="Apple Inc.")
    report_date: date = Field(..., description="실적 발표 예정일", example="2025-07-31")
    fiscal_date_ending: Optional[date] = Field(None, description="회계 연도 종료일", example="2025-06-30")
    estimate: Optional[float] = Field(None, description="예상 EPS", example=1.41)
    currency: Optional[str] = Field(None, description="통화", example="USD")
    event_description: Optional[str] = Field(None, description="이벤트 설명")
    fetched_at: Optional[datetime] = Field(None, description="데이터 수집 시간")

class EarningsCalendarResponse(EarningsCalendarBase):
    """실적 캘린더 응답 스키마"""
    
    model_config = {
        "from_attributes": True,
        "json_encoders": {
            datetime: lambda v: v.isoformat() if v else None,
            date: lambda v: v.isoformat() if v else None,
        }
    }

class EarningsCalendarListResponse(BaseModel):
    """실적 캘린더 목록 응답 스키마"""
    items: List[EarningsCalendarResponse] = Field(..., description="실적 발표 일정 목록")
    total_count: int = Field(..., description="전체 항목 수", example=150)

class EarningsCalendarQueryParams(BaseModel):
    """실적 캘린더 쿼리 파라미터"""
    start_date: Optional[date] = Field(None, description="조회 시작일", example="2025-08-01")
    end_date: Optional[date] = Field(None, description="조회 종료일", example="2025-08-31")
    symbol: Optional[str] = Field(None, description="특정 주식 심볼", example="AAPL")
    limit: int = Field(50, ge=1, le=1000, description="최대 조회 개수", example=50)
    offset: int = Field(0, ge=0, description="건너뛸 개수", example=0)