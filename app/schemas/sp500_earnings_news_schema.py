# app/schemas/sp500_earnings_news_schema.py
from pydantic import BaseModel, Field, field_validator
from typing import Optional, List
from datetime import datetime

class SP500EarningsNewsBase(BaseModel):
    """S&P 500 실적 뉴스 기본 스키마"""
    calendar_id: int = Field(..., description="실적 캘린더 ID", example=8)
    source_table: Optional[str] = Field(None, description="소스 테이블명", example="earnings_news_finnhub")
    title: Optional[str] = Field(None, description="뉴스 제목")
    url: Optional[str] = Field(None, description="뉴스 URL")
    summary: Optional[str] = Field(None, description="뉴스 요약")
    content: Optional[str] = Field(None, description="뉴스 본문")
    source: Optional[str] = Field(None, description="뉴스 소스", example="Yahoo")
    published_at: Optional[datetime] = Field(None, description="뉴스 게시 시간")
    news_section: Optional[str] = Field(None, description="뉴스 섹션", example="forecast")
    days_from_earnings: Optional[int] = Field(None, description="실적 발표일로부터 며칠 차이", example=-14)

class SP500EarningsNewsResponse(SP500EarningsNewsBase):
    """S&P 500 실적 뉴스 응답 스키마"""
    id: int = Field(..., description="고유 ID", example=1)
    fetched_at: Optional[datetime] = Field(None, description="뉴스 수집 시간")
    created_at: Optional[datetime] = Field(None, description="생성 시간")
    
    # 추가 계산된 필드들
    is_forecast_news: Optional[bool] = Field(None, description="예측 뉴스 여부")
    is_reaction_news: Optional[bool] = Field(None, description="반응 뉴스 여부")
    has_content: Optional[bool] = Field(None, description="본문 내용 존재 여부")
    short_title: Optional[str] = Field(None, description="축약된 제목 (50자)")
    
    model_config = {
        "from_attributes": True,
        "json_encoders": {
            datetime: lambda v: v.isoformat() if v else None,
        }
    }
    
    @field_validator('title')
    @classmethod
    def clean_title(cls, v):
        """제목 정리 (불필요한 공백 제거)"""
        return v.strip() if v else None
    
    @field_validator('summary')
    @classmethod
    def clean_summary(cls, v):
        """요약 정리"""
        return v.strip() if v else None

class SP500EarningsNewsListResponse(BaseModel):
    """S&P 500 실적 뉴스 목록 응답 스키마"""
    calendar_id: int = Field(..., description="실적 캘린더 ID", example=8)
    items: List[SP500EarningsNewsResponse] = Field(..., description="뉴스 목록")
    total_count: int = Field(..., description="전체 뉴스 수", example=50)
    forecast_count: int = Field(..., description="예측 뉴스 수", example=30)
    reaction_count: int = Field(..., description="반응 뉴스 수", example=20)
    message: Optional[str] = Field(None, description="메시지")

class SP500EarningsNewsWithCalendarResponse(BaseModel):
    """실적 캘린더 정보가 포함된 뉴스 응답"""
    # 캘린더 정보
    calendar_info: dict = Field(..., description="관련 실적 캘린더 정보")
    
    # 뉴스 목록 (섹션별 분류)
    forecast_news: List[SP500EarningsNewsResponse] = Field(..., description="예측 뉴스 목록")
    reaction_news: List[SP500EarningsNewsResponse] = Field(..., description="반응 뉴스 목록")
    
    # 통계 정보
    total_news_count: int = Field(..., description="총 뉴스 수")
    forecast_news_count: int = Field(..., description="예측 뉴스 수")
    reaction_news_count: int = Field(..., description="반응 뉴스 수")
    
    message: Optional[str] = Field(None, description="메시지")

class SP500EarningsNewsQueryParams(BaseModel):
    """S&P 500 실적 뉴스 쿼리 파라미터"""
    news_section: Optional[str] = Field(None, description="뉴스 섹션 필터", example="forecast")
    source: Optional[str] = Field(None, description="특정 뉴스 소스", example="Yahoo")
    start_date: Optional[datetime] = Field(None, description="게시일 시작")
    end_date: Optional[datetime] = Field(None, description="게시일 종료")
    has_content: Optional[bool] = Field(None, description="본문 내용 존재하는 것만", example=True)
    limit: int = Field(50, ge=1, le=200, description="최대 조회 개수", example=20)
    offset: int = Field(0, ge=0, description="건너뛸 개수", example=0)

# 뉴스 통계 정보 (추가 기능)
class SP500EarningsNewsStats(BaseModel):
    """특정 캘린더의 뉴스 통계"""
    calendar_id: int = Field(..., description="실적 캘린더 ID")
    total_news: int = Field(..., description="총 뉴스 수")
    forecast_news: int = Field(..., description="예측 뉴스 수")
    reaction_news: int = Field(..., description="반응 뉴스 수")
    news_sources: List[str] = Field(..., description="뉴스 소스 목록")
    earliest_news: Optional[datetime] = Field(None, description="가장 오래된 뉴스 날짜")
    latest_news: Optional[datetime] = Field(None, description="가장 최신 뉴스 날짜")
    has_content_count: int = Field(..., description="본문이 있는 뉴스 수")

# 뉴스 검색용 응답 (추후 확장)
class SP500EarningsNewsSearchResponse(BaseModel):
    """뉴스 검색 결과"""
    query: str = Field(..., description="검색어")
    results: List[SP500EarningsNewsResponse] = Field(..., description="검색 결과")
    total_count: int = Field(..., description="검색 결과 총 개수")
    search_time: float = Field(..., description="검색 소요 시간 (초)")
    message: Optional[str] = Field(None, description="메시지")