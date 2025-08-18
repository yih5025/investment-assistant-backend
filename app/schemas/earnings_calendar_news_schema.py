from datetime import datetime, date, timezone
from typing import Optional, List
from pydantic import BaseModel, Field


class EarningsCalendarNewsBase(BaseModel):
    """기본 실적 캘린더 뉴스 데이터"""
    symbol: str = Field(..., description="기업 심볼", example="AAPL")
    report_date: date = Field(..., description="실적 발표 예정일")
    url: str = Field(..., description="뉴스 URL")
    headline: Optional[str] = Field(None, description="뉴스 헤드라인")
    source: Optional[str] = Field(None, description="뉴스 소스")
    published_at: datetime = Field(..., description="뉴스 발행 시간")


class EarningsCalendarNewsResponse(EarningsCalendarNewsBase):
    """API 응답용 스키마 (전체 정보)"""
    category: Optional[str] = Field(None, description="뉴스 카테고리")
    article_id: Optional[str] = Field(None, description="Finnhub 기사 ID")
    image: Optional[str] = Field(None, description="뉴스 이미지 URL")
    related: Optional[str] = Field(None, description="관련 종목 코드")
    summary: Optional[str] = Field(None, description="뉴스 요약")
    fetched_at: Optional[datetime] = Field(None, description="데이터 수집 시간")
    
    # 편의 필드
    short_headline: Optional[str] = Field(None, description="짧은 헤드라인")
    short_summary: Optional[str] = Field(None, description="짧은 요약")
    related_symbols: List[str] = Field(default_factory=list, description="관련 종목 리스트")
    has_image: bool = Field(False, description="이미지 존재 여부")

    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                "symbol": "AAPL",
                "report_date": "2025-07-25",
                "url": "https://finnhub.io/news/apple-earnings-preview",
                "headline": "Apple Q3 2025 Earnings Preview: iPhone Sales in Focus",
                "source": "MarketWatch",
                "published_at": "2025-07-20T14:30:15",
                "summary": "Apple is set to report Q3 earnings next week...",
                "has_image": True,
                "related_symbols": ["AAPL"]
            }
        }


class EarningsCalendarNewsListItem(EarningsCalendarNewsBase):
    """뉴스 목록용 스키마 (요약 정보)"""
    short_headline: Optional[str] = Field(None, description="짧은 헤드라인")
    has_image: bool = Field(False, description="이미지 존재 여부")
    fetched_at: Optional[datetime] = Field(None, description="데이터 수집 시간")

    class Config:
        from_attributes = True


class EarningsCalendarNewsListResponse(BaseModel):
    """페이징된 뉴스 목록 응답"""
    total: int = Field(..., description="전체 뉴스 개수")
    items: List[EarningsCalendarNewsListItem] = Field(..., description="뉴스 목록")
    symbol: Optional[str] = Field(None, description="현재 조회 기업 심볼")
    report_date: Optional[date] = Field(None, description="현재 조회 실적 발표일")
    page: int = Field(..., description="현재 페이지")
    limit: int = Field(..., description="페이지당 항목 수")
    has_next: bool = Field(..., description="다음 페이지 존재 여부")


class EarningsCalendarNewsSearchResponse(BaseModel):
    """검색 결과 응답"""
    total: int
    items: List[EarningsCalendarNewsResponse]
    search_query: str = Field(..., description="검색어")
    page: int
    limit: int
    has_next: bool



# 요청 파라미터 검증용 스키마
class DateRangeParams(BaseModel):
    """날짜 범위 필터링 파라미터"""
    start_date: Optional[date] = Field(None, description="시작 날짜")
    end_date: Optional[date] = Field(None, description="종료 날짜")