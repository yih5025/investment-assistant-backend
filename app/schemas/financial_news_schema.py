from datetime import datetime
from typing import Optional, List, Literal
from pydantic import BaseModel, HttpUrl, Field, field_validator


# 카테고리 타입 정의 (타입 안전성)
CategoryType = Literal["crypto", "forex", "merger", "general"]


class FinancialNewsBase(BaseModel):
    """기본 금융 뉴스 데이터 (공통 필드)"""
    category: CategoryType = Field(..., description="뉴스 카테고리")
    news_id: int = Field(..., description="Finnhub 뉴스 고유 ID", ge=1)
    published_at: datetime = Field(..., description="뉴스 발행 시간")
    headline: Optional[str] = Field(None, description="뉴스 헤드라인")
    source: Optional[str] = Field(None, description="뉴스 원본 소스")


class FinancialNewsResponse(FinancialNewsBase):
    """
    API 응답용 스키마 (전체 정보)
    
    주요 특징:
    - 모든 필드 포함
    - 계산된 편의 필드 추가 (short_headline, related_symbols 등)
    - 이미지 URL 검증
    """
    image: Optional[str] = Field(None, description="뉴스 이미지 URL")
    related: Optional[str] = Field(None, description="관련 종목 코드 (쉼표 구분)")
    summary: Optional[str] = Field(None, description="뉴스 요약")
    url: Optional[str] = Field(None, description="원본 뉴스 URL")
    fetched_at: Optional[datetime] = Field(None, description="데이터 수집 시간")
    
    # 편의 필드 (모델에서 계산)
    short_headline: Optional[str] = Field(None, description="짧은 헤드라인 (100자 제한)")
    short_summary: Optional[str] = Field(None, description="짧은 요약 (200자 제한)")
    related_symbols: List[str] = Field(default_factory=list, description="관련 종목 리스트")
    has_image: bool = Field(False, description="이미지 존재 여부")
    category_display_name: Optional[str] = Field(None, description="카테고리 표시명")

    class Config:
        from_attributes = True
        
        # JSON 예시 (API 문서용)
        json_schema_extra = {
            "example": {
                "category": "crypto",
                "news_id": 12345678901,
                "published_at": "2025-07-20T14:30:15",
                "headline": "Bitcoin reaches new all-time high amid institutional adoption",
                "source": "CoinDesk",
                "image": "https://example.com/bitcoin-news.jpg",
                "related": "BTCUSD,ETHUSD",
                "summary": "Bitcoin price surged to a new record high as major institutions...",
                "url": "https://coindesk.com/bitcoin-ath-2025",
                "fetched_at": "2025-07-20T15:00:00",
                "short_headline": "Bitcoin reaches new all-time high amid institutional adoption",
                "short_summary": "Bitcoin price surged to a new record high as major institutions continue to adopt...",
                "related_symbols": ["BTCUSD", "ETHUSD"],
                "has_image": True,
                "category_display_name": "암호화폐"
            }
        }


class FinancialNewsListItem(FinancialNewsBase):
    """
    뉴스 목록용 스키마 (요약 정보)
    
    목적: 
    - 리스트 조회 시 성능 최적화 (summary, url 제외)
    - 모바일 앱 등에서 빠른 로딩
    """
    short_headline: Optional[str] = Field(None, description="짧은 헤드라인")
    has_image: bool = Field(False, description="이미지 존재 여부")
    related_symbols: List[str] = Field(default_factory=list, description="관련 종목 리스트")
    category_display_name: Optional[str] = Field(None, description="카테고리 표시명")
    fetched_at: Optional[datetime] = Field(None, description="데이터 수집 시간")

    class Config:
        from_attributes = True


class FinancialNewsListResponse(BaseModel):
    """
    페이징된 금융 뉴스 목록 응답
    
    구조:
    - total: 전체 개수
    - items: 뉴스 목록
    - category: 현재 조회 카테고리 (필터링 시)
    - page, limit: 페이징 정보
    - has_next: 다음 페이지 존재 여부
    """
    total: int = Field(..., description="전체 뉴스 개수")
    items: List[FinancialNewsListItem] = Field(..., description="뉴스 목록")
    category: Optional[str] = Field(None, description="현재 조회 카테고리")
    page: int = Field(..., description="현재 페이지 (1부터 시작)")
    limit: int = Field(..., description="페이지당 항목 수")
    has_next: bool = Field(..., description="다음 페이지 존재 여부")

    class Config:
        json_schema_extra = {
            "example": {
                "total": 850,
                "items": [
                    {
                        "category": "crypto",
                        "news_id": 12345678901,
                        "published_at": "2025-07-20T14:30:15",
                        "headline": "Bitcoin reaches new all-time high",
                        "source": "CoinDesk",
                        "has_image": True,
                        "related_symbols": ["BTCUSD", "ETHUSD"],
                        "category_display_name": "암호화폐"
                    }
                ],
                "category": "crypto",
                "page": 1,
                "limit": 20,
                "has_next": True
            }
        }


class FinancialNewsSearchResponse(BaseModel):
    """검색 결과 응답"""
    total: int
    items: List[FinancialNewsResponse]
    search_query: str = Field(..., description="검색어")
    categories: Optional[List[str]] = Field(None, description="검색 대상 카테고리")
    page: int
    limit: int
    has_next: bool


class CategoryStatsResponse(BaseModel):
    """카테고리별 통계 응답"""
    category: str = Field(..., description="카테고리")
    category_display_name: str = Field(..., description="카테고리 표시명")
    count: int = Field(..., description="뉴스 개수")
    latest_news_date: Optional[datetime] = Field(None, description="최신 뉴스 날짜")


class CategoriesStatsResponse(BaseModel):
    """전체 카테고리 통계 응답"""
    categories: List[CategoryStatsResponse] = Field(..., description="카테고리별 통계")
    total_news: int = Field(..., description="전체 뉴스 개수")
    available_categories: List[str] = Field(..., description="사용 가능한 카테고리 목록")


# 요청 파라미터 검증용 스키마들
class CategoryFilterParams(BaseModel):
    """카테고리 필터링 파라미터"""
    categories: Optional[List[CategoryType]] = Field(None, description="필터링할 카테고리 목록")
    
    @field_validator('categories')
    @classmethod
    def validate_categories(cls, v):
        """카테고리 유효성 검증"""
        if v:
            valid_categories = ["crypto", "forex", "merger", "general"]
            for category in v:
                if category not in valid_categories:
                    raise ValueError(f'유효하지 않은 카테고리: {category}. 사용 가능한 카테고리: {valid_categories}')
        return v


class DateRangeParams(BaseModel):
    """날짜 범위 필터링 파라미터"""
    start_date: Optional[datetime] = Field(None, description="시작 날짜")
    end_date: Optional[datetime] = Field(None, description="종료 날짜")
    
    @field_validator('end_date')
    @classmethod
    def validate_date_range(cls, v, info):
        """종료 날짜가 시작 날짜보다 늦는지 검증"""
        if v and info.data.get('start_date'):
            if v < info.data['start_date']:
                raise ValueError('종료 날짜는 시작 날짜보다 늦어야 합니다')
        return v


class RelatedSymbolsParams(BaseModel):
    """관련 종목 필터링 파라미터"""
    symbols: Optional[List[str]] = Field(None, description="관련 종목 필터링")
    
    @field_validator('symbols')
    @classmethod
    def validate_symbols(cls, v):
        """종목 코드 형식 검증"""
        if v:
            for symbol in v:
                if not symbol or len(symbol.strip()) < 2:
                    raise ValueError(f'유효하지 않은 종목 코드: {symbol}')
        return [symbol.strip().upper() for symbol in v] if v else v