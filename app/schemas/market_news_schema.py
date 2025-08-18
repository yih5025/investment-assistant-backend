from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, HttpUrl, Field, validator


class MarketNewsBase(BaseModel):
    """기본 뉴스 데이터 (공통 필드)"""
    source: str = Field(..., description="뉴스 소스", example="BBC News")
    url: str = Field(..., description="뉴스 URL")
    author: Optional[str] = Field(None, description="기사 작성자", example="Max Matza - BBC News")
    title: str = Field(..., description="뉴스 제목", min_length=1, max_length=500)
    description: Optional[str] = Field(None, description="뉴스 요약")
    published_at: datetime = Field(..., description="뉴스 발행 시간")


class MarketNewsResponse(MarketNewsBase):
    """
    API 응답용 스키마 (전체 정보)
    
    주요 특징:
    - content 포함 (전체 본문)
    - fetched_at 포함 (데이터 수집 시간)
    - short_description, content_preview 추가 (프론트엔드 편의)
    """
    content: Optional[str] = Field(None, description="뉴스 본문 내용")
    fetched_at: Optional[datetime] = Field(None, description="데이터 수집 시간")
    
    # 편의 필드 (모델에서 계산)
    short_description: Optional[str] = Field(None, description="짧은 설명 (100자 제한)")
    content_preview: Optional[str] = Field(None, description="본문 미리보기 (200자 제한)")

    class Config:
        from_attributes = True  # SQLAlchemy 모델에서 변환 허용
        
        # JSON 예시 (API 문서용)
        json_schema_extra = {
            "example": {
                "source": "BBC News",
                "url": "https://www.bbc.com/news/articles/c23g5xpggzmo",
                "author": "Max Matza - BBC News",
                "title": "Trump administration asks court to release some Epstein docs",
                "description": "The legal manoeuvre comes as Trump sues a newspaper...",
                "published_at": "2025-07-19T00:00:14",
                "content": "The US justice department has asked a judge...",
                "fetched_at": "2025-07-20T00:00:19.438356",
                "short_description": "The legal manoeuvre comes as Trump sues a newspaper that wrote about his past connections...",
                "content_preview": "The US justice department has asked a judge to unseal material related to sex offender..."
            }
        }


class MarketNewsListItem(MarketNewsBase):
    """
    뉴스 목록용 스키마 (요약 정보)
    
    목적: 
    - 리스트 조회 시 성능 최적화 (content 제외)
    - 모바일 등에서 빠른 로딩
    """
    short_description: Optional[str] = Field(None, description="짧은 설명")
    fetched_at: Optional[datetime] = Field(None, description="데이터 수집 시간")

    class Config:
        from_attributes = True


class MarketNewsListResponse(BaseModel):
    """
    페이징된 뉴스 목록 응답
    
    구조:
    - total: 전체 개수
    - items: 뉴스 목록
    - page, limit: 페이징 정보
    - has_next: 다음 페이지 존재 여부
    """
    total: int = Field(..., description="전체 뉴스 개수")
    items: List[MarketNewsListItem] = Field(..., description="뉴스 목록")
    page: int = Field(..., description="현재 페이지 (1부터 시작)")
    limit: int = Field(..., description="페이지당 항목 수")
    has_next: bool = Field(..., description="다음 페이지 존재 여부")

    class Config:
        json_schema_extra = {
            "example": {
                "total": 1250,
                "items": [
                    {
                        "source": "BBC News",
                        "url": "https://www.bbc.com/news/articles/c23g5xpggzmo",
                        "title": "Trump administration asks court to release some Epstein docs",
                        "published_at": "2025-07-19T00:00:14"
                    }
                ],
                "page": 1,
                "limit": 20,
                "has_next": True
            }
        }


class MarketNewsSearchResponse(BaseModel):
    """
    검색 결과 응답 (검색어 하이라이팅 지원)
    """
    total: int
    items: List[MarketNewsResponse]
    search_query: str = Field(..., description="검색어")
    page: int
    limit: int
    has_next: bool


# 요청 파라미터 검증용 스키마들
class DateRangeParams(BaseModel):
    """날짜 범위 필터링 파라미터"""
    start_date: Optional[datetime] = Field(None, description="시작 날짜")
    end_date: Optional[datetime] = Field(None, description="종료 날짜")
    
    @validator('end_date')
    def validate_date_range(cls, v, values):
        """종료 날짜가 시작 날짜보다 늦는지 검증"""
        if v and 'start_date' in values and values['start_date']:
            if v < values['start_date']:
                raise ValueError('종료 날짜는 시작 날짜보다 늦어야 합니다')
        return v


class SourceFilterParams(BaseModel):
    """소스 필터링 파라미터"""
    sources: Optional[List[str]] = Field(None, description="필터링할 소스 목록")
    exclude_sources: Optional[List[str]] = Field(None, description="제외할 소스 목록")