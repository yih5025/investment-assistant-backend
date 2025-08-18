from pydantic import BaseModel, Field
from datetime import datetime
from typing import List, Optional, Dict, Any

# =============================================================================
# 기본 뉴스 스키마
# =============================================================================

class CompanyNewsBase(BaseModel):
    """기업 뉴스 기본 스키마"""
    symbol: str = Field(..., description="주식 심볼", example="AAPL")
    url: str = Field(..., description="뉴스 URL")
    source: Optional[str] = Field(None, description="뉴스 소스", example="Reuters")
    title: Optional[str] = Field(None, description="뉴스 제목")
    description: Optional[str] = Field(None, description="뉴스 요약")
    content: Optional[str] = Field(None, description="뉴스 전체 내용")
    published_at: datetime = Field(..., description="뉴스 발행 시간")
    fetched_at: Optional[datetime] = Field(None, description="데이터 수집 시간")

class CompanyNewsResponse(CompanyNewsBase):
    """단일 뉴스 응답 스키마"""
    
    class Config:
        from_attributes = True  # SQLAlchemy 모델에서 변환 허용


# =============================================================================
# 트렌딩 주식 정보 스키마
# =============================================================================

class TrendingStockInfo(BaseModel):
    """트렌딩 주식 정보 스키마"""
    symbol: str = Field(..., description="주식 심볼")
    category: str = Field(..., description="카테고리", 
                         example="top_gainers")
    rank_position: Optional[int] = Field(None, description="순위")
    price: Optional[float] = Field(None, description="현재 가격")
    change_percentage: Optional[str] = Field(None, description="변동률", 
                                           example="+5.2%")
    volume: Optional[int] = Field(None, description="거래량")


class TrendingStockWithNews(TrendingStockInfo):
    """뉴스가 포함된 트렌딩 주식 스키마"""
    news_count: int = Field(..., description="뉴스 개수")
    news: List[CompanyNewsResponse] = Field(..., description="뉴스 목록")


# =============================================================================
# 배치 정보 스키마
# =============================================================================

class BatchInfo(BaseModel):
    """배치 정보 스키마"""
    batch_id: int = Field(..., description="배치 ID")
    last_updated: datetime = Field(..., description="마지막 업데이트 시간")
    total_symbols: int = Field(..., description="총 심볼 개수")
    period_days: int = Field(..., description="뉴스 조회 기간(일)")


class CategorySummary(BaseModel):
    """카테고리별 요약 정보"""
    top_gainers: int = Field(..., description="상승 주식 개수")
    top_losers: int = Field(..., description="하락 주식 개수") 
    most_actively_traded: int = Field(..., description="활발한 주식 개수")


# =============================================================================
# API 응답 스키마들
# =============================================================================

class TrendingNewsResponse(BaseModel):
    """트렌딩 뉴스 전체 응답 스키마 (통합 버전)"""
    batch_info: BatchInfo = Field(..., description="배치 정보")
    categories: CategorySummary = Field(..., description="카테고리별 요약")
    stocks: List[TrendingStockWithNews] = Field(..., description="주식별 뉴스")
    
    class Config:
        json_schema_extra = {
            "example": {
                "batch_info": {
                    "batch_id": 11,
                    "last_updated": "2025-07-29T16:15:58",
                    "total_symbols": 50,
                    "period_days": 3
                },
                "categories": {
                    "top_gainers": 20,
                    "top_losers": 10,
                    "most_actively_traded": 20
                },
                "stocks": [
                    {
                        "symbol": "TSLA",
                        "category": "most_actively_traded",
                        "rank_position": 20,
                        "price": 321.20,
                        "change_percentage": "-1.3483%",
                        "news_count": 5,
                        "news": []
                    }
                ]
            }
        }


class CategoryNewsResponse(BaseModel):
    """카테고리별 뉴스 응답 스키마"""
    batch_info: BatchInfo = Field(..., description="배치 정보")
    category: str = Field(..., description="카테고리명")
    symbols_count: int = Field(..., description="해당 카테고리 심볼 개수")
    stocks: List[TrendingStockWithNews] = Field(..., description="주식별 뉴스")


class SymbolNewsResponse(BaseModel):
    """특정 심볼 뉴스 응답 스키마"""
    symbol: str = Field(..., description="주식 심볼")
    period_days: int = Field(..., description="조회 기간")
    total_news: int = Field(..., description="총 뉴스 개수")
    latest_batch_info: Optional[Dict[str, Any]] = Field(
        None, 
        description="최신 배치에서의 트렌딩 정보"
    )
    news: List[CompanyNewsResponse] = Field(..., description="뉴스 목록")


# =============================================================================
# 쿼리 파라미터 스키마
# =============================================================================

class NewsQueryParams(BaseModel):
    """뉴스 조회 쿼리 파라미터"""
    days: int = Field(3, ge=1, le=7, description="뉴스 조회 기간 (1-7일)")
    limit: int = Field(20, ge=1, le=100, description="뉴스 개수 제한")
    offset: int = Field(0, ge=0, description="페이징 오프셋")