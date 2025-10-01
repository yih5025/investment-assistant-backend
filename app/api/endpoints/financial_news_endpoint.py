from datetime import datetime, timezone
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, Path
from sqlalchemy.orm import Session

from app.dependencies import get_db
from app.services.financial_news_service import FinancialNewsService
from app.schemas.financial_news_schema import (
    FinancialNewsResponse,
    FinancialNewsListResponse,
    FinancialNewsSearchResponse,
    CategoriesStatsResponse,
    CategoryType
)

# 라우터 생성 (태그로 API 문서 그룹화)
router = APIRouter(
    tags=["Financial News"],
    responses={
        404: {"description": "뉴스를 찾을 수 없습니다"},
        422: {"description": "잘못된 요청 파라미터"},
        500: {"description": "서버 내부 오류"}
    }
)


@router.get(
    "/",
    response_model=FinancialNewsListResponse,
    summary="카테고리 금융 뉴스 목록 (Finnhub 수집 데이터)"
)
async def get_financial_news_list(
    page: int = Query(1, ge=1, description="페이지 번호 (1부터 시작)"),
    limit: int = Query(20, ge=1, le=100, description="페이지당 항목 수 (최대 100)"),
    categories: Optional[List[CategoryType]] = Query(None, description="카테고리 필터 (crypto, forex, merger, general)"),
    start_date: Optional[datetime] = Query(None, description="시작 날짜 (YYYY-MM-DD 또는 ISO 형식)"),
    end_date: Optional[datetime] = Query(None, description="종료 날짜 (YYYY-MM-DD 또는 ISO 형식)"),
    symbols: Optional[List[str]] = Query(None, description="관련 종목 필터 (BTCUSD, AAPL 등)"),
    sources: Optional[List[str]] = Query(None, description="뉴스 소스 필터"),
    db: Session = Depends(get_db)
):
    """
    Finnhub 카테고리별로 수집해 DB(`market_news_finnhub`)에 저장된 뉴스를 조회합니다.
    
    - 데이터 수집: Airflow DAG `ingest_market_news_finnhub_k8s`
      - 카테고리: crypto, forex, merger, general (Finnhub 공식 카테고리)
      - 증분 수집: `minId` 기반
      - 스케줄: 매시간 수집
    
    - 주요 기능:
      - 카테고리/기간/소스/종목(related) 필터
      - 페이징: page, limit
    
    - 예시:
      - GET /api/v1/financial-news/?categories=crypto&limit=10
      - GET /api/v1/financial-news/?symbols=BTCUSD,ETHUSD&start_date=2025-07-01
      - GET /api/v1/financial-news/?categories=merger&sources=Reuters
    """
    try:
        skip = (page - 1) * limit
        service = FinancialNewsService(db)
        
        result = service.get_financial_news_list(
            skip=skip,
            limit=limit,
            categories=categories,
            start_date=start_date,
            end_date=end_date,
            symbols=symbols,
            sources=sources
        )
        
        return result
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"금융 뉴스 목록 조회 중 오류가 발생했습니다: {str(e)}"
        )


@router.get(
    "/category/{category}",
    response_model=FinancialNewsListResponse,
    summary="카테고리별 금융 뉴스 (Finnhub)"
)
async def get_category_financial_news(
    category: CategoryType = Path(..., description="뉴스 카테고리"),
    page: int = Query(1, ge=1, description="페이지 번호"),
    limit: int = Query(20, ge=1, le=100, description="페이지당 항목 수"),
    start_date: Optional[datetime] = Query(None, description="시작 날짜"),
    end_date: Optional[datetime] = Query(None, description="종료 날짜"),
    db: Session = Depends(get_db)
):
    """
    Finnhub의 단일 카테고리 뉴스만 최신순으로 조회합니다.
    
    - 지원: crypto, forex, merger, general
    - 예시:
      - GET /api/v1/financial-news/category/crypto?limit=15
      - GET /api/v1/financial-news/category/forex?start_date=2025-07-01
    """
    try:
        skip = (page - 1) * limit
        service = FinancialNewsService(db)
        
        result = service.get_category_news(
            category=category,
            skip=skip,
            limit=limit,
            start_date=start_date,
            end_date=end_date
        )
        
        return result
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"카테고리별 뉴스 조회 중 오류가 발생했습니다: {str(e)}"
        )


@router.get(
    "/recent",
    response_model=List[FinancialNewsResponse],
    summary="최근 N시간 금융 뉴스 (Finnhub)"
)
async def get_recent_financial_news(
    hours: int = Query(24, ge=1, le=168, description="몇 시간 이내 뉴스 (최대 7일)"),
    limit: int = Query(10, ge=1, le=50, description="최대 개수 (최대 50)"),
    categories: Optional[List[CategoryType]] = Query(None, description="대상 카테고리"),
    db: Session = Depends(get_db)
):
    """
    최근 N시간 내 수집된 Finnhub 카테고리 뉴스를 최신순으로 반환합니다.
    
    - 포함: headline, summary, image, url 등
    - 예시:
      - GET /api/v1/financial-news/recent?hours=6&limit=5
      - GET /api/v1/financial-news/recent?categories=crypto,forex
    """
    try:
        service = FinancialNewsService(db)
        result = service.get_recent_financial_news(
            hours=hours,
            limit=limit,
            categories=categories
        )
        
        return result
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"최근 뉴스 조회 중 오류가 발생했습니다: {str(e)}"
        )


@router.get(
    "/search",
    response_model=FinancialNewsSearchResponse,
    summary="금융 뉴스 검색 (headline/summary/source)"
)
async def search_financial_news(
    q: str = Query(..., min_length=2, description="검색어 (최소 2글자)"),
    page: int = Query(1, ge=1, description="페이지 번호"),
    limit: int = Query(20, ge=1, le=100, description="페이지당 항목 수"),
    categories: Optional[List[CategoryType]] = Query(None, description="검색 대상 카테고리"),
    start_date: Optional[datetime] = Query(None, description="검색 시작 날짜"),
    end_date: Optional[datetime] = Query(None, description="검색 종료 날짜"),
    db: Session = Depends(get_db)
):
    """
    Finnhub 수집 데이터에서 헤드라인/요약/소스명을 검색합니다.
    
    - 대소문자 무시, 부분 매칭
    - 카테고리 및 날짜 범위 필터 지원
    - 예시:
      - GET /api/v1/financial-news/search?q=Bitcoin
      - GET /api/v1/financial-news/search?q=merger&categories=merger
      - GET /api/v1/financial-news/search?q=Federal&start_date=2025-07-01
    """
    try:
        skip = (page - 1) * limit
        service = FinancialNewsService(db)
        
        result = service.search_financial_news(
            query_text=q,
            skip=skip,
            limit=limit,
            categories=categories,
            start_date=start_date,
            end_date=end_date
        )
        
        return result
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"뉴스 검색 중 오류가 발생했습니다: {str(e)}"
        )


@router.get(
    "/detail/{category}/{news_id}",
    response_model=FinancialNewsResponse,
    summary="뉴스 상세 조회 (Finnhub category+news_id)"
)
async def get_financial_news_detail(
    category: CategoryType = Path(..., description="뉴스 카테고리"),
    news_id: int = Path(..., description="Finnhub 뉴스 ID", ge=1),
    db: Session = Depends(get_db)
):
    """
    Finnhub 원본의 `category + news_id`를 키로 상세 정보를 반환합니다.
    
    - 포함: 이미지, 요약, 원문 URL, 관련 종목 등
    - 예시: GET /api/v1/financial-news/detail/crypto/12345678901
    """
    try:
        service = FinancialNewsService(db)
        result = service.get_news_by_id(category=category, news_id=news_id)
        
        if not result:
            raise HTTPException(
                status_code=404,
                detail=f"뉴스를 찾을 수 없습니다. category: {category}, news_id: {news_id}"
            )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"뉴스 상세 조회 중 오류가 발생했습니다: {str(e)}"
        )


@router.get(
    "/symbol/{symbol}",
    response_model=FinancialNewsListResponse,
    summary="종목(related) 기반 뉴스"
)
async def get_news_by_symbol(
    symbol: str = Path(..., description="종목 코드 (예: BTCUSD, AAPL)"),
    page: int = Query(1, ge=1, description="페이지 번호"),
    limit: int = Query(20, ge=1, le=100, description="페이지당 항목 수"),
    categories: Optional[List[CategoryType]] = Query(None, description="검색 대상 카테고리"),
    db: Session = Depends(get_db)
):
    """
    `related` 필드에 특정 심볼이 언급된 뉴스를 최신순으로 조회합니다.
    
    - 예시:
      - GET /api/v1/financial-news/symbol/BTCUSD?categories=crypto
      - GET /api/v1/financial-news/symbol/AAPL?limit=15
    """
    try:
        skip = (page - 1) * limit
        service = FinancialNewsService(db)
        
        result = service.get_news_by_symbol(
            symbol=symbol.upper(),
            skip=skip,
            limit=limit,
            categories=categories
        )
        
        return result
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"종목별 뉴스 조회 중 오류가 발생했습니다: {str(e)}"
        )


@router.get("/stats", summary="Financial News 통계")
async def get_financial_news_stats(db: Session = Depends(get_db)):
    """
    Financial News 통계 정보를 반환합니다.
    
    - Hero 섹션 등에서 사용할 간단한 통계 정보
    """
    try:
        service = FinancialNewsService(db)
        stats_result = service.get_categories_statistics()
        
        return {
            "total_count": stats_result.total_news
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"통계 조회 중 오류가 발생했습니다: {str(e)}"
        )


@router.get(
    "/categories/stats",
    response_model=CategoriesStatsResponse,
    summary="카테고리별 통계 (개수/최신일시/전체)"
)
async def get_categories_statistics(
    db: Session = Depends(get_db)
):
    """
    Finnhub 카테고리별 개수/최신일시/총계를 반환합니다.
    
    - 용도: 대시보드 요약, 필터 UI, 모니터링
    """
    try:
        service = FinancialNewsService(db)
        result = service.get_categories_statistics()
        
        return result
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"카테고리 통계 조회 중 오류가 발생했습니다: {str(e)}"
        )


@router.get("/trending-symbols", summary="트렌딩 종목 (언급 빈도 기준)")
async def get_trending_symbols(
    days: int = Query(7, ge=1, le=30, description="조회할 일수 (최대 30일)"),
    limit: int = Query(10, ge=1, le=50, description="반환할 종목 수"),
    db: Session = Depends(get_db)
):
    """
    최근 N일 동안 가장 많이 언급된 종목을 반환합니다.
    
    - 기준: `related` 필드 언급 횟수
    - 정렬: 언급 수 내림차순
    """
    try:
        service = FinancialNewsService(db)
        trending_data = service.get_trending_symbols(days=days, limit=limit)
        
        return {
            "trending_symbols": [
                {"symbol": symbol, "mentions": count}
                for symbol, count in trending_data
            ],
            "period_days": days,
            "total_symbols": len(trending_data)
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"트렌딩 종목 조회 중 오류가 발생했습니다: {str(e)}"
        )


@router.get("/health", summary="Financial News API 상태")
async def health_check(db: Session = Depends(get_db)):
    """
    Finnhub 기반 수집 데이터의 기본 상태 정보를 반환합니다.
    
    - 항목: DB 연결, 카테고리별 개수, 최신 수집 시각 등
    """
    try:
        service = FinancialNewsService(db)
        
        # 카테고리별 통계로 DB 연결 및 데이터 상태 확인
        stats = service.get_categories_statistics()
        
        # 카테고리별 개수 딕셔너리 생성
        categories_count = {
            cat.category: cat.count for cat in stats.categories
        }
        
        # 최신 뉴스 날짜 찾기
        latest_date = None
        if stats.categories:
            latest_dates = [cat.latest_news_date for cat in stats.categories if cat.latest_news_date]
            if latest_dates:
                latest_date = max(latest_dates)
        
        return {
            "status": "healthy",
            "database": "connected",
            "categories_count": categories_count,
            "total_news": stats.total_news,
            "latest_news_date": latest_date,
            "available_categories": stats.available_categories,
            "api_version": "1.0.0",
            "timestamp": datetime.now(timezone.utc)
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail=f"서비스 상태 확인 실패: {str(e)}"
        )