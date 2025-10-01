from datetime import datetime
import pytz
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, Path
from sqlalchemy.orm import Session

from app.dependencies import get_db
from app.services.market_news_service import MarketNewsService
from app.models.market_news_model import MarketNews
from app.schemas.market_news_schema import (
    MarketNewsResponse,
    MarketNewsListResponse,
    MarketNewsSearchResponse
)

# 라우터 생성 (태그로 API 문서 그룹화)
router = APIRouter(
    tags=["Market News"],
    responses={
        404: {"description": "뉴스를 찾을 수 없습니다"},
        500: {"description": "서버 내부 오류 발생"}
    }
)


@router.get(
    "/",
    response_model=MarketNewsListResponse,
    summary="시장 뉴스 목록 (NewsAPI 수집 데이터)"
)
async def get_market_news_list(
    page: int = Query(1, ge=1, description="페이지 번호 (1부터 시작)"),
    limit: int = Query(20, ge=1, le=100, description="페이지당 항목 수 (최대 100)"),
    start_date: Optional[datetime] = Query(None, description="시작 날짜 (YYYY-MM-DD 또는 ISO 형식)"),
    end_date: Optional[datetime] = Query(None, description="종료 날짜 (YYYY-MM-DD 또는 ISO 형식)"),
    sources: Optional[List[str]] = Query(None, description="포함할 소스 목록"),
    exclude_sources: Optional[List[str]] = Query(None, description="제외할 소스 목록"),
    db: Session = Depends(get_db)
):
    """
    NewsAPI에서 수집해 DB(`market_news`)에 저장된 시장 뉴스를 페이징하여 조회합니다.
    
    - 데이터 수집: Airflow DAG `ingest_market_newsapi_to_db_k8s`
      - Top-Headlines: category=business, technology, general, politics
      - Everything: economy, business, technology, IPO, inflation, tariff, trade war, sanctions, war, politics, election, government policy, congress, diplomatic, nuclear, military 키워드
      - 주요 소스: Bloomberg, Reuters, CNBC, WSJ, Business Insider, Financial Times
      - 스케줄: 매일 04:00 (UTC 기준 시스템 설정에 따름)
    
    - 주요 기능:
      - 페이징: page, limit
      - 날짜 필터링: start_date, end_date (ISO)
      - 소스 필터링: 포함(sources)/제외(exclude_sources)
      - 최신순 정렬: published_at desc
    
    - 사용 예시:
      - GET /api/v1/market-news/?page=1&limit=10
      - GET /api/v1/market-news/?start_date=2025-07-01&end_date=2025-07-31
      - GET /api/v1/market-news/?sources=Reuters,CNBC
    
    - 응답: total, items(요약), page, limit, has_next
    """
    try:
        # 페이지 번호를 offset으로 변환 (page 1 = skip 0)
        skip = (page - 1) * limit
        
        # 서비스 인스턴스 생성
        service = MarketNewsService(db)
        
        # 뉴스 목록 조회
        result = service.get_news_list(
            skip=skip,
            limit=limit,
            start_date=start_date,
            end_date=end_date,
            sources=sources,
            exclude_sources=exclude_sources
        )
        
        return result
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"뉴스 목록 조회 중 오류가 발생했습니다: {str(e)}"
        )


@router.get(
    "/recent",
    response_model=List[MarketNewsResponse],
    summary="최근 시장 뉴스 조회 (24–168시간)"
)
async def get_recent_market_news(
    hours: int = Query(24, ge=1, le=168, description="몇 시간 이내 뉴스 (최대 7일)"),
    limit: int = Query(10, ge=1, le=50, description="최대 개수 (최대 50)"),
    db: Session = Depends(get_db)
):
    """
    최근 N시간 내 수집된 시장 뉴스를 최신순으로 반환합니다.
    
    - 데이터 출처: NewsAPI (Airflow 일일 수집 결과)
    - 포함 정보: 본문 `content` 포함
    - 예시:
      - GET /api/v1/market-news/recent?hours=6&limit=5
      - GET /api/v1/market-news/recent
    """
    try:
        service = MarketNewsService(db)
        result = service.get_recent_news(hours=hours, limit=limit)
        
        return result
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"최근 뉴스 조회 중 오류가 발생했습니다: {str(e)}"
        )


@router.get(
    "/search",
    response_model=MarketNewsSearchResponse,
    summary="시장 뉴스 전문 검색 (제목/설명/본문)"
)
async def search_market_news(
    q: str = Query(..., min_length=2, description="검색어 (최소 2글자)"),
    page: int = Query(1, ge=1, description="페이지 번호"),
    limit: int = Query(20, ge=1, le=100, description="페이지당 항목 수"),
    start_date: Optional[datetime] = Query(None, description="검색 시작 날짜"),
    end_date: Optional[datetime] = Query(None, description="검색 종료 날짜"),
    db: Session = Depends(get_db)
):
    """
    PostgreSQL Full-Text Search로 NewsAPI 수집 기사(제목/설명/본문)를 검색합니다.
    
    - 정렬: 관련도 우선
    - 날짜 범위 필터 지원
    - 예시:
      - GET /api/v1/market-news/search?q=Trump
      - GET /api/v1/market-news/search?q=economy&start_date=2025-07-01
      - GET /api/v1/market-news/search?q="stock market"
    """
    try:
        skip = (page - 1) * limit
        service = MarketNewsService(db)
        
        result = service.search_news(
            query_text=q,
            skip=skip,
            limit=limit,
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
    "/detail",
    response_model=MarketNewsResponse,
    summary="시장 뉴스 상세 조회 (source+url)"
)
async def get_market_news_detail(
    source: str = Query(..., description="뉴스 소스"),
    url: str = Query(..., description="뉴스 URL"),
    db: Session = Depends(get_db)
):
    """
    수집된 기사 중 한 건의 상세 정보를 반환합니다.
    
    - 식별키: source + url
    - 포함: 전체 본문(content), 요약, 미리보기 등
    - 예시: GET /api/v1/market-news/detail?source=Reuters&url=https://www.reuters.com/...
    """
    try:
        service = MarketNewsService(db)
        result = service.get_news_by_url(source=source, url=url)
        
        if not result:
            raise HTTPException(
                status_code=404,
                detail=f"뉴스를 찾을 수 없습니다. source: {source}, url: {url}"
            )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"뉴스 상세 조회 중 오류가 발생했습니다: {str(e)}"
        )


@router.get("/sources", summary="뉴스 소스별 집계 (개수 기준)")
async def get_news_sources_stats(
    db: Session = Depends(get_db)
):
    """
    수집된 기사에서 소스별 건수를 집계합니다.
    
    - 용도: 소스 필터 UI, 수집 품질 모니터링
    - 정렬: 개수 내림차순
    """
    try:
        service = MarketNewsService(db)
        sources_data = service.get_news_sources()
        
        return {
            "sources": [
                {"source": source, "count": count}
                for source, count in sources_data
            ]
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"소스 통계 조회 중 오류가 발생했습니다: {str(e)}"
        )


@router.get("/daily-stats", summary="일별 뉴스 발행량 (최근 N일)")
async def get_daily_news_stats(
    days: int = Query(30, ge=1, le=365, description="조회할 일수 (최대 1년)"),
    db: Session = Depends(get_db)
):
    """
    최근 N일 동안의 일별 기사 건수를 반환합니다.
    
    - 용도: 발행 트렌드 차트, 수집 상태 모니터링
    """
    try:
        service = MarketNewsService(db)
        daily_data = service.get_daily_news_count(days=days)
        
        # 전체 뉴스 개수 계산
        total_news = sum(count for _, count in daily_data)
        
        return {
            "daily_stats": [
                {"date": date, "count": count}
                for date, count in daily_data
            ],
            "period_days": days,
            "total_news": total_news
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"일별 통계 조회 중 오류가 발생했습니다: {str(e)}"
        )


@router.get("/stats", summary="Market News 통계")
async def get_market_news_stats(db: Session = Depends(get_db)):
    """
    Market News 통계 정보를 반환합니다.
    
    - Hero 섹션 등에서 사용할 간단한 통계 정보
    """
    try:
        service = MarketNewsService(db)
        
        # 전체 뉴스 개수
        total_count = service.db.query(MarketNews).count()
        
        # 최신 뉴스 날짜
        latest_news = service.db.query(MarketNews.published_at).order_by(
            MarketNews.published_at.desc()
        ).first()
        
        latest_date = latest_news[0] if latest_news else None
        
        return {
            "total_count": total_count,
            "latest_news_date": latest_date
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"통계 조회 중 오류가 발생했습니다: {str(e)}"
        )


@router.get("/health", summary="Market News API 상태")
async def health_check(db: Session = Depends(get_db)):
    """
    NewsAPI 기반 수집 데이터의 기본 상태 정보를 반환합니다.
    
    - 항목: DB 연결, 전체 개수, 최신 수집 시각 등
    """
    try:
        service = MarketNewsService(db)
        
        # 기본 통계 조회로 DB 연결 테스트
        total_count = service.db.query(MarketNews).count()
        
        # 최신 뉴스 날짜 조회
        latest_news = service.db.query(MarketNews.published_at).order_by(
            MarketNews.published_at.desc()
        ).first()
        
        latest_date = latest_news[0] if latest_news else None
        
        return {
            "status": "healthy",
            "database": "connected",
            "total_news": total_count,
            "latest_news_date": latest_date,
            "api_version": "1.0.0",
            "timestamp": datetime.now(pytz.UTC)
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail=f"서비스 상태 확인 실패: {str(e)}"
        )