# app/api/endpoints/etf_polling_endpoint.py
from fastapi import APIRouter, HTTPException, Depends, Query, Path
from fastapi.responses import JSONResponse
from typing import List, Optional, Dict, Any
import logging
from datetime import datetime
import pytz
from app.database import get_db
from app.services.etf_service import ETFService
from app.schemas.etf_schema import (
    ETFListResponse, ETFDetailCompleteResponse, ETFSearchResponse, 
    ETFMarketOverviewResponse, ServiceStats, HealthCheckResponse, 
    ErrorResponse, TimeframeEnum, SortOrderEnum, create_error_response
)
from app.schemas import etf_schema

# 로깅 설정
logger = logging.getLogger(__name__)

# 라우터 생성
router = APIRouter()

# 서비스 인스턴스 생성 (의존성)
def get_etf_service() -> ETFService:
    """ETFService 의존성 제공"""
    return ETFService()

# =========================
# ETF 리스트 및 폴링 엔드포인트
# =========================

@router.get("/", response_model=ETFListResponse, summary="전체 ETF 리스트 조회")
async def get_all_etfs(
    limit: int = Query(default=50, ge=1, le=200, description="반환할 최대 ETF 개수"),
    etf_service: ETFService = Depends(get_etf_service)
):
    """
    전체 ETF 리스트 조회
    
    **주요 기능:**
    - 모든 ETF의 현재가 조회
    - 전일 대비 변동률 계산
    - 거래량 정보 포함
    - 시장 상태 정보 제공
    
    **사용 예시:**
    ```
    GET /etf/?limit=50
    ```
    
    **응답 데이터:**
    - ETF 심볼, 명칭, 현재가
    - 전일 대비 변동액 및 변동률
    - 거래량, 시장 상태
    """
    try:
        logger.info(f"전체 ETF 리스트 조회 요청 (limit: {limit})")
        
        result = etf_service.get_etf_list(limit)
        
        if result.get('error'):
            logger.error(f"ETF 리스트 조회 실패: {result['error']}")
            raise HTTPException(
                status_code=500,
                detail=create_error_response(
                    error_type="LIST_ERROR",
                    message=f"ETF list retrieval failed: {result['error']}",
                    path="/etf/"
                ).model_dump()
            )
        
        logger.info(f"ETF 리스트 조회 성공: {result['total_count']}개")
        return ETFListResponse(**result)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"예상치 못한 오류: {e}")
        raise HTTPException(
            status_code=500,
            detail=create_error_response(
                error_type="INTERNAL_ERROR",
                message="Internal server error occurred",
                path="/etf/"
            ).model_dump()
        )

# 🔥 /polling 엔드포인트 제거됨 - WebSocket으로 대체
# 실시간 데이터는 /ws/etf WebSocket 엔드포인트를 사용하세요
# GET /api/v1/ws/etf

# =========================
# 개별 ETF 상세 조회 엔드포인트
# =========================

@router.get("/symbol/{symbol}", response_model=etf_schema.ETFDetailResponse, summary="개별 ETF 상세 정보 조회")
async def get_etf_symbol_details(symbol: str):
    """
    특정 ETF 심볼에 대한 모든 상세 정보를 반환합니다.
    기본 정보, 프로필, 보유 종목 데이터를 모두 포함합니다.
    """
    try:
        service = ETFService()
        details = service.get_etf_details_by_symbol(symbol)
        
        if not details:
            raise HTTPException(status_code=404, detail=f"ETF not found: {symbol}")
        
        return details
    except Exception as e:
        logger.error(f"ETF 상세 정보 API 오류 ({symbol}): {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/symbol/{symbol}/chart", summary="ETF 차트 데이터만 조회")
async def get_etf_chart_data(
    symbol: str = Path(..., description="ETF 심볼 (예: SPY)", regex=r"^[A-Z]{2,5}$"),
    timeframe: TimeframeEnum = Query(default=TimeframeEnum.ONE_DAY, description="차트 시간대"),
    etf_service: ETFService = Depends(get_etf_service)
):
    """
    ETF 차트 데이터만 조회
    
    **주요 기능:**
    - 시간대별 가격 차트 데이터
    - 거래량 정보 포함
    - 프론트엔드 차트 렌더링 최적화
    
    **차트 시간대:**
    - `1D`: 1일 (24시간)
    - `1W`: 1주일 (7일)
    - `1M`: 1개월 (30일)
    
    **사용 예시:**
    ```
    GET /etf/symbol/SPY/chart?timeframe=1D
    GET /etf/symbol/QQQ/chart?timeframe=1W
    ```
    
    **응답 구조:**
    - `symbol`: ETF 심볼
    - `timeframe`: 차트 시간대
    - `chart_data`: 차트 데이터 포인트 배열
    - `data_points`: 데이터 포인트 개수
    - `market_status`: 시장 상태
    - `last_updated`: 최종 업데이트 시간
    """
    try:
        symbol = symbol.upper()
        logger.info(f"ETF 차트 데이터 조회: {symbol} (timeframe: {timeframe.value})")
        
        result = etf_service.get_chart_data_only(symbol, timeframe.value)
        
        if result.get('error'):
            logger.error(f"ETF {symbol} 차트 데이터 조회 실패: {result['error']}")
            raise HTTPException(
                status_code=404 if "not found" in result['error'].lower() else 500,
                detail=create_error_response(
                    error_type="ETF_NOT_FOUND" if "not found" in result['error'].lower() else "CHART_ERROR",
                    message=result['error'],
                    path=f"/etf/symbol/{symbol}/chart"
                ).model_dump()
            )
        
        logger.info(f"ETF {symbol} 차트 데이터 조회 성공: {result.get('data_points', 0)}개 포인트")
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"예상치 못한 오류: {e}")
        raise HTTPException(
            status_code=500,
            detail=create_error_response(
                error_type="INTERNAL_ERROR",
                message="Internal server error occurred",
                path=f"/etf/symbol/{symbol}/chart"
            ).model_dump()
        )

@router.get("/symbol/{symbol}/basic", summary="ETF 기본 정보만 조회")
async def get_etf_basic_info(
    symbol: str = Path(..., description="ETF 심볼 (예: SPY)", regex=r"^[A-Z]{2,5}$"),
    etf_service: ETFService = Depends(get_etf_service)
):
    """
    ETF 기본 정보만 조회 (차트 데이터 제외, 빠른 응답)
    
    **주요 기능:**
    - ETF 현재가, 변동률, 거래량
    - 차트 데이터 제외로 빠른 응답
    - 실시간 가격 업데이트용
    
    **사용 예시:**
    ```
    GET /etf/symbol/SPY/basic
    ```
    """
    try:
        symbol = symbol.upper()
        logger.info(f"ETF 기본 정보 조회: {symbol}")
        
        result = etf_service.get_etf_basic_info(symbol)
        
        if result.get('error'):
            logger.error(f"ETF {symbol} 기본 정보 조회 실패: {result['error']}")
            raise HTTPException(
                status_code=404 if "not found" in result['error'].lower() else 500,
                detail=create_error_response(
                    error_type="ETF_NOT_FOUND" if "not found" in result['error'].lower() else "BASIC_INFO_ERROR",
                    message=result['error'],
                    path=f"/etf/symbol/{symbol}/basic"
                ).model_dump()
            )
        
        logger.info(f"ETF {symbol} 기본 정보 조회 성공")
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"예상치 못한 오류: {e}")
        raise HTTPException(
            status_code=500,
            detail=create_error_response(
                error_type="INTERNAL_ERROR",
                message="Internal server error occurred",
                path=f"/etf/symbol/{symbol}/basic"
            ).model_dump()
        )

# =========================
# 검색 엔드포인트
# =========================

@router.get("/search", response_model=ETFSearchResponse, summary="ETF 검색")
async def search_etfs(
    q: str = Query(..., description="검색어 (심볼)", min_length=1, max_length=10),
    limit: int = Query(default=20, ge=1, le=50, description="반환할 최대 결과 개수"),
    etf_service: ETFService = Depends(get_etf_service)
):
    """
    ETF 검색 (심볼 기준)
    
    **주요 기능:**
    - ETF 심볼로 검색
    - 부분 일치 검색 지원
    - 심볼 알파벳 순 정렬
    
    **사용 예시:**
    ```
    GET /etf/search?q=SPY
    GET /etf/search?q=QQ&limit=10
    ```
    
    **검색 대상:**
    - ETF 심볼 (예: SPY, QQQ, VTI)
    """
    try:
        logger.info(f"ETF 검색 요청: '{q}' (limit: {limit})")
        
        result = etf_service.search_etfs(q.strip(), limit)
        
        if result.get('error'):
            logger.error(f"ETF 검색 실패: {result['error']}")
            raise HTTPException(
                status_code=500,
                detail=create_error_response(
                    error_type="SEARCH_ERROR",
                    message=f"ETF search failed: {result['error']}",
                    path="/etf/search"
                ).model_dump()
            )
        
        logger.info(f"ETF 검색 성공: '{q}' -> {result['total_count']}개 결과")
        return ETFSearchResponse(**result)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"예상치 못한 오류: {e}")
        raise HTTPException(
            status_code=500,
            detail=create_error_response(
                error_type="INTERNAL_ERROR",
                message="Internal server error occurred",
                path="/etf/search"
            ).model_dump()
        )

# =========================
# 시장 개요 엔드포인트
# =========================

@router.get("/market-overview", response_model=ETFMarketOverviewResponse, summary="ETF 시장 개요")
async def get_etf_market_overview(
    etf_service: ETFService = Depends(get_etf_service)
):
    """
    ETF 시장 전체 개요 조회
    
    **주요 기능:**
    - 전체 ETF 개수, 평균 가격
    - 최고/최저 가격, 총 거래량
    - 시장 상태 정보
    
    **사용 예시:**
    ```
    GET /etf/market-overview
    ```
    """
    try:
        logger.info("ETF 시장 개요 조회 요청")
        
        result = etf_service.get_market_overview()
        
        if result.get('error'):
            logger.error(f"ETF 시장 개요 조회 실패: {result['error']}")
            raise HTTPException(
                status_code=500,
                detail=create_error_response(
                    error_type="MARKET_OVERVIEW_ERROR",
                    message=f"Market overview failed: {result['error']}",
                    path="/etf/market-overview"
                ).model_dump()
            )
        
        logger.info("ETF 시장 개요 조회 성공")
        return ETFMarketOverviewResponse(**result)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"예상치 못한 오류: {e}")
        raise HTTPException(
            status_code=500,
            detail=create_error_response(
                error_type="INTERNAL_ERROR",
                message="Internal server error occurred",
                path="/etf/market-overview"
            ).model_dump()
        )

# =========================
# 서비스 상태 및 관리 엔드포인트
# =========================

@router.get("/stats", response_model=ServiceStats, summary="ETF 서비스 통계")
async def get_etf_service_stats(
    etf_service: ETFService = Depends(get_etf_service)
):
    """
    ETF 서비스 통계 정보 조회
    
    **반환 정보:**
    - API 요청 수, DB 쿼리 수
    - 에러 수, 마지막 요청 시간
    - 서비스 가동 시간
    """
    try:
        logger.info("ETF 서비스 통계 조회")
        
        stats = etf_service.get_service_stats()
        return ServiceStats(**stats)
        
    except Exception as e:
        logger.error(f"ETF 서비스 통계 조회 실패: {e}")
        raise HTTPException(
            status_code=500,
            detail=create_error_response(
                error_type="STATS_ERROR",
                message="Failed to retrieve service stats",
                path="/etf/stats"
            ).model_dump()
        )

@router.get("/health", response_model=HealthCheckResponse, summary="ETF 서비스 헬스 체크")
async def get_etf_health_check(
    etf_service: ETFService = Depends(get_etf_service)
):
    """
    ETF 서비스 헬스 체크
    
    **확인 항목:**
    - 데이터베이스 연결 상태
    - 데이터 신선도 (최근 업데이트 여부)
    - 시장 상태
    """
    try:
        logger.info("ETF 서비스 헬스 체크")
        
        health = etf_service.health_check()
        
        # 헬스 상태에 따라 적절한 HTTP 상태 코드 반환
        status_code = 200 if health.get('status') == 'healthy' else 503
        
        return JSONResponse(
            status_code=status_code,
            content=health
        )
        
    except Exception as e:
        logger.error(f"ETF 헬스 체크 실패: {e}")
        return JSONResponse(
            status_code=503,
            content={
                'status': 'unhealthy',
                'database': 'error',
                'error': str(e),
                'last_check': datetime.now(pytz.UTC).isoformat()
            }
        )