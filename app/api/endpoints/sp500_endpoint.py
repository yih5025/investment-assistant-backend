# app/api/endpoints/sp500_endpoint.py
from fastapi import APIRouter, HTTPException, Depends, Query, Path
from fastapi.responses import JSONResponse
from typing import List, Optional, Dict, Any
import logging
from datetime import datetime

from app.services.sp500_service import SP500Service
from app.services.company_overview_service import CompanyOverviewService  # 🆕 추가
from app.schemas.sp500_schema import (
    StockListResponse, StockDetail, CategoryStockResponse,
    SearchResponse, MarketOverviewResponse,
    ServiceStats, HealthCheckResponse, ErrorResponse,
    TimeframeEnum, create_error_response
)

# 로깅 설정
logger = logging.getLogger(__name__)

# 라우터 생성
router = APIRouter()

# 서비스 인스턴스 생성 (의존성)
def get_sp500_service() -> SP500Service:
    """SP500Service 의존성 제공"""
    return SP500Service()

def get_company_overview_service() -> CompanyOverviewService:
    """CompanyOverviewService 의존성 제공"""  # 🆕 추가
    return CompanyOverviewService()

# =========================
# 🎯 주식 리스트 및 개요 엔드포인트
# =========================

@router.get("/", response_model=StockListResponse, summary="전체 주식 리스트 조회")
async def get_all_stocks(
    limit: int = Query(default=500, ge=1, le=1000, description="반환할 최대 주식 개수"),
    sp500_service: SP500Service = Depends(get_sp500_service)
):
    """
    전체 SP500 주식 리스트 조회
    
    **주요 기능:**
    - 모든 SP500 주식의 현재가 조회
    - 전일 대비 변동률 계산
    - 거래량 정보 포함
    - 시장 상태 정보 제공
    
    **사용 예시:**
    ```
    GET /stocks/sp500/?limit=100
    ```
    
    **응답 데이터:**
    - 주식 심볼, 회사명, 현재가
    - 변동 금액, 변동률 (전일 대비)
    - 거래량, 섹터 정보
    - 시장 상태 (개장/마감)
    """
    try:
        logger.info(f"📊 전체 주식 리스트 조회 요청 (limit: {limit})")
        
        result = sp500_service.get_stock_list(limit)
        
        if result.get('error'):
            logger.error(f"❌ 주식 리스트 조회 실패: {result['error']}")
            raise HTTPException(
                status_code=500,
                detail=create_error_response(
                    error_type="DATA_FETCH_ERROR",
                    message=f"Failed to fetch stock list: {result['error']}",
                    path="/stocks/sp500/"
                ).model_dump()
            )
        
        logger.info(f"✅ 주식 리스트 조회 성공: {result['total_count']}개")
        return StockListResponse(**result)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ 예상치 못한 오류: {e}")
        raise HTTPException(
            status_code=500,
            detail=create_error_response(
                error_type="INTERNAL_ERROR",
                message="Internal server error occurred",
                path="/stocks/sp500/"
            ).model_dump()
        )

@router.get("/market-overview", response_model=MarketOverviewResponse, summary="시장 개요 조회")
async def get_market_overview(
    sp500_service: SP500Service = Depends(get_sp500_service)
):
    """
    전체 시장 개요 및 하이라이트 조회
    
    **주요 기능:**
    - 시장 전체 요약 통계
    - 상위 상승/하락 종목 (각 5개)
    - 가장 활발한 거래 종목 (5개)
    - 시장 상태 정보
    
    **사용 예시:**
    ```
    GET /stocks/sp500/market-overview
    ```
    
    **응답 데이터:**
    - 총 심볼 수, 평균 가격, 최고/최저가
    - 상위 상승 종목 리스트
    - 상위 하락 종목 리스트
    - 활발한 거래 종목 리스트
    """
    try:
        logger.info("📊 시장 개요 조회 요청")
        
        result = sp500_service.get_market_overview()
        
        if result.get('error'):
            logger.error(f"❌ 시장 개요 조회 실패: {result['error']}")
            raise HTTPException(
                status_code=500,
                detail=create_error_response(
                    error_type="MARKET_DATA_ERROR",
                    message=f"Failed to fetch market overview: {result['error']}",
                    path="/stocks/sp500/market-overview"
                ).model_dump()
            )
        
        logger.info("✅ 시장 개요 조회 성공")
        return MarketOverviewResponse(**result)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ 예상치 못한 오류: {e}")
        raise HTTPException(
            status_code=500,
            detail=create_error_response(
                error_type="INTERNAL_ERROR",
                message="Internal server error occurred",
                path="/stocks/sp500/market-overview"
            ).model_dump()
        )

# =========================
# 🎯 개별 주식 상세 조회 엔드포인트 (Company Overview 통합) 🆕
# =========================

@router.get("/symbol/{symbol}", summary="개별 주식 상세 정보 조회 (회사 정보 포함)")
async def get_stock_detail_with_company_info(
    symbol: str = Path(..., description="주식 심볼 (예: AAPL)", regex=r"^[A-Z]{1,5}$"),
    sp500_service: SP500Service = Depends(get_sp500_service),
    company_service: CompanyOverviewService = Depends(get_company_overview_service)  # 🆕 추가
):
    """
    개별 주식 상세 정보 및 회사 정보 통합 조회 (차트 데이터 제외)
    
    **주요 기능:**
    - 특정 주식의 현재가 및 변동 정보 (SP500 WebSocket 데이터)
    - **회사 상세 정보 (Company Overview 데이터)** 🆕
    - 재무 지표 (P/E, ROE, 시가총액 등)
    - 배당 정보, 분석가 목표주가 등
    
    **사용 예시:**
    ```
    GET /stocks/sp500/symbol/AAPL
    GET /stocks/sp500/symbol/TSLA
    ```
    
    **응답 데이터:**
    - **실시간 데이터**: 현재가, 변동 금액/률, 거래량
    - **회사 정보**: 회사명, 섹터, 산업, 본사 위치, 웹사이트
    - **재무 지표**: P/E 비율, ROE, 순이익률, 시가총액, 배당 수익률
    - **주가 정보**: 52주 고/저가, 베타, 이동평균
    - **분석가 정보**: 목표주가, 성장률 전망
    
    **Note**: 차트 데이터는 별도 엔드포인트 `/chart/{symbol}` 사용
    """
    try:
        symbol = symbol.upper()
        logger.info(f"📊 {symbol} 주식 상세 정보 조회 요청 (차트 제외)")
        
        # 1. SP500 실시간 데이터 조회 (가격, 변동률 등 - 차트 제외)
        stock_result = sp500_service.get_stock_basic_info(symbol)  # 차트 없는 기본 정보만
        
        if stock_result.get('error'):
            if 'No data found' in stock_result['error']:
                logger.warning(f"⚠️ {symbol} 주식 데이터 없음")
                raise HTTPException(
                    status_code=404,
                    detail=create_error_response(
                        error_type="STOCK_NOT_FOUND",
                        message=f"No stock data found for symbol: {symbol}",
                        code="STOCK_404",
                        path=f"/stocks/sp500/symbol/{symbol}"
                    ).model_dump()
                )
            else:
                logger.error(f"❌ {symbol} 주식 데이터 조회 실패: {stock_result['error']}")
                raise HTTPException(
                    status_code=500,
                    detail=create_error_response(
                        error_type="STOCK_DATA_ERROR",
                        message=f"Failed to fetch stock data: {stock_result['error']}",
                        path=f"/stocks/sp500/symbol/{symbol}"
                    ).model_dump()
                )
        
        # 2. Company Overview 데이터 조회 🆕
        company_result = company_service.get_company_basic_metrics(symbol)
        
        # 3. 데이터 통합 🆕
        if company_result['data_available']:
            # Company Overview 데이터가 있는 경우 - 풍부한 정보 제공
            enhanced_result = {
                **stock_result,  # 기존 주식 데이터 (가격, 변동률 등)
                
                # 🆕 회사 기본 정보 추가
                'company_info': {
                    'has_company_data': True,
                    'company_name': company_result.get('company_name'),
                    'sector': company_result.get('sector'),
                    'industry': company_result.get('industry'),
                    'website': company_result.get('website'),
                    'description': company_result.get('description')
                },
                
                # 🆕 재무 지표 추가
                'financial_metrics': {
                    'market_capitalization': company_result.get('market_cap'),
                    'pe_ratio': company_result.get('pe_ratio'),
                    'dividend_yield': company_result.get('dividend_yield'),
                    'beta': company_result.get('beta'),
                    'roe': company_result.get('roe'),
                    'profit_margin': company_result.get('profit_margin')
                },
                
                # 🆕 데이터 소스 정보
                'data_sources': {
                    'stock_data': 'sp500_websocket_trades',
                    'company_data': 'company_overview',
                    'company_batch_id': company_result.get('batch_id')
                }
            }
            
            logger.info(f"✅ {symbol} 통합 데이터 조회 성공 (회사 정보 포함)")
            
        else:
            # Company Overview 데이터가 없는 경우 - 기본 주식 데이터만
            enhanced_result = {
                **stock_result,  # 기존 주식 데이터만
                
                # 🆕 회사 정보 없음 표시
                'company_info': {
                    'has_company_data': False,
                    'message': company_result.get('message', f'{symbol} 회사 정보가 아직 수집되지 않았습니다')
                },
                
                'financial_metrics': {
                    'message': '재무 지표 데이터가 없습니다. 데이터 수집이 완료되면 제공됩니다.'
                },
                
                'data_sources': {
                    'stock_data': 'sp500_websocket_trades',
                    'company_data': 'not_available'
                }
            }
            
            logger.info(f"✅ {symbol} 기본 데이터 조회 성공 (회사 정보 없음)")
        
        return JSONResponse(content=enhanced_result)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ 예상치 못한 오류: {e}")
        raise HTTPException(
            status_code=500,
            detail=create_error_response(
                error_type="INTERNAL_ERROR",
                message="Internal server error occurred",
                path=f"/stocks/sp500/symbol/{symbol}"
            ).model_dump()
        )

# =========================
# 🎯 차트 데이터 전용 엔드포인트 🆕
# =========================

@router.get("/chart/{symbol}", summary="주식 차트 데이터 조회")
async def get_stock_chart_data(
    symbol: str = Path(..., description="주식 심볼 (예: AAPL)", regex=r"^[A-Z]{1,5}$"),
    timeframe: TimeframeEnum = Query(default=TimeframeEnum.ONE_DAY, description="차트 시간대"),
    sp500_service: SP500Service = Depends(get_sp500_service)
):
    """
    주식 차트 데이터 전용 조회
    
    **주요 기능:**
    - 시간대별 차트 데이터만 제공
    - 가격 및 거래량 시계열 데이터
    - 다양한 시간대 지원
    
    **지원하는 차트 시간대:**
    - `1M`: 1분 차트
    - `5M`: 5분 차트
    - `1H`: 1시간 차트
    - `1D`: 1일 차트 (기본값)
    - `1W`: 1주 차트
    - `1MO`: 1개월 차트
    
    **사용 예시:**
    ```
    GET /stocks/sp500/chart/AAPL?timeframe=1D
    GET /stocks/sp500/chart/TSLA?timeframe=1H
    GET /stocks/sp500/chart/MSFT?timeframe=1W
    ```
    
    **응답 데이터:**
    - 시간별 가격 데이터 (timestamp, price, volume)
    - 차트 렌더링에 필요한 모든 데이터 포인트
    - 시간대별 최적화된 데이터 샘플링
    """
    try:
        symbol = symbol.upper()
        logger.info(f"📈 {symbol} 차트 데이터 조회 요청 (timeframe: {timeframe})")
        
        # 차트 데이터만 조회
        chart_result = sp500_service.get_chart_data_only(symbol, timeframe.value)
        
        if chart_result.get('error'):
            if 'No data found' in chart_result['error']:
                logger.warning(f"⚠️ {symbol} 차트 데이터 없음")
                raise HTTPException(
                    status_code=404,
                    detail=create_error_response(
                        error_type="CHART_DATA_NOT_FOUND",
                        message=f"No chart data found for symbol: {symbol}",
                        code="CHART_404",
                        path=f"/stocks/sp500/chart/{symbol}"
                    ).model_dump()
                )
            else:
                logger.error(f"❌ {symbol} 차트 데이터 조회 실패: {chart_result['error']}")
                raise HTTPException(
                    status_code=500,
                    detail=create_error_response(
                        error_type="CHART_DATA_ERROR",
                        message=f"Failed to fetch chart data: {chart_result['error']}",
                        path=f"/stocks/sp500/chart/{symbol}"
                    ).model_dump()
                )
        
        logger.info(f"✅ {symbol} 차트 데이터 조회 성공 (timeframe: {timeframe}, 데이터: {len(chart_result.get('chart_data', []))}개)")
        return JSONResponse(content=chart_result)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ 예상치 못한 오류: {e}")
        raise HTTPException(
            status_code=500,
            detail=create_error_response(
                error_type="INTERNAL_ERROR",
                message="Internal server error occurred",
                path=f"/stocks/sp500/chart/{symbol}"
            ).model_dump()
        )

# =========================
# 🎯 카테고리별 주식 조회 엔드포인트
# =========================

@router.get("/gainers", response_model=CategoryStockResponse, summary="상위 상승 종목 조회")
async def get_top_gainers(
    limit: int = Query(default=20, ge=1, le=100, description="반환할 최대 종목 개수"),
    sp500_service: SP500Service = Depends(get_sp500_service)
):
    """
    상위 상승 종목 조회
    
    **주요 기능:**
    - 전일 대비 상승률이 높은 종목들 조회
    - 상승률 기준 내림차순 정렬
    - 현재가, 변동률, 거래량 정보 포함
    
    **사용 예시:**
    ```
    GET /stocks/sp500/gainers?limit=10
    ```
    """
    try:
        logger.info(f"📈 상위 상승 종목 조회 요청 (limit: {limit})")
        
        result = sp500_service.get_top_gainers(limit)
        
        if result.get('error'):
            logger.error(f"❌ 상위 상승 종목 조회 실패: {result['error']}")
            raise HTTPException(
                status_code=500,
                detail=create_error_response(
                    error_type="DATA_FETCH_ERROR",
                    message=f"Failed to fetch top gainers: {result['error']}",
                    path="/stocks/sp500/gainers"
                ).model_dump()
            )
        
        logger.info(f"✅ 상위 상승 종목 조회 성공: {result['total_count']}개")
        return CategoryStockResponse(**result)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ 예상치 못한 오류: {e}")
        raise HTTPException(
            status_code=500,
            detail=create_error_response(
                error_type="INTERNAL_ERROR",
                message="Internal server error occurred",
                path="/stocks/sp500/gainers"
            ).dict()
        )

@router.get("/losers", response_model=CategoryStockResponse, summary="상위 하락 종목 조회")
async def get_top_losers(
    limit: int = Query(default=20, ge=1, le=100, description="반환할 최대 종목 개수"),
    sp500_service: SP500Service = Depends(get_sp500_service)
):
    """
    상위 하락 종목 조회
    
    **주요 기능:**
    - 전일 대비 하락률이 높은 종목들 조회
    - 하락률 기준 내림차순 정렬 (가장 많이 떨어진 순)
    - 현재가, 변동률, 거래량 정보 포함
    
    **사용 예시:**
    ```
    GET /stocks/sp500/losers?limit=15
    ```
    """
    try:
        logger.info(f"📉 상위 하락 종목 조회 요청 (limit: {limit})")
        
        result = sp500_service.get_top_losers(limit)
        
        if result.get('error'):
            logger.error(f"❌ 상위 하락 종목 조회 실패: {result['error']}")
            raise HTTPException(
                status_code=500,
                detail=create_error_response(
                    error_type="DATA_FETCH_ERROR",
                    message=f"Failed to fetch top losers: {result['error']}",
                    path="/stocks/sp500/losers"
                ).model_dump()
            )
        
        logger.info(f"✅ 상위 하락 종목 조회 성공: {result['total_count']}개")
        return CategoryStockResponse(**result)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ 예상치 못한 오류: {e}")
        raise HTTPException(
            status_code=500,
            detail=create_error_response(
                error_type="INTERNAL_ERROR",
                message="Internal server error occurred",
                path="/stocks/sp500/losers"
            ).dict()
        )

@router.get("/most-active", response_model=CategoryStockResponse, summary="가장 활발한 거래 종목 조회")
async def get_most_active(
    limit: int = Query(default=20, ge=1, le=100, description="반환할 최대 종목 개수"),
    sp500_service: SP500Service = Depends(get_sp500_service)
):
    """
    가장 활발한 거래 종목 조회
    
    **주요 기능:**
    - 거래량이 가장 많은 종목들 조회
    - 거래량 기준 내림차순 정렬
    - 현재가, 변동률, 거래량 정보 포함
    
    **사용 예시:**
    ```
    GET /stocks/sp500/most-active?limit=25
    ```
    """
    try:
        logger.info(f"📊 활발한 거래 종목 조회 요청 (limit: {limit})")
        
        result = sp500_service.get_most_active(limit)
        
        if result.get('error'):
            logger.error(f"❌ 활발한 거래 종목 조회 실패: {result['error']}")
            raise HTTPException(
                status_code=500,
                detail=create_error_response(
                    error_type="DATA_FETCH_ERROR",
                    message=f"Failed to fetch most active stocks: {result['error']}",
                    path="/stocks/sp500/most-active"
                ).model_dump()
            )
        
        logger.info(f"✅ 활발한 거래 종목 조회 성공: {result['total_count']}개")
        return CategoryStockResponse(**result)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ 예상치 못한 오류: {e}")
        raise HTTPException(
            status_code=500,
            detail=create_error_response(
                error_type="INTERNAL_ERROR",
                message="Internal server error occurred",
                path="/stocks/sp500/most-active"
            ).dict()
        )

# =========================
# 🎯 검색 및 필터 엔드포인트
# =========================

@router.get("/search", response_model=SearchResponse, summary="주식 검색")
async def search_stocks(
    q: str = Query(..., description="검색어 (심볼 또는 회사명)", min_length=1, max_length=50),
    limit: int = Query(default=20, ge=1, le=100, description="반환할 최대 결과 개수"),
    sp500_service: SP500Service = Depends(get_sp500_service)
):
    """
    주식 검색 (심볼 또는 회사명 기준)
    
    **주요 기능:**
    - 주식 심볼 또는 회사명으로 검색
    - 부분 일치 검색 지원
    - 심볼 알파벳 순 정렬
    
    **사용 예시:**
    ```
    GET /stocks/sp500/search?q=apple
    GET /stocks/sp500/search?q=AAPL
    GET /stocks/sp500/search?q=tech&limit=10
    ```
    
    **검색 대상:**
    - 주식 심볼 (예: AAPL, MSFT)
    - 회사명 (예: Apple, Microsoft)
    """
    try:
        logger.info(f"🔍 주식 검색 요청: '{q}' (limit: {limit})")
        
        result = sp500_service.search_stocks(q.strip(), limit)
        
        if result.get('error'):
            logger.error(f"❌ 주식 검색 실패: {result['error']}")
            raise HTTPException(
                status_code=500,
                detail=create_error_response(
                    error_type="SEARCH_ERROR",
                    message=f"Search failed: {result['error']}",
                    path="/stocks/sp500/search"
                ).model_dump()
            )
        
        logger.info(f"✅ 주식 검색 성공: '{q}' -> {result['total_count']}개 결과")
        return SearchResponse(**result)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ 예상치 못한 오류: {e}")
        raise HTTPException(
            status_code=500,
            detail=create_error_response(
                error_type="INTERNAL_ERROR",
                message="Internal server error occurred",
                path="/stocks/sp500/search"
            ).dict()
        )

# =========================
# 🎯 Company Overview 전용 엔드포인트 제거 (복잡성 감소) 🆕
# =========================

# Company Overview 관련 복잡한 엔드포인트들은 제거하고
# /symbol/{symbol} 에서 간단하게 통합 제공

# =========================
# 🎯 서비스 상태 및 관리 엔드포인트
# =========================

@router.get("/health", response_model=HealthCheckResponse, summary="서비스 헬스 체크")
async def health_check(
    sp500_service: SP500Service = Depends(get_sp500_service)
):
    """
    SP500 서비스 헬스 체크
    
    **주요 기능:**
    - 데이터베이스 연결 상태 확인
    - 데이터 신선도 체크
    - 시장 상태 정보 제공
    - 서비스 전반적인 상태 진단
    
    **사용 예시:**
    ```
    GET /stocks/sp500/health
    ```
    
    **응답 상태:**
    - `healthy`: 모든 서비스가 정상
    - `degraded`: 일부 서비스에 문제
    - `unhealthy`: 서비스 사용 불가
    """
    try:
        logger.info("🏥 SP500 서비스 헬스 체크 요청")
        
        result = sp500_service.health_check()
        
        logger.info(f"✅ 헬스 체크 완료: {result['status']}")
        return HealthCheckResponse(**result)
        
    except Exception as e:
        logger.error(f"❌ 헬스 체크 실패: {e}")
        return HealthCheckResponse(
            status="unhealthy",
            database="error",
            data_freshness="unknown",
            market_status={
                'is_open': False,
                'status': 'UNKNOWN',
                'current_time_et': 'N/A',
                'current_time_utc': 'N/A',
                'timezone': 'US/Eastern'
            },
            last_check=datetime.utcnow().isoformat(),
            error=str(e)
        )

@router.get("/stats", response_model=ServiceStats, summary="서비스 통계 조회")
async def get_service_stats(
    sp500_service: SP500Service = Depends(get_sp500_service)
):
    """
    SP500 서비스 통계 정보 조회
    
    **주요 기능:**
    - API 요청 횟수 통계
    - 데이터베이스 쿼리 통계
    - 에러 발생 현황
    - 서비스 가동 시간
    
    **사용 예시:**
    ```
    GET /stocks/sp500/stats
    ```
    """
    try:
        logger.info("📊 SP500 서비스 통계 조회 요청")
        
        result = sp500_service.get_service_stats()
        
        logger.info("✅ 서비스 통계 조회 성공")
        return ServiceStats(**result)
        
    except Exception as e:
        logger.error(f"❌ 서비스 통계 조회 실패: {e}")
        raise HTTPException(
            status_code=500,
            detail=create_error_response(
                error_type="STATS_ERROR",
                message="Failed to fetch service statistics",
                path="/stocks/sp500/stats"
            ).model_dump()
        )