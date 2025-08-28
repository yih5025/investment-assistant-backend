# app/api/endpoints/topgainers_endpoint.py
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
import logging
from app.database import get_db
from app.services.topgainers_service import TopGainersService
from app.schemas.topgainers_schema import TopGainerData

logger = logging.getLogger(__name__)

# 라우터 생성
router = APIRouter()

# 🎯 TopGainers 전용 서비스 인스턴스
topgainers_service = TopGainersService()

async def get_topgainers_service() -> TopGainersService:
    """TopGainers 서비스 의존성 주입"""
    if not topgainers_service.redis_client:
        await topgainers_service.init_redis()
    return topgainers_service

@router.get("/", response_model=List[TopGainerData])
async def get_topgainers_data(
    category: Optional[str] = Query(
        None, 
        description="카테고리 필터",
        regex="^(top_gainers|top_losers|most_actively_traded)$"
    ),
    limit: int = Query(50, ge=1, le=100, description="반환할 최대 개수"),
    service: TopGainersService = Depends(get_topgainers_service)
):
    """
    🎯 TopGainers 메인 데이터 조회 API
    
    **핵심 기능:**
    - 최신 batch_id의 50개 심볼 데이터 조회
    - 장중: Redis 실시간 데이터 / 장마감: PostgreSQL 종가 데이터  
    - 카테고리별 필터링 지원
    - 카테고리 정보 포함하여 프론트엔드 배너 표시 지원
    
    **Parameters:**
    - **category**: 카테고리 필터 (선택사항)
        - `top_gainers`: 상승 주식 (약 20개)
        - `top_losers`: 하락 주식 (약 10개) 
        - `most_actively_traded`: 활발히 거래되는 주식 (약 20개)
        - `None`: 전체 50개 심볼
    - **limit**: 반환할 최대 개수 (1-100)
    
    **Response:**
    ```json
    [
        {
            "batch_id": 32,
            "symbol": "GXAI",
            "category": "top_gainers",
            "price": 2.06,
            "change_amount": 0.96,
            "change_percentage": "87.27%",
            "volume": 246928896,
            "rank_position": 2,
            "last_updated": "2025-08-21T10:30:00Z"
        }
    ]
    ```
    """
    try:
        # 🎯 시장 상태에 따른 최적화된 데이터 조회
        data = await service.get_market_data_with_categories(category, limit)
        
        if not data:
            # 데이터가 없어도 에러가 아닌 빈 배열 반환
            return []
        
        return data
        
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"TopGainers 데이터 조회 실패: {str(e)}"
        )
    
# =========================
# 2. TopGainers 폴링 엔드포인트 추가  
# =========================

# app/api/endpoints/topgainers_endpoint.py (기존 파일에 추가)

@router.get("/polling", response_model=dict, summary="TopGainers 실시간 폴링 데이터 (더보기 방식)")
async def get_topgainers_polling_data(
    limit: int = Query(default=50, ge=1, le=200, description="반환할 항목 수 (누적)"),
    category: Optional[str] = Query(
        None, 
        description="카테고리 필터",
        regex="^(top_gainers|top_losers|most_actively_traded)$"
    ),
    service: TopGainersService = Depends(get_topgainers_service)
):
    """
    TopGainers 실시간 폴링 데이터 (WebSocket 대체용, "더보기" 방식)
    
    **동작 방식:**
    - limit=50: 상위 50개 반환 (처음 로딩)
    - limit=100: 상위 100개 반환 (더보기 클릭)
    - limit=150: 상위 150개 반환 (더보기 클릭)
    
    **카테고리 필터:**
    - `null`: 전체 카테고리 (기본값)
    - `top_gainers`: 상승 주식만
    - `top_losers`: 하락 주식만
    - `most_actively_traded`: 활발한 거래 주식만
    
    **사용 예시:**
    ```
    GET /api/v1/stocks/topgainers/polling?limit=50                    # 전체 50개
    GET /api/v1/stocks/topgainers/polling?limit=100&category=top_gainers  # 상승주 100개
    ```
    
    **특징:**
    - WebSocket과 동일한 Redis 데이터 소스 사용
    - 카테고리 정보 포함하여 반환
    - 순위 정보 포함 (rank_position)
    """
    try:
        logger.info(f"📡 TopGainers 폴링 데이터 요청 (limit: {limit}, category: {category})")
        
        result = await service.get_realtime_polling_data(
            limit=limit,
            category=category
        )
        
        if result.get('error'):
            logger.error(f"❌ TopGainers 폴링 데이터 조회 실패: {result['error']}")
            raise HTTPException(status_code=500, detail=result['error'])
        
        logger.info(f"✅ TopGainers 폴링 데이터 조회 성공: {len(result['data'])}개 반환")
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ TopGainers 폴링 오류: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/categories", response_model=dict)
@router.get("/categories/", response_model=dict)  # 슬래시가 있는 경우도 처리
async def get_topgainers_categories(
    service: TopGainersService = Depends(get_topgainers_service)
):
    """
    🎯 TopGainers 카테고리 정보 조회
    
    **용도:**
    - 프론트엔드에서 배너 구성 시 사용
    - 각 카테고리별 심볼 개수 확인
    - 데이터 업데이트 상태 확인
    
    **Response:**
    ```json
    {
        "categories": {
            "top_gainers": 20,
            "top_losers": 10,
            "most_actively_traded": 20
        },
        "total": 50,
        "batch_id": 32,
        "last_updated": "2025-08-21T10:30:00Z",
        "market_status": "OPEN",
        "data_source": "redis"
    }
    ```
    """
    try:
        stats = service.get_category_statistics()
        return stats
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"카테고리 정보 조회 실패: {str(e)}"
        )

@router.get("/stats", response_model=dict)
async def get_topgainers_stats(
    service: TopGainersService = Depends(get_topgainers_service)
):
    """
    🎯 TopGainers 서비스 통계 및 성능 정보
    
    **용도:**
    - 서비스 성능 모니터링
    - API 호출 통계 확인  
    - Redis/DB 사용률 분석
    - 에러율 추적
    
    **Response:**
    ```json
    {
        "performance": {
            "api_calls": 1250,
            "redis_calls": 800,
            "db_calls": 450,
            "cache_hits": 750,
            "market_closed_calls": 200,
            "errors": 5
        },
        "data_status": {
            "last_update": "2025-08-21T10:30:00Z",
            "cached_symbols": 50,
            "cache_age_seconds": 120
        },
        "health": {
            "redis_available": true,
            "market_status": "OPEN",
            "error_rate": 0.4
        }
    }
    ```
    """
    try:
        stats = service.get_service_stats()
        return stats
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"통계 정보 조회 실패: {str(e)}"
        )

@router.get("/symbol/{symbol}", response_model=Optional[TopGainerData])
async def get_topgainers_symbol(
    symbol: str,
    category: Optional[str] = Query(None, description="카테고리 필터"),
    service: TopGainersService = Depends(get_topgainers_service)
):
    """
    🎯 특정 심볼의 TopGainers 데이터 조회
    
    **용도:**
    - 개별 주식 상세 정보 조회
    - 특정 심볼이 어떤 카테고리에 속하는지 확인
    
    **Parameters:**
    - **symbol**: 주식 심볼 (예: GXAI, NVDA)
    - **category**: 카테고리 필터 (선택사항)
    
    **Response:**
    - 데이터가 있으면 TopGainerData 객체
    - 없으면 null
    """
    try:
        # 심볼 유효성 검사
        symbol = symbol.upper().strip()
        if not symbol or len(symbol) > 10:
            raise HTTPException(status_code=400, detail="유효하지 않은 심볼입니다")
        
        data = await service.get_symbol_data(symbol, category)
        
        if not data:
            raise HTTPException(
                status_code=404, 
                detail=f"심볼 '{symbol}'{f' (카테고리: {category})' if category else ''} 데이터를 찾을 수 없습니다"
            )
        
        return data
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"심볼 조회 실패: {str(e)}"
        )

@router.get("/health", response_model=dict)
async def get_topgainers_health(
    service: TopGainersService = Depends(get_topgainers_service)
):
    """
    🎯 TopGainers 서비스 헬스체크
    
    **용도:**
    - 서비스 상태 모니터링
    - Redis/DB 연결 상태 확인
    - 데이터 최신성 검증
    
    **Response:**
    ```json
    {
        "timestamp": "2025-08-21T10:30:00Z",
        "status": "healthy",
        "services": {
            "redis": {"status": "connected"},
            "database": {"status": "connected", "latest_batch": 32}
        }
    }
    ```
    """
    try:
        health_info = await service.health_check()
        return health_info
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"헬스체크 실패: {str(e)}"
        )

# =========================
# 🎯 간소화된 엔드포인트 설명
# =========================

"""
📋 TopGainers API 엔드포인트 요약

1. GET /topgainers/
   - 메인 데이터 조회 API
   - 장중: Redis 실시간 / 장마감: DB 종가
   - 카테고리 필터링 지원
   - 프론트엔드 배너용 데이터 제공

2. GET /topgainers/categories  
   - 카테고리 정보 (top_gainers: 20개, top_losers: 10개, most_actively_traded: 20개)
   - 프론트엔드에서 배너 구성할 때 사용
   
3. GET /topgainers/stats
   - 서비스 통계 및 성능 정보  
   - API 호출수, 에러율, 캐시 히트율 등
   
4. GET /topgainers/symbol/{symbol} (추가)
   - 개별 심볼 조회
   - 심볼이 어떤 카테고리에 속하는지 확인용
   
5. GET /topgainers/health (추가)
   - 헬스체크용
   - Redis/DB 연결 상태 확인

🎯 핵심 개선 사항:
- 불필요한 페이징 제거 (배치 기반이므로 최대 50개)
- 배치별 조회 제거 (최신 배치만 사용)  
- 복잡한 필터링 제거 (카테고리만 지원)
- WebSocket 의존성 제거 (TopGainersService로 분리)
- 명확한 용도별 API 구분
"""