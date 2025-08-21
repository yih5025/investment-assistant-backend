# app/api/endpoints/sp500_endpoint.py
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import desc, func

from app.database import get_db
from app.models.finnhub_trades_model import FinnhubTrades
from app.schemas.common import PaginatedResponse
from app.schemas.websocket_schema import SP500Data, db_to_sp500_data

router = APIRouter()

@router.get("/", response_model=PaginatedResponse[SP500Data])
async def get_sp500_list(
    category: Optional[str] = Query(None, description="카테고리 필터 (top_gainers, top_losers, most_actively_traded)"),
    limit: int = Query(50, ge=1, le=100, description="반환할 최대 개수"),
    page: int = Query(1, ge=1, description="페이지 번호"),
    sort_by: str = Query("volume", description="정렬 기준 (volume, price, timestamp)"),
    db: Session = Depends(get_db)
):
    """
    SP500 리스트 조회
    
    - **category**: 특정 카테고리 필터링 (선택사항)
    - **limit**: 페이지당 항목 수 (1-100)
    - **page**: 페이지 번호
    - **sort_by**: 정렬 기준 (volume, price, timestamp)
    """
    try:
        # 페이징 계산
        offset = (page - 1) * limit
        
        # 각 심볼의 최신 데이터만 조회하는 서브쿼리
        latest_trades_subquery = db.query(
            FinnhubTrades.symbol,
            func.max(FinnhubTrades.timestamp_ms).label('max_timestamp')
        ).group_by(FinnhubTrades.symbol).subquery()
        
        # 메인 쿼리
        query = db.query(FinnhubTrades).join(
            latest_trades_subquery,
            (FinnhubTrades.symbol == latest_trades_subquery.c.symbol) &
            (FinnhubTrades.timestamp_ms == latest_trades_subquery.c.max_timestamp)
        )
        
        # 카테고리 필터링
        if category:
            query = query.filter(FinnhubTrades.category == category)
        
        # 정렬
        if sort_by == "volume":
            query = query.order_by(desc(FinnhubTrades.volume))
        elif sort_by == "price":
            query = query.order_by(desc(FinnhubTrades.price))
        elif sort_by == "timestamp":
            query = query.order_by(desc(FinnhubTrades.timestamp_ms))
        else:
            query = query.order_by(desc(FinnhubTrades.volume))  # 기본값
        
        # 총 개수 조회
        total = query.count()
        total_pages = (total + limit - 1) // limit
        
        # 페이징 적용
        db_objects = query.offset(offset).limit(limit).all()
        
        # Pydantic 모델로 변환
        items = [db_to_sp500_data(obj) for obj in db_objects]
        
        return PaginatedResponse(
            items=items,
            total=total,
            page=page,
            limit=limit,
            total_pages=total_pages
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"SP500 조회 실패: {str(e)}")

@router.get("/latest", response_model=List[SP500Data])
async def get_latest_sp500(
    category: Optional[str] = Query(None, description="카테고리 필터"),
    limit: int = Query(15, ge=1, le=100, description="반환할 최대 개수"),
    sort_by: str = Query("volume", description="정렬 기준"),
    db: Session = Depends(get_db)
):
    """
    최신 SP500 데이터 조회 (WebSocket API fallback용)
    
    이 엔드포인트는 장 마감 시간에 WebSocket 대신 사용됩니다.
    각 심볼의 가장 최신 거래 데이터를 반환합니다.
    """
    try:
        # 각 심볼의 최신 데이터만 조회하는 서브쿼리
        latest_trades_subquery = db.query(
            FinnhubTrades.symbol,
            func.max(FinnhubTrades.timestamp_ms).label('max_timestamp')
        ).group_by(FinnhubTrades.symbol).subquery()
        
        # 메인 쿼리
        query = db.query(FinnhubTrades).join(
            latest_trades_subquery,
            (FinnhubTrades.symbol == latest_trades_subquery.c.symbol) &
            (FinnhubTrades.timestamp_ms == latest_trades_subquery.c.max_timestamp)
        )
        
        # 카테고리 필터링
        if category:
            query = query.filter(FinnhubTrades.category == category)
        
        # 정렬
        if sort_by == "volume":
            query = query.order_by(desc(FinnhubTrades.volume))
        elif sort_by == "price":
            query = query.order_by(desc(FinnhubTrades.price))
        elif sort_by == "timestamp":
            query = query.order_by(desc(FinnhubTrades.timestamp_ms))
        else:
            query = query.order_by(desc(FinnhubTrades.volume))  # 기본값
        
        # 제한 적용
        db_objects = query.limit(limit).all()
        
        # Pydantic 모델로 변환
        return [db_to_sp500_data(obj) for obj in db_objects]
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"최신 SP500 조회 실패: {str(e)}")

@router.get("/symbol/{symbol}", response_model=Optional[SP500Data])
async def get_sp500_by_symbol(
    symbol: str,
    db: Session = Depends(get_db)
):
    """특정 심볼의 SP500 최신 데이터 조회"""
    try:
        # 해당 심볼의 최신 데이터 조회
        db_object = db.query(FinnhubTrades).filter(
            FinnhubTrades.symbol == symbol.upper()
        ).order_by(desc(FinnhubTrades.timestamp_ms)).first()
        
        if not db_object:
            raise HTTPException(status_code=404, detail=f"심볼 {symbol} 데이터 없음")
        
        return db_to_sp500_data(db_object)
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"심볼 조회 실패: {str(e)}")

@router.get("/categories", response_model=List[str])
async def get_sp500_categories(db: Session = Depends(get_db)):
    """사용 가능한 SP500 카테고리 목록 조회"""
    try:
        # 사용 가능한 카테고리들 조회
        categories = db.query(FinnhubTrades.category).filter(
            FinnhubTrades.category.isnot(None)
        ).distinct().all()
        
        return [cat[0] for cat in categories if cat[0]]
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"카테고리 조회 실패: {str(e)}")

@router.get("/stats", response_model=dict)
async def get_sp500_stats(db: Session = Depends(get_db)):
    """SP500 통계 정보 조회"""
    try:
        # 전체 심볼 개수
        total_symbols = db.query(FinnhubTrades.symbol).distinct().count()
        
        # 카테고리별 개수
        category_counts = {}
        categories = db.query(
            FinnhubTrades.category,
            func.count(func.distinct(FinnhubTrades.symbol))
        ).filter(
            FinnhubTrades.category.isnot(None)
        ).group_by(FinnhubTrades.category).all()
        
        for category, count in categories:
            if category:
                category_counts[category] = count
        
        # 최근 업데이트 시간
        latest_trade = db.query(FinnhubTrades).order_by(
            desc(FinnhubTrades.timestamp_ms)
        ).first()
        
        last_updated = None
        if latest_trade:
            from datetime import datetime
            last_updated = datetime.fromtimestamp(
                latest_trade.timestamp_ms / 1000
            ).isoformat()
        
        # 전체 거래 레코드 수
        total_records = db.query(FinnhubTrades).count()
        
        return {
            "total_symbols": total_symbols,
            "total_records": total_records,
            "categories": category_counts,
            "last_updated": last_updated
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"통계 조회 실패: {str(e)}")

@router.get("/active", response_model=List[SP500Data])
async def get_most_active_sp500(
    limit: int = Query(10, ge=1, le=50, description="반환할 최대 개수"),
    hours: int = Query(24, ge=1, le=168, description="최근 몇 시간 내 데이터"),
    db: Session = Depends(get_db)
):
    """최근 가장 활발한 SP500 종목들 조회 (거래량 기준)"""
    try:
        from datetime import datetime, timedelta
        
        # 시간 범위 계산
        cutoff_time = datetime.utcnow() - timedelta(hours=hours)
        cutoff_timestamp_ms = int(cutoff_time.timestamp() * 1000)
        
        # 각 심볼별 최신 데이터 중 거래량이 높은 순으로 조회
        latest_trades_subquery = db.query(
            FinnhubTrades.symbol,
            func.max(FinnhubTrades.timestamp_ms).label('max_timestamp')
        ).filter(
            FinnhubTrades.timestamp_ms >= cutoff_timestamp_ms
        ).group_by(FinnhubTrades.symbol).subquery()
        
        query = db.query(FinnhubTrades).join(
            latest_trades_subquery,
            (FinnhubTrades.symbol == latest_trades_subquery.c.symbol) &
            (FinnhubTrades.timestamp_ms == latest_trades_subquery.c.max_timestamp)
        ).order_by(desc(FinnhubTrades.volume)).limit(limit)
        
        db_objects = query.all()
        
        return [db_to_sp500_data(obj) for obj in db_objects]
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"활발한 종목 조회 실패: {str(e)}")