# app/api/endpoints/topgainers_endpoint.py
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.top_gainers_model import TopGainers
from app.schemas.common import PaginatedResponse
from app.schemas.websocket_schema import TopGainerData, db_to_topgainer_data

router = APIRouter()

@router.get("/", response_model=PaginatedResponse[TopGainerData])
async def get_topgainers_list(
    category: Optional[str] = Query(None, description="카테고리 필터 (top_gainers, top_losers, most_actively_traded)"),
    limit: int = Query(50, ge=1, le=100, description="반환할 최대 개수"),
    page: int = Query(1, ge=1, description="페이지 번호"),
    db: Session = Depends(get_db)
):
    """
    TopGainers 리스트 조회
    
    - **category**: 특정 카테고리 필터링 (선택사항)
    - **limit**: 페이지당 항목 수 (1-100)
    - **page**: 페이지 번호
    """
    try:
        # 최신 batch_id 조회
        latest_batch = TopGainers.get_latest_batch_id(db)
        if not latest_batch:
            return PaginatedResponse(
                items=[],
                total=0,
                page=page,
                limit=limit,
                total_pages=0
            )
        
        batch_id = latest_batch[0]
        
        # 페이징 계산
        offset = (page - 1) * limit
        
        # 데이터 조회
        if category:
            # 특정 카테고리만 조회
            db_objects = TopGainers.get_by_category(db, category, batch_id, limit, offset)
            # 총 개수 조회
            total_query = db.query(TopGainers).filter(
                TopGainers.batch_id == batch_id,
                TopGainers.category == category
            )
        else:
            # 모든 카테고리 조회
            db_objects = db.query(TopGainers).filter(
                TopGainers.batch_id == batch_id
            ).order_by(TopGainers.rank_position).offset(offset).limit(limit).all()
            
            # 총 개수 조회
            total_query = db.query(TopGainers).filter(
                TopGainers.batch_id == batch_id
            )
        
        total = total_query.count()
        total_pages = (total + limit - 1) // limit
        
        # Pydantic 모델로 변환
        items = [db_to_topgainer_data(obj) for obj in db_objects]
        
        return PaginatedResponse(
            items=items,
            total=total,
            page=page,
            limit=limit,
            total_pages=total_pages
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"TopGainers 조회 실패: {str(e)}")

@router.get("/latest", response_model=List[TopGainerData])
async def get_latest_topgainers(
    category: Optional[str] = Query(None, description="카테고리 필터"),
    limit: int = Query(10, ge=1, le=100, description="반환할 최대 개수"),
    db: Session = Depends(get_db)
):
    """
    최신 TopGainers 데이터 조회 (WebSocket API fallback용)
    
    이 엔드포인트는 장 마감 시간에 WebSocket 대신 사용됩니다.
    """
    try:
        # 최신 batch_id 조회
        latest_batch = TopGainers.get_latest_batch_id(db)
        if not latest_batch:
            return []
        
        batch_id = latest_batch[0]
        
        # 데이터 조회
        if category:
            db_objects = TopGainers.get_by_category(db, category, batch_id, limit)
        else:
            # 기본적으로 top_gainers 카테고리 우선
            db_objects = db.query(TopGainers).filter(
                TopGainers.batch_id == batch_id
            ).order_by(
                TopGainers.category.desc(),  # top_gainers가 먼저 오도록
                TopGainers.rank_position
            ).limit(limit).all()
        
        # Pydantic 모델로 변환
        return [db_to_topgainer_data(obj) for obj in db_objects]
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"최신 TopGainers 조회 실패: {str(e)}")

@router.get("/categories", response_model=List[str])
async def get_topgainers_categories(db: Session = Depends(get_db)):
    """사용 가능한 TopGainers 카테고리 목록 조회"""
    try:
        # 최신 batch에서 사용 가능한 카테고리들 조회
        latest_batch = TopGainers.get_latest_batch_id(db)
        if not latest_batch:
            return []
        
        batch_id = latest_batch[0]
        
        categories = db.query(TopGainers.category).filter(
            TopGainers.batch_id == batch_id
        ).distinct().all()
        
        return [cat[0] for cat in categories if cat[0]]
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"카테고리 조회 실패: {str(e)}")

@router.get("/symbol/{symbol}", response_model=Optional[TopGainerData])
async def get_topgainers_by_symbol(
    symbol: str,
    db: Session = Depends(get_db)
):
    """특정 심볼의 TopGainers 데이터 조회"""
    try:
        # 최신 batch_id 조회
        latest_batch = TopGainers.get_latest_batch_id(db)
        if not latest_batch:
            return None
        
        batch_id = latest_batch[0]
        
        # 심볼로 조회
        db_object = db.query(TopGainers).filter(
            TopGainers.batch_id == batch_id,
            TopGainers.symbol == symbol.upper()
        ).first()
        
        if not db_object:
            raise HTTPException(status_code=404, detail=f"심볼 {symbol} 데이터 없음")
        
        return db_to_topgainer_data(db_object)
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"심볼 조회 실패: {str(e)}")

@router.get("/stats", response_model=dict)
async def get_topgainers_stats(db: Session = Depends(get_db)):
    """TopGainers 통계 정보 조회"""
    try:
        # 최신 batch_id 조회
        latest_batch = TopGainers.get_latest_batch_id(db)
        if not latest_batch:
            return {"total": 0, "categories": {}, "last_updated": None}
        
        batch_id = latest_batch[0]
        
        # 전체 개수
        total = db.query(TopGainers).filter(TopGainers.batch_id == batch_id).count()
        
        # 카테고리별 개수
        category_counts = {}
        categories = db.query(
            TopGainers.category,
            db.func.count(TopGainers.symbol)
        ).filter(
            TopGainers.batch_id == batch_id
        ).group_by(TopGainers.category).all()
        
        for category, count in categories:
            if category:
                category_counts[category] = count
        
        # 마지막 업데이트 시간
        last_updated_obj = db.query(TopGainers).filter(
            TopGainers.batch_id == batch_id
        ).order_by(TopGainers.last_updated.desc()).first()
        
        last_updated = None
        if last_updated_obj:
            last_updated = last_updated_obj.last_updated.isoformat()
        
        return {
            "total": total,
            "batch_id": batch_id,
            "categories": category_counts,
            "last_updated": last_updated
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"통계 조회 실패: {str(e)}")