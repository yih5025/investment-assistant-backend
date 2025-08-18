from fastapi import APIRouter, Depends, Query, Path, HTTPException
from sqlalchemy.orm import Session

from app.dependencies import get_db
from app.schemas.company_news_schema import (
    TrendingNewsResponse,
    CategoryNewsResponse, 
    SymbolNewsResponse,
    BatchInfo,
    CategorySummary
)
from app.services.company_news_service import CompanyNewsService

# Company News 라우터 생성
router = APIRouter(
    tags=["Company News"],
    responses={
        404: {"description": "요청한 기업 뉴스 데이터를 찾을 수 없습니다"},
        422: {"description": "잘못된 요청 파라미터"},
        500: {"description": "서버 내부 오류"}
    }
)

@router.get("/trending", response_model=TrendingNewsResponse, summary="트렌딩 주식 뉴스 조회")
async def get_trending_news(
    db: Session = Depends(get_db),
    days: int = Query(3, ge=1, le=7, description="뉴스 조회 기간 (1-7일)"),
    limit: int = Query(20, ge=1, le=100, description="각 심볼당 최대 뉴스 개수"),
    offset: int = Query(0, ge=0, description="페이징 오프셋")
):
    """
    **트렌딩 주식의 뉴스를 조회합니다.**
    
    최신 batch_id의 모든 트렌딩 주식(50개)에 대한 뉴스를 통합 조회합니다.
    
    ### 포함 카테고리:
    - **top_gainers**: 상승 주식 (~20개)
    - **top_losers**: 하락 주식 (~10개)  
    - **most_actively_traded**: 활발한 주식 (~20개)
    
    ### 파라미터:
    - **days**: 뉴스 조회 기간 (1-7일, 기본 3일)
    - **limit**: 각 심볼당 최대 뉴스 개수 (기본 20개)
    - **offset**: 페이징 오프셋
    
    ### 응답:
    - 배치 정보 + 카테고리 요약 + 주식별 뉴스 목록
    """
    try:
        service = CompanyNewsService(db)
        
        # 트렌딩 뉴스 조회
        batch_info, stocks_with_news = service.get_trending_news(days, limit, offset)
        
        if not batch_info:
            raise HTTPException(
                status_code=404, 
                detail="트렌딩 데이터를 찾을 수 없습니다. top_gainers 테이블을 확인해주세요."
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"트렌딩 뉴스 조회 중 오류가 발생했습니다: {str(e)}"
        )
    
    # 카테고리별 요약 생성
    category_counts = {"top_gainers": 0, "top_losers": 0, "most_actively_traded": 0}
    for stock in stocks_with_news:
        category = stock.get("category", "")
        if category in category_counts:
            category_counts[category] += 1
    
    # 응답 구성
    return TrendingNewsResponse(
        batch_info=BatchInfo(**batch_info),
        categories=CategorySummary(**category_counts),
        stocks=stocks_with_news
    )


@router.get("/trending/category/{category}", response_model=CategoryNewsResponse)
async def get_category_news(
    category: str = Path(..., description="카테고리", 
                        pattern="^(top_gainers|top_losers|most_actively_traded)$"),
    db: Session = Depends(get_db),
    days: int = Query(3, ge=1, le=7, description="뉴스 조회 기간 (1-7일)"),
    limit: int = Query(20, ge=1, le=100, description="각 심볼당 최대 뉴스 개수"),
    offset: int = Query(0, ge=0, description="페이징 오프셋")
):
    """
    특정 카테고리의 트렌딩 주식 뉴스를 조회합니다.
    
    카테고리별로 필터링된 트렌딩 주식의 뉴스를 조회합니다.
    
    Args:
        category: 카테고리 (top_gainers, top_losers, most_actively_traded)
        days: 뉴스 조회 기간 (기본 3일)
        limit: 각 심볼당 최대 뉴스 개수 (기본 20개)
        offset: 페이징 오프셋
        
    Returns:
        CategoryNewsResponse: 배치 정보 + 해당 카테고리 주식별 뉴스
    """
    service = CompanyNewsService(db)
    
    # 카테고리별 뉴스 조회
    batch_info, stocks_with_news = service.get_category_news(category, days, limit, offset)
    
    if not batch_info:
        raise HTTPException(
            status_code=404,
            detail=f"'{category}' 카테고리의 트렌딩 데이터를 찾을 수 없습니다."
        )
    
    # 응답 구성
    return CategoryNewsResponse(
        batch_info=BatchInfo(
            batch_id=batch_info["batch_id"],
            last_updated=batch_info["last_updated"],
            total_symbols=batch_info["total_symbols"], 
            period_days=batch_info["period_days"]
        ),
        category=batch_info["category"],
        symbols_count=batch_info["symbols_count"],
        stocks=stocks_with_news
    )


@router.get("/symbol/{symbol}", response_model=SymbolNewsResponse)
async def get_symbol_news(
    symbol: str = Path(..., description="주식 심볼", example="AAPL"),
    db: Session = Depends(get_db),
    days: int = Query(3, ge=1, le=7, description="뉴스 조회 기간 (1-7일)"),
    limit: int = Query(20, ge=1, le=100, description="최대 뉴스 개수"),
    offset: int = Query(0, ge=0, description="페이징 오프셋")
):
    """
    특정 심볼의 뉴스를 조회합니다.
    
    특정 주식 심볼에 대한 뉴스를 조회하고, 최신 배치에서 트렌딩 정보도 함께 제공합니다.
    
    Args:
        symbol: 주식 심볼 (대소문자 구분 없음)
        days: 뉴스 조회 기간 (기본 3일)
        limit: 최대 뉴스 개수 (기본 20개)
        offset: 페이징 오프셋
        
    Returns:
        SymbolNewsResponse: 심볼 뉴스 + 트렌딩 정보
    """
    service = CompanyNewsService(db)
    
    # 심볼 대문자 변환 (일관성을 위해)
    symbol = symbol.upper()
    
    # 심볼별 뉴스와 트렌딩 정보 조회
    news_list, trending_info = service.get_symbol_news_with_trending_info(
        symbol, days, limit, offset
    )
    
    # 뉴스가 없는 경우 404 대신 빈 배열 반환 (더 사용자 친화적)
    if not news_list:
        print(f"⚠️ '{symbol}' 심볼의 뉴스가 {days}일 이내에 없습니다.")
    
    # 총 뉴스 개수 조회 (페이징 정보용)
    total_news = service.count_news_by_symbol(symbol, days)
    
    # 응답 구성
    return SymbolNewsResponse(
        symbol=symbol,
        period_days=days,
        total_news=total_news,
        latest_batch_info=trending_info if trending_info.get("is_trending") else None,
        news=news_list
    )


@router.get("/trending/categories", response_model=dict)
async def get_available_categories(db: Session = Depends(get_db)):
    """
    사용 가능한 트렌딩 카테고리 목록을 조회합니다.
    
    현재 시스템에서 지원하는 카테고리와 각 카테고리의 설명을 반환합니다.
    
    Returns:
        dict: 카테고리 정보
    """
    return {
        "categories": {
            "top_gainers": {
                "name": "상승 주식",
                "description": "가장 많이 상승한 주식들",
                "typical_count": "약 20개"
            },
            "top_losers": {
                "name": "하락 주식", 
                "description": "가장 많이 하락한 주식들",
                "typical_count": "약 10개"
            },
            "most_actively_traded": {
                "name": "활발한 주식",
                "description": "거래량이 가장 많은 주식들", 
                "typical_count": "약 20개"
            }
        },
        "total_symbols": "약 50개",
        "update_frequency": "하루 1회",
        "data_source": "top_gainers 테이블"
    }


@router.get("/trending/batch/{batch_id}", response_model=TrendingNewsResponse)
async def get_batch_news(
    batch_id: int = Path(..., description="배치 ID", example=11),
    db: Session = Depends(get_db),
    days: int = Query(3, ge=1, le=7, description="뉴스 조회 기간 (1-7일)"),
    limit: int = Query(20, ge=1, le=100, description="각 심볼당 최대 뉴스 개수"),
    offset: int = Query(0, ge=0, description="페이징 오프셋")
):
    """
    특정 배치 ID의 트렌딩 주식 뉴스를 조회합니다.
    
    특정 batch_id의 트렌딩 데이터를 기준으로 뉴스를 조회합니다.
    과거 데이터 분석이나 특정 시점의 트렌딩 뉴스를 확인할 때 유용합니다.
    
    Args:
        batch_id: 조회할 배치 ID
        days: 뉴스 조회 기간 (기본 3일)
        limit: 각 심볼당 최대 뉴스 개수 (기본 20개)
        offset: 페이징 오프셋
        
    Returns:
        TrendingNewsResponse: 해당 배치의 트렌딩 뉴스
    """
    service = CompanyNewsService(db)
    
    # 해당 배치의 트렌딩 주식 조회
    trending_stocks = service.get_trending_stocks(batch_id)
    
    if not trending_stocks:
        raise HTTPException(
            status_code=404,
            detail=f"배치 ID {batch_id}의 데이터를 찾을 수 없습니다."
        )
    
    # 배치 정보 구성
    batch_info = {
        "batch_id": batch_id,
        "last_updated": trending_stocks[0].last_updated,
        "total_symbols": len(trending_stocks),
        "period_days": days
    }
    
    # 심볼별 뉴스 조회
    stocks_with_news = []
    
    for stock in trending_stocks:
        news_list = service.get_news_by_symbol(stock.symbol, days, limit)
        
        stock_data = {
            "symbol": stock.symbol,
            "category": stock.category,
            "rank_position": int(stock.rank_position) if stock.rank_position else None,
            "price": float(stock.price) if stock.price else None,
            "change_percentage": stock.change_percentage,
            "volume": int(stock.volume) if stock.volume else None,
            "news_count": len(news_list),
            "news": news_list
        }
        stocks_with_news.append(stock_data)
    
    # 카테고리별 요약 생성
    category_counts = {"top_gainers": 0, "top_losers": 0, "most_actively_traded": 0}
    for stock in stocks_with_news:
        category = stock.get("category", "")
        if category in category_counts:
            category_counts[category] += 1
    
    # 응답 구성
    return TrendingNewsResponse(
        batch_info=BatchInfo(**batch_info),
        categories=CategorySummary(**category_counts),
        stocks=stocks_with_news
    )