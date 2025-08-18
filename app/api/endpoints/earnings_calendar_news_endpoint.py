from datetime import date, datetime
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import and_, extract, func, or_  # ← 이것들 추가

from app.database import get_db
from app.services.earnings_calendar_news_service import EarningsCalendarNewsService
from app.schemas.earnings_calendar_news_schema import (
    EarningsCalendarNewsResponse,
    EarningsCalendarNewsListResponse,
    EarningsCalendarNewsSearchResponse
)

router = APIRouter()

@router.get("/{symbol}/{report_date}", response_model=EarningsCalendarNewsListResponse)
async def get_earnings_news_by_symbol_and_date(
    symbol: str,
    report_date: date,
    skip: int = Query(0, ge=0, description="페이징 offset"),
    limit: int = Query(20, ge=1, le=100, description="페이징 limit"),
    db: Session = Depends(get_db)
):
    """
    특정 기업의 특정 실적 발표에 대한 뉴스 조회
    
    용도: 달력 UI에서 "7월 25일 AAPL 실적 발표" 클릭 시 해당 뉴스들 표시
    
    Args:
        symbol: 기업 심볼 (예: AAPL)
        report_date: 실적 발표일 (YYYY-MM-DD)
        skip: 페이징 offset
        limit: 페이징 limit (1-100)
    
    Returns:
        EarningsCalendarNewsListResponse: 해당 실적 관련 뉴스 목록
        
    Example:
        GET /api/v1/earnings-calendar-news/AAPL/2025-07-31?skip=0&limit=10
    """
    service = EarningsCalendarNewsService(db)
    
    try:
        result = service.get_news_by_symbol_and_date(
            symbol=symbol.upper(),
            report_date=report_date,
            skip=skip,
            limit=limit
        )
        return result
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"실적 관련 뉴스 조회 중 오류 발생: {str(e)}"
        )

@router.get("/{symbol}", response_model=EarningsCalendarNewsListResponse)
async def get_earnings_news_by_symbol(
    symbol: str,
    skip: int = Query(0, ge=0, description="페이징 offset"),
    limit: int = Query(20, ge=1, le=100, description="페이징 limit"),
    start_date: Optional[date] = Query(None, description="실적 발표일 시작 필터 (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="실적 발표일 종료 필터 (YYYY-MM-DD)"),
    db: Session = Depends(get_db)
):
    """
    특정 기업의 모든 실적 관련 뉴스 조회
    
    Args:
        symbol: 기업 심볼 (예: AAPL)
        skip: 페이징 offset
        limit: 페이징 limit
        start_date: 실적 발표일 시작 필터
        end_date: 실적 발표일 종료 필터
    
    Returns:
        EarningsCalendarNewsListResponse: 해당 기업의 실적 관련 뉴스 목록
        
    Example:
        GET /api/v1/earnings-calendar-news/AAPL?start_date=2025-01-01&end_date=2025-12-31
    """
    service = EarningsCalendarNewsService(db)
    
    # 날짜 범위 검증
    if start_date and end_date and start_date > end_date:
        raise HTTPException(
            status_code=400,
            detail="시작 날짜는 종료 날짜보다 이전이어야 합니다"
        )
    
    try:
        result = service.get_news_by_symbol(
            symbol=symbol.upper(),
            skip=skip,
            limit=limit,
            start_date=start_date,
            end_date=end_date
        )
        return result
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"기업별 실적 뉴스 조회 중 오류 발생: {str(e)}"
        )

@router.get("/", response_model=EarningsCalendarNewsListResponse)
async def get_earnings_calendar_news_list(
    skip: int = Query(0, ge=0, description="페이징 offset"),
    limit: int = Query(20, ge=1, le=100, description="페이징 limit"),
    symbols: Optional[List[str]] = Query(None, description="기업 심볼 필터 (쉼표로 구분)"),
    start_report_date: Optional[date] = Query(None, description="실적 발표일 시작"),
    end_report_date: Optional[date] = Query(None, description="실적 발표일 종료"),
    start_published_date: Optional[datetime] = Query(None, description="뉴스 발행일 시작"),
    end_published_date: Optional[datetime] = Query(None, description="뉴스 발행일 종료"),
    sources: Optional[List[str]] = Query(None, description="뉴스 소스 필터"),
    db: Session = Depends(get_db)
):
    """
    전체 실적 캘린더 뉴스 목록 조회
    
    Args:
        skip: 페이징 offset
        limit: 페이징 limit
        symbols: 기업 심볼 필터 (예: ["AAPL", "GOOGL", "MSFT"])
        start_report_date: 실적 발표일 시작
        end_report_date: 실적 발표일 종료
        start_published_date: 뉴스 발행일 시작
        end_published_date: 뉴스 발행일 종료
        sources: 뉴스 소스 필터 (예: ["Yahoo", "Reuters"])
    
    Returns:
        EarningsCalendarNewsListResponse: 필터링된 뉴스 목록
        
    Example:
        GET /api/v1/earnings-calendar-news/?symbols=AAPL,GOOGL&start_report_date=2025-07-01
    """
    service = EarningsCalendarNewsService(db)
    
    # 날짜 범위 검증
    if start_report_date and end_report_date and start_report_date > end_report_date:
        raise HTTPException(
            status_code=400,
            detail="실적 발표일 시작은 종료일보다 이전이어야 합니다"
        )
    
    if start_published_date and end_published_date and start_published_date > end_published_date:
        raise HTTPException(
            status_code=400,
            detail="뉴스 발행일 시작은 종료일보다 이전이어야 합니다"
        )
    
    # symbols를 대문자로 변환
    if symbols:
        symbols = [symbol.upper() for symbol in symbols]
    
    try:
        result = service.get_earnings_calendar_news_list(
            skip=skip,
            limit=limit,
            symbols=symbols,
            start_report_date=start_report_date,
            end_report_date=end_report_date,
            start_published_date=start_published_date,
            end_published_date=end_published_date,
            sources=sources
        )
        return result
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"실적 뉴스 목록 조회 중 오류 발생: {str(e)}"
        )

@router.get("/{symbol}/{report_date}/detail", response_model=EarningsCalendarNewsResponse)
async def get_earnings_news_detail(
    symbol: str,
    report_date: date,
    url: str = Query(..., description="뉴스 URL"),
    db: Session = Depends(get_db)
):
    """
    특정 뉴스 상세 조회 (3중 복합 키: symbol + report_date + url)
    
    Args:
        symbol: 기업 심볼
        report_date: 실적 발표일
        url: 뉴스 URL
    
    Returns:
        EarningsCalendarNewsResponse: 뉴스 상세 정보
        
    Example:
        GET /api/v1/earnings-calendar-news/AAPL/2025-07-31/detail?url=https://example.com/news
    """
    service = EarningsCalendarNewsService(db)
    
    try:
        result = service.get_news_detail(
            symbol=symbol.upper(),
            report_date=report_date,
            url=url
        )
        
        if not result:
            raise HTTPException(
                status_code=404,
                detail=f"해당 뉴스를 찾을 수 없습니다: {symbol} / {report_date} / {url}"
            )
        
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"뉴스 상세 조회 중 오류 발생: {str(e)}"
        )

@router.get("/search", response_model=EarningsCalendarNewsSearchResponse)
async def search_earnings_calendar_news(
    q: str = Query(..., min_length=2, description="검색어 (최소 2글자)"),
    skip: int = Query(0, ge=0, description="페이징 offset"),
    limit: int = Query(20, ge=1, le=100, description="페이징 limit"),
    symbols: Optional[List[str]] = Query(None, description="검색 대상 기업 심볼"),
    start_date: Optional[date] = Query(None, description="실적 발표일 범위 시작"),
    end_date: Optional[date] = Query(None, description="실적 발표일 범위 종료"),
    db: Session = Depends(get_db)
):
    """
    실적 관련 뉴스 검색
    
    Args:
        q: 검색어 (제목, 요약, 소스에서 검색)
        skip: 페이징 offset
        limit: 페이징 limit
        symbols: 검색 대상 기업 심볼들
        start_date: 실적 발표일 범위 시작
        end_date: 실적 발표일 범위 종료
    
    Returns:
        EarningsCalendarNewsSearchResponse: 검색 결과
        
    Example:
        GET /api/v1/earnings-calendar-news/search?q=earnings&symbols=AAPL,GOOGL
    """
    service = EarningsCalendarNewsService(db)
    
    # 날짜 범위 검증
    if start_date and end_date and start_date > end_date:
        raise HTTPException(
            status_code=400,
            detail="시작 날짜는 종료 날짜보다 이전이어야 합니다"
        )
    
    # symbols를 대문자로 변환
    if symbols:
        symbols = [symbol.upper() for symbol in symbols]
    
    try:
        result = service.search_earnings_calendar_news(
            query_text=q,
            skip=skip,
            limit=limit,
            symbols=symbols,
            start_date=start_date,
            end_date=end_date
        )
        return result
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"뉴스 검색 중 오류 발생: {str(e)}"
        )

# 추가: 캘린더 전용 API (프론트엔드 캘린더 UI를 위한 최적화된 엔드포인트)
@router.get("/calendar/{symbol}", response_model=List[dict])
async def get_earnings_calendar_with_news_count(
    symbol: str,
    year: int = Query(..., ge=2020, le=2030, description="조회할 연도"),
    month: Optional[int] = Query(None, ge=1, le=12, description="조회할 월 (선택사항)"),
    db: Session = Depends(get_db)
):
    """
    캘린더 UI용: 특정 기업의 실적 일정과 각 날짜별 뉴스 개수
    
    프론트엔드 캘린더에서 "7월 25일에 AAPL 실적 발표 (뉴스 5건)" 같은 표시용
    
    Args:
        symbol: 기업 심볼
        year: 조회할 연도
        month: 조회할 월 (없으면 전체 연도)
    
    Returns:
        List[dict]: 실적 일정별 뉴스 개수 정보
        
    Example Response:
        [
            {
                "report_date": "2025-07-31",
                "company_name": "Apple Inc.",
                "estimate": 1.41,
                "news_count": 15,
                "has_news": true
            }
        ]
    """
    service = EarningsCalendarNewsService(db)
    
    try:
        # 해당 연도/월의 실적 일정 조회
        from sqlalchemy import extract
        from app.models.earnings_calendar_model import EarningsCalendar
        
        query = db.query(EarningsCalendar).filter(
            and_(
                EarningsCalendar.symbol == symbol.upper(),
                extract('year', EarningsCalendar.report_date) == year
            )
        )
        
        if month:
            query = query.filter(extract('month', EarningsCalendar.report_date) == month)
        
        earnings_schedules = query.order_by(EarningsCalendar.report_date).all()
        
        # 각 실적 일정별 뉴스 개수 조회
        result = []
        for schedule in earnings_schedules:
            # 해당 실적 발표일의 뉴스 개수 조회
            news_response = service.get_news_by_symbol_and_date(
                symbol=schedule.symbol,
                report_date=schedule.report_date,
                skip=0,
                limit=1  # 개수만 필요하므로 1개만 조회
            )
            
            result.append({
                "report_date": schedule.report_date.isoformat(),
                "company_name": schedule.company_name,
                "estimate": float(schedule.estimate) if schedule.estimate else None,
                "currency": schedule.currency,
                "news_count": news_response.total,
                "has_news": news_response.total > 0
            })
        
        return result
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"캘린더 데이터 조회 중 오류 발생: {str(e)}"
        )

# 추가: 대시보드용 요약 정보
@router.get("/summary/recent", response_model=dict)
async def get_recent_earnings_news_summary(
    days: int = Query(7, ge=1, le=30, description="최근 N일간의 데이터"),
    top_symbols: int = Query(10, ge=5, le=50, description="상위 N개 기업"),
    db: Session = Depends(get_db)
):
    """
    대시보드용: 최근 실적 뉴스 요약 정보
    
    Args:
        days: 최근 N일간의 데이터
        top_symbols: 상위 N개 기업
    
    Returns:
        dict: 요약 통계 정보
        
    Example Response:
        {
            "period": "최근 7일",
            "total_earnings_events": 25,
            "total_news": 342,
            "top_symbols_by_news": [
                {"symbol": "AAPL", "company_name": "Apple Inc.", "news_count": 45},
                {"symbol": "GOOGL", "company_name": "Alphabet Inc.", "news_count": 38}
            ],
            "recent_earnings": [
                {"symbol": "AAPL", "report_date": "2025-07-31", "news_count": 15}
            ]
        }
    """
    from datetime import datetime, timedelta
    from sqlalchemy import func
    from app.models.earnings_calendar_model import EarningsCalendar
    
    try:
        # 기간 설정
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days)
        
        # 해당 기간의 실적 일정들
        recent_earnings = db.query(EarningsCalendar).filter(
            and_(
                EarningsCalendar.report_date >= start_date,
                EarningsCalendar.report_date <= end_date
            )
        ).all()
        
        # 각 기업별 뉴스 개수 집계
        symbol_news_count = {}
        total_news = 0
        
        for earning in recent_earnings:
            service = EarningsCalendarNewsService(db)
            news_response = service.get_news_by_symbol_and_date(
                symbol=earning.symbol,
                report_date=earning.report_date,
                skip=0,
                limit=1
            )
            
            if earning.symbol not in symbol_news_count:
                symbol_news_count[earning.symbol] = {
                    'company_name': earning.company_name,
                    'news_count': 0
                }
            
            symbol_news_count[earning.symbol]['news_count'] += news_response.total
            total_news += news_response.total
        
        # 상위 기업 정렬
        top_symbols_list = sorted(
            [
                {
                    'symbol': symbol,
                    'company_name': data['company_name'],
                    'news_count': data['news_count']
                }
                for symbol, data in symbol_news_count.items()
            ],
            key=lambda x: x['news_count'],
            reverse=True
        )[:top_symbols]
        
        # 최근 실적 일정들 (뉴스 개수 포함)
        recent_earnings_list = []
        for earning in recent_earnings[-10:]:  # 최근 10개만
            service = EarningsCalendarNewsService(db)
            news_response = service.get_news_by_symbol_and_date(
                symbol=earning.symbol,
                report_date=earning.report_date,
                skip=0,
                limit=1
            )
            
            recent_earnings_list.append({
                'symbol': earning.symbol,
                'company_name': earning.company_name,
                'report_date': earning.report_date.isoformat(),
                'news_count': news_response.total
            })
        
        return {
            'period': f'최근 {days}일',
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat(),
            'total_earnings_events': len(recent_earnings),
            'total_news': total_news,
            'top_symbols_by_news': top_symbols_list,
            'recent_earnings': recent_earnings_list
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"요약 정보 조회 중 오류 발생: {str(e)}"
        )