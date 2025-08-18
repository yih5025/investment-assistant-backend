from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import date, datetime

from app.schemas.earnings_calendar_schema import (
    EarningsCalendarResponse,
    EarningsCalendarListResponse,
    EarningsCalendarQueryParams
)
from app.services.earnings_calendar_service import EarningsCalendarService
from app.dependencies import get_db

# 실적 캘린더 라우터 생성
router = APIRouter(
    tags=["Earnings Calendar"],
    responses={
        404: {"description": "요청한 실적 데이터를 찾을 수 없습니다"},
        422: {"description": "잘못된 요청 파라미터"},
        500: {"description": "서버 내부 오류"}
    }
)

@router.get(
    "/",
    response_model=EarningsCalendarListResponse,
    summary="실적 발표 캘린더 조회",
    description="날짜 범위, 심볼 등의 조건으로 실적 발표 일정을 조회합니다. 프론트엔드 달력에 표시할 데이터를 제공합니다."
)
async def get_earnings_calendar(
    start_date: Optional[date] = Query(None, description="조회 시작일 (예: 2025-08-01)", example="2025-08-01"),
    end_date: Optional[date] = Query(None, description="조회 종료일 (예: 2025-08-31)", example="2025-08-31"),
    symbol: Optional[str] = Query(None, description="주식 심볼 (예: AAPL, TSLA)", example="AAPL"),
    limit: int = Query(50, ge=1, le=1000, description="최대 조회 개수"),
    offset: int = Query(0, ge=0, description="건너뛸 개수 (페이징용)"),
    db: Session = Depends(get_db)
):
    """
    실적 발표 캘린더 데이터를 조회합니다.
    
    프론트엔드에서 달력 컴포넌트에 표시할 데이터를 제공합니다.
    날짜 범위를 지정하여 특정 월/주의 실적 일정을 가져올 수 있습니다.
    
    Args:
        start_date: 조회 시작일 (없으면 제한 없음)
        end_date: 조회 종료일 (없으면 제한 없음)
        symbol: 특정 심볼 필터링 (없으면 전체)
        limit: 페이징 - 최대 조회 개수 (기본 50개)
        offset: 페이징 - 건너뛸 개수 (기본 0개)
        
    Returns:
        EarningsCalendarListResponse: 실적 일정 목록과 총 개수
        
    Example:
        GET /api/v1/earnings-calendar/?start_date=2025-08-01&end_date=2025-08-31&limit=20
        
        Response:
        {
            "items": [
                {
                    "symbol": "AAPL",
                    "company_name": null,
                    "report_date": "2025-07-31",
                    "fiscal_date_ending": "2025-06-30",
                    "estimate": 1.41,
                    "currency": "USD",
                    "event_description": null,
                    "fetched_at": "2025-07-18T07:14:00.411389"
                }
            ],
            "total_count": 156
        }
    """
    try:
        # 쿼리 파라미터 객체 생성 및 검증
        params = EarningsCalendarQueryParams(
            start_date=start_date,
            end_date=end_date,
            symbol=symbol,
            limit=limit,
            offset=offset
        )
        
        # 서비스 클래스를 통해 비즈니스 로직 처리
        service = EarningsCalendarService(db)
        earnings_list, total_count = service.get_earnings_calendar(params)
        
        # SQLAlchemy 객체를 Pydantic 응답 모델로 변환
        items = [
            EarningsCalendarResponse.model_validate(earnings) 
            for earnings in earnings_list
        ]
        
        # 최종 응답 구성
        return EarningsCalendarListResponse(
            items=items,
            total_count=total_count
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"실적 캘린더 조회 중 오류가 발생했습니다: {str(e)}"
        )

@router.get(
    "/today",
    response_model=List[EarningsCalendarResponse],
    summary="오늘의 실적 발표 일정",
    description="오늘 예정된 실적 발표 일정을 조회합니다. 대시보드의 '오늘의 주요 일정'에 사용됩니다."
)
async def get_today_earnings(db: Session = Depends(get_db)):
    """
    오늘의 실적 발표 일정을 조회합니다.
    
    Returns:
        List[EarningsCalendarResponse]: 오늘의 실적 발표 일정 목록
        
    Example:
        GET /api/v1/earnings-calendar/today
        
        Response:
        [
            {
                "symbol": "AAPL",
                "report_date": "2025-07-27",  // 오늘 날짜
                "estimate": 1.41,
                "currency": "USD"
            }
        ]
    """
    try:
        service = EarningsCalendarService(db)
        earnings_list = service.get_today_earnings()
        
        return [
            EarningsCalendarResponse.model_validate(earnings) 
            for earnings in earnings_list
        ]
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"오늘의 실적 발표 일정 조회 중 오류가 발생했습니다: {str(e)}"
        )

@router.get(
    "/upcoming",
    response_model=List[EarningsCalendarResponse],
    summary="다가오는 실적 발표 일정",
    description="향후 N일 내의 실적 발표 일정을 조회합니다. 대시보드의 '이번 주 주요 일정'에 사용됩니다."
)
async def get_upcoming_earnings(
    days: int = Query(7, ge=1, le=30, description="조회할 일수 (기본 7일)", example=7),
    db: Session = Depends(get_db)
):
    """
    다가오는 실적 발표 일정을 조회합니다.
    
    Args:
        days: 오늘부터 며칠 후까지 조회할지 설정 (1~30일)
        
    Returns:
        List[EarningsCalendarResponse]: 향후 실적 발표 일정 목록
        
    Example:
        GET /api/v1/earnings-calendar/upcoming?days=14
        
        Response: 향후 14일간의 실적 발표 일정
    """
    try:
        service = EarningsCalendarService(db)
        earnings_list = service.get_upcoming_earnings(days)
        
        return [
            EarningsCalendarResponse.model_validate(earnings) 
            for earnings in earnings_list
        ]
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"다가오는 실적 발표 일정 조회 중 오류가 발생했습니다: {str(e)}"
        )

@router.get(
    "/symbol/{symbol}",
    response_model=List[EarningsCalendarResponse],
    summary="특정 심볼의 실적 발표 일정",
    description="특정 주식 심볼의 실적 발표 일정을 조회합니다. 종목 상세 페이지에서 사용됩니다."
)
async def get_earnings_by_symbol(
    symbol: str,
    limit: int = Query(10, ge=1, le=100, description="최대 조회 개수", example=10),
    db: Session = Depends(get_db)
):
    """
    특정 심볼의 실적 발표 일정을 조회합니다.
    
    Args:
        symbol: 주식 심볼 (대소문자 구분 안함)
        limit: 최대 조회 개수
        
    Returns:
        List[EarningsCalendarResponse]: 해당 심볼의 실적 발표 일정
        
    Example:
        GET /api/v1/earnings-calendar/symbol/AAPL?limit=5
        
        Response: AAPL의 향후 실적 발표 일정 5개
    """
    try:
        service = EarningsCalendarService(db)
        earnings_list = service.get_earnings_by_symbol(symbol.upper(), limit)
        
        if not earnings_list:
            raise HTTPException(
                status_code=404,
                detail=f"심볼 '{symbol.upper()}'에 대한 실적 발표 일정을 찾을 수 없습니다."
            )
        
        return [
            EarningsCalendarResponse.model_validate(earnings) 
            for earnings in earnings_list
        ]
        
    except HTTPException:
        # HTTPException은 그대로 재발생
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"심볼별 실적 발표 일정 조회 중 오류가 발생했습니다: {str(e)}"
        )

@router.get(
    "/range",
    response_model=List[EarningsCalendarResponse],
    summary="날짜 범위별 실적 발표 일정",
    description="특정 날짜 범위의 실적 발표 일정을 조회합니다. 달력 컴포넌트에서 특정 월의 데이터를 가져올 때 사용됩니다."
)
async def get_earnings_by_date_range(
    start_date: date = Query(..., description="조회 시작일 (필수)", example="2025-08-01"),
    end_date: date = Query(..., description="조회 종료일 (필수)", example="2025-08-31"),
    db: Session = Depends(get_db)
):
    """
    특정 날짜 범위의 실적 발표 일정을 조회합니다.
    
    주로 프론트엔드 달력 컴포넌트에서 특정 월의 데이터를 로드할 때 사용됩니다.
    페이징 없이 해당 기간의 모든 데이터를 반환합니다.
    
    Args:
        start_date: 조회 시작일 (필수)
        end_date: 조회 종료일 (필수)
        
    Returns:
        List[EarningsCalendarResponse]: 해당 기간의 모든 실적 발표 일정
        
    Example:
        GET /api/v1/earnings-calendar/range?start_date=2025-08-01&end_date=2025-08-31
        
        Response: 2025년 8월의 모든 실적 발표 일정
    """
    try:
        # 날짜 범위 검증
        if start_date > end_date:
            raise HTTPException(
                status_code=400,
                detail="시작일이 종료일보다 늦을 수 없습니다."
            )
        
        service = EarningsCalendarService(db)
        earnings_list = service.get_earnings_by_date_range(start_date, end_date)
        
        return [
            EarningsCalendarResponse.model_validate(earnings) 
            for earnings in earnings_list
        ]
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"날짜 범위별 실적 발표 일정 조회 중 오류가 발생했습니다: {str(e)}"
        )

@router.get(
    "/statistics",
    summary="실적 캘린더 통계 정보",
    description="실적 캘린더 데이터의 통계 정보를 제공합니다. 대시보드 요약 정보에 사용됩니다."
)
async def get_earnings_statistics(db: Session = Depends(get_db)):
    """
    실적 캘린더 통계 정보를 조회합니다.
    
    Returns:
        dict: 통계 정보 (총 개수, 오늘 일정, 이번 주 일정 등)
        
    Example:
        GET /api/v1/earnings-calendar/statistics
        
        Response:
        {
            "total_earnings": 1250,
            "today_earnings": 5,
            "this_week_earnings": 28,
            "companies_with_estimates": 890,
            "latest_update": "2025-07-18T07:14:00"
        }
    """
    try:
        service = EarningsCalendarService(db)
        
        # 기본 통계 정보 수집
        total_count = service.get_total_earnings_count()
        today_count = len(service.get_today_earnings())
        upcoming_count = len(service.get_upcoming_earnings(7))  # 7일간
        
        # 추가 통계 정보
        companies_with_estimates = service.get_companies_with_estimates_count()
        latest_update = service.get_latest_update_time()
        
        return {
            "total_earnings": total_count,
            "today_earnings": today_count,
            "this_week_earnings": upcoming_count,
            "companies_with_estimates": companies_with_estimates,
            "latest_update": latest_update.isoformat() if latest_update else None,
            "currency_breakdown": {
                "USD": service.get_count_by_currency("USD"),
                "others": service.get_count_by_currency_not("USD")
            }
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"실적 캘린더 통계 조회 중 오류가 발생했습니다: {str(e)}"
        )

@router.get("/test", tags=["API TEST"])
async def test():
    return {"message": "API v1/earnings-calendar/test"}