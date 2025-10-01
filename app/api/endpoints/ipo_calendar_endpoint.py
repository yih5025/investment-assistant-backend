# app/api/endpoints/ipo_calendar_endpoint.py
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import Optional
from datetime import date, datetime
import logging

from app.schemas.ipo_calendar_schema import (
    IPOCalendarResponse,
    IPOCalendarListResponse,
    IPOCalendarMonthlyResponse,
    IPOCalendarStatistics
)
from app.services.ipo_calendar_service import IPOCalendarService
from app.dependencies import get_db

logger = logging.getLogger(__name__)

# IPO 캘린더 라우터 생성
router = APIRouter(
    tags=["IPO Calendar"],
    responses={
        404: {"description": "요청한 IPO 데이터를 찾을 수 없습니다"},
        500: {"description": "서버 내부 오류"}
    }
)

@router.get(
    "/",
    response_model=IPOCalendarListResponse,
    summary="IPO 캘린더 전체 조회",
    description="프론트엔드 캘린더에 표시할 IPO 일정을 조회합니다. 날짜 범위와 거래소 필터링이 가능합니다."
)
async def get_ipo_calendar(
    start_date: Optional[date] = Query(None, description="조회 시작일", example="2025-10-01"),
    end_date: Optional[date] = Query(None, description="조회 종료일", example="2025-12-31"),
    exchange: Optional[str] = Query(None, description="거래소 필터 (NYSE, NASDAQ 등)", example="NYSE"),
    limit: int = Query(100, ge=1, le=1000, description="최대 조회 개수"),
    db: Session = Depends(get_db)
):
    """
    **IPO 캘린더 전체 조회**
    
    프론트엔드에서 캘린더 컴포넌트에 표시할 모든 IPO 일정을 조회합니다.
    
    **주요 기능:**
    - 📅 날짜 범위 필터링 (시작일/종료일)
    - 🏢 거래소별 필터링 (NYSE, NASDAQ 등)
    - 📊 최신순 정렬
    - 🔢 페이징 지원
    
    **사용 예시:**
    GET /api/v1/ipo-calendar/
    GET /api/v1/ipo-calendar/?start_date=2025-10-01&end_date=2025-12-31
    GET /api/v1/ipo-calendar/?exchange=NYSE&limit=50
    **응답 데이터:**
    - IPO 일정 목록 (심볼, 회사명, 날짜, 공모가 등)
    - 전체 항목 수
    - 조회 날짜 범위
    """
    try:
        service = IPOCalendarService(db)
        items, total_count = service.get_all_ipos(
            start_date=start_date,
            end_date=end_date,
            exchange=exchange,
            limit=limit
        )
        
        return IPOCalendarListResponse(
            items=[IPOCalendarResponse.model_validate(item) for item in items],
            total_count=total_count,
            start_date=start_date,
            end_date=end_date
        )
        
    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail=f"잘못된 요청: {str(e)}"
        )
    except Exception as e:
        logger.error(f"IPO 캘린더 조회 실패: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"IPO 캘린더 조회 중 오류가 발생했습니다"
        )


@router.get(
    "/monthly",
    response_model=IPOCalendarMonthlyResponse,
    summary="이번 달 IPO 일정 조회",
    description="현재 월의 IPO 일정만 조회합니다. 특정 연월을 지정할 수도 있습니다."
)
async def get_monthly_ipos(
    year: Optional[int] = Query(None, description="연도 (미지정 시 현재)", example=2025),
    month: Optional[int] = Query(None, ge=1, le=12, description="월 (미지정 시 현재)", example=10),
    db: Session = Depends(get_db)
):
    """
    **이번 달 IPO 일정 조회**
    
    대시보드나 '이번 달 주요 일정' 섹션에서 사용됩니다.
    
    **주요 기능:**
    - 📅 현재 월 IPO 일정 자동 조회
    - 🗓️ 특정 연월 지정 가능
    - 📊 날짜순 정렬
    
    **사용 예시:**
    GET /api/v1/ipo-calendar/monthly
    GET /api/v1/ipo-calendar/monthly?year=2025&month=11
    **응답 데이터:**
    - 조회 월 정보
    - 해당 월 IPO 목록
    - 개수 통계
    """
    try:
        service = IPOCalendarService(db)
        items = service.get_monthly_ipos(year=year, month=month)
        
        # 조회 월 결정
        if year and month:
            month_str = f"{year}-{month:02d}"
        else:
            today = date.today()
            month_str = f"{today.year}-{today.month:02d}"
        
        return IPOCalendarMonthlyResponse(
            month=month_str,
            items=[IPOCalendarResponse.model_validate(item) for item in items],
            total_count=len(items)
        )
        
    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail=f"잘못된 요청: {str(e)}"
        )
    except Exception as e:
        logger.error(f"월별 IPO 조회 실패: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"월별 IPO 조회 중 오류가 발생했습니다"
        )


@router.get(
    "/statistics",
    response_model=IPOCalendarStatistics,
    summary="IPO 통계 정보",
    description="IPO 캘린더의 다양한 통계 정보를 제공합니다."
)
async def get_ipo_statistics(db: Session = Depends(get_db)):
    """
    **IPO 통계 정보**
    
    대시보드에서 IPO 관련 통계를 표시하는 데 사용됩니다.
    
    **제공 정보:**
    - 📊 전체 IPO 개수
    - 📅 이번 달 / 다음 달 IPO 개수
    - 🏢 거래소별 분포 (NYSE, NASDAQ 등)
    - 💰 평균 공모가 범위
    - ⏰ 향후 7일 내 IPO 개수
    
    **사용 예시:**
    GET /api/v1/ipo-calendar/statistics
    **응답 예시:**
    ```json
    {
        "total_ipos": 45,
        "this_month": 12,
        "next_month": 8,
        "by_exchange": {
            "NYSE": 20,
            "NASDAQ": 25
        },
        "avg_price_range": {
            "low": 18.5,
            "high": 24.3
        },
        "upcoming_7days": 3
    }
    """
    try:
        service = IPOCalendarService(db)
        stats = service.get_statistics()
        
        return IPOCalendarStatistics(**stats)
    
    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail=f"잘못된 요청: {str(e)}"
        )
    except Exception as e:
        logger.error(f"IPO 통계 조회 실패: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"IPO 통계 조회 중 오류가 발생했습니다"
        )