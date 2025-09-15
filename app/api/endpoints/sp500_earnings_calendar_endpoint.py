# app/api/endpoints/sp500_earnings_calendar_endpoint.py
from fastapi import APIRouter, Depends, HTTPException, Query, Path
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import date, datetime

from app.schemas.sp500_earnings_calendar_schema import (
    SP500EarningsCalendarResponse,
    SP500EarningsCalendarListResponse,
    SP500EarningsCalendarWeeklyResponse,
    SP500EarningsCalendarBySymbolResponse,
    SP500EarningsCalendarQueryParams,
    SP500EarningsCalendarStats
)
from app.services.sp500_earnings_calendar_service import SP500EarningsCalendarService
from app.dependencies import get_db

# S&P 500 실적 캘린더 라우터 생성
router = APIRouter(
    tags=["SP500 Earnings Calendar"],
    responses={
        404: {"description": "요청한 실적 데이터를 찾을 수 없습니다"},
        422: {"description": "잘못된 요청 파라미터"},
        500: {"description": "서버 내부 오류"}
    }
)

@router.get(
    "/",
    response_model=SP500EarningsCalendarListResponse,
    summary="S&P 500 실적 발표 캘린더 전체 조회",
    description="프론트엔드 캘린더 컴포넌트에 표시할 모든 S&P 500 실적 발표 일정을 조회합니다. 날짜 제한 없이 전체 데이터를 제공하되, 필터링 옵션을 제공합니다."
)
async def get_sp500_earnings_calendar(
    start_date: Optional[date] = Query(None, description="조회 시작일 (옵션)", example="2025-08-01"),
    end_date: Optional[date] = Query(None, description="조회 종료일 (옵션)", example="2025-12-31"),
    symbol: Optional[str] = Query(None, description="주식 심볼 필터 (옵션)", example="AAPL"),
    sector: Optional[str] = Query(None, description="GICS 섹터 필터 (옵션)", example="Information Technology"),
    limit: int = Query(100, ge=1, le=10000, description="최대 조회 개수", example=100),
    db: Session = Depends(get_db)
):
    """
    **S&P 500 실적 발표 캘린더 전체 조회**
    
    프론트엔드에서 캘린더 컴포넌트에 표시할 모든 S&P 500 실적 발표 일정을 조회합니다.
    
    **주요 기능:**
    - 📅 전체 실적 일정 조회 (날짜 제한 없음)
    - 🔍 다양한 필터링 옵션 (날짜, 심볼, 섹터)
    - 📊 기본 100개 조회 (limit으로 조절 가능)
    - 🏢 회사 정보 및 GICS 분류 포함
    
    **사용 예시:**
    ```
    GET /api/v1/sp500-earnings-calendar/
    GET /api/v1/sp500-earnings-calendar/?sector=Information Technology
    GET /api/v1/sp500-earnings-calendar/?start_date=2025-08-01&end_date=2025-08-31&limit=50
    ```
    
    **응답 데이터:**
    - 실적 일정 목록과 총 개수
    - 각 이벤트의 상세 정보
    """
    try:
        # 쿼리 파라미터 객체 생성 및 검증 (has_estimate=None, offset=0 기본값 사용)
        params = SP500EarningsCalendarQueryParams(
            start_date=start_date,
            end_date=end_date,
            symbol=symbol,
            sector=sector,
            has_estimate=None,  # 기본값: 필터링 안함
            limit=limit,
            offset=0  # 기본값: 첫 페이지부터
        )
        
        # 서비스 클래스를 통해 비즈니스 로직 처리
        service = SP500EarningsCalendarService(db)
        earnings_list, total_count = service.get_all_calendar_events(params)
        
        # SQLAlchemy 객체를 Pydantic 응답 모델로 변환
        items = [
            SP500EarningsCalendarResponse.model_validate(earnings) 
            for earnings in earnings_list
        ]
        
        # 최종 응답 구성
        return SP500EarningsCalendarListResponse(
            items=items,
            total_count=total_count,
            message=f"총 {total_count}개의 S&P 500 실적 일정을 조회했습니다."
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"S&P 500 실적 캘린더 조회 중 오류가 발생했습니다: {str(e)}"
        )

@router.get(
    "/weekly",
    response_model=SP500EarningsCalendarWeeklyResponse,
    summary="이번 주 S&P 500 실적 발표 일정",
    description="캘린더 하단에 표시할 이번 주(월~일) S&P 500 실적 발표 일정을 조회합니다."
)
async def get_weekly_sp500_earnings(db: Session = Depends(get_db)):
    """
    **이번 주 S&P 500 실적 발표 일정 조회**
    
    프론트엔드 캘린더 하단의 "이번 주 주요 실적" 섹션에 표시할 데이터를 제공합니다.
    
    **주요 기능:**
    - 📅 이번 주(월~일) 실적 일정만 조회
    - 🗓️ 주간 범위 정보 포함 (week_start, week_end)
    - 📊 이번 주 실적 개수 제공
    - ⏰ 날짜순 정렬
    
    **사용 예시:**
    ```
    GET /api/v1/sp500-earnings-calendar/weekly
    ```
    
    **응답 데이터:**
    - 주간 범위 정보 (시작일, 종료일)
    - 이번 주 실적 일정 목록
    - 이번 주 총 실적 개수
    """
    try:
        # 서비스 클래스를 통해 이번 주 일정 조회
        service = SP500EarningsCalendarService(db)
        weekly_events, week_start, week_end = service.get_weekly_events()
        
        # SQLAlchemy 객체를 Pydantic 응답 모델로 변환
        events = [
            SP500EarningsCalendarResponse.model_validate(event) 
            for event in weekly_events
        ]
        
        # 최종 응답 구성
        return SP500EarningsCalendarWeeklyResponse(
            week_start=week_start,
            week_end=week_end,
            events=events,
            total_count=len(events),
            message=f"이번 주({week_start} ~ {week_end}) 총 {len(events)}개의 실적 일정이 있습니다."
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"이번 주 실적 일정 조회 중 오류가 발생했습니다: {str(e)}"
        )

@router.get(
    "/symbol/{symbol}",
    response_model=SP500EarningsCalendarBySymbolResponse,
    summary="특정 심볼의 S&P 500 실적 발표 일정",
    description="특정 주식 심볼의 모든 실적 발표 일정을 조회합니다. (옵션 기능)"
)
async def get_sp500_earnings_by_symbol(
    symbol: str = Path(..., description="주식 심볼", example="AAPL", regex=r"^[A-Z]{1,5}$"),
    limit: int = Query(10, ge=1, le=50, description="최대 조회 개수", example=10),
    db: Session = Depends(get_db)
):
    """
    **특정 심볼의 S&P 500 실적 발표 일정 조회**
    
    특정 주식 심볼의 과거 및 향후 실적 발표 일정을 조회합니다.
    
    **주요 기능:**
    - 🔍 특정 심볼의 모든 실적 일정
    - 📅 최신순 정렬 (향후 → 과거)
    - 🏢 회사 정보 포함
    - 📊 해당 심볼의 총 실적 개수
    
    **사용 예시:**
    ```
    GET /api/v1/sp500-earnings-calendar/symbol/AAPL
    GET /api/v1/sp500-earnings-calendar/symbol/TSLA?limit=5
    ```
    
    **응답 데이터:**
    - 심볼 및 회사명
    - 해당 심볼의 실적 일정 목록
    - 총 실적 개수
    """
    try:
        symbol = symbol.upper()
        
        # 서비스 클래스를 통해 특정 심볼 일정 조회
        service = SP500EarningsCalendarService(db)
        earnings_list = service.get_earnings_by_symbol(symbol, limit)
        
        if not earnings_list:
            raise HTTPException(
                status_code=404,
                detail=f"심볼 '{symbol}'에 대한 S&P 500 실적 일정을 찾을 수 없습니다."
            )
        
        # SQLAlchemy 객체를 Pydantic 응답 모델로 변환
        earnings = [
            SP500EarningsCalendarResponse.model_validate(earning) 
            for earning in earnings_list
        ]
        
        # 회사명 추출 (첫 번째 레코드에서)
        company_name = earnings_list[0].company_name if earnings_list else None
        
        # 총 개수 조회 (제한 없이)
        all_earnings = service.get_earnings_by_symbol(symbol, limit=None)
        total_count = len(all_earnings)
        
        # 최종 응답 구성
        return SP500EarningsCalendarBySymbolResponse(
            symbol=symbol,
            company_name=company_name,
            earnings=earnings,
            total_count=total_count,
            message=f"{symbol}의 실적 일정 {len(earnings)}개를 조회했습니다. (전체 {total_count}개)"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"심볼별 실적 일정 조회 중 오류가 발생했습니다: {str(e)}"
        )

@router.get(
    "/statistics",
    response_model=SP500EarningsCalendarStats,
    summary="S&P 500 실적 캘린더 통계",
    description="S&P 500 실적 캘린더의 전체 통계 정보를 제공합니다."
)
async def get_sp500_earnings_statistics(db: Session = Depends(get_db)):
    """
    **S&P 500 실적 캘린더 통계 정보**
    
    대시보드에 표시할 실적 캘린더 관련 통계 정보를 제공합니다.
    
    **통계 항목:**
    - 📊 총 회사 수, 총 이벤트 수
    - 💰 예상 수익이 있는 이벤트 수
    - 📰 뉴스가 있는 이벤트 수  
    - ⏰ 향후 예정된 이벤트 수
    - 🏭 포함된 섹터 목록
    - 🕐 마지막 업데이트 시간
    
    **사용 예시:**
    ```
    GET /api/v1/sp500-earnings-calendar/statistics
    ```
    """
    try:
        # 서비스 클래스를 통해 통계 정보 조회
        service = SP500EarningsCalendarService(db)
        stats = service.get_calendar_statistics()
        
        # Pydantic 응답 모델로 변환
        return SP500EarningsCalendarStats(**stats)
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"실적 캘린더 통계 조회 중 오류가 발생했습니다: {str(e)}"
        )

@router.get(
    "/upcoming",
    response_model=List[SP500EarningsCalendarResponse],
    summary="향후 S&P 500 실적 발표 일정",
    description="향후 N일 내의 S&P 500 실적 발표 일정을 조회합니다."
)
async def get_upcoming_sp500_earnings(
    days: int = Query(30, ge=1, le=365, description="조회할 일수", example=30),
    db: Session = Depends(get_db)
):
    """
    **향후 S&P 500 실적 발표 일정**
    
    오늘부터 향후 N일 내의 실적 발표 일정을 조회합니다.
    
    **사용 예시:**
    ```
    GET /api/v1/sp500-earnings-calendar/upcoming
    GET /api/v1/sp500-earnings-calendar/upcoming?days=60
    ```
    """
    try:
        # 서비스 클래스를 통해 향후 일정 조회
        service = SP500EarningsCalendarService(db)
        upcoming_events = service.get_upcoming_events(days)
        
        # SQLAlchemy 객체를 Pydantic 응답 모델로 변환
        return [
            SP500EarningsCalendarResponse.model_validate(event) 
            for event in upcoming_events
        ]
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"향후 실적 일정 조회 중 오류가 발생했습니다: {str(e)}"
        )

@router.get(
    "/search",
    response_model=List[SP500EarningsCalendarResponse],
    summary="S&P 500 실적 이벤트 검색",
    description="키워드로 S&P 500 실적 이벤트를 검색합니다."
)
async def search_sp500_earnings(
    q: str = Query(..., description="검색어 (심볼, 회사명, 이벤트 제목)", example="Apple"),
    limit: int = Query(20, ge=1, le=100, description="최대 조회 개수", example=20),
    db: Session = Depends(get_db)
):
    """
    **S&P 500 실적 이벤트 검색**
    
    심볼, 회사명, 이벤트 제목을 대상으로 검색합니다.
    
    **사용 예시:**
    ```
    GET /api/v1/sp500-earnings-calendar/search?q=Apple
    GET /api/v1/sp500-earnings-calendar/search?q=AAPL&limit=5
    ```
    """
    try:
        if len(q.strip()) < 2:
            raise HTTPException(
                status_code=422,
                detail="검색어는 최소 2글자 이상이어야 합니다."
            )
        
        # 서비스 클래스를 통해 검색
        service = SP500EarningsCalendarService(db)
        search_results = service.search_events(q.strip(), limit)
        
        # SQLAlchemy 객체를 Pydantic 응답 모델로 변환
        return [
            SP500EarningsCalendarResponse.model_validate(result) 
            for result in search_results
        ]
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"실적 이벤트 검색 중 오류가 발생했습니다: {str(e)}"
        )