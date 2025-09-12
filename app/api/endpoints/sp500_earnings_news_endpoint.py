# app/api/endpoints/sp500_earnings_news_endpoint.py
from fastapi import APIRouter, Depends, HTTPException, Query, Path
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime

from app.schemas.sp500_earnings_news_schema import (
    SP500EarningsNewsResponse,
    SP500EarningsNewsListResponse,
    SP500EarningsNewsWithCalendarResponse,
    SP500EarningsNewsQueryParams,
)
from app.services.sp500_earnings_news_service import SP500EarningsNewsService
from app.dependencies import get_db

# S&P 500 실적 뉴스 라우터 생성
router = APIRouter(
    prefix="/sp500-earnings-news",
    tags=["SP500 Earnings News"],
    responses={
        404: {"description": "요청한 뉴스 데이터를 찾을 수 없습니다"},
        422: {"description": "잘못된 요청 파라미터"},
        500: {"description": "서버 내부 오류"}
    }
)

@router.get(
    "/calendar/{calendar_id}",
    response_model=SP500EarningsNewsWithCalendarResponse,
    summary="특정 실적 이벤트의 관련 뉴스 조회",
    description="캘린더에서 특정 실적 일정을 클릭했을 때 관련 뉴스 목록을 조회합니다. 예측 뉴스와 반응 뉴스로 분류하여 제공합니다."
)
async def get_earnings_news_by_calendar_id(
    calendar_id: int = Path(..., description="실적 캘린더 ID", example=8, ge=1),
    db: Session = Depends(get_db)
):
    """
    **특정 실적 이벤트의 관련 뉴스 조회 (메인 기능)**
    
    프론트엔드에서 캘린더의 특정 실적 일정을 클릭했을 때 호출되는 핵심 API입니다.
    
    **주요 기능:**
    - 📅 실적 캘린더 정보와 함께 뉴스 제공
    - 📰 예측 뉴스 (forecast) / 반응 뉴스 (reaction) 분류
    - 📊 뉴스 개수 통계 정보 포함
    - ⏰ 최신순 정렬
    
    **사용자 시나리오:**
    1. 사용자가 캘린더에서 "AAPL 실적 발표" 클릭
    2. 모달/페이지에서 AAPL 실적 관련 뉴스 목록 표시
    3. 예측 뉴스 / 반응 뉴스 탭으로 구분 표시
    
    **사용 예시:**
    ```
    GET /api/v1/sp500-earnings-news/calendar/8
    GET /api/v1/sp500-earnings-news/calendar/49
    ```
    
    **응답 데이터:**
    - 관련 캘린더 이벤트 정보 (심볼, 회사명, 실적 날짜 등)
    - 예측 뉴스 목록 (실적 발표 전)
    - 반응 뉴스 목록 (실적 발표 후)
    - 뉴스 통계 (총 개수, 예측/반응 개수)
    """
    try:
        # 서비스 클래스를 통해 캘린더 정보와 뉴스 조회
        service = SP500EarningsNewsService(db)
        news_data = service.get_news_with_calendar_info(calendar_id)
        
        if not news_data:
            raise HTTPException(
                status_code=404,
                detail=f"캘린더 ID {calendar_id}에 해당하는 실적 이벤트를 찾을 수 없습니다."
            )
        
        # SQLAlchemy 객체를 Pydantic 응답 모델로 변환
        forecast_news = [
            SP500EarningsNewsResponse.model_validate(news) 
            for news in news_data["forecast_news"]
        ]
        
        reaction_news = [
            SP500EarningsNewsResponse.model_validate(news) 
            for news in news_data["reaction_news"]
        ]
        
        # 최종 응답 구성
        return SP500EarningsNewsWithCalendarResponse(
            calendar_info=news_data["calendar_info"],
            forecast_news=forecast_news,
            reaction_news=reaction_news,
            total_news_count=news_data["total_news_count"],
            forecast_news_count=news_data["forecast_news_count"],
            reaction_news_count=news_data["reaction_news_count"],
            message=f"{news_data['calendar_info']['symbol']} 실적 관련 뉴스 {news_data['total_news_count']}개를 조회했습니다."
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"실적 관련 뉴스 조회 중 오류가 발생했습니다: {str(e)}"
        )

@router.get(
    "/calendar/{calendar_id}/forecast",
    response_model=List[SP500EarningsNewsResponse],
    summary="특정 실적 이벤트의 예측 뉴스만 조회",
    description="특정 실적 이벤트의 예측 뉴스(실적 발표 전 뉴스)만 조회합니다."
)
async def get_forecast_news_by_calendar_id(
    calendar_id: int = Path(..., description="실적 캘린더 ID", example=8, ge=1),
    limit: int = Query(20, ge=1, le=100, description="최대 조회 개수", example=20),
    db: Session = Depends(get_db)
):
    """
    **특정 실적 이벤트의 예측 뉴스만 조회**
    
    실적 발표 전에 나온 예측/전망 관련 뉴스만 필터링하여 조회합니다.
    
    **사용 예시:**
    ```
    GET /api/v1/sp500-earnings-news/calendar/8/forecast
    GET /api/v1/sp500-earnings-news/calendar/8/forecast?limit=10
    ```
    """
    try:
        # 서비스 클래스를 통해 예측 뉴스 조회
        service = SP500EarningsNewsService(db)
        forecast_news = service.get_forecast_news(calendar_id, limit)
        
        if not forecast_news:
            raise HTTPException(
                status_code=404,
                detail=f"캘린더 ID {calendar_id}에 해당하는 예측 뉴스를 찾을 수 없습니다."
            )
        
        # SQLAlchemy 객체를 Pydantic 응답 모델로 변환
        return [
            SP500EarningsNewsResponse.model_validate(news) 
            for news in forecast_news
        ]
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"예측 뉴스 조회 중 오류가 발생했습니다: {str(e)}"
        )

@router.get(
    "/calendar/{calendar_id}/reaction",
    response_model=List[SP500EarningsNewsResponse],
    summary="특정 실적 이벤트의 반응 뉴스만 조회",
    description="특정 실적 이벤트의 반응 뉴스(실적 발표 후 뉴스)만 조회합니다."
)
async def get_reaction_news_by_calendar_id(
    calendar_id: int = Path(..., description="실적 캘린더 ID", example=8, ge=1),
    limit: int = Query(20, ge=1, le=100, description="최대 조회 개수", example=20),
    db: Session = Depends(get_db)
):
    """
    **특정 실적 이벤트의 반응 뉴스만 조회**
    
    실적 발표 후에 나온 반응/분석 관련 뉴스만 필터링하여 조회합니다.
    
    **사용 예시:**
    ```
    GET /api/v1/sp500-earnings-news/calendar/8/reaction
    GET /api/v1/sp500-earnings-news/calendar/8/reaction?limit=10
    ```
    """
    try:
        # 서비스 클래스를 통해 반응 뉴스 조회
        service = SP500EarningsNewsService(db)
        reaction_news = service.get_reaction_news(calendar_id, limit)
        
        if not reaction_news:
            raise HTTPException(
                status_code=404,
                detail=f"캘린더 ID {calendar_id}에 해당하는 반응 뉴스를 찾을 수 없습니다."
            )
        
        # SQLAlchemy 객체를 Pydantic 응답 모델로 변환
        return [
            SP500EarningsNewsResponse.model_validate(news) 
            for news in reaction_news
        ]
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"반응 뉴스 조회 중 오류가 발생했습니다: {str(e)}"
        )

@router.get(
    "/calendar/{calendar_id}/all",
    response_model=SP500EarningsNewsListResponse,
    summary="특정 실적 이벤트의 모든 뉴스 조회 (필터링 옵션)",
    description="특정 실적 이벤트의 모든 뉴스를 조회하되, 다양한 필터링 옵션을 제공합니다."
)
async def get_all_news_by_calendar_id(
    calendar_id: int = Path(..., description="실적 캘린더 ID", example=8, ge=1),
    news_section: Optional[str] = Query(None, description="뉴스 섹션 (forecast/reaction)", example="forecast"),
    source: Optional[str] = Query(None, description="뉴스 소스 필터", example="Yahoo"),
    has_content: Optional[bool] = Query(None, description="본문 내용이 있는 뉴스만", example=True),
    limit: int = Query(50, ge=1, le=200, description="최대 조회 개수", example=50),
    offset: int = Query(0, ge=0, description="건너뛸 개수 (페이징)", example=0),
    db: Session = Depends(get_db)
):
    """
    **특정 실적 이벤트의 모든 뉴스 조회 (고급 필터링)**
    
    다양한 필터링 옵션과 페이징을 제공하는 고급 뉴스 조회 API입니다.
    
    **사용 예시:**
    ```
    GET /api/v1/sp500-earnings-news/calendar/8/all
    GET /api/v1/sp500-earnings-news/calendar/8/all?news_section=forecast&source=Yahoo
    GET /api/v1/sp500-earnings-news/calendar/8/all?has_content=true&limit=20
    ```
    """
    try:
        # 쿼리 파라미터 객체 생성
        params = SP500EarningsNewsQueryParams(
            news_section=news_section,
            source=source,
            has_content=has_content,
            limit=limit,
            offset=offset
        )
        
        # 서비스 클래스를 통해 뉴스 조회
        service = SP500EarningsNewsService(db)
        news_list, total_count = service.get_news_by_calendar_id(calendar_id, params)
        
        if not news_list:
            raise HTTPException(
                status_code=404,
                detail=f"캘린더 ID {calendar_id}에 해당하는 뉴스를 찾을 수 없습니다."
            )
        
        # SQLAlchemy 객체를 Pydantic 응답 모델로 변환
        items = [
            SP500EarningsNewsResponse.model_validate(news) 
            for news in news_list
        ]
        
        # 섹션별 개수 계산
        forecast_count = len([item for item in items if item.news_section == "forecast"])
        reaction_count = len([item for item in items if item.news_section == "reaction"])
        
        # 최종 응답 구성
        return SP500EarningsNewsListResponse(
            calendar_id=calendar_id,
            items=items,
            total_count=total_count,
            forecast_count=forecast_count,
            reaction_count=reaction_count,
            message=f"캘린더 ID {calendar_id}의 뉴스 {len(items)}개를 조회했습니다. (전체 {total_count}개)"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"뉴스 조회 중 오류가 발생했습니다: {str(e)}"
        )