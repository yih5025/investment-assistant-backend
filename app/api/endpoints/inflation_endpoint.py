from typing import List
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.dependencies import get_db
from app.services.inflation_service import InflationService
from app.schemas.inflation_schema import (
    InflationResponse,
    InflationListResponse,
    InflationChartResponse,
    InflationStatsResponse
)

# Inflation 라우터 생성
router = APIRouter(
    tags=["Inflation"],
    responses={
        404: {"description": "요청한 인플레이션 데이터를 찾을 수 없습니다"},
        422: {"description": "잘못된 요청 파라미터"},
        500: {"description": "서버 내부 오류"}
    }
)

@router.get("/", response_model=InflationListResponse, summary="인플레이션 전체 데이터 조회")
async def get_inflation_list(
    order_by: str = Query("desc", pattern="^(desc|asc)$", description="정렬 순서"),
    db: Session = Depends(get_db)
):
    """
    **인플레이션 전체 데이터를 조회합니다.**
    
    미국의 연간 인플레이션율 데이터를 제공합니다.
    
    ### 파라미터:
    - **order_by**: 정렬 순서 ('desc': 최신순, 'asc': 과거순)
    
    ### 응답:
    - 전체 인플레이션 데이터 목록과 총 개수
    """
    try:
        service = InflationService(db)
        inflation_data = service.get_all_inflation_data(order_by=order_by)
        
        if not inflation_data:
            raise HTTPException(
                status_code=404,
                detail="인플레이션 데이터를 찾을 수 없습니다"
            )
        
        return InflationListResponse(
            total_count=len(inflation_data),
            items=inflation_data
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"인플레이션 데이터 조회 중 오류가 발생했습니다: {str(e)}"
        )

@router.get("/chart", response_model=InflationChartResponse, summary="인플레이션 차트 데이터 조회")
async def get_inflation_chart_data(db: Session = Depends(get_db)):
    """
    **인플레이션 차트 시각화용 데이터를 조회합니다.**
    
    프론트엔드 그래프 최적화 형태로 데이터를 제공합니다.
    
    ### 포함 데이터:
    - 연도별 인플레이션율
    - 통계 정보 (최신값, 평균, 최고/최저값)
    - 차트 렌더링 최적화 구조
    
    ### 응답:
    - 차트용 최적화된 인플레이션 데이터
    """
    try:
        service = InflationService(db)
        result = service.get_chart_data()
        
        if not result:
            raise HTTPException(
                status_code=404,
                detail="차트용 인플레이션 데이터를 찾을 수 없습니다"
            )
        
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"인플레이션 차트 데이터 조회 중 오류가 발생했습니다: {str(e)}"
        )

@router.get("/recent", response_model=InflationListResponse, summary="최근 인플레이션 데이터 조회")
async def get_recent_inflation(
    years: int = Query(10, ge=1, le=20, description="조회할 연수 (1-20년)"),
    db: Session = Depends(get_db)
):
    """
    **최근 N년의 인플레이션 데이터를 조회합니다.**
    
    ### 파라미터:
    - **years**: 조회할 연수 (1~20년, 기본값: 10년)
    
    ### 응답:
    - 최근 N년의 인플레이션 데이터 목록
    """
    try:
        service = InflationService(db)
        inflation_data = service.get_recent_years(years)
        
        if not inflation_data:
            raise HTTPException(
                status_code=404,
                detail=f"최근 {years}년의 인플레이션 데이터를 찾을 수 없습니다"
            )
        
        return InflationListResponse(
            total_count=len(inflation_data),
            items=inflation_data
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"최근 인플레이션 데이터 조회 중 오류가 발생했습니다: {str(e)}"
        )

@router.get("/year/{year}", response_model=InflationResponse)
async def get_inflation_by_year(
    year: int,
    db: Session = Depends(get_db)
):
    """
    특정 연도 인플레이션 데이터 조회
    
    Args:
        year: 조회할 연도 (1900~2100)
        
    Returns:
        해당 연도 인플레이션 데이터
    """
    try:
        service = InflationService(db)
        inflation_data = service.get_year_data(year)
        
        if not inflation_data:
            raise HTTPException(
                status_code=404, 
                detail=f"{year}년 인플레이션 데이터를 찾을 수 없습니다"
            )
        
        return inflation_data
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"연도별 데이터 조회 실패: {str(e)}")

@router.get("/statistics", response_model=InflationStatsResponse, summary="인플레이션 통계 정보 조회")
async def get_inflation_statistics(db: Session = Depends(get_db)):
    """
    **인플레이션 통계 정보를 조회합니다.**
    
    ### 포함 정보:
    - 최신 인플레이션율
    - 평균, 최고/최저값
    - 전체 데이터 연수
    - 트렌드 분석
    
    ### 응답:
    - 종합적인 인플레이션 통계 정보
    """
    try:
        service = InflationService(db)
        return service.get_statistics()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"통계 정보 조회 실패: {str(e)}")

@router.get("/range", response_model=InflationListResponse)
async def get_inflation_range(
    start_year: int = Query(..., ge=1900, description="시작 연도"),
    end_year: int = Query(..., le=2100, description="종료 연도"),
    db: Session = Depends(get_db)
):
    """
    연도 범위별 인플레이션 데이터 조회
    
    Args:
        start_year: 시작 연도
        end_year: 종료 연도
        
    Returns:
        지정 범위의 인플레이션 데이터
    """
    if start_year > end_year:
        raise HTTPException(
            status_code=400, 
            detail="시작 연도는 종료 연도보다 작거나 같아야 합니다"
        )
    
    try:
        service = InflationService(db)
        inflation_data = service.get_range_data(start_year, end_year)
        
        return InflationListResponse(
            total_count=len(inflation_data),
            items=inflation_data
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"범위별 데이터 조회 실패: {str(e)}")