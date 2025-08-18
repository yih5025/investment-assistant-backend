from typing import List
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import asc

from app.dependencies import get_db
from app.models.cpi_model import CPI
from app.services.cpi_service import CPIService
from app.schemas.cpi_schema import (
    CPIResponse,
    CPIListResponse,
    CPIChartResponse,
    CPIStatsResponse,
    CPIInflationAnalysis,
    CPIComparisonResponse,
    CPIMonthlyDetailResponse
)

# CPI 라우터 생성
router = APIRouter(
    tags=["CPI"],
    responses={
        404: {"description": "요청한 CPI 데이터를 찾을 수 없습니다"},
        422: {"description": "잘못된 요청 파라미터"},
        500: {"description": "서버 내부 오류"}
    }
)

@router.get("/", response_model=CPIListResponse, summary="CPI 전체 데이터 조회")
async def get_cpi_list(
    order_by: str = Query("desc", pattern="^(desc|asc)$", description="정렬 순서"),
    db: Session = Depends(get_db)
):
    """
    **소비자물가지수(CPI) 전체 데이터를 조회합니다.**
    
    미국 도시 소비자들의 상품/서비스 가격 변화를 측정하는 지수입니다.
    
    ### 주요 특징:
    - 월별 데이터 제공
    - 인플레이션 측정의 핵심 지표
    - 기준: 1982-1984년 = 100
    
    ### 파라미터:
    - **order_by**: 정렬 순서 ('desc': 최신순, 'asc': 과거순)
    
    ### 응답:
    - 전체 CPI 데이터 목록과 총 개수
    """
    try:
        service = CPIService(db)
        data = service.get_all_cpi_data(order_by=order_by)
        
        if not data:
            raise HTTPException(
                status_code=404, 
                detail="CPI 데이터를 찾을 수 없습니다"
            )
        
        return CPIListResponse(
            total_count=len(data),
            items=data
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"CPI 데이터 조회 중 오류가 발생했습니다: {str(e)}"
        )

@router.get("/chart", response_model=CPIChartResponse, summary="CPI 차트 데이터 조회")
async def get_cpi_chart_data(db: Session = Depends(get_db)):
    """
    **CPI 차트 시각화용 데이터를 조회합니다.**
    
    프론트엔드 그래프 최적화 형태로 데이터를 제공합니다.
    
    ### 포함 데이터:
    - 월별 CPI 값 및 인플레이션율
    - 전월 대비 변화율
    - 연간 인플레이션율 (전년 동월 대비)
    - 12개월 평균, 최고/최저값
    
    ### 응답:
    - 차트용 최적화된 CPI 데이터
    """
    try:
        service = CPIService(db)
        result = service.get_chart_data()
        
        if not result:
            raise HTTPException(
                status_code=404,
                detail="차트용 CPI 데이터를 찾을 수 없습니다"
            )
        
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"CPI 차트 데이터 조회 중 오류가 발생했습니다: {str(e)}"
        )

@router.get("/recent", response_model=CPIListResponse, summary="최근 CPI 데이터 조회")
async def get_recent_cpi(
    months: int = Query(12, ge=1, le=60, description="조회할 개월 수 (1-60개월)"),
    db: Session = Depends(get_db)
):
    """
    **최근 N개월의 CPI 데이터를 조회합니다.**
    
    ### 파라미터:
    - **months**: 조회할 개월 수 (1~60개월, 기본값: 12개월)
    
    ### 응답:
    - 최근 N개월의 CPI 데이터 목록
    """
    try:
        service = CPIService(db)
        data = service.get_recent_months(months)
        
        if not data:
            raise HTTPException(
                status_code=404,
                detail=f"최근 {months}개월의 CPI 데이터를 찾을 수 없습니다"
            )
        
        return CPIListResponse(
            total_count=len(data),
            items=data
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"최근 CPI 데이터 조회 중 오류가 발생했습니다: {str(e)}"
        )

@router.get("/statistics", response_model=CPIStatsResponse, summary="CPI 통계 정보 조회")
async def get_cpi_statistics(db: Session = Depends(get_db)):
    """
    **CPI 통계 정보를 조회합니다.**
    
    ### 포함 정보:
    - 현재 CPI 값
    - 전월 대비 변화율
    - 연간 인플레이션율
    - 트렌드 분석
    
    ### 응답:
    - 종합적인 CPI 통계 정보
    """
    try:
        service = CPIService(db)
        result = service.get_statistics()
        
        if not result:
            raise HTTPException(
                status_code=404,
                detail="CPI 통계 정보를 찾을 수 없습니다"
            )
        
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"CPI 통계 정보 조회 중 오류가 발생했습니다: {str(e)}"
        )

@router.get("/inflation-analysis", response_model=CPIInflationAnalysis)
async def get_cpi_inflation_analysis(db: Session = Depends(get_db)):
    """
    상세 인플레이션 분석
    
    상세한 인플레이션 트렌드 분석:
    - 최근 6개월/12개월 인플레이션율
    - 인플레이션 가속/감속 트렌드
    - 변동성 점수 (0-10)
    - 가격 안정성 상태 (안정/보통/고인플레이션)
    """
    try:
        service = CPIService(db)
        return service.get_inflation_analysis()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/current", response_model=CPIResponse)
async def get_current_cpi(db: Session = Depends(get_db)):
   """
   현재 CPI 조회
   
   Returns:
       가장 최신의 소비자물가지수 데이터
   """
   try:
       service = CPIService(db)
       recent_data = service.get_recent_months(1)
       
       if not recent_data:
           raise HTTPException(
               status_code=404, 
               detail="현재 CPI 데이터를 찾을 수 없습니다"
           )
       
       return recent_data[0]
   except HTTPException:
       raise
   except Exception as e:
       raise HTTPException(status_code=500, detail=str(e))

@router.get("/compare", response_model=CPIComparisonResponse)
async def compare_cpi_periods(
       start_date: str = Query(..., description="시작 날짜 (YYYY-MM-DD)", pattern=r"^\d{4}-\d{2}-\d{2}$"),
    end_date: str = Query(..., description="종료 날짜 (YYYY-MM-DD)", pattern=r"^\d{4}-\d{2}-\d{2}$"),
   db: Session = Depends(get_db)
):
   """
   기간별 CPI 비교 분석
   
   두 시점 간의 CPI 변화를 상세 분석:
   - 절대 변화값 및 변화율
   - 월평균 변화율
   - 누적 인플레이션율
   
   Args:
       start_date: 시작 날짜 (YYYY-MM-DD 형태)
       end_date: 종료 날짜 (YYYY-MM-DD 형태)
   
   Example:
       GET /api/v1/cpi/compare?start_date=2023-01-01&end_date=2024-01-01
   """
   # 날짜 유효성 검증
   from datetime import datetime
   try:
       start_dt = datetime.strptime(start_date, '%Y-%m-%d')
       end_dt = datetime.strptime(end_date, '%Y-%m-%d')
       
       if start_dt >= end_dt:
           raise HTTPException(
               status_code=400,
               detail="시작 날짜는 종료 날짜보다 이전이어야 합니다"
           )
   except ValueError:
       raise HTTPException(
           status_code=400,
           detail="날짜 형식이 올바르지 않습니다. YYYY-MM-DD 형태로 입력해주세요"
       )
   
   try:
       service = CPIService(db)
       return service.get_period_comparison(start_date, end_date)
   except ValueError as e:
       raise HTTPException(status_code=404, detail=str(e))
   except Exception as e:
       raise HTTPException(status_code=500, detail=str(e))

@router.get("/year/{year}", response_model=CPIListResponse)
async def get_cpi_by_year(
    year: int,
    db: Session = Depends(get_db)
):
   """
   특정 연도의 CPI 데이터 조회
   
   Args:
       year: 조회할 연도 (1900~2100)
       
   Returns:
       해당 연도의 모든 월별 CPI 데이터
   """
   try:
       service = CPIService(db)
       
       # 해당 연도의 데이터 조회 (1월 1일 ~ 12월 31일)
       from datetime import date
       start_date = date(year, 1, 1)
       end_date = date(year, 12, 31)
       
       # 데이터베이스에서 해당 연도 데이터 조회
       cpi_data = (
           service.db.query(service.db.query(CPI.__table__).filter(
               CPI.date >= start_date,
               CPI.date <= end_date
           ).order_by(asc(CPI.date)))
           .all()
       )
       
       if not cpi_data:
           raise HTTPException(
               status_code=404, 
               detail=f"{year}년 CPI 데이터를 찾을 수 없습니다"
           )
       
       result = []
       for cpi in cpi_data:
           result.append(CPIResponse(
               date=cpi.date,
               cpi_value=float(cpi.cpi_value) if cpi.cpi_value else None,
               interval_type=cpi.interval_type or "monthly",
               unit=cpi.unit or "index 1982-1984=100",
               name=cpi.name or "Consumer Price Index for all Urban Consumers"
           ))
       
       return CPIListResponse(
           total_count=len(result),
           items=result
       )
   except HTTPException:
       raise
   except Exception as e:
       raise HTTPException(status_code=500, detail=str(e))

@router.get("/month/{year}/{month}", response_model=CPIMonthlyDetailResponse)
async def get_cpi_monthly_detail(
    year: int,
    month: int,
    db: Session = Depends(get_db)
):
    """
    특정월 CPI 상세 정보 조회
    
    CPI 원본값과 해당월 인플레이션율을 함께 제공:
    - CPI 지수 값
    - 전년 동월 대비 인플레이션율
    - 전월 대비 변화율
    - 전월 대비 절대값 변화
    
    Args:
        year: 연도 (1900~2100)
        month: 월 (1~12)
        
    Example:
        GET /api/v1/cpi/month/2024/6
        → 2024년 6월 CPI 상세 정보
    """
    try:
        service = CPIService(db)
        return service.get_monthly_detail(year, month)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))