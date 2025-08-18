import datetime
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.dependencies import get_db
from app.services.treasury_yield_service import TreasuryYieldService
from app.schemas.treasury_yield_schema import (
    TreasuryYieldListResponse,
    TreasuryYieldChartResponse,
    YieldCurveResponse,
    TreasuryYieldStatisticsResponse,
    TreasuryYieldTrendsResponse,
    TreasuryYieldRecentResponse
)

# 라우터 생성
router = APIRouter(
    tags=["Treasury Yield", "국채수익률"],
    responses={404: {"description": "Not found"}}
)


@router.get(
    "/",
    response_model=TreasuryYieldListResponse,
    summary="국채 수익률 목록 조회",
    description="""
    국채 수익률 전체 목록을 페이징과 필터링으로 조회합니다.
    
    **주요 기능:**
    - 페이징 처리 (page, size)
    - 만기별 필터링 (maturity)
    - 날짜 범위 필터링 (start_date, end_date)
    - 정렬 옵션 (order_by)
    
    **정렬 옵션:**
    - date_desc: 날짜 내림차순 (기본값)
    - date_asc: 날짜 오름차순
    - yield_desc: 수익률 내림차순
    - yield_asc: 수익률 오름차순
    """
)
async def get_treasury_yields(
    page: int = Query(1, ge=1, description="페이지 번호"),
    size: int = Query(50, ge=1, le=1000, description="페이지 크기"),
    maturity: Optional[str] = Query(None, regex="^(2year|10year|30year)$", description="만기 필터"),
    start_date: Optional[datetime.date] = Query(None, description="시작 날짜"),
    end_date: Optional[datetime.date] = Query(None, description="종료 날짜"),
    order_by: str = Query("date_desc", regex="^(date_desc|date_asc|yield_desc|yield_asc)$", description="정렬 방식"),
    db: Session = Depends(get_db)
):
    """국채 수익률 목록 조회 (페이징, 필터링)"""
    try:
        return TreasuryYieldService.get_treasury_yields(
            db=db,
            page=page,
            size=size,
            maturity=maturity,
            start_date=start_date,
            end_date=end_date,
            order_by=order_by
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"데이터 조회 중 오류가 발생했습니다: {str(e)}")


@router.get(
    "/recent",
    response_model=TreasuryYieldRecentResponse,
    summary="최근 국채 수익률 데이터",
    description="""
    가장 최근의 국채 수익률 데이터를 조회합니다.
    
    **제공 정보:**
    - 최신 날짜의 모든 만기 수익률
    - 비교 기간 전 데이터
    - 변화량 및 변화율 계산
    """
)
async def get_recent_treasury_yields(
    days: int = Query(30, ge=1, le=365, description="비교 기간 (일)"),
    db: Session = Depends(get_db)
):
    """최근 국채 수익률 데이터 조회"""
    try:
        return TreasuryYieldService.get_recent_data(db=db, days=days)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"최근 데이터 조회 중 오류가 발생했습니다: {str(e)}")


@router.get(
    "/chart",
    response_model=TreasuryYieldChartResponse,
    summary="차트용 시계열 데이터",
    description="""
    특정 만기의 국채 수익률 시계열 데이터를 차트 형태로 제공합니다.
    
    **차트 특징:**
    - X축: 시간 (날짜)
    - Y축: 수익률 (%)
    - 만기별로 별도 조회 필요
    
    **제공 통계:**
    - 현재값, 최소값, 최대값, 평균값
    - 변화량, 변동성 계산
    
    **사용 예시:**
    - 10년 국채 수익률 1년간 추이
    - 2년 국채 vs 10년 국채 비교 (별도 호출)
    """
)
async def get_treasury_yield_chart(
    maturity: str = Query("10year", regex="^(2year|10year|30year)$", description="만기 선택"),
    period: str = Query("1year", regex="^(1month|3months|6months|1year|2years)$", description="조회 기간"),
    db: Session = Depends(get_db)
):
    """차트용 시계열 데이터 조회 (만기별)"""
    try:
        return TreasuryYieldService.get_chart_data(
            db=db,
            maturity=maturity,
            period=period
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"차트 데이터 조회 중 오류가 발생했습니다: {str(e)}")


@router.get(
    "/statistics",
    response_model=TreasuryYieldStatisticsResponse,
    summary="국채 수익률 통계 정보",
    description="""
    국채 수익률의 다양한 통계 정보를 제공합니다.
    
    **만기별 통계:**
    - 현재 수익률
    - 1개월/3개월/1년 평균
    - 1년 최고/최저값
    - 변동성 지표
    
    **활용 용도:**
    - 현재 수익률 수준 평가
    - 역사적 범위 내 위치 파악
    - 변동성 위험 측정
    """
)
async def get_treasury_yield_statistics(
    db: Session = Depends(get_db)
):
    """국채 수익률 통계 정보 조회"""
    try:
        return TreasuryYieldService.get_statistics(db=db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"통계 데이터 조회 중 오류가 발생했습니다: {str(e)}")


@router.get(
    "/maturity/{maturity}",
    response_model=TreasuryYieldListResponse,
    summary="특정 만기별 수익률 조회",
    description="""
    특정 만기의 국채 수익률 데이터만 조회합니다.
    
    **지원 만기:**
    - 2year: 2년 국채
    - 10year: 10년 국채  
    - 30year: 30년 국채
    
    **정렬:** 날짜 내림차순 (최신순)
    """
)
async def get_treasury_yield_by_maturity(
    maturity: str,
    page: int = Query(1, ge=1, description="페이지 번호"),
    size: int = Query(50, ge=1, le=1000, description="페이지 크기"),
    db: Session = Depends(get_db)
):
    """특정 만기별 국채 수익률 조회"""
    
    # 유효한 만기 검증
    valid_maturities = ["2year", "10year", "30year"]
    if maturity not in valid_maturities:
        raise HTTPException(
            status_code=400, 
            detail=f"유효하지 않은 만기입니다. 사용 가능한 값: {', '.join(valid_maturities)}"
        )
    
    try:
        return TreasuryYieldService.get_by_maturity(
            db=db,
            maturity=maturity,
            page=page,
            size=size
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"만기별 데이터 조회 중 오류가 발생했습니다: {str(e)}")


@router.get(
    "/yield-curve",
    response_model=YieldCurveResponse,
    summary="수익률 곡선 데이터",
    description="""
    특정 시점의 수익률 곡선을 제공합니다.
    
    **수익률 곡선이란?**
    - X축: 만기 (2년, 10년, 30년)
    - Y축: 수익률 (%)
    - 같은 날짜의 서로 다른 만기 수익률을 연결한 곡선
    
    **곡선 유형:**
    - normal: 장기 > 단기 (정상적인 상황)
    - flat: 장기 ≈ 단기 (전환점 신호)
    - inverted: 장기 < 단기 (경기침체 신호!)
    
    **분석 정보:**
    - 2년-10년 스프레드
    - 10년-30년 스프레드
    - 역전 신호 여부
    
    **투자 활용:**
    - 경기 사이클 판단
    - 연준 정책 방향 예측
    - 포트폴리오 자산 배분 결정
    """
)
async def get_yield_curve(
    target_date: Optional[datetime.date] = Query(None, description="기준일 (기본값: 최신 데이터)"),
    db: Session = Depends(get_db)
):
    """수익률 곡선 데이터 조회"""
    try:
        return TreasuryYieldService.get_yield_curve(db=db, target_date=target_date)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"수익률 곡선 조회 중 오류가 발생했습니다: {str(e)}")


@router.get(
    "/trends",
    response_model=TreasuryYieldTrendsResponse,
    summary="트렌드 분석",
    description="""
    국채 수익률의 추세 분석을 제공합니다.
    
    **분석 기간:**
    - 1개월, 3개월, 1년 변화량
    - 추세 방향 (rising/falling/flat)
    - 추세 강도 (strong/moderate/weak)
    
    **만기별 분석:**
    - 2년, 10년, 30년 각각의 트렌드
    - 만기간 트렌드 차이 비교
    
    **투자 신호:**
    - 급격한 변화 감지
    - 트렌드 전환점 식별
    - 시장 전망 제공
    
    **활용 예시:**
    - 금리 상승/하락 추세 확인
    - 연준 정책 변화 대응
    - 채권/주식 투자 타이밍 판단
    """
)
async def get_treasury_yield_trends(
    db: Session = Depends(get_db)
):
    """국채 수익률 트렌드 분석"""
    try:
        return TreasuryYieldService.get_trends(db=db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"트렌드 분석 중 오류가 발생했습니다: {str(e)}")