import datetime  # 전체 모듈 import로 변경
from typing import List, Optional
from pydantic import BaseModel, Field
from decimal import Decimal


# Base Schema
class TreasuryYieldBase(BaseModel):
    """국채 수익률 기본 스키마"""
    date: datetime.date = Field(..., description="기준일자")  # datetime.date로 명시
    maturity: str = Field(..., description="만기 (2year, 10year, 30year)")
    interval_type: str = Field(default="monthly", description="주기")
    yield_rate: Optional[Decimal] = Field(None, description="수익률 (%)")


# Response Schemas
class TreasuryYieldResponse(TreasuryYieldBase):
    """국채 수익률 응답 스키마"""
    created_at: datetime.datetime = Field(..., description="생성일시")  # datetime.datetime로 명시
    updated_at: datetime.datetime = Field(..., description="수정일시")

    class Config:
        from_attributes = True


class TreasuryYieldListResponse(BaseModel):
    """국채 수익률 목록 응답 스키마"""
    items: List[TreasuryYieldResponse]
    total: int = Field(..., description="전체 개수")
    page: int = Field(..., description="현재 페이지")
    size: int = Field(..., description="페이지 크기")
    pages: int = Field(..., description="전체 페이지 수")


# Chart Schema
class TreasuryYieldChartPoint(BaseModel):
    """차트용 데이터 포인트"""
    date: datetime.date = Field(..., description="기준일자")
    yield_rate: Optional[Decimal] = Field(None, description="수익률 (%)")


class TreasuryYieldChartResponse(BaseModel):
    """차트 데이터 응답 스키마"""
    maturity: str = Field(..., description="만기")
    period: str = Field(..., description="조회 기간")
    data: List[TreasuryYieldChartPoint] = Field(..., description="차트 데이터")
    summary: dict = Field(..., description="요약 정보")


# Yield Curve Schema
class YieldCurvePoint(BaseModel):
    """수익률 곡선 데이터 포인트"""
    maturity: str = Field(..., description="만기")
    years: int = Field(..., description="만기 연수")
    yield_rate: Optional[Decimal] = Field(None, description="수익률 (%)")


class YieldCurveResponse(BaseModel):
    """수익률 곡선 응답 스키마"""
    date: datetime.date = Field(..., description="기준일자")
    curve_type: str = Field(..., description="곡선 유형 (normal/flat/inverted)")
    data: List[YieldCurvePoint] = Field(..., description="곡선 데이터")
    analysis: dict = Field(..., description="분석 정보")


# Statistics Schema  
class TreasuryYieldStatistics(BaseModel):
    """국채 수익률 통계 스키마"""
    maturity: str = Field(..., description="만기")
    current_rate: Optional[Decimal] = Field(None, description="현재 수익률")
    avg_1month: Optional[Decimal] = Field(None, description="1개월 평균")
    avg_3month: Optional[Decimal] = Field(None, description="3개월 평균")
    avg_1year: Optional[Decimal] = Field(None, description="1년 평균")
    min_1year: Optional[Decimal] = Field(None, description="1년 최저")
    max_1year: Optional[Decimal] = Field(None, description="1년 최고")
    volatility: Optional[Decimal] = Field(None, description="변동성")


class TreasuryYieldStatisticsResponse(BaseModel):
    """통계 응답 스키마"""
    as_of_date: datetime.date = Field(..., description="기준일자")
    statistics: List[TreasuryYieldStatistics] = Field(..., description="만기별 통계")
    market_summary: dict = Field(..., description="시장 요약")


# Trends Schema
class TreasuryYieldTrend(BaseModel):
    """트렌드 분석 스키마"""
    maturity: str = Field(..., description="만기")
    trend_direction: str = Field(..., description="추세 방향 (rising/falling/flat)")
    change_1month: Optional[Decimal] = Field(None, description="1개월 변화량")
    change_3month: Optional[Decimal] = Field(None, description="3개월 변화량")
    change_1year: Optional[Decimal] = Field(None, description="1년 변화량")
    trend_strength: str = Field(..., description="추세 강도 (strong/moderate/weak)")


class TreasuryYieldTrendsResponse(BaseModel):
    """트렌드 분석 응답 스키마"""
    analysis_date: datetime.date = Field(..., description="분석 기준일")
    trends: List[TreasuryYieldTrend] = Field(..., description="만기별 트렌드")
    market_outlook: dict = Field(..., description="시장 전망")


# Recent Data Schema
class TreasuryYieldRecentResponse(BaseModel):
    """최근 데이터 응답 스키마"""
    latest_date: datetime.date = Field(..., description="최신 데이터 일자")
    data: List[TreasuryYieldResponse] = Field(..., description="최근 데이터")
    previous_data: List[TreasuryYieldResponse] = Field(..., description="이전 데이터 (비교용)")
    changes: dict = Field(..., description="변화량 정보")