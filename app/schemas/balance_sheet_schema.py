from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import date, datetime
from decimal import Decimal

class BalanceSheetBase(BaseModel):
    """재무상태표 기본 스키마"""
    symbol: str = Field(..., description="기업 심볼")
    fiscaldateending: date = Field(..., description="회계연도 종료일")
    reportedcurrency: Optional[str] = Field(None, description="보고 통화")
    
    # 핵심 재무 지표
    totalassets: Optional[Decimal] = Field(None, description="총자산")
    totalcurrentassets: Optional[Decimal] = Field(None, description="유동자산")
    totalnoncurrentassets: Optional[Decimal] = Field(None, description="비유동자산")
    totalliabilities: Optional[Decimal] = Field(None, description="총부채")
    totalcurrentliabilities: Optional[Decimal] = Field(None, description="유동부채")
    totalnoncurrentliabilities: Optional[Decimal] = Field(None, description="비유동부채")
    totalshareholderequity: Optional[Decimal] = Field(None, description="총 주주자본")
    
    # 현금 관련
    cashandcashequivalentsatcarryingvalue: Optional[Decimal] = Field(None, description="현금 및 현금성자산")
    cashandshortterminvestments: Optional[Decimal] = Field(None, description="현금 및 단기투자")
    
    # 운영 자산
    inventory: Optional[Decimal] = Field(None, description="재고자산")
    currentnetreceivables: Optional[Decimal] = Field(None, description="매출채권")
    
    # 부채 세부
    currentaccountspayable: Optional[Decimal] = Field(None, description="매입채무")
    longtermdebt: Optional[Decimal] = Field(None, description="장기부채")
    
    # 자본 세부
    commonstock: Optional[Decimal] = Field(None, description="보통주")
    retainedearnings: Optional[Decimal] = Field(None, description="이익잉여금")
    commonstocksharesoutstanding: Optional[Decimal] = Field(None, description="발행주식수")
    
    fetched_at: Optional[datetime] = Field(None, description="데이터 수집 시간")

    class Config:
        from_attributes = True

class BalanceSheetResponse(BalanceSheetBase):
    """재무상태표 응답 스키마 (추가 계산 필드 포함)"""
    
    # 계산된 필드들
    total_assets_billions: Optional[float] = Field(None, description="총자산 (십억 단위)")
    market_cap_category: Optional[str] = Field(None, description="시가총액 카테고리")
    fiscal_date_str: Optional[str] = Field(None, description="회계연도 (문자열)")

class FinancialRatio(BaseModel):
    """재무비율 스키마"""
    name: str = Field(..., description="비율명")
    value: Optional[float] = Field(None, description="비율값")
    status: str = Field(..., description="상태 (우수/양호/보통/위험)")
    description: str = Field(..., description="해석 설명")
    benchmark: Optional[float] = Field(None, description="업계 벤치마크")

class FinancialHealthGrade(BaseModel):
    """재무건전성 등급 스키마"""
    grade: str = Field(..., description="등급 (A/B/C/D)")
    score: int = Field(..., description="점수 (0-100)")
    status: str = Field(..., description="상태 설명")

class FinancialTrend(BaseModel):
    """재무 트렌드 스키마"""
    metric: str = Field(..., description="지표명")
    current_value: Optional[float] = Field(None, description="현재값")
    previous_value: Optional[float] = Field(None, description="이전값")
    change_amount: Optional[float] = Field(None, description="변화량")
    change_rate: Optional[float] = Field(None, description="변화율 (%)")
    trend_direction: str = Field(..., description="트렌드 방향 (증가/감소/유지)")

class FinancialAnalysis(BaseModel):
    """재무분석 결과 스키마"""
    symbol: str = Field(..., description="기업 심볼")
    analysis_date: date = Field(..., description="분석 기준일")
    
    # 재무건전성 등급
    financial_health: FinancialHealthGrade = Field(..., description="재무건전성 등급")
    
    # 핵심 재무비율들
    key_ratios: dict = Field(..., description="핵심 재무비율 그룹")
    
    # 해석 및 분석
    interpretation: dict = Field(..., description="재무 해석 결과")
    
    # 섹터 비교 (선택적)
    sector_comparison: Optional[dict] = Field(None, description="업계 비교")

class BalanceSheetTrends(BaseModel):
    """재무상태표 시계열 트렌드 스키마"""
    symbol: str = Field(..., description="기업 심볼")
    period_count: int = Field(..., description="분석 기간 수")
    trends: List[FinancialTrend] = Field(..., description="트렌드 리스트")
    summary: str = Field(..., description="트렌드 요약")

class BalanceSheetStatistics(BaseModel):
    """재무상태표 전체 통계 스키마"""
    total_companies: int = Field(..., description="총 기업 수")
    latest_period: date = Field(..., description="최신 보고 기간")
    average_total_assets: float = Field(..., description="평균 총자산")
    median_debt_ratio: float = Field(..., description="중간값 부채비율")
    
    # 섹터별 통계
    sector_stats: Optional[dict] = Field(None, description="섹터별 통계")
    
    # 규모별 분포
    size_distribution: dict = Field(..., description="규모별 기업 분포")

# 공통 응답 포맷
class BalanceSheetListResponse(BaseModel):
    """재무상태표 목록 응답"""
    items: List[BalanceSheetResponse] = Field(..., description="재무상태표 목록")
    total: int = Field(..., description="전체 개수")
    page: int = Field(..., description="현재 페이지")
    size: int = Field(..., description="페이지 크기")
    has_next: bool = Field(..., description="다음 페이지 존재 여부")

class PeriodBalanceSheetResponse(BaseModel):
    """특정 기간 재무상태표 응답"""
    period: date = Field(..., description="기간")
    companies: List[BalanceSheetResponse] = Field(..., description="해당 기간 기업들")
    summary: dict = Field(..., description="기간 요약 통계")