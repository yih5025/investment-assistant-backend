# app/schemas/company_overview_schema.py
from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict, Any
from datetime import datetime
from decimal import Decimal

class CompanyBasicInfo(BaseModel):
    """회사 기본 정보"""
    symbol: str = Field(..., description="주식 심볼")
    name: str = Field(..., description="회사명")
    sector: Optional[str] = Field(None, description="섹터")
    industry: Optional[str] = Field(None, description="산업군")
    address: Optional[str] = Field(None, description="본사 주소")
    official_site: Optional[str] = Field(None, description="공식 웹사이트")
    description: Optional[str] = Field(None, description="사업 설명")
    country: Optional[str] = Field(None, description="국가")
    exchange: Optional[str] = Field(None, description="거래소")

class FinancialMetrics(BaseModel):
    """재무 지표"""
    market_capitalization: Optional[int] = Field(None, description="시가총액")
    revenue_ttm: Optional[int] = Field(None, description="매출 (TTM)")
    gross_profit_ttm: Optional[int] = Field(None, description="매출총이익 (TTM)")
    ebitda: Optional[int] = Field(None, description="EBITDA")
    
    # 비율 지표
    pe_ratio: Optional[float] = Field(None, description="P/E 비율")
    peg_ratio: Optional[float] = Field(None, description="PEG 비율")
    price_to_book_ratio: Optional[float] = Field(None, description="PBR")
    price_to_sales_ratio_ttm: Optional[float] = Field(None, description="PSR")
    ev_to_revenue: Optional[float] = Field(None, description="EV/Revenue")
    ev_to_ebitda: Optional[float] = Field(None, description="EV/EBITDA")
    
    # 수익성 지표
    profit_margin: Optional[float] = Field(None, description="순이익률")
    operating_margin_ttm: Optional[float] = Field(None, description="영업이익률")
    return_on_assets_ttm: Optional[float] = Field(None, description="ROA")
    return_on_equity_ttm: Optional[float] = Field(None, description="ROE")
    
    # 주당 지표
    eps: Optional[float] = Field(None, description="주당 순이익")
    diluted_eps_ttm: Optional[float] = Field(None, description="희석 주당순이익")
    book_value: Optional[float] = Field(None, description="주당 장부가")
    revenue_per_share_ttm: Optional[float] = Field(None, description="주당 매출")

class DividendInfo(BaseModel):
    """배당 정보"""
    dividend_per_share: Optional[float] = Field(None, description="주당 배당금")
    dividend_yield: Optional[float] = Field(None, description="배당 수익률")
    dividend_date: Optional[str] = Field(None, description="배당지급일")
    ex_dividend_date: Optional[str] = Field(None, description="배당락일")

class StockPriceInfo(BaseModel):
    """주가 정보"""
    week_52_high: Optional[float] = Field(None, description="52주 최고가")
    week_52_low: Optional[float] = Field(None, description="52주 최저가")
    day_50_moving_average: Optional[float] = Field(None, description="50일 이동평균")
    day_200_moving_average: Optional[float] = Field(None, description="200일 이동평균")
    beta: Optional[float] = Field(None, description="베타")

class GrowthMetrics(BaseModel):
    """성장 지표"""
    quarterly_earnings_growth_yoy: Optional[float] = Field(None, description="분기 실적 성장률")
    quarterly_revenue_growth_yoy: Optional[float] = Field(None, description="분기 매출 성장률")

class AnalystInfo(BaseModel):
    """분석가 정보"""
    analyst_target_price: Optional[float] = Field(None, description="분석가 목표주가")
    trailing_pe: Optional[float] = Field(None, description="후행 P/E")
    forward_pe: Optional[float] = Field(None, description="선행 P/E")

class CompanyOverviewData(BaseModel):
    """Company Overview 전체 데이터"""
    batch_id: int = Field(..., description="배치 ID")
    
    # 기본 정보
    basic_info: CompanyBasicInfo
    
    # 재무 지표
    financial_metrics: FinancialMetrics
    
    # 배당 정보
    dividend_info: DividendInfo
    
    # 주가 정보
    stock_price_info: StockPriceInfo
    
    # 성장 지표
    growth_metrics: GrowthMetrics
    
    # 분석가 정보
    analyst_info: AnalystInfo
    
    # 추가 정보
    shares_outstanding: Optional[int] = Field(None, description="발행 주식 수")
    fiscal_year_end: Optional[str] = Field(None, description="회계연도 마감월")
    latest_quarter: Optional[str] = Field(None, description="최근 분기")
    
    # 메타데이터
    created_at: str = Field(..., description="생성 시간")

class CompanyOverviewSummary(BaseModel):
    """Company Overview 요약 정보 (리스트용)"""
    symbol: str = Field(..., description="주식 심볼")
    name: str = Field(..., description="회사명")
    sector: Optional[str] = Field(None, description="섹터")
    industry: Optional[str] = Field(None, description="산업군")
    market_capitalization: Optional[int] = Field(None, description="시가총액")
    pe_ratio: Optional[float] = Field(None, description="P/E 비율")
    dividend_yield: Optional[float] = Field(None, description="배당 수익률")
    beta: Optional[float] = Field(None, description="베타")

class SectorStatistics(BaseModel):
    """섹터 통계"""
    sector: str = Field(..., description="섹터명")
    company_count: int = Field(..., description="기업 수")
    avg_pe_ratio: Optional[float] = Field(None, description="평균 P/E 비율")
    avg_roe: Optional[float] = Field(None, description="평균 ROE")
    avg_profit_margin: Optional[float] = Field(None, description="평균 순이익률")
    avg_dividend_yield: Optional[float] = Field(None, description="평균 배당 수익률")
    total_market_cap: Optional[int] = Field(None, description="총 시가총액")
    batch_id: int = Field(..., description="배치 ID")

class SectorComparison(BaseModel):
    """섹터 비교 결과"""
    metric_name: str = Field(..., description="지표명")
    company_value: Optional[float] = Field(None, description="회사 값")
    sector_average: Optional[float] = Field(None, description="섹터 평균")
    difference: Optional[float] = Field(None, description="차이")
    percentage_diff: Optional[float] = Field(None, description="비율 차이 (%)")
    comparison: str = Field(..., description="비교 결과 (Higher/Lower/Similar)")

class CompanyOverviewResponse(BaseModel):
    """Company Overview 응답"""
    success: bool = Field(..., description="성공 여부")
    symbol: Optional[str] = Field(None, description="주식 심볼")
    data: Optional[CompanyOverviewData] = Field(None, description="회사 데이터")
    data_available: bool = Field(..., description="데이터 가용성")
    message: str = Field(..., description="메시지")
    batch_id: Optional[int] = Field(None, description="사용된 배치 ID")
    suggestions: Optional[List[str]] = Field(None, description="제안사항")
    
class CompanyOverviewListResponse(BaseModel):
    """Company Overview 리스트 응답"""
    success: bool = Field(..., description="성공 여부")
    companies: List[CompanyOverviewSummary] = Field(..., description="회사 리스트")
    total_count: int = Field(..., description="총 개수")
    batch_id: Optional[int] = Field(None, description="사용된 배치 ID")
    data_available: bool = Field(..., description="데이터 가용성")
    message: str = Field(..., description="메시지")

class SectorAnalysisResponse(BaseModel):
    """섹터 분석 응답"""
    success: bool = Field(..., description="성공 여부")
    sector: str = Field(..., description="섹터명")
    sector_stats: Optional[SectorStatistics] = Field(None, description="섹터 통계")
    companies: List[CompanyOverviewSummary] = Field(default=[], description="섹터 내 회사들")
    total_companies: int = Field(default=0, description="총 회사 수")
    data_available: bool = Field(..., description="데이터 가용성")
    message: str = Field(..., description="메시지")
    suggestions: Optional[List[str]] = Field(None, description="제안사항")

class SectorComparisonResponse(BaseModel):
    """섹터 비교 응답"""
    success: bool = Field(..., description="성공 여부")
    symbol: str = Field(..., description="주식 심볼")
    company_sector: Optional[str] = Field(None, description="회사 섹터")
    comparison: Optional[Dict[str, SectorComparison]] = Field(None, description="비교 결과")
    sector_stats: Optional[SectorStatistics] = Field(None, description="섹터 통계")
    data_available: bool = Field(..., description="데이터 가용성")
    message: str = Field(..., description="메시지")

class DataAvailabilityResponse(BaseModel):
    """데이터 가용성 응답"""
    data_available: bool = Field(..., description="데이터 가용성")
    symbol: Optional[str] = Field(None, description="조회한 심볼")
    latest_batch_id: Optional[int] = Field(None, description="최신 배치 ID")
    latest_batch_companies: Optional[int] = Field(None, description="최신 배치 회사 수")
    total_batches: Optional[int] = Field(None, description="총 배치 수")
    all_batches: Optional[List[int]] = Field(None, description="모든 배치 ID 리스트")
    message: str = Field(..., description="메시지")

class SectorListResponse(BaseModel):
    """섹터 리스트 응답"""
    success: bool = Field(..., description="성공 여부")
    sectors: List[Dict[str, Any]] = Field(..., description="섹터 리스트")
    total_sectors: int = Field(..., description="총 섹터 수")
    batch_id: Optional[int] = Field(None, description="배치 ID")
    data_available: bool = Field(..., description="데이터 가용성")
    message: Optional[str] = Field(None, description="메시지")

# =========================
# 🎯 데이터 변환 유틸리티
# =========================

def db_to_company_basic_info(company: Any) -> CompanyBasicInfo:
    """DB 모델을 CompanyBasicInfo로 변환"""
    return CompanyBasicInfo(
        symbol=company.symbol,
        name=company.name,
        sector=company.sector,
        industry=company.industry,
        address=company.address,
        official_site=company.official_site,
        description=company.description,
        country=company.country,
        exchange=company.exchange
    )

def db_to_financial_metrics(company: Any) -> FinancialMetrics:
    """DB 모델을 FinancialMetrics로 변환"""
    return FinancialMetrics(
        market_capitalization=company.market_capitalization,
        revenue_ttm=company.revenue_ttm,
        gross_profit_ttm=company.gross_profit_ttm,
        ebitda=company.ebitda,
        pe_ratio=float(company.pe_ratio) if company.pe_ratio else None,
        peg_ratio=float(company.peg_ratio) if company.peg_ratio else None,
        price_to_book_ratio=float(company.price_to_book_ratio) if company.price_to_book_ratio else None,
        price_to_sales_ratio_ttm=float(company.price_to_sales_ratio_ttm) if company.price_to_sales_ratio_ttm else None,
        ev_to_revenue=float(company.ev_to_revenue) if company.ev_to_revenue else None,
        ev_to_ebitda=float(company.ev_to_ebitda) if company.ev_to_ebitda else None,
        profit_margin=float(company.profit_margin) if company.profit_margin else None,
        operating_margin_ttm=float(company.operating_margin_ttm) if company.operating_margin_ttm else None,
        return_on_assets_ttm=float(company.return_on_assets_ttm) if company.return_on_assets_ttm else None,
        return_on_equity_ttm=float(company.return_on_equity_ttm) if company.return_on_equity_ttm else None,
        eps=float(company.eps) if company.eps else None,
        diluted_eps_ttm=float(company.diluted_eps_ttm) if company.diluted_eps_ttm else None,
        book_value=float(company.book_value) if company.book_value else None,
        revenue_per_share_ttm=float(company.revenue_per_share_ttm) if company.revenue_per_share_ttm else None
    )

def db_to_dividend_info(company: Any) -> DividendInfo:
    """DB 모델을 DividendInfo로 변환"""
    return DividendInfo(
        dividend_per_share=float(company.dividend_per_share) if company.dividend_per_share else None,
        dividend_yield=float(company.dividend_yield) if company.dividend_yield else None,
        dividend_date=company.dividend_date,
        ex_dividend_date=company.ex_dividend_date
    )

def db_to_stock_price_info(company: Any) -> StockPriceInfo:
    """DB 모델을 StockPriceInfo로 변환"""
    return StockPriceInfo(
        week_52_high=float(company.week_52_high) if company.week_52_high else None,
        week_52_low=float(company.week_52_low) if company.week_52_low else None,
        day_50_moving_average=float(company.day_50_moving_average) if company.day_50_moving_average else None,
        day_200_moving_average=float(company.day_200_moving_average) if company.day_200_moving_average else None,
        beta=float(company.beta) if company.beta else None
    )

def db_to_growth_metrics(company: Any) -> GrowthMetrics:
    """DB 모델을 GrowthMetrics로 변환"""
    return GrowthMetrics(
        quarterly_earnings_growth_yoy=float(company.quarterly_earnings_growth_yoy) if company.quarterly_earnings_growth_yoy else None,
        quarterly_revenue_growth_yoy=float(company.quarterly_revenue_growth_yoy) if company.quarterly_revenue_growth_yoy else None
    )

def db_to_analyst_info(company: Any) -> AnalystInfo:
    """DB 모델을 AnalystInfo로 변환"""
    return AnalystInfo(
        analyst_target_price=float(company.analyst_target_price) if company.analyst_target_price else None,
        trailing_pe=float(company.trailing_pe) if company.trailing_pe else None,
        forward_pe=float(company.forward_pe) if company.forward_pe else None
    )

def db_to_company_overview_data(company: Any) -> CompanyOverviewData:
    """DB 모델을 CompanyOverviewData로 변환"""
    return CompanyOverviewData(
        batch_id=company.batch_id,
        basic_info=db_to_company_basic_info(company),
        financial_metrics=db_to_financial_metrics(company),
        dividend_info=db_to_dividend_info(company),
        stock_price_info=db_to_stock_price_info(company),
        growth_metrics=db_to_growth_metrics(company),
        analyst_info=db_to_analyst_info(company),
        shares_outstanding=company.shares_outstanding,
        fiscal_year_end=company.fiscal_year_end,
        latest_quarter=company.latest_quarter,
        created_at=company.created_at.isoformat() if company.created_at else ""
    )

def db_to_company_overview_summary(company: Any) -> CompanyOverviewSummary:
    """DB 모델을 CompanyOverviewSummary로 변환"""
    return CompanyOverviewSummary(
        symbol=company.symbol,
        name=company.name,
        sector=company.sector,
        industry=company.industry,
        market_capitalization=company.market_capitalization,
        pe_ratio=float(company.pe_ratio) if company.pe_ratio else None,
        dividend_yield=float(company.dividend_yield) if company.dividend_yield else None,
        beta=float(company.beta) if company.beta else None
    )

def create_sector_comparison(metric_name: str, company_value: float, sector_avg: float) -> SectorComparison:
    """섹터 비교 객체 생성"""
    if company_value is None or sector_avg is None:
        return SectorComparison(
            metric_name=metric_name,
            company_value=company_value,
            sector_average=sector_avg,
            difference=None,
            percentage_diff=None,
            comparison='N/A'
        )
    
    difference = company_value - sector_avg
    percentage_diff = (difference / sector_avg) * 100 if sector_avg != 0 else None
    
    if percentage_diff is not None:
        if percentage_diff > 10:
            comparison = 'Much Higher'
        elif percentage_diff > 0:
            comparison = 'Higher'
        elif percentage_diff < -10:
            comparison = 'Much Lower'
        elif percentage_diff < 0:
            comparison = 'Lower'
        else:
            comparison = 'Similar'
    else:
        comparison = 'N/A'
    
    return SectorComparison(
        metric_name=metric_name,
        company_value=company_value,
        sector_average=sector_avg,
        difference=round(difference, 4) if difference is not None else None,
        percentage_diff=round(percentage_diff, 2) if percentage_diff is not None else None,
        comparison=comparison
    )

# =========================
# 🎯 요청 스키마들
# =========================

class CompanyOverviewRequest(BaseModel):
    """Company Overview 조회 요청"""
    symbol: str = Field(..., description="주식 심볼", min_length=1, max_length=10)
    batch_id: Optional[int] = Field(None, description="특정 배치 ID (없으면 최신)")

    @validator('symbol')
    def symbol_uppercase(cls, v):
        """심볼을 대문자로 변환"""
        return v.upper().strip()

class SectorAnalysisRequest(BaseModel):
    """섹터 분석 요청"""
    sector: str = Field(..., description="섹터명", min_length=1, max_length=100)
    batch_id: Optional[int] = Field(None, description="특정 배치 ID (없으면 최신)")
    limit: int = Field(default=50, description="반환할 최대 회사 수", ge=1, le=200)

    @validator('sector')
    def sector_title(cls, v):
        """섹터명을 타이틀 케이스로 변환"""
        return v.strip().title()

class CompanyListRequest(BaseModel):
    """회사 리스트 조회 요청"""
    batch_id: Optional[int] = Field(None, description="특정 배치 ID (없으면 최신)")
    limit: int = Field(default=500, description="반환할 최대 회사 수", ge=1, le=1000)

# =========================
# 🎯 에러 응답 스키마
# =========================

class CompanyOverviewError(BaseModel):
    """Company Overview 에러 응답"""
    success: bool = Field(default=False, description="성공 여부")
    error_type: str = Field(..., description="에러 타입")
    message: str = Field(..., description="에러 메시지")
    symbol: Optional[str] = Field(None, description="관련 심볼")
    suggestions: Optional[List[str]] = Field(None, description="제안사항")

def create_company_overview_error(
    error_type: str, 
    message: str, 
    symbol: str = None, 
    suggestions: List[str] = None
) -> CompanyOverviewError:
    """Company Overview 에러 응답 생성"""
    return CompanyOverviewError(
        error_type=error_type,
        message=message,
        symbol=symbol,
        suggestions=suggestions or []
    )