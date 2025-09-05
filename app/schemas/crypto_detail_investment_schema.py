# app/schemas/crypto_detail_investment_schema.py

from pydantic import BaseModel, Field
from typing import Optional, List, Dict
from decimal import Decimal
from datetime import datetime


class BasicCoinInfo(BaseModel):
    """기본 코인 정보"""
    coingecko_id: str
    symbol: str
    name: str
    image_large: Optional[str] = None
    market_cap_rank: Optional[int] = None
    categories: Optional[List[str]] = []


class MarketData(BaseModel):
    """시장 데이터"""
    current_price_usd: Optional[Decimal] = None
    current_price_krw: Optional[Decimal] = None
    market_cap_usd: Optional[int] = None
    total_volume_usd: Optional[int] = None
    
    # 가격 변동률
    price_change_percentage_24h: Optional[Decimal] = None
    price_change_percentage_7d: Optional[Decimal] = None
    price_change_percentage_30d: Optional[Decimal] = None
    
    # ATH/ATL 데이터
    ath_usd: Optional[Decimal] = None
    ath_change_percentage: Optional[Decimal] = None
    ath_date: Optional[datetime] = None
    atl_usd: Optional[Decimal] = None
    atl_change_percentage: Optional[Decimal] = None
    atl_date: Optional[datetime] = None


class SupplyData(BaseModel):
    """공급량 데이터"""
    total_supply: Optional[Decimal] = None
    circulating_supply: Optional[Decimal] = None
    max_supply: Optional[Decimal] = None
    
    # 계산된 공급 비율
    circulating_supply_percentage: Optional[Decimal] = Field(None, description="유통 공급량 비율 (%)")
    scarcity_score: Optional[str] = Field(None, description="희소성 점수 (높음/보통/낮음)")


class KimchiPremiumData(BaseModel):
    """김치 프리미엄 분석 데이터"""
    korean_price_usd: Optional[Decimal] = None
    global_avg_price_usd: Optional[Decimal] = None
    kimchi_premium_percent: Optional[Decimal] = Field(None, description="김치 프리미엄 (%)")
    price_diff_usd: Optional[Decimal] = Field(None, description="절대 가격 차이 (USD)")
    
    # 거래량 및 유동성
    korean_volume_usd: Optional[Decimal] = None
    total_global_volume_usd: Optional[Decimal] = None
    korean_exchange: Optional[str] = None
    global_exchange_count: Optional[int] = None
    
    # 스프레드 및 품질 지표
    korean_spread: Optional[Decimal] = None
    avg_global_spread: Optional[Decimal] = None
    
    # 차익거래 분석
    arbitrage_opportunity: Optional[str] = Field(None, description="차익거래 기회 (HIGH/MEDIUM/LOW)")
    net_profit_per_unit: Optional[Decimal] = Field(None, description="단위당 순수익")
    transaction_cost_estimate: Optional[Decimal] = Field(None, description="거래비용 추정 (%)")


class DerivativesData(BaseModel):
    """파생상품 시장 분석 데이터"""
    # 펀딩비 분석
    avg_funding_rate: Optional[Decimal] = None
    positive_funding_count: Optional[int] = None
    negative_funding_count: Optional[int] = None
    funding_rate_interpretation: Optional[str] = Field(None, description="펀딩비 해석")
    market_sentiment: Optional[str] = Field(None, description="시장 심리 (강세/중립/약세)")
    
    # 미체결약정
    total_open_interest: Optional[Decimal] = None
    open_interest_rank: Optional[int] = Field(None, description="미체결약정 기준 순위")
    volume_24h_usd: Optional[Decimal] = None
    
    # 기관 관심도
    institutional_interest: Optional[str] = Field(None, description="기관 관심도 (높음/보통/낮음)")
    market_maturity: Optional[str] = Field(None, description="시장 성숙도")


class GlobalMarketContext(BaseModel):
    """글로벌 시장 맥락"""
    total_market_cap_usd: Optional[Decimal] = None
    market_cap_change_24h: Optional[Decimal] = None
    btc_dominance: Optional[Decimal] = None
    eth_dominance: Optional[Decimal] = None
    
    # 시장 위치
    market_share_percentage: Optional[Decimal] = Field(None, description="전체 시장 점유율")
    dominance_change_7d: Optional[Decimal] = Field(None, description="7일간 도미넌스 변화")
    vs_market_performance: Optional[Decimal] = Field(None, description="시장 대비 성과")
    
    # 시장 상태
    market_status: Optional[str] = Field(None, description="시장 상태 (bullish/bearish/neutral)")
    active_cryptocurrencies: Optional[int] = None
    markets: Optional[int] = Field(None, description="활성 거래소 수")


class RiskAnalysis(BaseModel):
    """위험도 분석"""
    # 변동성 지표
    volatility_24h: Optional[Decimal] = None
    volatility_7d: Optional[Decimal] = None
    volatility_30d: Optional[Decimal] = None
    volatility_risk: Optional[str] = Field(None, description="변동성 위험 (높음/보통/낮음)")
    
    # 유동성 리스크
    liquidity_risk: Optional[str] = Field(None, description="유동성 위험 (높음/보통/낮음)")
    bid_ask_spread: Optional[Decimal] = None
    volume_stability: Optional[str] = Field(None, description="거래량 안정성")
    
    # 시장 위치 리스크
    market_position_risk: Optional[str] = Field(None, description="시장 위치 리스크")
    rank_stability: Optional[str] = Field(None, description="순위 안정성")
    
    # 종합 위험도
    overall_risk_score: Optional[str] = Field(None, description="종합 위험도 (높음/보통/낮음)")


class InvestmentOpportunity(BaseModel):
    """투자 기회 분석"""
    # 기관 채택
    institutional_adoption: Optional[str] = Field(None, description="기관 채택 현황")
    etf_status: Optional[str] = Field(None, description="ETF 승인 상태")
    
    # 공급 제한 효과
    supply_constraint: Optional[str] = Field(None, description="공급 제한 효과")
    inflation_hedge: Optional[str] = Field(None, description="인플레이션 헤지 적합성")
    
    # 김치 프리미엄 활용
    arbitrage_potential: Optional[str] = Field(None, description="차익거래 잠재력")
    
    # 종합 투자 환경
    investment_environment: Optional[str] = Field(None, description="현재 투자 환경")
    key_drivers: Optional[List[str]] = Field([], description="주요 상승 동력")


class PortfolioGuidance(BaseModel):
    """포트폴리오 배분 가이드"""
    conservative_allocation: Optional[str] = Field(None, description="보수적 투자자 권장 비중")
    moderate_allocation: Optional[str] = Field(None, description="중간 위험 선호자 권장 비중")
    aggressive_allocation: Optional[str] = Field(None, description="적극적 투자자 권장 비중")
    
    investment_strategies: Optional[List[str]] = Field([], description="추천 투자 전략")
    time_horizon: Optional[str] = Field(None, description="권장 투자 기간")


class CryptoInvestmentAnalysisResponse(BaseModel):
    """Crypto Detail Investment Analysis 전체 응답"""
    basic_info: BasicCoinInfo
    market_data: MarketData
    supply_data: SupplyData
    kimchi_premium: KimchiPremiumData
    derivatives: DerivativesData
    global_context: GlobalMarketContext
    risk_analysis: RiskAnalysis
    investment_opportunity: InvestmentOpportunity
    portfolio_guidance: PortfolioGuidance
    
    # 메타데이터
    last_updated: datetime = Field(default_factory=datetime.now)
    data_sources: List[str] = Field(default=["CoinGecko", "Derivatives", "Tickers", "Global"])
    
    class Config:
        json_encoders = {
            Decimal: lambda x: float(x) if x is not None else None,
            datetime: lambda x: x.isoformat() if x is not None else None
        }
        schema_extra = {
            "example": {
                "basic_info": {
                    "coingecko_id": "bitcoin",
                    "symbol": "BTC",
                    "name": "Bitcoin",
                    "market_cap_rank": 1
                },
                "market_data": {
                    "current_price_usd": 43250.00,
                    "price_change_percentage_24h": 2.8
                },
                "kimchi_premium": {
                    "kimchi_premium_percent": 2.8,
                    "arbitrage_opportunity": "MEDIUM"
                }
            }
        }

class ExchangeComparisonData(BaseModel):
    """거래소별 비교 데이터"""
    korean_exchange: str = Field(description="국내 거래소명")
    korean_price_usd: Decimal = Field(description="국내 거래소 가격 (USD)")
    korean_volume_usd: float = Field(description="국내 거래소 거래량 (USD)")
    korean_spread: Optional[float] = Field(description="국내 거래소 스프레드 (%)")
    
    global_exchange: str = Field(description="해외 거래소명")
    global_price_usd: Decimal = Field(description="해외 거래소 가격 (USD)")
    global_volume_usd: float = Field(description="해외 거래소 거래량 (USD)")
    global_spread: Optional[float] = Field(description="해외 거래소 스프레드 (%)")
    
    premium_percentage: Decimal = Field(description="김치 프리미엄 (%)")
    price_diff_usd: Decimal = Field(description="절대 가격 차이 (USD)")
    volume_ratio: Decimal = Field(description="거래량 비율 (국내/해외)")


class DetailedKimchiPremiumResponse(BaseModel):
    """거래소별 상세 김치 프리미엄 응답"""
    symbol: str = Field(description="암호화폐 심볼")
    timestamp: datetime = Field(description="분석 시점")
    
    # 요약 정보 (기존)
    summary: KimchiPremiumData = Field(description="요약 김치 프리미엄 정보")
    
    # 상세 비교 정보 (신규)
    exchange_comparisons: List[ExchangeComparisonData] = Field(description="거래소별 상세 비교")
    
    # 통계 정보
    statistics: dict = Field(description="전체 통계 정보")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat(),
            Decimal: lambda v: float(v)
        }