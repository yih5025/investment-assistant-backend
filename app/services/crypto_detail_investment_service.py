# app/services/crypto_detail_investment_service.py

from sqlalchemy.orm import Session
from sqlalchemy import and_, func, desc, text
from typing import Optional, Dict, Any, List
from decimal import Decimal
from datetime import datetime, timedelta

from ..models.coingecko_coin_details_model import CoingeckoCoinDetails
from ..models.coingecko_tickers_model import CoingeckoTickers
from ..models.coingecko_derivatives_model import CoingeckoDerivatives
from ..models.coingecko_global_model import CoingeckoGlobal
from ..schemas.crypto_detail_investment_schema import (
    CryptoInvestmentAnalysisResponse, BasicCoinInfo, MarketData, SupplyData,
    KimchiPremiumData, DerivativesData, GlobalMarketContext, RiskAnalysis,
    InvestmentOpportunity, PortfolioGuidance
)


class CryptoInvestmentService:
    """Crypto Detail Investment Analysis 서비스"""
    
    def __init__(self, db: Session):
        self.db = db
    
    async def get_investment_analysis(self, symbol: str) -> Optional[CryptoInvestmentAnalysisResponse]:
        """투자 분석 데이터 종합 조회"""
        
        # 1. 기본 코인 정보 조회
        coin_details = await self._get_coin_details(symbol)
        if not coin_details:
            return None
        
        # 2. 각 분석 영역별 데이터 수집
        basic_info = await self._build_basic_info(coin_details)
        market_data = await self._build_market_data(coin_details)
        supply_data = await self._build_supply_data(coin_details)
        kimchi_premium = await self._analyze_kimchi_premium(symbol)
        derivatives = await self._analyze_derivatives(symbol)
        global_context = await self._build_global_context(coin_details)
        risk_analysis = await self._analyze_risk(coin_details, derivatives)
        investment_opportunity = await self._analyze_investment_opportunity(
            coin_details, kimchi_premium, derivatives
        )
        portfolio_guidance = await self._build_portfolio_guidance(coin_details, risk_analysis)
        
        return CryptoInvestmentAnalysisResponse(
            basic_info=basic_info,
            market_data=market_data,
            supply_data=supply_data,
            kimchi_premium=kimchi_premium,
            derivatives=derivatives,
            global_context=global_context,
            risk_analysis=risk_analysis,
            investment_opportunity=investment_opportunity,
            portfolio_guidance=portfolio_guidance
        )
    
    async def _get_coin_details(self, symbol: str) -> Optional[CoingeckoCoinDetails]:
        """코인 기본 정보 조회"""
        return self.db.query(CoingeckoCoinDetails).filter(
            func.upper(CoingeckoCoinDetails.symbol) == symbol.upper()
        ).first()
    
    async def _build_basic_info(self, coin: CoingeckoCoinDetails) -> BasicCoinInfo:
        """기본 코인 정보 구성"""
        categories = []
        if coin.categories:
            import json
            try:
                categories = json.loads(coin.categories) if isinstance(coin.categories, str) else coin.categories
            except:
                categories = []
        
        return BasicCoinInfo(
            coingecko_id=coin.coingecko_id,
            symbol=coin.symbol,
            name=coin.name,
            image_large=coin.image_large,
            market_cap_rank=coin.market_cap_rank,
            categories=categories[:5]  # 상위 5개 카테고리만
        )
    
    async def _build_market_data(self, coin: CoingeckoCoinDetails) -> MarketData:
        """시장 데이터 구성"""
        return MarketData(
            current_price_usd=coin.current_price_usd,
            current_price_krw=coin.current_price_krw,
            market_cap_usd=coin.market_cap_usd,
            total_volume_usd=coin.total_volume_usd,
            price_change_percentage_24h=coin.price_change_percentage_24h,
            price_change_percentage_7d=coin.price_change_percentage_7d,
            price_change_percentage_30d=coin.price_change_percentage_30d,
            ath_usd=coin.ath_usd,
            ath_change_percentage=coin.ath_change_percentage,
            ath_date=coin.ath_date,
            atl_usd=coin.atl_usd,
            atl_change_percentage=coin.atl_change_percentage,
            atl_date=coin.atl_date
        )
    
    async def _build_supply_data(self, coin: CoingeckoCoinDetails) -> SupplyData:
        """공급량 데이터 구성"""
        # 유통 공급량 비율 계산
        circulating_percentage = None
        scarcity_score = None
        
        if coin.max_supply and coin.circulating_supply:
            circulating_percentage = (float(coin.circulating_supply) / float(coin.max_supply)) * 100
            
            # 희소성 점수 계산
            if circulating_percentage > 95:
                scarcity_score = "매우 높음"
            elif circulating_percentage > 80:
                scarcity_score = "높음"
            elif circulating_percentage > 50:
                scarcity_score = "보통"
            else:
                scarcity_score = "낮음"
        elif not coin.max_supply:
            scarcity_score = "무제한 공급"
        
        return SupplyData(
            total_supply=coin.total_supply,
            circulating_supply=coin.circulating_supply,
            max_supply=coin.max_supply,
            circulating_supply_percentage=Decimal(str(circulating_percentage)) if circulating_percentage else None,
            scarcity_score=scarcity_score
        )
    
    async def _analyze_kimchi_premium(self, symbol: str) -> KimchiPremiumData:
        """김치 프리미엄 분석"""
        # 최신 배치 ID 조회
        latest_batch = self.db.query(CoingeckoTickers.batch_id).order_by(
            desc(CoingeckoTickers.collected_at)
        ).first()
        
        if not latest_batch:
            return KimchiPremiumData()
        
        # 한국 거래소 가격 조회 (Upbit 우선)
        korean_ticker = self.db.query(CoingeckoTickers).filter(
            and_(
                CoingeckoTickers.batch_id == latest_batch.batch_id,
                func.upper(CoingeckoTickers.coin_symbol) == symbol.upper(),
                CoingeckoTickers.is_korean_exchange == True,
                CoingeckoTickers.converted_last_usd.isnot(None)
            )
        ).order_by(desc(CoingeckoTickers.converted_volume_usd)).first()
        
        # 글로벌 거래소 평균 가격 조회
        global_tickers = self.db.query(
            func.avg(CoingeckoTickers.converted_last_usd).label('avg_price'),
            func.sum(CoingeckoTickers.converted_volume_usd).label('total_volume'),
            func.count().label('exchange_count'),
            func.avg(CoingeckoTickers.bid_ask_spread_percentage).label('avg_spread')
        ).filter(
            and_(
                CoingeckoTickers.batch_id == latest_batch.batch_id,
                func.upper(CoingeckoTickers.coin_symbol) == symbol.upper(),
                CoingeckoTickers.is_korean_exchange == False,
                CoingeckoTickers.market_identifier.in_(['binance', 'coinbase-exchange', 'kraken']),
                CoingeckoTickers.converted_last_usd.isnot(None),
                CoingeckoTickers.trust_score == 'green'
            )
        ).first()
        
        if not korean_ticker or not global_tickers.avg_price:
            return KimchiPremiumData()
        
        # 김치 프리미엄 계산
        korean_price = float(korean_ticker.converted_last_usd)
        global_price = float(global_tickers.avg_price)
        premium_percent = ((korean_price - global_price) / global_price) * 100
        
        # 차익거래 기회 평가
        arbitrage_opportunity = "LOW"
        if abs(premium_percent) > 3 and korean_ticker.converted_volume_usd > 100000:
            arbitrage_opportunity = "HIGH"
        elif abs(premium_percent) > 1.5 and korean_ticker.converted_volume_usd > 50000:
            arbitrage_opportunity = "MEDIUM"
        
        # 거래비용 추정 (0.5%)
        transaction_cost = 0.5
        net_profit = (korean_price - global_price) - (korean_price * transaction_cost / 100)
        
        return KimchiPremiumData(
            korean_price_usd=Decimal(str(korean_price)),
            global_avg_price_usd=Decimal(str(global_price)),
            kimchi_premium_percent=Decimal(str(round(premium_percent, 2))),
            price_diff_usd=Decimal(str(korean_price - global_price)),
            korean_volume_usd=korean_ticker.converted_volume_usd,
            total_global_volume_usd=global_tickers.total_volume,
            korean_exchange=korean_ticker.market_name,
            global_exchange_count=global_tickers.exchange_count,
            korean_spread=korean_ticker.bid_ask_spread_percentage,
            avg_global_spread=global_tickers.avg_spread,
            arbitrage_opportunity=arbitrage_opportunity,
            net_profit_per_unit=Decimal(str(round(net_profit, 2))),
            transaction_cost_estimate=Decimal(str(transaction_cost))
        )
    
    async def _analyze_derivatives(self, symbol: str) -> DerivativesData:
        """파생상품 시장 분석"""
        # 최신 배치 데이터 조회
        latest_batch = self.db.query(CoingeckoDerivatives.batch_id).order_by(
            desc(CoingeckoDerivatives.collected_at)
        ).first()
        
        if not latest_batch:
            return DerivativesData()
        
        # 파생상품 데이터 집계
        derivatives_stats = self.db.query(
            func.avg(CoingeckoDerivatives.funding_rate).label('avg_funding_rate'),
            func.sum(CoingeckoDerivatives.open_interest_usd).label('total_open_interest'),
            func.sum(CoingeckoDerivatives.volume_24h_usd).label('total_volume'),
            func.count().label('total_markets'),
            func.sum(
                func.case([(CoingeckoDerivatives.funding_rate > 0, 1)], else_=0)
            ).label('positive_funding_count'),
            func.sum(
                func.case([(CoingeckoDerivatives.funding_rate < 0, 1)], else_=0)
            ).label('negative_funding_count')
        ).filter(
            and_(
                CoingeckoDerivatives.batch_id == latest_batch.batch_id,
                func.upper(CoingeckoDerivatives.index_id) == symbol.upper(),
                CoingeckoDerivatives.funding_rate.isnot(None)
            )
        ).first()
        
        if not derivatives_stats.avg_funding_rate:
            return DerivativesData()
        
        # 펀딩비 해석
        funding_rate = float(derivatives_stats.avg_funding_rate)
        if funding_rate > 0.01:
            interpretation = "기관들이 강한 Long 우세"
            sentiment = "강세"
        elif funding_rate > 0.005:
            interpretation = "기관들이 약한 Long 우세"
            sentiment = "약세"
        elif funding_rate > -0.005:
            interpretation = "중립적 포지션"
            sentiment = "중립"
        else:
            interpretation = "기관들이 Short 우세"
            sentiment = "약세"
        
        # 기관 관심도 평가
        open_interest = derivatives_stats.total_open_interest or 0
        if open_interest > 10000000000:  # 100억 달러 이상
            institutional_interest = "매우 높음"
        elif open_interest > 5000000000:   # 50억 달러 이상
            institutional_interest = "높음"
        elif open_interest > 1000000000:   # 10억 달러 이상
            institutional_interest = "보통"
        else:
            institutional_interest = "낮음"
        
        return DerivativesData(
            avg_funding_rate=Decimal(str(funding_rate)),
            positive_funding_count=derivatives_stats.positive_funding_count,
            negative_funding_count=derivatives_stats.negative_funding_count,
            funding_rate_interpretation=interpretation,
            market_sentiment=sentiment,
            total_open_interest=derivatives_stats.total_open_interest,
            volume_24h_usd=derivatives_stats.total_volume,
            institutional_interest=institutional_interest,
            market_maturity="성숙" if derivatives_stats.total_markets > 10 else "신흥"
        )
    
    async def _build_global_context(self, coin: CoingeckoCoinDetails) -> GlobalMarketContext:
        """글로벌 시장 맥락 구성"""
        # 최신 글로벌 데이터 조회
        global_data = self.db.query(CoingeckoGlobal).order_by(
            desc(CoingeckoGlobal.collected_at)
        ).first()
        
        if not global_data:
            return GlobalMarketContext()
        
        # 시장 점유율 계산
        market_share = None
        if coin.market_cap_usd and global_data.total_market_cap_usd:
            market_share = (coin.market_cap_usd / float(global_data.total_market_cap_usd)) * 100
        
        # 시장 상태 판단
        market_status = "neutral"
        if global_data.market_cap_change_percentage_24h_usd:
            change = float(global_data.market_cap_change_percentage_24h_usd)
            if change > 2:
                market_status = "bullish"
            elif change < -2:
                market_status = "bearish"
        
        return GlobalMarketContext(
            total_market_cap_usd=global_data.total_market_cap_usd,
            market_cap_change_24h=global_data.market_cap_change_percentage_24h_usd,
            btc_dominance=global_data.btc_dominance,
            eth_dominance=global_data.eth_dominance,
            market_share_percentage=Decimal(str(round(market_share, 2))) if market_share else None,
            market_status=market_status,
            active_cryptocurrencies=global_data.active_cryptocurrencies,
            markets=global_data.markets
        )
    
    async def _analyze_risk(self, coin: CoingeckoCoinDetails, derivatives: DerivativesData) -> RiskAnalysis:
        """위험도 분석"""
        # 변동성 계산
        volatility_24h = abs(coin.price_change_percentage_24h) if coin.price_change_percentage_24h else None
        volatility_7d = abs(coin.price_change_percentage_7d) if coin.price_change_percentage_7d else None
        volatility_30d = abs(coin.price_change_percentage_30d) if coin.price_change_percentage_30d else None
        
        # 변동성 위험 평가
        volatility_risk = "보통"
        if volatility_24h and volatility_24h > 10:
            volatility_risk = "높음"
        elif volatility_24h and volatility_24h < 3:
            volatility_risk = "낮음"
        
        # 유동성 위험 평가
        liquidity_risk = "낮음"
        volume_stability = "높음"
        
        if coin.total_volume_usd:
            if coin.total_volume_usd < 1000000:  # 100만 달러 미만
                liquidity_risk = "높음"
                volume_stability = "낮음"
            elif coin.total_volume_usd < 10000000:  # 1000만 달러 미만
                liquidity_risk = "보통"
                volume_stability = "보통"
        
        # 시장 위치 리스크
        market_position_risk = "낮음"
        rank_stability = "안정"
        
        if coin.market_cap_rank:
            if coin.market_cap_rank > 100:
                market_position_risk = "높음"
                rank_stability = "불안정"
            elif coin.market_cap_rank > 50:
                market_position_risk = "보통"
                rank_stability = "보통"
        
        # 종합 위험도 계산
        risk_factors = 0
        if volatility_risk == "높음":
            risk_factors += 2
        elif volatility_risk == "보통":
            risk_factors += 1
        
        if liquidity_risk == "높음":
            risk_factors += 2
        elif liquidity_risk == "보통":
            risk_factors += 1
        
        if market_position_risk == "높음":
            risk_factors += 1
        
        if risk_factors >= 4:
            overall_risk = "높음"
        elif risk_factors >= 2:
            overall_risk = "보통"
        else:
            overall_risk = "낮음"
        
        return RiskAnalysis(
            volatility_24h=volatility_24h,
            volatility_7d=volatility_7d,
            volatility_30d=volatility_30d,
            volatility_risk=volatility_risk,
            liquidity_risk=liquidity_risk,
            volume_stability=volume_stability,
            market_position_risk=market_position_risk,
            rank_stability=rank_stability,
            overall_risk_score=overall_risk
        )
    
    async def _analyze_investment_opportunity(
        self, 
        coin: CoingeckoCoinDetails, 
        kimchi: KimchiPremiumData, 
        derivatives: DerivativesData
    ) -> InvestmentOpportunity:
        """투자 기회 분석"""
        
        # 기관 채택 현황
        institutional_adoption = "진행중"
        etf_status = "검토중"
        
        if derivatives.institutional_interest == "매우 높음":
            institutional_adoption = "활발함"
            etf_status = "승인 가능성 높음"
        
        # 공급 제한 효과
        supply_constraint = "보통"
        inflation_hedge = "적합"
        
        if coin.max_supply and coin.circulating_supply:
            supply_ratio = float(coin.circulating_supply) / float(coin.max_supply)
            if supply_ratio > 0.9:
                supply_constraint = "높음"
                inflation_hedge = "매우 적합"
        
        # 차익거래 잠재력
        arbitrage_potential = kimchi.arbitrage_opportunity or "LOW"
        
        # 주요 상승 동력
        key_drivers = []
        if derivatives.institutional_interest in ["높음", "매우 높음"]:
            key_drivers.append("기관 관심 증가")
        if supply_constraint == "높음":
            key_drivers.append("공급량 제한")
        if kimchi.arbitrage_opportunity in ["MEDIUM", "HIGH"]:
            key_drivers.append("김치 프리미엄 활용")
        if derivatives.market_sentiment == "강세":
            key_drivers.append("파생상품 강세 신호")
        
        # 투자 환경 종합 평가
        positive_factors = len(key_drivers)
        if positive_factors >= 3:
            investment_environment = "매우 긍정적"
        elif positive_factors >= 2:
            investment_environment = "긍정적"
        elif positive_factors >= 1:
            investment_environment = "보통"
        else:
            investment_environment = "신중한 접근 필요"
        
        return InvestmentOpportunity(
            institutional_adoption=institutional_adoption,
            etf_status=etf_status,
            supply_constraint=supply_constraint,
            inflation_hedge=inflation_hedge,
            arbitrage_potential=arbitrage_potential.lower(),
            investment_environment=investment_environment,
            key_drivers=key_drivers
        )
    
    async def _build_portfolio_guidance(
        self, 
        coin: CoingeckoCoinDetails, 
        risk: RiskAnalysis
    ) -> PortfolioGuidance:
        """포트폴리오 배분 가이드"""
        
        # 위험도에 따른 배분 권장
        if risk.overall_risk_score == "낮음":
            conservative = "10-15%"
            moderate = "15-25%"
            aggressive = "25-40%"
        elif risk.overall_risk_score == "보통":
            conservative = "5-10%"
            moderate = "10-20%"
            aggressive = "20-35%"
        else:  # 높음
            conservative = "2-5%"
            moderate = "5-10%"
            aggressive = "10-25%"
        
        # 투자 전략 권장
        strategies = ["정기 적립 투자 (DCA)"]
        
        if coin.market_cap_rank and coin.market_cap_rank <= 10:
            strategies.append("장기 보유 전략")
            time_horizon = "3-5년 이상"
        else:
            strategies.append("단기-중기 거래")
            time_horizon = "6개월-2년"
        
        if risk.volatility_risk == "높음":
            strategies.append("변동성 관리 필수")
        
        return PortfolioGuidance(
            conservative_allocation=conservative,
            moderate_allocation=moderate,
            aggressive_allocation=aggressive,
            investment_strategies=strategies,
            time_horizon=time_horizon
        )