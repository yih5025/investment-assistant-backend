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
    InvestmentOpportunity, PortfolioGuidance, DetailedKimchiPremiumResponse, ExchangeComparisonData
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
    
    async def get_detailed_kimchi_premium(self, symbol: str, sort_by: str = "premium_desc", min_volume: float = 0) -> Optional[Dict]:
        """거래소별 상세 김치 프리미엄 분석 (정렬, 필터링 포함)"""
        
        # 1. 모든 거래소 데이터 조회 (summary 체크 제거)
        all_tickers = await self._get_all_tickers_for_symbol(symbol)
        if not all_tickers:
            return None
        
        # 3. 국내/해외 거래소 분리
        korean_tickers = [t for t in all_tickers if t.exchange_id in ['bithumb', 'upbit']]
        global_tickers = [t for t in all_tickers if t.exchange_id not in ['bithumb', 'upbit']]
        
        if not korean_tickers or not global_tickers:
            return None
        
        # 4. 거래소별 상세 비교 계산
        exchange_comparisons = []
        
        for korean_ticker in korean_tickers:
            for global_ticker in global_tickers:
                # 최소 거래량 필터 적용
                if (korean_ticker.converted_volume_usd < min_volume and 
                    global_ticker.converted_volume_usd < min_volume):
                    continue
                    
                korean_price = float(korean_ticker.converted_last_usd)
                global_price = float(global_ticker.converted_last_usd)
                
                # 김치 프리미엄 계산
                premium_percent = ((korean_price - global_price) / global_price) * 100
                price_diff = korean_price - global_price
                
                # 거래량 비율 계산
                volume_ratio = (korean_ticker.converted_volume_usd / global_ticker.converted_volume_usd 
                            if global_ticker.converted_volume_usd > 0 else 0)
                
                exchange_comparisons.append({
                    "korean_exchange": korean_ticker.exchange_id,
                    "korean_price_usd": round(korean_price, 2),
                    "korean_volume_usd": korean_ticker.converted_volume_usd,
                    "korean_spread": korean_ticker.bid_ask_spread_percentage,
                    
                    "global_exchange": global_ticker.exchange_id,
                    "global_price_usd": round(global_price, 2),
                    "global_volume_usd": global_ticker.converted_volume_usd,
                    "global_spread": global_ticker.bid_ask_spread_percentage,
                    
                    "premium_percentage": round(premium_percent, 3),
                    "price_diff_usd": round(price_diff, 2),
                    "volume_ratio": round(volume_ratio, 3)
                })
        
        # 5. 정렬 적용
        if sort_by == "premium_desc":
            exchange_comparisons.sort(key=lambda x: x["premium_percentage"], reverse=True)
        elif sort_by == "premium_asc":
            exchange_comparisons.sort(key=lambda x: x["premium_percentage"], reverse=False)
        elif sort_by == "volume_desc":
            exchange_comparisons.sort(
                key=lambda x: max(x["korean_volume_usd"], x["global_volume_usd"]), reverse=True
            )
        elif sort_by == "volume_asc":
            exchange_comparisons.sort(
                key=lambda x: max(x["korean_volume_usd"], x["global_volume_usd"]), reverse=False
            )
        
        # 6. 통계 정보 계산
        if exchange_comparisons:
            premiums = [comp["premium_percentage"] for comp in exchange_comparisons]
            
            statistics = {
                "total_comparisons": len(exchange_comparisons),
                "korean_exchanges_count": len(korean_tickers),
                "global_exchanges_count": len(global_tickers),
                "filters_applied": {
                    "sort_by": sort_by,
                    "min_volume": min_volume
                },
                "premium_stats": {
                    "average": round(sum(premiums) / len(premiums), 3),
                    "max": round(max(premiums), 3),
                    "min": round(min(premiums), 3),
                    "positive_count": len([p for p in premiums if p > 0]),
                    "negative_count": len([p for p in premiums if p < 0])
                },
                "volume_stats": {
                    "total_korean_volume": sum(t.converted_volume_usd for t in korean_tickers),
                    "total_global_volume": sum(t.converted_volume_usd for t in global_tickers)
                }
            }
        else:
            statistics = {
                "total_comparisons": 0,
                "message": f"No data found with min_volume >= {min_volume}"
            }
        
        return {
            "symbol": symbol.upper(),
            "timestamp": datetime.utcnow().isoformat(),
            "summary": "Summary removed for debugging",  # 임시로 간단한 값
            "exchange_comparisons": exchange_comparisons,
            "statistics": statistics
        }

    async def _get_all_tickers_for_symbol(self, symbol: str):
        """특정 심볼의 모든 거래소 티커 데이터 조회 - 거래소별 최신 데이터 1개씩"""
        
        # 한국 거래소와 해외 거래소를 각각 따로 조회 (더 안전한 방법)
        
        # 1. 한국 거래소 최신 데이터 조회
        korean_tickers = self.db.query(CoingeckoTickers).filter(
            and_(
                func.upper(CoingeckoTickers.symbol) == symbol.upper(),
                CoingeckoTickers.exchange_id.in_(['upbit', 'bithumb']),
                CoingeckoTickers.converted_last_usd.isnot(None),
                CoingeckoTickers.converted_volume_usd > 0
            )
        ).order_by(desc(CoingeckoTickers.created_at)).limit(50).all()
        
        # 2. 해외 거래소 최신 데이터 조회  
        global_tickers = self.db.query(CoingeckoTickers).filter(
            and_(
                func.upper(CoingeckoTickers.symbol) == symbol.upper(),
                ~CoingeckoTickers.exchange_id.in_(['upbit', 'bithumb']),
                CoingeckoTickers.converted_last_usd.isnot(None),
                CoingeckoTickers.converted_volume_usd > 0
            )
        ).order_by(desc(CoingeckoTickers.created_at)).limit(100).all()
        
        # 3. 합치고 거래소별로 가장 최신 것만 선택
        all_tickers = korean_tickers + global_tickers
        exchange_latest = {}
        for ticker in all_tickers:
            if ticker.exchange_id not in exchange_latest:
                exchange_latest[ticker.exchange_id] = ticker
        
        return list(exchange_latest.values())
    
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
    
    # app/services/crypto_detail_investment_service.py 의 _analyze_kimchi_premium 메서드 수정

    async def _analyze_kimchi_premium(self, symbol: str) -> KimchiPremiumData:
        """김치 프리미엄 분석 - 수치 그대로 반환"""
        
        # 거래소별 최신 데이터 조회 (윈도우 함수 사용)
        all_tickers = await self._get_all_tickers_for_symbol(symbol)
        
        print(f"🔍 DEBUG: {symbol} - all_tickers count: {len(all_tickers) if all_tickers else 0}")
        
        if not all_tickers:
            print(f"❌ DEBUG: {symbol} - No tickers found")
            return KimchiPremiumData()
        
        # 국내/해외 거래소 분리
        korean_tickers = [t for t in all_tickers if t.exchange_id in ['bithumb', 'upbit']]
        global_tickers = [t for t in all_tickers if t.exchange_id not in ['bithumb', 'upbit']]
        
        print(f"🔍 DEBUG: {symbol} - korean_tickers: {len(korean_tickers)}, global_tickers: {len(global_tickers)}")
        
        if not korean_tickers or not global_tickers:
            print(f"❌ DEBUG: {symbol} - Missing korean or global tickers")
            return KimchiPremiumData()
        
        # 국내 거래소 중 거래량 가장 큰 거래소 선택
        try:
            korean_main = max(korean_tickers, key=lambda x: x.converted_volume_usd)
            korean_price = float(korean_main.converted_last_usd)
            korean_volume = korean_main.converted_volume_usd
            print(f"✅ DEBUG: {symbol} - korean_main: {korean_main.exchange_id}, price: {korean_price}")
        except Exception as e:
            print(f"❌ DEBUG: {symbol} - Korean ticker error: {e}")
            return KimchiPremiumData()
        
        # 해외 거래소 단순 평균 (거래량 가중평균 아님)
        try:
            global_prices = [float(t.converted_last_usd) for t in global_tickers]
            global_volumes = [t.converted_volume_usd for t in global_tickers]
            global_avg_price = sum(global_prices) / len(global_prices)
            total_global_volume = sum(global_volumes)
            print(f"✅ DEBUG: {symbol} - global_avg_price: {global_avg_price}")
        except Exception as e:
            print(f"❌ DEBUG: {symbol} - Global ticker error: {e}")
            return KimchiPremiumData()
        
        # 김치 프리미엄 계산 (수치만)
        try:
            premium_percent = ((korean_price - global_avg_price) / global_avg_price) * 100
            price_diff = korean_price - global_avg_price
            print(f"✅ DEBUG: {symbol} - premium_percent: {premium_percent}")
        except Exception as e:
            print(f"❌ DEBUG: {symbol} - Premium calculation error: {e}")
            return KimchiPremiumData()
        
        # 스프레드 계산
        try:
            korean_spread = korean_main.bid_ask_spread_percentage
            global_spreads = [t.bid_ask_spread_percentage for t in global_tickers if t.bid_ask_spread_percentage is not None]
            avg_global_spread = sum(global_spreads) / len(global_spreads) if global_spreads else None
            print(f"✅ DEBUG: {symbol} - Spread calculation success")
        except Exception as e:
            print(f"❌ DEBUG: {symbol} - Spread calculation error: {e}")
            return KimchiPremiumData()
        
        return KimchiPremiumData(
            korean_price_usd=Decimal(str(round(korean_price, 2))),
            global_avg_price_usd=Decimal(str(round(global_avg_price, 2))),
            kimchi_premium_percent=Decimal(str(round(premium_percent, 3))),
            price_diff_usd=Decimal(str(round(price_diff, 2))),
            korean_volume_usd=korean_volume,
            total_global_volume_usd=total_global_volume,
            korean_exchange=korean_main.exchange_id,
            global_exchange_count=len(global_tickers),
            korean_spread=korean_spread,
            avg_global_spread=avg_global_spread,
            arbitrage_opportunity=None,  # 판단 제거
            net_profit_per_unit=None,    # 판단 제거
            transaction_cost_estimate=None  # 판단 제거
        )
    

    async def get_kimchi_premium_with_details(self, symbol: str) -> Optional[Dict]:
        """김치 프리미엄 + 거래소별 상세 정보 조회"""
        
        # 1. 기본 김치 프리미엄 계산
        kimchi_data = await self._analyze_kimchi_premium(symbol)
        if not kimchi_data.korean_price_usd:
            return None
        
        # 2. 거래소별 상세 정보 조회
        exchange_details = await self._get_all_exchange_details(symbol)
        if not exchange_details:
            return None
        
        # 3. 국내/해외 거래소 분리
        korean_exchanges = []
        global_exchanges = []
        
        for exchange in exchange_details:
            exchange_info = {
                "exchange_id": exchange.exchange_id,
                "price_usd": float(exchange.converted_last_usd),
                "volume_24h_usd": exchange.converted_volume_usd,
                "spread_percentage": exchange.bid_ask_spread_percentage
            }
            
            if exchange.exchange_id in ['bithumb', 'upbit']:
                korean_exchanges.append(exchange_info)
            else:
                global_exchanges.append(exchange_info)
        
        return {
            "symbol": symbol.upper(),
            "kimchi_premium": kimchi_data,
            # "korean_exchanges": korean_exchanges,
            # "global_exchanges": global_exchanges,
            # "exchange_count": {
            #     "korean": len(korean_exchanges),
            #     "global": len(global_exchanges)
            # },
            # "last_updated": datetime.utcnow().isoformat()
        }

    async def _get_all_exchange_details(self, symbol: str):
        """모든 거래소 상세 정보 조회 (내부 메서드)"""
        
        # 최근 24시간 내의 데이터 조회
        from datetime import datetime, timedelta
        
        cutoff_time = datetime.utcnow() - timedelta(hours=24)
        
        exchange_details = self.db.query(
            CoingeckoTickers.exchange_id,
            CoingeckoTickers.converted_last_usd,
            CoingeckoTickers.converted_volume_usd,
            CoingeckoTickers.bid_ask_spread_percentage
        ).filter(
            and_(
                func.upper(CoingeckoTickers.symbol) == symbol.upper(),
                CoingeckoTickers.converted_last_usd.isnot(None),
                CoingeckoTickers.created_at >= cutoff_time
            )
        ).order_by(desc(CoingeckoTickers.created_at)).all()
        
        return exchange_details

    async def get_kimchi_premium_chart_data(self, symbol: str, hours: int = 24) -> Optional[Dict]:
        """김치 프리미엄 차트용 데이터 - 거래소별 시간대별 데이터"""
        
        from sqlalchemy import text
        from datetime import datetime, timedelta
        
        # 1. 현재 김치 프리미엄 분석 (거래소별 최신 데이터)
        current_analysis = await self._analyze_kimchi_premium(symbol)
        if not current_analysis.korean_price_usd:
            return None
        
        # 2. 차트용 시간대별 데이터 조회
        query = text("""
            SELECT exchange_id,
                   created_at,
                   converted_last_usd,
                   converted_volume_usd,
                   bid_ask_spread_percentage
            FROM coingecko_tickers_bithumb 
            WHERE UPPER(symbol) = UPPER(:symbol)
              AND converted_last_usd IS NOT NULL 
              AND created_at >= NOW() - INTERVAL ':hours hours'
            ORDER BY exchange_id, created_at DESC
        """)
        
        result = self.db.execute(query, {"symbol": symbol, "hours": hours})
        chart_rows = result.fetchall()
        
        if not chart_rows:
            return None
        
        # 3. 거래소별 데이터 그룹화
        korean_exchanges = {}
        global_exchanges = {}
        
        for row in chart_rows:
            exchange_id = row.exchange_id
            data_point = {
                "timestamp": row.created_at.isoformat(),
                "price_usd": float(row.converted_last_usd),
                "volume_usd": row.converted_volume_usd,
                "spread_percentage": float(row.bid_ask_spread_percentage) if row.bid_ask_spread_percentage else None
            }
            
            if exchange_id in ['upbit', 'bithumb']:
                if exchange_id not in korean_exchanges:
                    korean_exchanges[exchange_id] = []
                korean_exchanges[exchange_id].append(data_point)
            else:
                if exchange_id not in global_exchanges:
                    global_exchanges[exchange_id] = []
                global_exchanges[exchange_id].append(data_point)
        
        # 4. 김치 프리미엄 추이 계산 (시간별)
        premium_trend = self._calculate_premium_trend(chart_rows)
        
        return {
            "symbol": symbol.upper(),
            "timestamp": datetime.utcnow().isoformat(),
            "time_range_hours": hours,
            "current_analysis": {
                "korean_price_usd": float(current_analysis.korean_price_usd),
                "global_avg_price_usd": float(current_analysis.global_avg_price_usd),
                "kimchi_premium_percent": float(current_analysis.kimchi_premium_percent),
                "price_diff_usd": float(current_analysis.price_diff_usd),
                "korean_exchange": current_analysis.korean_exchange,
                "global_exchange_count": current_analysis.global_exchange_count
            },
            "chart_data": {
                "korean_exchanges": korean_exchanges,
                "global_exchanges": dict(list(global_exchanges.items())[:10]),  # 상위 10개 거래소만
                "premium_trend": premium_trend
            },
            "statistics": {
                "total_data_points": len(chart_rows),
                "korean_exchanges_count": len(korean_exchanges),
                "global_exchanges_count": len(global_exchanges),
                "data_freshness": f"last_{hours}_hours"
            }
        }

    def _calculate_premium_trend(self, chart_rows) -> List[Dict]:
        """시간별 김치 프리미엄 추이 계산"""
        from collections import defaultdict
        
        # 시간별로 데이터 그룹화
        time_groups = defaultdict(lambda: {"korean": [], "global": []})
        
        for row in chart_rows:
            timestamp = row.created_at.replace(minute=0, second=0, microsecond=0)  # 시간 단위로 그룹화
            price = float(row.converted_last_usd)
            volume = row.converted_volume_usd
            
            if row.exchange_id in ['upbit', 'bithumb']:
                time_groups[timestamp]["korean"].append({"price": price, "volume": volume})
            else:
                time_groups[timestamp]["global"].append({"price": price, "volume": volume})
        
        # 각 시간대별 김치 프리미엄 계산
        premium_trend = []
        for timestamp in sorted(time_groups.keys(), reverse=True)[:24]:  # 최근 24시간만
            korean_data = time_groups[timestamp]["korean"]
            global_data = time_groups[timestamp]["global"]
            
            if not korean_data or not global_data:
                continue
            
            # 거래량 가중평균으로 가격 계산
            korean_weighted_price = sum(d["price"] * d["volume"] for d in korean_data) / sum(d["volume"] for d in korean_data)
            global_weighted_price = sum(d["price"] * d["volume"] for d in global_data) / sum(d["volume"] for d in global_data)
            
            # 김치 프리미엄 계산
            premium_percent = ((korean_weighted_price - global_weighted_price) / global_weighted_price) * 100
            
            premium_trend.append({
                "timestamp": timestamp.isoformat(),
                "korean_price_usd": round(korean_weighted_price, 8),
                "global_price_usd": round(global_weighted_price, 8),
                "premium_percentage": round(premium_percent, 3),
                "price_diff_usd": round(korean_weighted_price - global_weighted_price, 8)
            })
        
        return sorted(premium_trend, key=lambda x: x["timestamp"])

    async def get_derivatives_analysis(self, symbol: str) -> Optional[DerivativesData]:
        """파생상품 분석 데이터만 조회 (public 메서드)"""
        return await self._analyze_derivatives(symbol)
    
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
            func.count().label('total_markets')
        ).filter(
            and_(
                CoingeckoDerivatives.batch_id == latest_batch.batch_id,
                func.upper(CoingeckoDerivatives.index_id) == symbol.upper(),
                CoingeckoDerivatives.funding_rate.isnot(None)
            )
        ).first()
        
        # 펀딩비 방향성 별도 계산
        positive_funding_count = self.db.query(
            func.count()
        ).filter(
            and_(
                CoingeckoDerivatives.batch_id == latest_batch.batch_id,
                func.upper(CoingeckoDerivatives.index_id) == symbol.upper(),
                CoingeckoDerivatives.funding_rate > 0
            )
        ).scalar() or 0
        
        negative_funding_count = self.db.query(
            func.count()
        ).filter(
            and_(
                CoingeckoDerivatives.batch_id == latest_batch.batch_id,
                func.upper(CoingeckoDerivatives.index_id) == symbol.upper(),
                CoingeckoDerivatives.funding_rate < 0
            )
        ).scalar() or 0
        
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
            positive_funding_count=positive_funding_count,
            negative_funding_count=negative_funding_count,
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