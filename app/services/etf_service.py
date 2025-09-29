# app/services/etf_service.py

import logging
import json
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import pytz
from sqlalchemy import func
from sqlalchemy.orm import Session

# --- [수정] 필요한 의존성 임포트 ---
from app.database import get_db
from app.models.etf_model import ETFBasicInfo, ETFProfileHoldings, ETFRealtimePrices
from app.schemas import etf_schema  # Pydantic 스키마를 직접 사용

logger = logging.getLogger(__name__)

class MarketTimeChecker:
    """미국 주식 시장 시간 체크 클래스"""
    
    def __init__(self):
        self.us_eastern = pytz.timezone('US/Eastern')
        self.market_holidays = {
            '2024-01-01', '2024-01-15', '2024-02-19', '2024-03-29',
            '2024-05-27', '2024-06-19', '2024-07-04', '2024-09-02',
            '2024-11-28', '2024-12-25',
            '2025-01-01', '2025-01-20', '2025-02-17', '2025-04-18',
            '2025-05-26', '2025-06-19', '2025-07-04', '2025-09-01',
            '2025-11-27', '2025-12-25'
        }
    
    def is_market_open(self) -> bool:
        """현재 미국 주식 시장이 열려있는지 확인"""
        now_et = datetime.now(pytz.utc).astimezone(self.us_eastern)
        if now_et.weekday() >= 5 or now_et.strftime('%Y-%m-%d') in self.market_holidays:
            return False
        market_open = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
        market_close = now_et.replace(hour=16, minute=0, second=0, microsecond=0)
        return market_open <= now_et <= market_close

    def get_market_status(self) -> Dict[str, Any]:
        """상세한 시장 상태 정보 반환"""
        now_utc = datetime.now(pytz.utc)
        now_et = now_utc.astimezone(self.us_eastern)
        is_open = self.is_market_open()
        return {
            'is_open': is_open,
            'current_time_et': now_et.strftime('%Y-%m-%d %H:%M:%S %Z'),
            'current_time_utc': now_utc.strftime('%Y-%m-%d %H:%M:%S UTC'),
            'status': 'OPEN' if is_open else 'CLOSED',
            'timezone': 'US/Eastern'
        }

class ETFService:
    def __init__(self):
        self.market_checker = MarketTimeChecker()
        # 서비스가 초기화될 때 DB 세션을 받지 않도록 수정
        # 각 함수 내에서 필요할 때 get_db()를 통해 세션을 얻음
        self.redis_client = None
        self.stats = {
            "api_requests": 0, "db_queries": 0, "cache_hits": 0,
            "errors": 0, "last_request": None
        }
        logger.info("ETFService 초기화 완료")


    # --- ▼ [신규/수정] 상세 정보 조회를 위한 단일 통합 함수 ---
    def get_etf_details_by_symbol(self, symbol: str) -> Optional[etf_schema.ETFDetailResponse]:
        """
        특정 ETF 심볼에 대한 모든 상세 정보를 조회하고, Pydantic 스키마 객체로 반환합니다.
        이 함수 하나로 기본 정보, 프로필, 변동률 계산, 차트 데이터 생성을 모두 처리합니다.
        """
        db: Session = next(get_db())
        try:
            symbol_upper = symbol.upper()
            
            # 1. DB에서 필요한 모든 데이터를 각각 조회
            basic_info_model = db.query(ETFBasicInfo).filter(ETFBasicInfo.symbol == symbol_upper).first()
            latest_price_model = db.query(ETFRealtimePrices).filter(ETFRealtimePrices.symbol == symbol_upper).order_by(ETFRealtimePrices.timestamp_ms.desc()).first()
            profile_model = db.query(ETFProfileHoldings).filter(ETFProfileHoldings.symbol == symbol_upper).first()

            if not basic_info_model or not latest_price_model:
                logger.warning(f"기본 정보 또는 실시간 가격 정보가 없어 상세 데이터를 반환할 수 없습니다: {symbol_upper}")
                return None

            # 2. 전일 종가 안정적으로 계산
            previous_close = self._get_robust_previous_close_price(db, symbol_upper, latest_price_model.created_at)

            # 3. 변동률 계산
            change_amount, change_percentage, is_positive = None, None, None
            if previous_close is not None and latest_price_model.price is not None:
                change_amount = latest_price_model.price - previous_close
                change_percentage = (change_amount / previous_close) * 100 if previous_close != 0 else 0
                is_positive = change_amount >= 0
            
            # 4. Pydantic 스키마 객체 생성 (기본 정보)
            basic_info_schema = etf_schema.ETFInfo(
                symbol=basic_info_model.symbol,
                name=basic_info_model.name,
                current_price=latest_price_model.price,
                change_amount=round(change_amount, 2) if change_amount is not None else None,
                change_percentage=round(change_percentage, 2) if change_percentage is not None else None,
                volume=latest_price_model.volume,
                previous_close=previous_close,
                is_positive=is_positive,
                last_updated=latest_price_model.created_at.isoformat() if latest_price_model.created_at else None
            )

            # 5. 프로필 정보 및 파생 데이터(차트, 지표) 스키마 생성
            profile_schema, sector_chart_data, holdings_chart_data, key_metrics = None, None, None, None
            if profile_model:
                profile_schema, sector_chart_data, holdings_chart_data, key_metrics = self._parse_profile_to_schemas(profile_model)

            # 6. 최종 응답 스키마 조합 후 반환
            return etf_schema.ETFDetailResponse(
                basic_info=basic_info_schema,
                profile=profile_schema,
                sector_chart_data=sector_chart_data,
                holdings_chart_data=holdings_chart_data,
                key_metrics=key_metrics,
                last_updated=datetime.now(pytz.utc)
            )

        except Exception as e:
            logger.error(f"{symbol} ETF 상세 정보 조회 중 심각한 오류 발생: {e}", exc_info=True)
            # 에러 발생 시 None을 반환하여 엔드포인트에서 404 또는 500 처리
            return None
        finally:
            db.close()

    def _get_robust_previous_close_price(self, db: Session, symbol: str, current_timestamp_utc: datetime) -> Optional[float]:
        """안정적으로 전일 종가를 조회 (주말/공휴일 처리)"""
        et_tz = pytz.timezone('US/Eastern')
        current_et_time = current_timestamp_utc.astimezone(et_tz)
        lookup_date = current_et_time.date() - timedelta(days=1)
        
        # 주말이거나 공휴일이면 유효한 마지막 거래일을 찾을 때까지 하루씩 이전으로 이동
        while lookup_date.weekday() >= 5 or lookup_date.strftime('%Y-%m-%d') in self.market_checker.market_holidays:
            lookup_date -= timedelta(days=1)
        
        # 해당 날짜의 마지막 거래 기록(가격)을 찾음
        previous_close_record = db.query(ETFRealtimePrices.price)\
            .filter(ETFRealtimePrices.symbol == symbol)\
            .filter(func.date(ETFRealtimePrices.created_at.op('AT TIME ZONE')('UTC').op('AT TIME ZONE')('US/Eastern')) == lookup_date)\
            .order_by(ETFRealtimePrices.timestamp_ms.desc())\
            .first()

        return previous_close_record[0] if previous_close_record else None

    def _parse_profile_to_schemas(self, profile: ETFProfileHoldings):
        """DB 모델을 받아서 여러 Pydantic 스키마로 변환하여 반환"""
        try:
            # sectors가 이미 리스트인 경우 그대로 사용, 문자열인 경우 JSON 파싱
            if isinstance(profile.sectors, list):
                sectors = profile.sectors
            elif isinstance(profile.sectors, str) and profile.sectors:
                sectors = json.loads(profile.sectors)
            else:
                sectors = []
            
            # holdings도 동일하게 처리
            if isinstance(profile.holdings, list):
                holdings = profile.holdings
            elif isinstance(profile.holdings, str) and profile.holdings:
                holdings = json.loads(profile.holdings)
            else:
                holdings = []
        except (json.JSONDecodeError, TypeError) as e:
            self.logger.warning(f"JSON 파싱 오류: {e}, 빈 리스트로 대체")
            sectors, holdings = [], []

        # ETF 이름 조회 (_get_batch_etf_names와 동일한 로직)
        etf_names = self._get_etf_names_sync([profile.symbol])
        etf_name = etf_names.get(profile.symbol, profile.symbol)
        
        profile_schema = etf_schema.ETFProfile(
            symbol=profile.symbol, name=etf_name, net_assets=profile.net_assets,
            net_expense_ratio=profile.net_expense_ratio, portfolio_turnover=profile.portfolio_turnover,
            dividend_yield=profile.dividend_yield, 
            inception_date=profile.inception_date.isoformat() if profile.inception_date else None,
            leveraged=profile.leveraged, sectors=sectors, holdings=holdings
        )
        
        sector_chart_data = [etf_schema.SectorChartData(name=s.get('sector', 'N/A'), value=float(s.get('weight', 0))*100, color=self._get_sector_color(i)) for i, s in enumerate(sectors)]
        holdings_chart_data = [etf_schema.HoldingChartData(symbol=h.get('symbol', 'N/A'), name=h.get('description', 'N/A'), weight=float(h.get('weight', 0))*100) for h in holdings[:10]] # 상위 10개만
        
        key_metrics = etf_schema.KeyMetrics(
            net_assets=etf_schema.format_currency(profile.net_assets),
            net_expense_ratio=etf_schema.format_percentage(profile.net_expense_ratio),
            dividend_yield=etf_schema.format_percentage(profile.dividend_yield),
            inception_year=etf_schema.format_date(profile.inception_date.isoformat() if profile.inception_date else None)
        )
        
        return profile_schema, sector_chart_data, holdings_chart_data, key_metrics

    
    async def get_realtime_polling_data(self, limit: int, sort_by: str = "price", order: str = "desc"):
        """
        ETF 실시간 폴링 데이터 (더보기 방식) + 변화율 계산
        """
        try:
            # Redis 클라이언트가 없으면 초기화
            if not self.redis_client:
                await self.init_redis()
            
            # Redis에서 기본 데이터 조회 후 변화율 직접 계산
            redis_data = await self.get_etf_from_redis(limit=limit*2)
            
            if not redis_data:
                logger.warning("Redis ETF 데이터 없음, DB fallback")
                return await self._get_db_polling_data_with_changes(limit, sort_by, order)
            
            # 실제 변화율 계산 로직
            all_data = []
            
            # 심볼 리스트 추출
            symbols = []
            for item in redis_data:
                if isinstance(item, dict) and 'symbol' in item:
                    symbols.append(item['symbol'])
            
            # 전날 종가 일괄 조회
            previous_close_prices = await self._get_batch_previous_close_prices(symbols)
            
            # ETF 이름 일괄 조회
            etf_names = await self._get_batch_etf_names(symbols)
            
            for item in redis_data:
                # Redis 데이터는 이미 딕셔너리 형태
                item_dict = item.copy() if isinstance(item, dict) else {}
                
                # 심볼과 현재가 추출
                symbol = item_dict.get('symbol', '')
                current_price = float(item_dict.get('price', 0)) if item_dict.get('price') else 0
                
                # ETF 이름 추가
                etf_name = etf_names.get(symbol, symbol)  # 이름이 없으면 심볼 사용
                item_dict['name'] = etf_name
                
                # 전날 종가 조회
                previous_close = previous_close_prices.get(symbol)
                
                if previous_close and previous_close > 0:
                    # 실제 변화율 계산
                    change_amount = current_price - previous_close
                    change_percentage = (change_amount / previous_close) * 100
                    
                    item_dict.update({
                        'current_price': current_price,
                        'previous_close': previous_close,
                        'change_amount': round(change_amount, 2),
                        'change_percentage': round(change_percentage, 2),
                        'is_positive': change_amount > 0,
                        'change_color': 'green' if change_amount > 0 else 'red' if change_amount < 0 else 'gray'
                    })
                else:
                    # 전날 종가 없는 경우 기본값
                    item_dict.update({
                        'current_price': current_price,
                        'previous_close': None,
                        'change_amount': None,
                        'change_percentage': None,
                        'is_positive': None,
                        'change_color': 'gray'
                    })
                
                all_data.append(item_dict)
            
            # 정렬 처리 (ETF는 price 기준만)
            if sort_by == "price":
                all_data.sort(key=lambda x: x.get('current_price', 0), reverse=(order == "desc"))
            elif sort_by == "change_percent":
                all_data.sort(key=lambda x: x.get('change_percentage', 0), reverse=(order == "desc"))
            
            # 순위 추가
            for i, item in enumerate(all_data):
                item['rank'] = i + 1
            
            # limit만큼 자르기
            limited_data = all_data[:limit]
            total_available = len(all_data)
            
            return {
                "data": limited_data,
                "metadata": {
                    "current_count": len(limited_data),
                    "total_available": total_available,
                    "has_more": limit < total_available,
                    "next_limit": min(limit + 50, total_available),
                    "timestamp": datetime.now(pytz.UTC).isoformat(),
                    "data_source": "redis_realtime_with_changes",
                    "market_status": self._get_market_status(),
                    "sort_info": {"sort_by": sort_by, "order": order},
                    "features": ["real_time_prices", "change_calculation", "previous_close_comparison"]
                }
            }
            
        except Exception as e:
            logger.error(f"ETF 실시간 폴링 데이터 조회 실패: {e}")
            return {"error": str(e)}

    async def _get_db_polling_data_with_changes(self, limit: int, sort_by: str, order: str):
        """DB fallback 시 ETF 데이터 조회 (변화율 포함)"""
        try:
            # ETF 기본 리스트 조회
            etf_list_result = self.get_etf_list(limit)
            
            if etf_list_result.get('error'):
                return {"error": etf_list_result['error']}
            
            etfs = etf_list_result.get('etfs', [])
            
            # 정렬 처리
            if sort_by == "price":
                etfs.sort(key=lambda x: x.get('current_price', 0), reverse=(order == "desc"))
            elif sort_by == "change_percent":
                etfs.sort(key=lambda x: x.get('change_percentage', 0), reverse=(order == "desc"))
            
            # 순위 추가
            for i, etf in enumerate(etfs):
                etf['rank'] = i + 1
            
            return {
                "data": etfs[:limit],
                "metadata": {
                    "current_count": len(etfs[:limit]),
                    "total_available": len(etfs),
                    "has_more": limit < len(etfs),
                    "next_limit": min(limit + 50, len(etfs)),
                    "timestamp": datetime.now(pytz.UTC).isoformat(),
                    "data_source": "database_with_changes",
                    "market_status": etf_list_result.get('market_status'),
                    "sort_info": {"sort_by": sort_by, "order": order}
                }
            }
            
        except Exception as e:
            logger.error(f"DB fallback 폴링 데이터 조회 실패: {e}")
            return {"error": str(e)}

    async def _get_batch_previous_close_prices(self, symbols: List[str]) -> Dict[str, float]:
        """
        여러 ETF 심볼의 전날 종가를 일괄 조회 (성능 최적화)
        
        Args:
            symbols: ETF 심볼 리스트
            
        Returns:
            Dict[str, float]: {symbol: previous_close_price}
        """
        try:
            db = next(get_db())
            previous_close_prices = {}
            
            # ETF 모델을 사용하여 각 심볼의 전날 종가 조회
            for symbol in symbols:
                prev_close = ETFRealtimePrices.get_previous_close_price(db, symbol)
                if prev_close:
                    previous_close_prices[symbol] = prev_close
            
            logger.debug(f"ETF 전날 종가 조회 완료: {len(previous_close_prices)}개 / {len(symbols)}개")
            return previous_close_prices
            
        except Exception as e:
            logger.error(f"ETF 전날 종가 일괄 조회 실패: {e}")
            return {}
        finally:
            if 'db' in locals():
                db.close()

    async def _get_batch_etf_names(self, symbols: List[str]) -> Dict[str, str]:
        """
        여러 ETF 심볼의 이름을 일괄 조회 (성능 최적화)
        
        Args:
            symbols: ETF 심볼 리스트
            
        Returns:
            Dict[str, str]: {symbol: etf_name}
        """
        try:
            db = next(get_db())
            etf_names = {}
            
            # ETFBasicInfo에서 심볼과 이름 일괄 조회
            etf_infos = db.query(ETFBasicInfo).filter(
                ETFBasicInfo.symbol.in_(symbols)
            ).all()
            
            for etf_info in etf_infos:
                etf_names[etf_info.symbol] = etf_info.name
            
            logger.debug(f"ETF 이름 조회 완료: {len(etf_names)}개 / {len(symbols)}개")
            return etf_names
            
        except Exception as e:
            logger.error(f"ETF 이름 일괄 조회 실패: {e}")
            return {}
        finally:
            if 'db' in locals():
                db.close()

    def _get_etf_names_sync(self, symbols: List[str]) -> Dict[str, str]:
        """
        여러 ETF 심볼의 이름을 일괄 조회 (동기 버전)
        
        Args:
            symbols: ETF 심볼 리스트
            
        Returns:
            Dict[str, str]: {symbol: etf_name}
        """
        try:
            etf_names = {}
            
            # ETFBasicInfo에서 심볼과 이름 일괄 조회
            etf_infos = self.db.query(ETFBasicInfo).filter(
                ETFBasicInfo.symbol.in_(symbols)
            ).all()
            
            for etf_info in etf_infos:
                etf_names[etf_info.symbol] = etf_info.name
            
            logger.debug(f"ETF 이름 조회 완료: {len(etf_names)}개 / {len(symbols)}개")
            return etf_names
            
        except Exception as e:
            logger.error(f"ETF 이름 조회 실패: {e}")
            return {}

    def get_etf_list(self, limit: int = 100) -> Dict[str, Any]:
        """
        ETF 리스트 페이지용 전체 ETF 현재가 조회
        """
        try:
            self.stats["api_requests"] += 1
            self.stats["last_request"] = datetime.now(pytz.UTC)
            
            db = next(get_db())
            
            # ETF 기본 정보와 실시간 가격 조회
            basic_infos = db.query(ETFBasicInfo).limit(limit * 2).all()
            
            if not basic_infos:
                logger.warning("ETF 기본 데이터 없음")
                return {
                    'etfs': [],
                    'total_count': 0,
                    'market_status': self.market_checker.get_market_status(),
                    'message': 'No ETF data available'
                }
            
            # 심볼 리스트 추출
            symbols = [etf.symbol for etf in basic_infos]
            
            # 배치 쿼리로 성능 최적화
            batch_change_info = ETFRealtimePrices.get_batch_price_changes(db, symbols)
            
            logger.info(f"ETF 배치 처리 완료: {len(batch_change_info)}/{len(symbols)}개 심볼")
            
            # 각 ETF의 변동 정보 조합
            etf_list = []
            for basic_info in basic_infos:
                symbol = basic_info.symbol
                change_info = batch_change_info.get(symbol)
                
                if change_info:
                    # 프론트엔드 형태로 데이터 구성
                    etf_item = {
                        'symbol': symbol,
                        'name': basic_info.name,
                        'current_price': change_info['current_price'],
                        'change_amount': change_info['change_amount'],
                        'change_percentage': change_info['change_percentage'],
                        'volume': change_info['volume'],
                        'last_updated': change_info['last_updated'],
                        'is_positive': change_info['change_amount'] > 0 if change_info['change_amount'] else None
                    }
                    etf_list.append(etf_item)
                else:
                    # 배치에서 누락된 경우 기본값으로 처리
                    logger.warning(f"{symbol} ETF 배치 처리에서 누락됨")
                    etf_item = {
                        'symbol': symbol,
                        'name': basic_info.name,
                        'current_price': 0,
                        'change_amount': 0,
                        'change_percentage': 0,
                        'volume': 0,
                        'last_updated': None,
                        'is_positive': None
                    }
                    etf_list.append(etf_item)
            
            # 가격 기준 정렬
            etf_list.sort(key=lambda x: x['current_price'] or 0, reverse=True)
            
            self.stats["db_queries"] += 1
            
            return {
                'etfs': etf_list,
                'total_count': len(etf_list),
                'market_status': self.market_checker.get_market_status(),
                'last_updated': datetime.now(pytz.UTC).isoformat(),
                'message': f'Successfully retrieved {len(etf_list)} ETFs'
            }
            
        except Exception as e:
            logger.error(f"ETF 리스트 조회 실패: {e}")
            self.stats["errors"] += 1
            return {
                'etfs': [],
                'total_count': 0,
                'market_status': self.market_checker.get_market_status(),
                'error': str(e)
            }
        finally:
            db.close()

    # =========================
    # ETF 개별 상세 정보 API
    # =========================
    
    def get_chart_data_only(self, symbol: str, timeframe: str = '1D') -> Dict[str, Any]:
        """
        ETF 차트 데이터만 조회
        
        Args:
            symbol: ETF 심볼 (예: 'SPY')
            timeframe: 차트 시간대 ('1D', '1W', '1M')
            
        Returns:
            Dict[str, Any]: 차트 데이터만 포함된 응답
        """
        try:
            self.stats["api_requests"] += 1
            self.stats["last_request"] = datetime.now(pytz.UTC)
            
            symbol = symbol.upper()
            db = next(get_db())
            
            # 차트 데이터 조회
            chart_data = ETFRealtimePrices.get_chart_data_by_timeframe(
                db, symbol, timeframe, limit=200
            )
            
            if not chart_data:
                return {
                    'symbol': symbol,
                    'timeframe': timeframe,
                    'chart_data': [],
                    'error': f'No chart data found for ETF {symbol}'
                }
            
            # 차트 데이터 포맷 변환 (프론트엔드용)
            formatted_chart_data = []
            for trade in chart_data:
                # 시간대별 timestamp 포맷팅
                formatted_timestamp = self._format_timestamp_by_timeframe(trade.created_at, timeframe)
                
                formatted_chart_data.append({
                    'timestamp': formatted_timestamp,
                    'price': float(trade.price),
                    'volume': trade.volume,
                    'datetime': trade.created_at.isoformat(),
                    'raw_timestamp': trade.timestamp_ms
                })
            
            self.stats["db_queries"] += 1
            
            return {
                'symbol': symbol,
                'timeframe': timeframe,
                'chart_data': formatted_chart_data,
                'data_points': len(formatted_chart_data),
                'market_status': self.market_checker.get_market_status(),
                'last_updated': datetime.now(pytz.UTC).isoformat()
            }
            
        except Exception as e:
            logger.error(f"{symbol} ETF 차트 데이터 조회 실패: {e}")
            self.stats["errors"] += 1
            return {
                'symbol': symbol,
                'timeframe': timeframe,
                'chart_data': [],
                'error': str(e)
            }
        finally:
            db.close()
    


    # =========================
    # 검색 API
    # =========================
    
    def search_etfs(self, query: str, limit: int = 20) -> Dict[str, Any]:
        """
        ETF 검색 (심볼 기준만)
        
        Args:
            query: 검색어 (심볼)
            limit: 반환할 최대 개수
            
        Returns:
            Dict[str, Any]: 검색 결과
        """
        try:
            self.stats["api_requests"] += 1
            
            db = next(get_db())
            
            query_upper = query.upper()
            
            # ETF 기본 정보에서 심볼로 검색
            matching_etfs = db.query(ETFBasicInfo).filter(
                ETFBasicInfo.symbol.like(f'%{query_upper}%')
            ).limit(limit).all()
            
            if not matching_etfs:
                return {
                    'query': query,
                    'results': [],
                    'total_count': 0,
                    'message': f'No ETFs found matching "{query}"'
                }
            
            # 검색 결과에 가격 정보 추가
            search_results = []
            for etf in matching_etfs:
                change_info = ETFRealtimePrices.get_price_change_info(db, etf.symbol)
                
                etf_data = {
                    'symbol': etf.symbol,
                    'name': etf.name,
                    'current_price': change_info['current_price'],
                    'change_amount': change_info['change_amount'],
                    'change_percentage': change_info['change_percentage'],
                    'volume': change_info['volume']
                }
                search_results.append(etf_data)
            
            # 심볼 알파벳 순 정렬
            search_results.sort(key=lambda x: x['symbol'])
            
            return {
                'query': query,
                'results': search_results,
                'total_count': len(search_results),
                'message': f'Found {len(search_results)} ETFs matching "{query}"'
            }
            
        except Exception as e:
            logger.error(f"ETF 검색 실패 ({query}): {e}")
            self.stats["errors"] += 1
            return {
                'query': query,
                'results': [],
                'total_count': 0,
                'error': str(e)
            }
        finally:
            db.close()

    # ========================= 
    # 시장 요약 정보 API
    # =========================
    
    def get_market_overview(self) -> Dict[str, Any]:
        """
        전체 ETF 시장 개요 조회
        
        Returns:
            Dict[str, Any]: ETF 시장 개요 정보
        """
        try:
            self.stats["api_requests"] += 1
            
            db = next(get_db())
            
            # 기본 시장 요약 정보
            market_summary = ETFRealtimePrices.get_market_summary(db)
            
            return {
                'market_summary': market_summary,
                'market_status': self.market_checker.get_market_status(),
                'last_updated': datetime.now(pytz.UTC).isoformat()
            }
            
        except Exception as e:
            logger.error(f"ETF 시장 개요 조회 실패: {e}")
            self.stats["errors"] += 1
            return {
                'market_summary': {},
                'market_status': self.market_checker.get_market_status(),
                'error': str(e)
            }
        finally:
            db.close()

    # =========================
    # 유틸리티 메서드
    # =========================
    
    def _get_market_status(self):
        """시장 상태 조회"""
        try:
            status = self.market_checker.get_market_status()
            return {
                "is_open": status["is_open"],
                "status": status["status"],
                "current_time_et": status.get("current_time_et", ""),
                "timezone": "US/Eastern"
            }
        except Exception as e:
            logger.error(f"시장 상태 조회 실패: {e}")
            return {"is_open": False, "status": "UNKNOWN", "error": str(e)}
    
    def format_timestamp_by_timeframe(self, dt: datetime, timeframe: str) -> str:
        """시간대별로 적절한 timestamp 포맷 생성"""
        if timeframe == '1D':
            return dt.strftime('%Y-%m-%d %H:%M')
        elif timeframe == '1W':
            return dt.strftime('%Y-%m-%d')
        else:  # '1M'
            return dt.strftime('%Y-%m-%d')
    
    def _get_sector_color(self, index: int) -> str:
        """섹터 차트용 색상 반환"""
        colors = [
            '#60a5fa', '#22d3ee', '#a78bfa', '#34d399', '#fbbf24',
            '#f87171', '#fb7185', '#a3a3a3', '#6b7280', '#9ca3af'
        ]
        return colors[index % len(colors)]
    
    def format_sector_chart_data(self, sectors: List[Dict]) -> List[Dict[str, Any]]:
        """섹터 데이터를 파이차트용으로 포맷"""
        return [{
            'name': sector['sector'].replace('_', ' ').title(),
            'value': sector['weight'] * 100,
            'color': sector.get('color', '#60a5fa')
        } for sector in sectors]
    
    def format_holdings_chart_data(self, holdings: List[Dict]) -> List[Dict[str, Any]]:
        """보유종목 데이터를 막대그래프용으로 포맷"""
        return [{
            'symbol': holding['symbol'],
            'name': holding['description'] or holding['symbol'],
            'weight': holding['weight'] * 100
        } for holding in holdings[:10]]  # 상위 10개만
    
    def format_key_metrics(self, profile: Dict) -> Dict[str, Any]:
        """주요 지표를 프론트엔드 표시용으로 포맷"""
        def format_number(value):
            if not value:
                return 'N/A'
            if value >= 1e12:
                return f'${value/1e12:.2f}T'
            if value >= 1e9:
                return f'${value/1e9:.2f}B'
            if value >= 1e6:
                return f'${value/1e6:.2f}M'
            return f'${value:,.0f}'
        
        def format_percentage(value):
            if not value:
                return 'N/A'
            return f'{value*100:.2f}%'
        
        return {
            'net_assets': format_number(profile.get('net_assets')),
            'net_expense_ratio': format_percentage(profile.get('net_expense_ratio')),
            'dividend_yield': format_percentage(profile.get('dividend_yield')),
            'inception_year': profile.get('inception_date', '')[:4] if profile.get('inception_date') else 'N/A'
        }

    # =========================
    # 서비스 상태 및 헬스체크
    # =========================
    
    def get_service_stats(self) -> Dict[str, Any]:
        """
        서비스 통계 정보 반환
        
        Returns:
            Dict[str, Any]: 서비스 통계
        """
        return {
            'service_name': 'ETFService',
            'stats': self.stats,
            'market_status': self.market_checker.get_market_status(),
            'uptime': datetime.now(pytz.UTC).isoformat()
        }
    
    def health_check(self) -> Dict[str, Any]:
        """
        서비스 헬스 체크
        
        Returns:
            Dict[str, Any]: 헬스 체크 결과
        """
        try:
            db = next(get_db())
            
            # 데이터베이스 연결 테스트
            result = db.execute("SELECT 1").fetchone()
            db_status = "healthy" if result else "unhealthy"
            
            # 최근 데이터 확인
            latest_data = db.execute(
                "SELECT MAX(created_at) FROM etf_realtime_prices"
            ).fetchone()
            
            data_freshness = "fresh"
            if latest_data and latest_data[0]:
                time_diff = datetime.now(pytz.UTC) - latest_data[0]
                if time_diff > timedelta(hours=1):
                    data_freshness = "stale"
            else:
                data_freshness = "no_data"
            
            return {
                'status': 'healthy',
                'database': db_status,
                'data_freshness': data_freshness,
                'market_status': self.market_checker.get_market_status(),
                'last_check': datetime.now(pytz.UTC).isoformat()
            }
            
        except Exception as e:
            logger.error(f"ETF 헬스 체크 실패: {e}")
            return {
                'status': 'unhealthy',
                'database': 'error',
                'error': str(e),
                'last_check': datetime.now(pytz.UTC).isoformat()
            }
        finally:
            db.close()

    # =========================
    # Redis 연동 메서드 추가
    # =========================

    async def init_redis(self) -> bool:
        """Redis 연결 초기화"""
        try:
            import redis.asyncio as redis
            
            self.redis_client = redis.Redis(
                host=settings.redis_host,
                port=settings.redis_port,
                db=settings.redis_db,
                password=settings.redis_password,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5
            )
            
            await self.redis_client.ping()
            logger.info("ETF Redis 연결 성공")
            return True
            
        except Exception as e:
            logger.warning(f"ETF Redis 연결 실패: {e}")
            self.redis_client = None
            return False

    async def get_etf_from_redis(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Redis에서 ETF 데이터 조회
        
        Args:
            limit: 반환할 최대 개수
            
        Returns:
            List[Dict]: ETF 데이터 리스트
        """
        if not self.redis_client:
            logger.warning("Redis 클라이언트가 없습니다. 빈 리스트 반환")
            return []
            
        try:
            # Redis 키 패턴: latest:etf:{symbol} (SP500과 다른 패턴 사용)
            pattern = "latest:etf:*"
            keys = await self.redis_client.keys(pattern)
            
            if not keys:
                logger.debug("📊 Redis ETF 데이터 없음")
                return []
            
            # 배치로 모든 키의 데이터 조회
            pipeline = self.redis_client.pipeline()
            for key in keys[:limit]:
                pipeline.get(key)
            
            results = await pipeline.execute()
            
            # JSON 파싱 및 데이터 변환
            etf_data = []
            for i, result in enumerate(results):
                if result:
                    try:
                        data = json.loads(result)
                        etf_data.append(data)
                    except json.JSONDecodeError as e:
                        logger.warning(f"ETF Redis JSON 파싱 실패 ({keys[i]}): {e}")
                        continue
            
            logger.debug(f"📊 Redis ETF 데이터 조회 완료: {len(etf_data)}개")
            return etf_data
            
        except Exception as e:
            logger.error(f"Redis ETF 데이터 조회 실패: {e}")
            return []