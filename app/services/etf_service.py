# app/services/etf_service.py
import logging
import json
from typing import List, Dict, Any, Optional, Union
from datetime import datetime, timedelta
import pytz
from app.config import settings
from app.database import get_db
from app.models.etf_model import ETFBasicInfo, ETFProfileHoldings, ETFRealtimePrices

logger = logging.getLogger(__name__)

class MarketTimeChecker:
    """미국 주식 시장 시간 체크 클래스 (SP500과 동일)"""
    
    def __init__(self):
        self.us_eastern = pytz.timezone('US/Eastern')
        
        # 미국 공휴일 (주식시장 휴장일)
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
        try:
            now_utc = datetime.now(pytz.UTC).replace(tzinfo=pytz.UTC)
            now_et = now_utc.astimezone(self.us_eastern)
            
            # 주말 체크
            if now_et.weekday() >= 5:  # 5=토요일, 6=일요일
                return False
            
            # 공휴일 체크
            today_str = now_et.strftime('%Y-%m-%d')
            if today_str in self.market_holidays:
                return False
            
            # 정규 거래시간: 9:30 AM - 4:00 PM ET
            market_open = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
            market_close = now_et.replace(hour=16, minute=0, second=0, microsecond=0)
            
            return market_open <= now_et <= market_close
            
        except Exception as e:
            logger.error(f"시장 시간 확인 중 오류: {e}")
            return False
    
    def get_market_status(self) -> Dict[str, Any]:
        """상세한 시장 상태 정보 반환"""
        try:
            now_utc = datetime.now(pytz.UTC).replace(tzinfo=pytz.UTC)
            now_et = now_utc.astimezone(self.us_eastern)
            
            is_open = self.is_market_open()
            
            return {
                'is_open': is_open,
                'current_time_et': now_et.strftime('%Y-%m-%d %H:%M:%S %Z'),
                'current_time_utc': now_utc.strftime('%Y-%m-%d %H:%M:%S UTC'),
                'status': 'OPEN' if is_open else 'CLOSED',
                'timezone': 'US/Eastern'
            }
            
        except Exception as e:
            logger.error(f"시장 상태 조회 오류: {e}")
            return {
                'is_open': False,
                'status': 'UNKNOWN',
                'error': str(e)
            }

class ETFService:
    """
    ETF API 전용 서비스 클래스
    
    ETF 리스트, 개별 ETF 상세 정보, 차트 데이터 등을 제공합니다.
    SP500 서비스 구조를 참고하되 ETF 특성에 맞게 최적화되었습니다.
    """
    
    def __init__(self):
        """ETFService 초기화"""
        self.market_checker = MarketTimeChecker()
        self.redis_client = None
        
        # 성능 통계
        self.stats = {
            "api_requests": 0,
            "db_queries": 0,
            "cache_hits": 0,
            "errors": 0,
            "last_request": None
        }
        
        logger.info("ETFService 초기화 완료")
    
    # =========================
    # ETF 리스트 페이지 API
    # =========================
    
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
            
            for item in redis_data:
                # Redis 데이터는 이미 딕셔너리 형태
                item_dict = item.copy() if isinstance(item, dict) else {}
                
                # 심볼과 현재가 추출
                symbol = item_dict.get('symbol', '')
                current_price = float(item_dict.get('price', 0)) if item_dict.get('price') else 0
                
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
    
    def get_etf_basic_info(self, symbol: str) -> Dict[str, Any]:
        """
        개별 ETF 기본 정보 조회 (차트 데이터 제외)
        
        Args:
            symbol: ETF 심볼 (예: 'SPY')
            
        Returns:
            Dict[str, Any]: 차트를 제외한 ETF 기본 정보
        """
        try:
            self.stats["api_requests"] += 1
            self.stats["last_request"] = datetime.now(pytz.UTC)
            
            symbol = symbol.upper()
            db = next(get_db())
            
            # ETF 기본 정보 조회
            basic_info = db.query(ETFBasicInfo).filter(ETFBasicInfo.symbol == symbol).first()
            if not basic_info:
                return {
                    'symbol': symbol,
                    'error': f'No basic info found for ETF {symbol}'
                }
            
            # 현재가 및 변동 정보 조회
            change_info = ETFRealtimePrices.get_price_change_info(db, symbol)
            
            if not change_info['current_price']:
                return {
                    'symbol': symbol,
                    'name': basic_info.name,
                    'error': f'No price data found for ETF {symbol}'
                }
            
            self.stats["db_queries"] += 1
            
            return {
                'symbol': symbol,
                'name': basic_info.name,
                'current_price': change_info['current_price'],
                'change_amount': change_info['change_amount'],
                'change_percentage': change_info['change_percentage'],
                'volume': change_info['volume'],
                'previous_close': change_info['previous_close'],
                'is_positive': change_info['change_amount'] > 0 if change_info['change_amount'] else None,
                'market_status': self.market_checker.get_market_status(),
                'last_updated': change_info['last_updated']
            }
            
        except Exception as e:
            logger.error(f"{symbol} ETF 기본 정보 조회 실패: {e}")
            self.stats["errors"] += 1
            return {
                'symbol': symbol,
                'error': str(e)
            }
        finally:
            db.close()
    
    def get_etf_profile(self, symbol: str) -> Dict[str, Any]:
        """
        ETF 프로필 및 보유종목 정보 조회
        
        Args:
            symbol: ETF 심볼 (예: 'SPY')
            
        Returns:
            Dict[str, Any]: ETF 프로필 정보 (섹터, 보유종목 등)
        """
        try:
            self.stats["api_requests"] += 1
            
            symbol = symbol.upper()
            db = next(get_db())
            
            # ETF 프로필 정보 조회
            profile = db.query(ETFProfileHoldings).filter(ETFProfileHoldings.symbol == symbol).first()
            
            if not profile:
                return {
                    'symbol': symbol,
                    'profile': None,
                    'error': f'No profile data found for ETF {symbol}'
                }
            
            # JSON 데이터 파싱
            sectors_data = []
            holdings_data = []
            
            try:
                if profile.sectors:
                    sectors_json = json.loads(profile.sectors)
                    for i, sector in enumerate(sectors_json):
                        sectors_data.append({
                            'sector': sector.get('sector', ''),
                            'weight': float(sector.get('weight', 0)),
                            'color': self._get_sector_color(i)
                        })
                
                if profile.holdings:
                    holdings_json = json.loads(profile.holdings)
                    for holding in holdings_json[:15]:  # 상위 15개만
                        holdings_data.append({
                            'symbol': holding.get('symbol', ''),
                            'description': holding.get('description', ''),
                            'weight': float(holding.get('weight', 0))
                        })
                        
            except json.JSONDecodeError as e:
                logger.error(f"{symbol} ETF JSON 파싱 실패: {e}")
                return {
                    'symbol': symbol,
                    'profile': None,
                    'error': 'Failed to parse ETF profile data'
                }
            
            self.stats["db_queries"] += 1
            
            return {
                'symbol': symbol,
                'profile': {
                    'net_assets': profile.net_assets,
                    'net_expense_ratio': profile.net_expense_ratio,
                    'portfolio_turnover': profile.portfolio_turnover,
                    'dividend_yield': profile.dividend_yield,
                    'inception_date': profile.inception_date,
                    'leveraged': profile.leveraged,
                    'sectors': sectors_data,
                    'holdings': holdings_data
                }
            }
            
        except Exception as e:
            logger.error(f"{symbol} ETF 프로필 조회 실패: {e}")
            self.stats["errors"] += 1
            return {
                'symbol': symbol,
                'profile': None,
                'error': str(e)
            }
        finally:
            db.close()
    
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
    
    def get_etf_detail_complete(self, symbol: str, timeframe: str = '1D') -> Dict[str, Any]:
        """
        ETF 상세 페이지용 완전한 정보 조회 (기본 정보 + 프로필 + 차트)
        
        Args:
            symbol: ETF 심볼 (예: 'SPY')
            timeframe: 차트 시간대 ('1D', '1W', '1M')
            
        Returns:
            Dict[str, Any]: ETF 상세 페이지 전체 정보
        """
        try:
            # 기본 정보 조회
            basic_info = self.get_etf_basic_info(symbol)
            if basic_info.get('error'):
                return basic_info
            
            # 프로필 정보 조회
            profile_info = self.get_etf_profile(symbol)
            
            # 차트 데이터 조회
            chart_info = self.get_chart_data_only(symbol, timeframe)
            
            # 결과 조합
            result = {
                'basic_info': basic_info,
                'profile': profile_info.get('profile'),
                'chart_data': chart_info.get('chart_data', []),
                'timeframe': timeframe,
                'last_updated': datetime.now(pytz.UTC).isoformat()
            }
            
            # 프론트엔드용 차트 데이터 생성
            if profile_info.get('profile'):
                profile = profile_info['profile']
                
                # 섹터 파이차트 데이터
                result['sector_chart_data'] = self._format_sector_chart_data(profile.get('sectors', []))
                
                # 보유종목 막대그래프 데이터
                result['holdings_chart_data'] = self._format_holdings_chart_data(profile.get('holdings', []))
                
                # 주요 지표 포맷팅
                result['key_metrics'] = self._format_key_metrics(profile)
            
            return result
            
        except Exception as e:
            logger.error(f"{symbol} ETF 상세 정보 조회 실패: {e}")
            self.stats["errors"] += 1
            return {
                'symbol': symbol,
                'error': str(e)
            }

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
    
    def _format_timestamp_by_timeframe(self, dt: datetime, timeframe: str) -> str:
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
    
    def _format_sector_chart_data(self, sectors: List[Dict]) -> List[Dict[str, Any]]:
        """섹터 데이터를 파이차트용으로 포맷"""
        return [{
            'name': sector['sector'].replace('_', ' ').title(),
            'value': sector['weight'] * 100,
            'color': sector.get('color', '#60a5fa')
        } for sector in sectors]
    
    def _format_holdings_chart_data(self, holdings: List[Dict]) -> List[Dict[str, Any]]:
        """보유종목 데이터를 막대그래프용으로 포맷"""
        return [{
            'symbol': holding['symbol'],
            'name': holding['description'] or holding['symbol'],
            'weight': holding['weight'] * 100
        } for holding in holdings[:10]]  # 상위 10개만
    
    def _format_key_metrics(self, profile: Dict) -> Dict[str, Any]:
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