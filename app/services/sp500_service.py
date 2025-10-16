# app/services/sp500_service.py
import logging
import json
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import pytz
import redis
from app.config import settings
from app.database import get_db
from app.models.sp500_model import SP500WebsocketTrades

logger = logging.getLogger(__name__)

# =========================
# 시장 시간 체크 클래스
# =========================

class MarketTimeChecker:
    """미국 주식 시장 시간 체크 클래스"""
    
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
            if now_et.weekday() >= 5:
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
            logger.error(f"❌ 시장 시간 확인 중 오류: {e}")
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
            logger.error(f"❌ 시장 상태 조회 오류: {e}")
            return {
                'is_open': False,
                'status': 'UNKNOWN',
                'error': str(e)
            }

# =========================
# SP500 서비스 클래스
# =========================

class SP500Service:
    """
    SP500 API 전용 서비스 클래스
    
    주식 리스트, 개별 주식 상세 정보, 차트 데이터 등을 제공합니다.
    WebSocket Push 방식과 분리되어 API 전용 로직만 처리합니다.
    """
    
    def __init__(self, redis_client: Optional[redis.Redis] = None):
        """
        SP500Service 초기화
        
        Args:
            redis_client: Redis 클라이언트 (옵션, WebSocket용)
        """
        self.market_checker = MarketTimeChecker()
        self.redis_client = redis_client
        
        # 성능 통계
        self.stats = {
            "api_requests": 0,
            "db_queries": 0,
            "cache_hits": 0,
            "errors": 0,
            "last_request": None
        }
        
        logger.info("✅ SP500Service 초기화 완료")
    
    # =========================
    # 주식 리스트 API
    # =========================
    
    def get_stock_list(self, limit: int = 500) -> Dict[str, Any]:
        """
        주식 리스트 페이지용 전체 주식 현재가 조회
        
        Args:
            limit: 반환할 최대 개수 (기본 500)
            
        Returns:
            Dict[str, Any]: 주식 리스트
        """
        try:
            self.stats["api_requests"] += 1
            self.stats["last_request"] = datetime.now(pytz.UTC)
            
            db = next(get_db())
            
            # JOIN을 통해 현재가 + 회사정보 조회
            stock_data_with_company = SP500WebsocketTrades.get_all_current_prices_with_company_info(db, limit)
            
            if not stock_data_with_company:
                logger.warning("주식 현재가 데이터 없음")
                return {
                    'stocks': [],
                    'total_count': 0,
                    'market_status': self.market_checker.get_market_status(),
                    'message': 'No stock data available'
                }
            
            # 심볼 리스트 추출
            symbols = [stock_data['symbol'] for stock_data in stock_data_with_company]
            
            # 배치 쿼리로 성능 최적화
            batch_change_info = SP500WebsocketTrades.get_batch_price_changes(db, symbols)
            
            logger.info(f"🔄 SP500 배치 처리 완료: {len(batch_change_info)}/{len(symbols)}개")
            
            # 각 주식의 변동 정보 조합
            stock_list = []
            for stock_data in stock_data_with_company:
                symbol = stock_data['symbol']
                change_info = batch_change_info.get(symbol)
                
                if change_info:
                    stock_item = {
                        'symbol': symbol,
                        'company_name': stock_data['company_name'],
                        'current_price': change_info['current_price'],
                        'change_amount': change_info['change_amount'],
                        'change_percentage': change_info['change_percentage'],
                        'volume': change_info['volume'],
                        'last_updated': change_info['last_updated'],
                        'is_positive': change_info['change_amount'] > 0 if change_info['change_amount'] else None
                    }
                    stock_list.append(stock_item)
                else:
                    logger.warning(f"⚠️ {symbol} 배치 처리에서 누락됨")
                    stock_item = {
                        'symbol': symbol,
                        'company_name': stock_data['company_name'],
                        'current_price': 0,
                        'change_amount': 0,
                        'change_percentage': 0,
                        'volume': 0,
                        'last_updated': None,
                        'is_positive': None
                    }
                    stock_list.append(stock_item)
            
            # 변동률 기준 정렬
            stock_list.sort(key=lambda x: x['change_percentage'] or 0, reverse=True)
            
            self.stats["db_queries"] += 1
            
            return {
                'stocks': stock_list,
                'total_count': len(stock_list),
                'market_status': self.market_checker.get_market_status(),
                'last_updated': datetime.now(pytz.UTC).isoformat(),
                'message': f'Successfully retrieved {len(stock_list)} stocks'
            }
            
        except Exception as e:
            logger.error(f"❌ 주식 리스트 조회 실패: {e}")
            self.stats["errors"] += 1
            return {
                'stocks': [],
                'total_count': 0,
                'market_status': self.market_checker.get_market_status(),
                'error': str(e)
            }
        finally:
            db.close()
    
    # =========================
    # 개별 주식 정보 API
    # =========================
    
    def get_stock_basic_info(self, symbol: str) -> Dict[str, Any]:
        """
        개별 주식 기본 정보 조회 (차트 데이터 제외)
        
        Args:
            symbol: 주식 심볼 (예: 'AAPL')
            
        Returns:
            Dict[str, Any]: 주식 기본 정보
        """
        try:
            self.stats["api_requests"] += 1
            self.stats["last_request"] = datetime.now(pytz.UTC)
            
            symbol = symbol.upper()
            db = next(get_db())
            
            # 현재가 및 변동 정보 조회
            change_info = SP500WebsocketTrades.get_price_change_info(db, symbol)
            
            if not change_info['current_price']:
                return {
                    'symbol': symbol,
                    'error': f'No data found for symbol {symbol}'
                }
            
            # 회사 기본 정보 조회
            company_name = self._get_company_name(symbol)
            
            self.stats["db_queries"] += 1
            
            return {
                'symbol': symbol,
                'company_name': company_name,
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
            logger.error(f"❌ {symbol} 주식 기본 정보 조회 실패: {e}")
            self.stats["errors"] += 1
            return {
                'symbol': symbol,
                'error': str(e)
            }
        finally:
            db.close()
    
    def get_chart_data_only(self, symbol: str, timeframe: str = '1D') -> Dict[str, Any]:
        """
        주식 차트 데이터만 조회
        
        Args:
            symbol: 주식 심볼
            timeframe: 차트 시간대 ('1M', '5M', '1H', '1D', '1W', '1MO')
            
        Returns:
            Dict[str, Any]: 차트 데이터
        """
        try:
            self.stats["api_requests"] += 1
            self.stats["last_request"] = datetime.now(pytz.UTC)
            
            symbol = symbol.upper()
            db = next(get_db())
            
            # 차트 데이터 조회
            chart_data = SP500WebsocketTrades.get_chart_data_by_timeframe(
                db, symbol, timeframe, limit=200
            )
            
            if not chart_data:
                return {
                    'symbol': symbol,
                    'timeframe': timeframe,
                    'chart_data': [],
                    'error': f'No chart data found for symbol {symbol}'
                }
            
            # 차트 데이터 포맷 변환
            formatted_chart_data = []
            for trade in chart_data:
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
            logger.error(f"❌ {symbol} 차트 데이터 조회 실패: {e}")
            self.stats["errors"] += 1
            return {
                'symbol': symbol,
                'timeframe': timeframe,
                'chart_data': [],
                'error': str(e)
            }
        finally:
            db.close()
    
    def get_stock_detail(self, symbol: str, timeframe: str = '1D') -> Dict[str, Any]:
        """
        개별 주식 상세 정보 조회 (기본 정보 + 차트)
        
        Args:
            symbol: 주식 심볼
            timeframe: 차트 시간대
            
        Returns:
            Dict[str, Any]: 주식 상세 정보
        """
        try:
            # 기본 정보 조회
            basic_info = self.get_stock_basic_info(symbol)
            if basic_info.get('error'):
                return basic_info
            
            # 차트 데이터 조회
            chart_info = self.get_chart_data_only(symbol, timeframe)
            
            # 두 정보 합치기
            combined_result = {
                **basic_info,
                'chart_data': chart_info.get('chart_data', []),
                'timeframe': timeframe
            }
            
            return combined_result
            
        except Exception as e:
            logger.error(f"❌ {symbol} 주식 상세 정보 조회 실패: {e}")
            self.stats["errors"] += 1
            return {
                'symbol': symbol,
                'error': str(e)
            }
    
    def _format_timestamp_by_timeframe(self, dt: datetime, timeframe: str) -> str:
        """시간대별로 적절한 timestamp 포맷 생성"""
        if timeframe in ['1M', '5M', '1H']:
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        elif timeframe == '1D':
            return dt.strftime('%Y-%m-%d %H:%M')
        else:  # '1W', '1MO'
            return dt.strftime('%Y-%m-%d')
    
    # =========================
    # 카테고리별 주식 조회 API
    # =========================
    
    def get_top_gainers(self, limit: int = 20) -> Dict[str, Any]:
        """상위 상승 종목 조회"""
        try:
            self.stats["api_requests"] += 1
            
            db = next(get_db())
            
            # 회사 정보 포함해서 조회
            all_stocks_with_company = SP500WebsocketTrades.get_all_current_prices_with_company_info(db, limit * 3)
            
            gainers = []
            for stock_data in all_stocks_with_company:
                change_info = SP500WebsocketTrades.get_price_change_info(db, stock_data['symbol'])
                
                # 상승 종목만 필터링
                if change_info['change_percentage'] and change_info['change_percentage'] > 0:
                    stock_item = {
                        'symbol': stock_data['symbol'],
                        'company_name': stock_data['company_name'],
                        'current_price': change_info['current_price'],
                        'change_amount': change_info['change_amount'],
                        'change_percentage': change_info['change_percentage'],
                        'volume': change_info['volume']
                    }
                    gainers.append(stock_item)
            
            # 상승률 기준 정렬
            gainers.sort(key=lambda x: x['change_percentage'], reverse=True)
            gainers = gainers[:limit]
            
            return {
                'category': 'top_gainers',
                'stocks': gainers,
                'total_count': len(gainers),
                'market_status': self.market_checker.get_market_status()
            }
            
        except Exception as e:
            logger.error(f"❌ 상위 상승 종목 조회 실패: {e}")
            self.stats["errors"] += 1
            return {'category': 'top_gainers', 'stocks': [], 'error': str(e)}
        finally:
            db.close()
    
    def get_top_losers(self, limit: int = 20) -> Dict[str, Any]:
        """상위 하락 종목 조회"""
        try:
            self.stats["api_requests"] += 1
            
            db = next(get_db())
            
            all_stocks_with_company = SP500WebsocketTrades.get_all_current_prices_with_company_info(db, limit * 3)
            
            losers = []
            for stock_data in all_stocks_with_company:
                change_info = SP500WebsocketTrades.get_price_change_info(db, stock_data['symbol'])
                
                # 하락 종목만 필터링
                if change_info['change_percentage'] and change_info['change_percentage'] < 0:
                    stock_item = {
                        'symbol': stock_data['symbol'],
                        'company_name': stock_data['company_name'],
                        'current_price': change_info['current_price'],
                        'change_amount': change_info['change_amount'],
                        'change_percentage': change_info['change_percentage'],
                        'volume': change_info['volume']
                    }
                    losers.append(stock_item)
            
            # 하락률 기준 정렬
            losers.sort(key=lambda x: x['change_percentage'])
            losers = losers[:limit]
            
            return {
                'category': 'top_losers',
                'stocks': losers,
                'total_count': len(losers),
                'market_status': self.market_checker.get_market_status()
            }
            
        except Exception as e:
            logger.error(f"❌ 상위 하락 종목 조회 실패: {e}")
            self.stats["errors"] += 1
            return {'category': 'top_losers', 'stocks': [], 'error': str(e)}
        finally:
            db.close()
    
    def get_most_active(self, limit: int = 20) -> Dict[str, Any]:
        """가장 활발한 거래 종목 조회"""
        try:
            self.stats["api_requests"] += 1
            
            db = next(get_db())
            
            all_stocks_with_company = SP500WebsocketTrades.get_all_current_prices_with_company_info(db, limit * 2)
            
            active_stocks = []
            for stock_data in all_stocks_with_company:
                change_info = SP500WebsocketTrades.get_price_change_info(db, stock_data['symbol'])
                
                if change_info['volume'] and change_info['volume'] > 0:
                    stock_item = {
                        'symbol': stock_data['symbol'],
                        'company_name': stock_data['company_name'],
                        'current_price': change_info['current_price'],
                        'change_amount': change_info['change_amount'],
                        'change_percentage': change_info['change_percentage'],
                        'volume': change_info['volume']
                    }
                    active_stocks.append(stock_item)
            
            # 거래량 기준 정렬
            active_stocks.sort(key=lambda x: x['volume'], reverse=True)
            active_stocks = active_stocks[:limit]
            
            return {
                'category': 'most_active',
                'stocks': active_stocks,
                'total_count': len(active_stocks),
                'market_status': self.market_checker.get_market_status()
            }
            
        except Exception as e:
            logger.error(f"❌ 활발한 거래 종목 조회 실패: {e}")
            self.stats["errors"] += 1
            return {'category': 'most_active', 'stocks': [], 'error': str(e)}
        finally:
            db.close()
    
    # =========================
    # 시장 요약 정보 API
    # =========================
    
    def get_market_overview(self) -> Dict[str, Any]:
        """전체 시장 개요 조회"""
        try:
            self.stats["api_requests"] += 1
            
            db = next(get_db())
            
            # 기본 시장 요약 정보
            market_summary = SP500WebsocketTrades.get_market_summary(db)
            
            # 상위 종목들 요약 조회
            top_gainers = self.get_top_gainers(5)['stocks']
            top_losers = self.get_top_losers(5)['stocks']
            most_active = self.get_most_active(5)['stocks']
            
            return {
                'market_summary': market_summary,
                'market_status': self.market_checker.get_market_status(),
                'highlights': {
                    'top_gainers': top_gainers,
                    'top_losers': top_losers,
                    'most_active': most_active
                },
                'last_updated': datetime.now(pytz.UTC).isoformat()
            }
            
        except Exception as e:
            logger.error(f"❌ 시장 개요 조회 실패: {e}")
            self.stats["errors"] += 1
            return {
                'market_summary': {},
                'market_status': self.market_checker.get_market_status(),
                'error': str(e)
            }
        finally:
            db.close()
    
    # =========================
    # 검색 API
    # =========================
    
    def search_stocks(self, query: str, limit: int = 20) -> Dict[str, Any]:
        """주식 검색 (심볼 또는 회사명)"""
        try:
            self.stats["api_requests"] += 1
            
            db = next(get_db())
            
            # 전체 주식 데이터 조회
            all_stocks = SP500WebsocketTrades.get_all_current_prices(db, 500)
            
            # 검색어 매칭
            search_results = []
            query_upper = query.upper()
            
            for trade in all_stocks:
                company_name = self._get_company_name(trade.symbol)
                
                # 심볼 또는 회사명 매칭
                if (query_upper in trade.symbol.upper() or 
                    query_upper in company_name.upper()):
                    
                    change_info = SP500WebsocketTrades.get_price_change_info(db, trade.symbol)
                    
                    stock_data = {
                        'symbol': trade.symbol,
                        'company_name': company_name,
                        'current_price': change_info['current_price'],
                        'change_amount': change_info['change_amount'],
                        'change_percentage': change_info['change_percentage'],
                        'volume': change_info['volume']
                    }
                    search_results.append(stock_data)
            
            # 심볼 알파벳 순 정렬
            search_results.sort(key=lambda x: x['symbol'])
            search_results = search_results[:limit]
            
            return {
                'query': query,
                'results': search_results,
                'total_count': len(search_results),
                'message': f'Found {len(search_results)} stocks matching "{query}"'
            }
            
        except Exception as e:
            logger.error(f"❌ 주식 검색 실패 ({query}): {e}")
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
    # 🆕 WebSocket용 헬퍼 함수들 (동기 방식)
    # =========================
    
    async def get_realtime_data(self, limit: int = 500) -> List[dict]:
        """
        WebSocket용 실시간 데이터 조회 (비동기)
        
        Args:
            limit: 반환할 최대 개수
            
        Returns:
            List[dict]: 실시간 데이터 리스트
        """
        try:
            # get_stock_list를 사용하여 데이터 조회
            result = self.get_stock_list(limit)
            return result.get('stocks', [])
        except Exception as e:
            logger.error(f"❌ WebSocket 실시간 데이터 조회 실패: {e}")
            return []
    
    async def get_symbol_data(self, symbol: str) -> Optional[dict]:
        """
        WebSocket용 특정 심볼 데이터 조회 (비동기)
        
        Args:
            symbol: 주식 심볼
            
        Returns:
            Optional[dict]: 심볼 데이터
        """
        try:
            return self.get_stock_basic_info(symbol)
        except Exception as e:
            logger.error(f"❌ WebSocket 심볼 {symbol} 조회 실패: {e}")
            return None
    
    def get_batch_previous_close_prices_sync(self, symbols: List[str]) -> Dict[str, float]:
        """
        여러 심볼의 전일 종가를 일괄 조회 (동기 방식, WebSocket용)
        
        Args:
            symbols: 주식 심볼 리스트
            
        Returns:
            Dict[str, float]: {symbol: previous_close_price}
        """
        try:
            db = next(get_db())
            previous_close_prices = {}
            
            for symbol in symbols:
                prev_close = SP500WebsocketTrades.get_previous_close_price(db, symbol)
                if prev_close:
                    previous_close_prices[symbol] = prev_close
            
            logger.debug(f"📊 전일 종가 조회 완료: {len(previous_close_prices)}개 / {len(symbols)}개")
            return previous_close_prices
            
        except Exception as e:
            logger.error(f"❌ 전일 종가 일괄 조회 실패: {e}")
            return {}
        finally:
            if 'db' in locals():
                db.close()
    
    def _get_company_name(self, symbol: str) -> str:
        """
        주식 심볼의 회사명 조회
        
        Args:
            symbol: 주식 심볼
            
        Returns:
            str: 회사명 (없으면 심볼 반환)
        """
        try:
            db = next(get_db())
            company_info = SP500WebsocketTrades.get_company_name(db, symbol)
            return company_info if company_info else symbol
        except Exception as e:
            logger.error(f"❌ 회사명 조회 실패 ({symbol}): {e}")
            return symbol
        finally:
            if 'db' in locals():
                db.close()
    
    # =========================
    # 🆕 WebSocket용 헬퍼 함수들 (비동기 방식)
    # =========================
    
    async def get_realtime_data(self, limit: int = 500) -> List[dict]:
        """
        WebSocket용 실시간 데이터 조회 (비동기)
        
        Args:
            limit: 반환할 최대 개수
            
        Returns:
            List[dict]: 실시간 데이터 리스트
        """
        try:
            # get_stock_list를 사용하여 데이터 조회
            result = self.get_stock_list(limit)
            return result.get('stocks', [])
        except Exception as e:
            logger.error(f"❌ WebSocket 실시간 데이터 조회 실패: {e}")
            return []
    
    async def get_symbol_data(self, symbol: str) -> Optional[dict]:
        """
        WebSocket용 특정 심볼 데이터 조회 (비동기)
        
        Args:
            symbol: 주식 심볼
            
        Returns:
            Optional[dict]: 심볼 데이터
        """
        try:
            return self.get_stock_basic_info(symbol)
        except Exception as e:
            logger.error(f"❌ WebSocket 심볼 {symbol} 조회 실패: {e}")
            return None
    
    # =========================
    # 서비스 상태 및 헬스 체크
    # =========================
    
    def get_service_stats(self) -> Dict[str, Any]:
        """서비스 통계 정보 반환"""
        return {
            "service": "SP500Service",
            "stats": self.stats,
            "market_status": self.market_checker.get_market_status()
        }
    
    def health_check(self) -> Dict[str, Any]:
        """서비스 헬스 체크"""
        try:
            is_healthy = self.stats["errors"] < 100
            
            return {
                "status": "healthy" if is_healthy else "degraded",
                "service": "SP500Service",
                "api_requests": self.stats["api_requests"],
                "db_queries": self.stats["db_queries"],
                "errors": self.stats["errors"],
                "last_request": self.stats["last_request"].isoformat() if self.stats["last_request"] else None,
                "market_status": self.market_checker.get_market_status()
            }
        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e)
            }


# =========================
# 🆕 Redis 조회 함수 (동기, WebSocket에서 사용)
# =========================

def get_sp500_data_from_redis(redis_client: redis.Redis, limit: int = 500) -> List[dict]:
    """
    동기 방식으로 Redis에서 SP500 데이터 조회 및 병합
    (WebSocket 핸들러에서 사용)
    
    Redis 키 구조:
    - sp500_realtime_data (Consumer): {symbol: {"symbol": "AAPL", "price": 150.5}}
    - sp500_market_data (DAG): {symbol: {"company_name": "Apple", "change_percentage": 1.5, ...}}
    
    Args:
        redis_client: Redis 클라이언트
        limit: 최대 반환 개수
        
    Returns:
        List[dict]: 병합된 SP500 데이터 리스트
    """
    try:
        realtime_key = "sp500_realtime_data"
        market_key = "sp500_market_data"
        
        realtime_data_raw = redis_client.hgetall(realtime_key)
        market_data_raw = redis_client.hgetall(market_key)
        
        if not realtime_data_raw:
            logger.warning("Redis에 실시간 데이터 없음")
            return []
        
        merged_data = []
        
        # 실시간 데이터 기준으로만 병합
        for symbol_bytes, json_str_bytes in realtime_data_raw.items():
            symbol = symbol_bytes.decode('utf-8') if isinstance(symbol_bytes, bytes) else symbol_bytes
            json_str = json_str_bytes.decode('utf-8') if isinstance(json_str_bytes, bytes) else json_str_bytes
            
            realtime_data = json.loads(json_str)
            
            # 시장 데이터 조회 (없으면 빈 dict)
            market_json_bytes = market_data_raw.get(symbol_bytes)
            market_data = {}
            if market_json_bytes:
                market_json_str = market_json_bytes.decode('utf-8') if isinstance(market_json_bytes, bytes) else market_json_bytes
                market_data = json.loads(market_json_str)
            
            # 병합
            stock_item = {
                'symbol': realtime_data.get('symbol', symbol),
                'price': realtime_data.get('price', 0),
                'current_price': realtime_data.get('price', 0),
                'timestamp': realtime_data.get('timestamp'),
                
                # market_data 없으면 기본값 사용
                'company_name': market_data.get('company_name', symbol),
                'change_amount': market_data.get('change_amount', 0),
                'change_percentage': market_data.get('change_percentage', 0),
                'volume_24h': market_data.get('volume_24h', 0),
                'last_updated': market_data.get('last_updated'),
                'is_positive': market_data.get('change_amount', 0) > 0 if market_data.get('change_amount') is not None else None
            }
            
            merged_data.append(stock_item)
        
        merged_data.sort(key=lambda x: x.get('change_percentage', 0), reverse=True)
        
        logger.debug(f"✅ Redis SP500 데이터 병합 완료: {len(merged_data)}개")
        return merged_data[:limit]
        
    except Exception as e:
        logger.error(f"❌ Redis 조회 실패: {e}")
        return []