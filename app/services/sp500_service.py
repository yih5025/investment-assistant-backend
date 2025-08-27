# app/services/sp500_service.py
import logging
from typing import List, Dict, Any, Optional, Union
from datetime import datetime, timedelta
import pytz
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.sp500_model import SP500WebsocketTrades

logger = logging.getLogger(__name__)

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

class SP500Service:
    """
    SP500 API 전용 서비스 클래스
    
    WebSocket 서비스와 분리된 API 전용 비즈니스 로직을 처리합니다.
    주식 리스트, 개별 주식 상세 정보, 차트 데이터 등을 제공합니다.
    """
    
    def __init__(self):
        """SP500Service 초기화"""
        self.market_checker = MarketTimeChecker()
        
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
    # 🎯 주식 리스트 페이지 API
    # =========================
    
    async def get_realtime_polling_data(self, limit: int, sort_by: str = "volume", order: str = "desc"):
        """
        SP500 실시간 폴링 데이터 ("더보기" 방식) + 변화율 계산 (기존 로직 유지)
        
        현재 SP500Service는 이미 전날 종가 기반 변화율 계산이 구현되어 있으므로,
        WebSocket 서비스와 연동만 추가
        """
        try:
            from app.services.websocket_service import WebSocketService
            
            # WebSocket 서비스와 동일한 데이터 소스 사용
            websocket_service = WebSocketService()
            if not websocket_service.redis_client:
                await websocket_service.init_redis()
            
            # 🎯 Redis에서 기본 데이터 조회 후 변화율 직접 계산
            redis_data = await websocket_service.get_sp500_from_redis(limit=1000)
            
            if not redis_data:
                logger.warning("📊 Redis SP500 데이터 없음, DB fallback")
                return await self._get_db_polling_data_with_changes(limit, sort_by, order)
            
            # 🎯 실제 변화율 계산 로직으로 변경
            all_data = []
            
            # 심볼 리스트 추출
            symbols = []
            for item in redis_data:
                if hasattr(item, 'symbol'):
                    symbols.append(item.symbol)
                elif hasattr(item, 'dict') and 'symbol' in item.dict():
                    symbols.append(item.dict()['symbol'])
                elif isinstance(item, dict) and 'symbol' in item:
                    symbols.append(item['symbol'])
            
            # 전날 종가 일괄 조회
            previous_close_prices = await self._get_batch_previous_close_prices(symbols)
            
            for item in redis_data:
                # 기본 데이터를 딕셔너리로 변환
                if hasattr(item, 'dict'):
                    item_dict = item.dict()
                elif hasattr(item, '__dict__'):
                    item_dict = item.__dict__.copy()
                else:
                    item_dict = dict(item) if hasattr(item, 'keys') else {}
                
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
            
            # 정렬 처리 (변화율 정보가 포함된 데이터)
            if sort_by == "volume":
                all_data.sort(key=lambda x: x.get('volume', 0), reverse=(order == "desc"))
            elif sort_by == "change_percent":
                all_data.sort(key=lambda x: x.get('change_percentage', 0), reverse=(order == "desc"))
            elif sort_by == "price":
                all_data.sort(key=lambda x: x.get('current_price', 0), reverse=(order == "desc"))
            
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
            logger.error(f"❌ SP500 실시간 폴링 데이터 조회 실패: {e}")
            return {"error": str(e)}

    async def _get_db_polling_data_with_changes(self, limit: int, sort_by: str, order: str):
        """DB fallback 시 기존 SP500 Service 로직 사용 (변화율 포함)"""
        try:
            # 기존 SP500 Service의 get_stock_list 사용 (변화율 계산 포함)
            stock_list_result = self.get_stock_list(limit)
            
            if stock_list_result.get('error'):
                return {"error": stock_list_result['error']}
            
            stocks = stock_list_result.get('stocks', [])
            
            # 정렬 처리
            if sort_by == "volume":
                stocks.sort(key=lambda x: x.get('volume', 0), reverse=(order == "desc"))
            elif sort_by == "change_percent":
                stocks.sort(key=lambda x: x.get('change_percentage', 0), reverse=(order == "desc"))
            elif sort_by == "price":
                stocks.sort(key=lambda x: x.get('current_price', 0), reverse=(order == "desc"))
            
            # 순위 추가
            for i, stock in enumerate(stocks):
                stock['rank'] = i + 1
            
            return {
                "data": stocks[:limit],
                "metadata": {
                    "current_count": len(stocks[:limit]),
                    "total_available": len(stocks),
                    "has_more": limit < len(stocks),
                    "next_limit": min(limit + 50, len(stocks)),
                    "timestamp": datetime.now(pytz.UTC).isoformat(),
                    "data_source": "database_with_changes",
                    "market_status": stock_list_result.get('market_status'),
                    "sort_info": {"sort_by": sort_by, "order": order}
                }
            }
            
        except Exception as e:
            logger.error(f"❌ DB fallback 폴링 데이터 조회 실패: {e}")
            return {"error": str(e)}

    async def _get_batch_previous_close_prices(self, symbols: List[str]) -> Dict[str, float]:
        """
        여러 심볼의 전날 종가를 일괄 조회 (성능 최적화)
        
        Args:
            symbols: 주식 심볼 리스트
            
        Returns:
            Dict[str, float]: {symbol: previous_close_price}
        """
        try:
            db = next(get_db())
            previous_close_prices = {}
            
            # SP500 모델을 사용하여 각 심볼의 전날 종가 조회
            for symbol in symbols:
                prev_close = SP500WebsocketTrades.get_previous_close_price(db, symbol)
                if prev_close:
                    previous_close_prices[symbol] = prev_close
            
            logger.debug(f"📊 전날 종가 조회 완료: {len(previous_close_prices)}개 / {len(symbols)}개")
            return previous_close_prices
            
        except Exception as e:
            logger.error(f"❌ 전날 종가 일괄 조회 실패: {e}")
            return {}
        finally:
            if 'db' in locals():
                db.close()

    def _get_market_status(self):
        """시장 상태 조회"""
        try:
            from app.services.websocket_service import MarketTimeChecker
            market_checker = MarketTimeChecker()
            status = market_checker.get_market_status()
            return {
                "is_open": status["is_open"],
                "status": status["status"],
                "current_time_et": status.get("current_time_et", ""),
                "timezone": "US/Eastern"
            }
        except Exception as e:
            logger.error(f"❌ 시장 상태 조회 실패: {e}")
            return {"is_open": False, "status": "UNKNOWN", "error": str(e)}

    def get_stock_list(self, limit: int = 500) -> Dict[str, Any]:
        """
        주식 리스트 페이지용 전체 주식 현재가 조회 (회사 정보 포함)
        """
        try:
            self.stats["api_requests"] += 1
            self.stats["last_request"] = datetime.now(pytz.UTC)
            
            db = next(get_db())
            
            # JOIN을 통해 한번에 현재가 + 회사정보 조회
            stock_data_with_company = SP500WebsocketTrades.get_all_current_prices_with_company_info(db, limit)
            
            if not stock_data_with_company:
                logger.warning("주식 현재가 데이터 없음")
                return {
                    'stocks': [],
                    'total_count': 0,
                    'market_status': self.market_checker.get_market_status(),
                    'message': 'No stock data available'
                }
            
            # 각 주식의 변동 정보 계산 (기존 로직 유지)
            stock_list = []
            for stock_data in stock_data_with_company:
                # 가격 변동 정보 조회
                change_info = SP500WebsocketTrades.get_price_change_info(db, stock_data['symbol'])
                
                # 프론트엔드 형태로 데이터 구성
                stock_item = {
                    'symbol': stock_data['symbol'],
                    'company_name': stock_data['company_name'],  # JOIN에서 가져온 회사명 직접 사용
                    'current_price': change_info['current_price'],
                    'change_amount': change_info['change_amount'],
                    'change_percentage': change_info['change_percentage'],
                    'volume': change_info['volume'],
                    'last_updated': change_info['last_updated'],
                    'is_positive': change_info['change_amount'] > 0 if change_info['change_amount'] else None
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
            logger.error(f"주식 리스트 조회 실패: {e}")
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
    # 🎯 개별 주식 정보 API (차트 분리) 🆕
    # =========================
    
    def get_stock_basic_info(self, symbol: str) -> Dict[str, Any]:
        """
        🆕 개별 주식 기본 정보 조회 (차트 데이터 제외)
        
        Args:
            symbol: 주식 심볼 (예: 'AAPL')
            
        Returns:
            Dict[str, Any]: 차트를 제외한 주식 기본 정보
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
            
            # 회사 기본 정보 조회 (섹터 정보 제거)
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
        🆕 주식 차트 데이터만 조회
        
        Args:
            symbol: 주식 심볼 (예: 'AAPL')
            timeframe: 차트 시간대 ('1M', '5M', '1H', '1D', '1W', '1MO')
            
        Returns:
            Dict[str, Any]: 차트 데이터만 포함된 응답
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
            
            # 차트 데이터 포맷 변환 (프론트엔드용)
            formatted_chart_data = []
            for trade in chart_data:
                formatted_chart_data.append({
                    'timestamp': trade.timestamp_ms,
                    'price': float(trade.price),
                    'volume': trade.volume,
                    'datetime': trade.created_at.isoformat()
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
        개별 주식 상세 정보 조회 (차트 데이터 포함) - 기존 호환성 유지
        
        Args:
            symbol: 주식 심볼 (예: 'AAPL')
            timeframe: 차트 시간대 ('1M', '5M', '1H', '1D', '1W', '1MO')
            
        Returns:
            Dict[str, Any]: 주식 상세 정보 (기본 정보 + 차트 데이터)
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
    
    # =========================
    # 🎯 카테고리별 주식 조회 API (섹터 정보 제거) 🆕
    # =========================
    
    def get_top_gainers(self, limit: int = 20) -> Dict[str, Any]:
        """
        상위 상승 종목 조회 (회사 정보 포함)
        """
        try:
            self.stats["api_requests"] += 1
            
            db = next(get_db())
            
            # JOIN을 통해 회사 정보 포함해서 조회
            all_stocks_with_company = SP500WebsocketTrades.get_all_current_prices_with_company_info(db, limit * 3)
            
            gainers = []
            for stock_data in all_stocks_with_company:
                change_info = SP500WebsocketTrades.get_price_change_info(db, stock_data['symbol'])
                
                # 상승 종목만 필터링
                if change_info['change_percentage'] and change_info['change_percentage'] > 0:
                    stock_item = {
                        'symbol': stock_data['symbol'],
                        'company_name': stock_data['company_name'],  # JOIN에서 가져온 회사명
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
            logger.error(f"상위 상승 종목 조회 실패: {e}")
            self.stats["errors"] += 1
            return {'category': 'top_gainers', 'stocks': [], 'error': str(e)}
        finally:
            db.close()

    
    def get_top_losers(self, limit: int = 20) -> Dict[str, Any]:
        """
        상위 하락 종목 조회
        """
        try:
            self.stats["api_requests"] += 1
            
            db = next(get_db())
            
            # 전체 주식 현재가 조회
            all_stocks_with_company = SP500WebsocketTrades.get_all_current_prices_with_company_info(db, limit * 3)
            
            # 변동률 계산 및 필터링
            losers = []
            for stock_data in all_stocks_with_company:
                change_info = SP500WebsocketTrades.get_price_change_info(db, stock_data['symbol'])
                
                # 하락 종목만 필터링
                if change_info['change_percentage'] and change_info['change_percentage'] < 0:
                    stock_item = {
                        'symbol': stock_data['symbol'],
                        'company_name': stock_data['company_name'],  # JOIN에서 가져온 회사명
                        'current_price': change_info['current_price'],
                        'change_amount': change_info['change_amount'],
                        'change_percentage': change_info['change_percentage'],
                        'volume': change_info['volume']
                    }
                    losers.append(stock_item)
            
            # 하락률 기준 정렬 (가장 많이 떨어진 순)
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
        """
        가장 활발한 거래 종목 조회 (회사 정보 포함)
        """
        try:
            self.stats["api_requests"] += 1
            
            db = next(get_db())
            
            # 전체 주식 현재가 조회
            all_stocks_with_company = SP500WebsocketTrades.get_all_current_prices_with_company_info(db, limit * 2)
            
            # 거래량 기준 정렬
            active_stocks = []
            for stock_data in all_stocks_with_company:
                change_info = SP500WebsocketTrades.get_price_change_info(db, stock_data['symbol'])
                
                if change_info['volume'] and change_info['volume'] > 0:
                    stock_item = {
                        'symbol': stock_data['symbol'],
                        'company_name': stock_data['company_name'],  # JOIN에서 가져온 회사명
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
    # 🎯 시장 요약 정보 API
    # =========================
    
    def get_market_overview(self) -> Dict[str, Any]:
        """
        전체 시장 개요 조회
        
        Returns:
            Dict[str, Any]: 시장 개요 정보
        """
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
    # 🎯 검색 및 필터 API (섹터 기능 제거) 🆕
    # =========================
    
    def search_stocks(self, query: str, limit: int = 20) -> Dict[str, Any]:
        """
        주식 검색 (심볼 또는 회사명 기준, 섹터 정보 제거)
        
        Args:
            query: 검색어 (심볼 또는 회사명)
            limit: 반환할 최대 개수
            
        Returns:
            Dict[str, Any]: 검색 결과
        """
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
                        # 🆕 섹터 정보 제거
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
    # 🎯 서비스 상태 및 통계
    # =========================
    
    def get_service_stats(self) -> Dict[str, Any]:
        """
        서비스 통계 정보 반환
        
        Returns:
            Dict[str, Any]: 서비스 통계
        """
        return {
            'service_name': 'SP500Service',
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
                "SELECT MAX(created_at) FROM sp500_websocket_trades"
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
            logger.error(f"❌ 헬스 체크 실패: {e}")
            return {
                'status': 'unhealthy',
                'database': 'error',
                'error': str(e),
                'last_check': datetime.now(pytz.UTC).isoformat()
            }
        finally:
            db.close()