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
            now_utc = datetime.utcnow().replace(tzinfo=pytz.UTC)
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
            now_utc = datetime.utcnow().replace(tzinfo=pytz.UTC)
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
        SP500 실시간 폴링 데이터 ("더보기" 방식)
        
        Args:
            limit: 반환할 항목 수 (1번부터 limit번까지)
            sort_by: 정렬 기준 (volume, change_percent, price)
            order: 정렬 순서 (asc, desc)
        
        Returns:
            dict: 폴링 응답 데이터
        """
        try:
            from app.services.websocket_service import WebSocketService
            
            # WebSocket 서비스와 동일한 데이터 소스 사용
            websocket_service = WebSocketService()
            if not websocket_service.redis_client:
                await websocket_service.init_redis()
            
            # 🎯 Redis에서 SP500 실시간 데이터 조회 (WebSocket과 동일한 소스)
            all_data = await websocket_service.get_sp500_from_redis(limit=1000)
            
            if not all_data:
                logger.warning("📊 Redis SP500 데이터 없음, DB fallback")
                # Redis 데이터가 없으면 DB에서 조회
                all_data = await websocket_service.get_sp500_from_db(limit=1000)
            
            # 정렬 처리
            if sort_by == "volume":
                all_data.sort(key=lambda x: x.volume or 0, reverse=(order == "desc"))
            elif sort_by == "change_percent":
                # change_percent 계산 (price 변화율)
                for item in all_data:
                    if hasattr(item, 'price') and item.price:
                        # 간단한 변화율 계산 (실제로는 더 복잡한 로직 필요)
                        item.change_percent = getattr(item, 'change_percent', 0)
                all_data.sort(key=lambda x: getattr(x, 'change_percent', 0), reverse=(order == "desc"))
            elif sort_by == "price":
                all_data.sort(key=lambda x: x.price or 0, reverse=(order == "desc"))
            
            # 순위 추가 (정렬 후)
            for i, item in enumerate(all_data):
                if hasattr(item, 'model_dump'):
                    item_dict = item.model_dump()
                    item_dict['rank'] = i + 1
                else:
                    item.rank = i + 1
            
            # limit만큼 자르기 (1번부터 limit번까지)
            limited_data = all_data[:limit]
            total_available = len(all_data)
            
            # 응답 데이터 구성
            return {
                "data": [item.model_dump() if hasattr(item, 'model_dump') else item for item in limited_data],
                "metadata": {
                    "current_count": len(limited_data),
                    "total_available": total_available,
                    "has_more": limit < total_available,
                    "next_limit": min(limit + 50, total_available),
                    "timestamp": datetime.now(pytz.UTC).isoformat(),
                    "data_source": "redis_realtime" if websocket_service.redis_client else "database_fallback",
                    "market_status": self._get_market_status(),
                    "sort_info": {
                        "sort_by": sort_by,
                        "order": order
                    }
                }
            }
            
        except Exception as e:
            logger.error(f"❌ SP500 실시간 폴링 데이터 조회 실패: {e}")
            return {"error": str(e)}

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
        주식 리스트 페이지용 전체 주식 현재가 조회
        
        Args:
            limit: 반환할 최대 주식 개수
            
        Returns:
            Dict[str, Any]: 주식 리스트 데이터
        """
        try:
            self.stats["api_requests"] += 1
            self.stats["last_request"] = datetime.now(pytz.UTC)
            
            db = next(get_db())
            
            # 전체 주식 현재가 조회
            current_prices = SP500WebsocketTrades.get_all_current_prices(db, limit)
            
            if not current_prices:
                logger.warning("📊 주식 현재가 데이터 없음")
                return {
                    'stocks': [],
                    'total_count': 0,
                    'market_status': self.market_checker.get_market_status(),
                    'message': 'No stock data available'
                }
            
            # 각 주식의 변동 정보 계산
            stock_list = []
            for trade in current_prices:
                # 가격 변동 정보 조회
                change_info = SP500WebsocketTrades.get_price_change_info(db, trade.symbol)
                
                # 프론트엔드 형태로 데이터 구성 (섹터 정보 제거)
                stock_data = {
                    'symbol': trade.symbol,
                    'company_name': self._get_company_name(trade.symbol),  # SP500 회사명 조회
                    'current_price': change_info['current_price'],
                    'change_amount': change_info['change_amount'],
                    'change_percentage': change_info['change_percentage'],
                    'volume': change_info['volume'],
                    'last_updated': change_info['last_updated'],
                    'is_positive': change_info['change_amount'] > 0 if change_info['change_amount'] else None
                }
                
                stock_list.append(stock_data)
            
            # 변동률 기준 정렬 (상승률 높은 순)
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
            self.stats["last_request"] = datetime.utcnow()
            
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
            self.stats["last_request"] = datetime.utcnow()
            
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
                'last_updated': datetime.utcnow().isoformat()
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
        상위 상승 종목 조회 (섹터 정보 제거)
        
        Args:
            limit: 반환할 최대 개수
            
        Returns:
            Dict[str, Any]: 상위 상승 종목 데이터
        """
        try:
            self.stats["api_requests"] += 1
            
            db = next(get_db())
            
            # 전체 주식 현재가 조회
            all_stocks = SP500WebsocketTrades.get_all_current_prices(db, limit * 3)
            
            # 변동률 계산 및 필터링
            gainers = []
            for trade in all_stocks:
                change_info = SP500WebsocketTrades.get_price_change_info(db, trade.symbol)
                
                # 상승 종목만 필터링
                if change_info['change_percentage'] and change_info['change_percentage'] > 0:
                    stock_data = {
                        'symbol': trade.symbol,
                        'company_name': self._get_company_name(trade.symbol),
                        'current_price': change_info['current_price'],
                        'change_amount': change_info['change_amount'],
                        'change_percentage': change_info['change_percentage'],
                        'volume': change_info['volume']
                        # 🆕 섹터 정보 제거
                    }
                    gainers.append(stock_data)
            
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
        """
        상위 하락 종목 조회 (섹터 정보 제거)
        
        Args:
            limit: 반환할 최대 개수
            
        Returns:
            Dict[str, Any]: 상위 하락 종목 데이터
        """
        try:
            self.stats["api_requests"] += 1
            
            db = next(get_db())
            
            # 전체 주식 현재가 조회
            all_stocks = SP500WebsocketTrades.get_all_current_prices(db, limit * 3)
            
            # 변동률 계산 및 필터링
            losers = []
            for trade in all_stocks:
                change_info = SP500WebsocketTrades.get_price_change_info(db, trade.symbol)
                
                # 하락 종목만 필터링
                if change_info['change_percentage'] and change_info['change_percentage'] < 0:
                    stock_data = {
                        'symbol': trade.symbol,
                        'company_name': self._get_company_name(trade.symbol),
                        'current_price': change_info['current_price'],
                        'change_amount': change_info['change_amount'],
                        'change_percentage': change_info['change_percentage'],
                        'volume': change_info['volume']
                        # 🆕 섹터 정보 제거
                    }
                    losers.append(stock_data)
            
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
        가장 활발한 거래 종목 조회 (섹터 정보 제거)
        
        Args:
            limit: 반환할 최대 개수
            
        Returns:
            Dict[str, Any]: 활발한 거래 종목 데이터
        """
        try:
            self.stats["api_requests"] += 1
            
            db = next(get_db())
            
            # 전체 주식 현재가 조회
            all_stocks = SP500WebsocketTrades.get_all_current_prices(db, limit * 2)
            
            # 거래량 기준 정렬
            active_stocks = []
            for trade in all_stocks:
                change_info = SP500WebsocketTrades.get_price_change_info(db, trade.symbol)
                
                if change_info['volume'] and change_info['volume'] > 0:
                    stock_data = {
                        'symbol': trade.symbol,
                        'company_name': self._get_company_name(trade.symbol),
                        'current_price': change_info['current_price'],
                        'change_amount': change_info['change_amount'],
                        'change_percentage': change_info['change_percentage'],
                        'volume': change_info['volume']
                        # 🆕 섹터 정보 제거
                    }
                    active_stocks.append(stock_data)
            
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
                'last_updated': datetime.utcnow().isoformat()
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
    
    # 🆕 섹터별 검색 함수 제거됨 (Company Overview에서 처리)
    
    # =========================
    # 🎯 헬퍼 메서드들 (섹터 관련 제거) 🆕
    # =========================
    
    def _get_company_name(self, symbol: str) -> str:
        """
        심볼로 회사명 조회 (SP500 companies 테이블 사용)
        
        Args:
            symbol: 주식 심볼
            
        Returns:
            str: 회사명
        """
        try:
            db = next(get_db())
            
            # sp500_companies 테이블에서 회사명 조회
            result = db.execute(
                "SELECT company_name FROM sp500_companies WHERE symbol = %s",
                (symbol,)
            ).fetchone()
            
            if result:
                return result[0]
            else:
                # 기본값: 심볼을 회사명으로 사용
                return f"{symbol} Inc."
                
        except Exception as e:
            logger.warning(f"⚠️ {symbol} 회사명 조회 실패: {e}")
            return f"{symbol} Inc."
        finally:
            db.close()
    
    # 🆕 _get_sector(), _get_company_info() 함수 제거됨
    # 섹터 및 상세 회사 정보는 Company Overview 서비스에서 처리
    
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
            'uptime': datetime.utcnow().isoformat()
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
                time_diff = datetime.utcnow() - latest_data[0]
                if time_diff > timedelta(hours=1):
                    data_freshness = "stale"
            else:
                data_freshness = "no_data"
            
            return {
                'status': 'healthy',
                'database': db_status,
                'data_freshness': data_freshness,
                'market_status': self.market_checker.get_market_status(),
                'last_check': datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"❌ 헬스 체크 실패: {e}")
            return {
                'status': 'unhealthy',
                'database': 'error',
                'error': str(e),
                'last_check': datetime.utcnow().isoformat()
            }
        finally:
            db.close()