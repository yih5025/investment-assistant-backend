# app/services/etf_service.py

import logging
import json
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import pytz
import redis
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.etf_model import ETFBasicInfo, ETFProfileHoldings, ETFRealtimePrices
from app.schemas import etf_schema
from app.config import settings

logger = logging.getLogger(__name__)

# =========================
# 시장 시간 체크 클래스
# =========================

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

# =========================
# ETF 서비스 클래스
# =========================

class ETFService:
    """
    ETF API 전용 서비스 클래스
    
    ETF 리스트, 개별 ETF 상세 정보, 차트 데이터 등을 제공합니다.
    WebSocket Push 방식과 분리되어 API 전용 로직만 처리합니다.
    """
    
    def __init__(self, redis_client: Optional[redis.Redis] = None):
        """
        ETFService 초기화
        
        Args:
            redis_client: Redis 클라이언트 (옵션, WebSocket용)
        """
        self.market_checker = MarketTimeChecker()
        self.redis_client = redis_client
        
        self.stats = {
            "api_requests": 0, 
            "db_queries": 0, 
            "cache_hits": 0,
            "errors": 0, 
            "last_request": None
        }
        
        logger.info("✅ ETFService 초기화 완료")

    # =========================
    # ETF 리스트 API
    # =========================
    
    def get_etf_list(self, limit: int = 500) -> Dict[str, Any]:
        """
        ETF 리스트 페이지용 전체 ETF 현재가 조회
        
        Args:
            limit: 반환할 최대 개수 (기본 500)
            
        Returns:
            Dict[str, Any]: ETF 리스트
        """
        try:
            self.stats["api_requests"] += 1
            self.stats["last_request"] = datetime.now(pytz.UTC)
            
            db = next(get_db())
            
            # ETF 기본 정보와 실시간 가격 조회
            basic_infos = db.query(ETFBasicInfo).limit(limit).all()
            
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
            
            logger.info(f"🔄 ETF 배치 처리 완료: {len(batch_change_info)}/{len(symbols)}개 심볼")
            
            # 각 ETF의 변동 정보 조합
            etf_list = []
            for basic_info in basic_infos:
                symbol = basic_info.symbol
                change_info = batch_change_info.get(symbol)
                
                if change_info:
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
                    logger.warning(f"⚠️ {symbol} ETF 배치 처리에서 누락됨")
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
            logger.error(f"❌ ETF 리스트 조회 실패: {e}")
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
    # 개별 ETF 상세 정보 API
    # =========================
    
    def get_etf_details_by_symbol(self, symbol: str) -> Optional[etf_schema.ETFDetailResponse]:
        """
        특정 ETF 심볼에 대한 모든 상세 정보 조회
        
        Args:
            symbol: ETF 심볼
            
        Returns:
            Optional[ETFDetailResponse]: ETF 상세 정보
        """
        db: Session = next(get_db())
        try:
            symbol_upper = symbol.upper()
            
            # DB에서 필요한 모든 데이터 조회
            basic_info_model = db.query(ETFBasicInfo).filter(ETFBasicInfo.symbol == symbol_upper).first()
            latest_price_model = db.query(ETFRealtimePrices).filter(
                ETFRealtimePrices.symbol == symbol_upper
            ).order_by(ETFRealtimePrices.timestamp_ms.desc()).first()
            profile_model = db.query(ETFProfileHoldings).filter(
                ETFProfileHoldings.symbol == symbol_upper
            ).first()

            if not basic_info_model or not latest_price_model:
                logger.warning(f"⚠️ 기본 정보 또는 실시간 가격 정보가 없음: {symbol_upper}")
                return None

            # 전일 종가 안정적으로 계산
            previous_close = self._get_robust_previous_close_price(
                db, symbol_upper, latest_price_model.created_at
            )

            # 변동률 계산
            change_amount, change_percentage, is_positive = None, None, None
            if previous_close is not None and latest_price_model.price is not None:
                change_amount = latest_price_model.price - previous_close
                change_percentage = (change_amount / previous_close) * 100 if previous_close != 0 else 0
                is_positive = change_amount >= 0
            
            # Pydantic 스키마 객체 생성
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

            # 프로필 정보 및 파생 데이터 스키마 생성
            profile_schema, sector_chart_data, holdings_chart_data, key_metrics = None, None, None, None
            if profile_model:
                profile_schema, sector_chart_data, holdings_chart_data, key_metrics = self._parse_profile_to_schemas(profile_model)

            # 최종 응답 스키마 조합 후 반환
            return etf_schema.ETFDetailResponse(
                basic_info=basic_info_schema,
                profile=profile_schema,
                sector_chart_data=sector_chart_data,
                holdings_chart_data=holdings_chart_data,
                key_metrics=key_metrics,
                last_updated=datetime.now(pytz.utc)
            )

        except Exception as e:
            logger.error(f"❌ {symbol} ETF 상세 정보 조회 중 오류: {e}", exc_info=True)
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
        
        # 해당 날짜의 마지막 거래 기록 찾음
        previous_close_record = db.query(ETFRealtimePrices.price)\
            .filter(ETFRealtimePrices.symbol == symbol)\
            .filter(func.date(ETFRealtimePrices.created_at.op('AT TIME ZONE')('UTC').op('AT TIME ZONE')('US/Eastern')) == lookup_date)\
            .order_by(ETFRealtimePrices.timestamp_ms.desc())\
            .first()

        return previous_close_record[0] if previous_close_record else None

    def _parse_profile_to_schemas(self, profile: ETFProfileHoldings):
        """DB 모델을 받아서 여러 Pydantic 스키마로 변환"""
        try:
            # sectors 파싱
            if isinstance(profile.sectors, list):
                sectors = profile.sectors
            elif isinstance(profile.sectors, str) and profile.sectors:
                sectors = json.loads(profile.sectors)
            else:
                sectors = []
            
            # holdings 파싱
            if isinstance(profile.holdings, list):
                holdings = profile.holdings
            elif isinstance(profile.holdings, str) and profile.holdings:
                holdings = json.loads(profile.holdings)
            else:
                holdings = []
        except (json.JSONDecodeError, TypeError) as e:
            logger.warning(f"⚠️ JSON 파싱 오류: {e}, 빈 리스트로 대체")
            sectors, holdings = [], []

        # ETF 이름 조회
        etf_names = self._get_etf_names_sync([profile.symbol])
        etf_name = etf_names.get(profile.symbol, profile.symbol)
        
        profile_schema = etf_schema.ETFProfile(
            symbol=profile.symbol, name=etf_name, net_assets=profile.net_assets,
            net_expense_ratio=profile.net_expense_ratio, portfolio_turnover=profile.portfolio_turnover,
            dividend_yield=profile.dividend_yield, 
            inception_date=profile.inception_date.isoformat() if profile.inception_date else None,
            leveraged=profile.leveraged, sectors=sectors, holdings=holdings
        )
        
        sector_chart_data = [
            etf_schema.SectorChartData(
                name=s.get('sector', 'N/A'), 
                value=float(s.get('weight', 0))*100, 
                color=self._get_sector_color(i)
            ) for i, s in enumerate(sectors)
        ]
        
        holdings_chart_data = [
            etf_schema.HoldingChartData(
                symbol=h.get('symbol', 'N/A'), 
                name=h.get('description', 'N/A'), 
                weight=float(h.get('weight', 0))*100
            ) for h in holdings[:10]
        ]
        
        key_metrics = etf_schema.KeyMetrics(
            net_assets=etf_schema.format_currency(profile.net_assets),
            net_expense_ratio=etf_schema.format_percentage(profile.net_expense_ratio),
            dividend_yield=etf_schema.format_percentage(profile.dividend_yield),
            inception_year=etf_schema.format_date(profile.inception_date.isoformat() if profile.inception_date else None)
        )
        
        return profile_schema, sector_chart_data, holdings_chart_data, key_metrics

    def _get_etf_names_sync(self, symbols: List[str]) -> Dict[str, str]:
        """ETF 이름 일괄 조회 (동기 버전)"""
        try:
            db = next(get_db())
            etf_names = {}
            
            etf_infos = db.query(ETFBasicInfo).filter(
                ETFBasicInfo.symbol.in_(symbols)
            ).all()
            
            for etf_info in etf_infos:
                etf_names[etf_info.symbol] = etf_info.name
            
            logger.debug(f"✅ ETF 이름 조회 완료: {len(etf_names)}개 / {len(symbols)}개")
            return etf_names
            
        except Exception as e:
            logger.error(f"❌ ETF 이름 조회 실패: {e}")
            return {}
        finally:
            if 'db' in locals():
                db.close()

    def _get_sector_color(self, index: int) -> str:
        """섹터 차트용 색상 반환"""
        colors = [
            '#60a5fa', '#22d3ee', '#a78bfa', '#34d399', '#fbbf24',
            '#f87171', '#fb7185', '#a3a3a3', '#6b7280', '#9ca3af'
        ]
        return colors[index % len(colors)]
    
    # =========================
    # 차트 데이터 API
    # =========================
    
    def get_chart_data_only(self, symbol: str, timeframe: str = '1D') -> Dict[str, Any]:
        """
        ETF 차트 데이터만 조회
        
        Args:
            symbol: ETF 심볼
            timeframe: 차트 시간대 ('1D', '1W', '1M')
            
        Returns:
            Dict[str, Any]: 차트 데이터
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
                # 데이터가 없어도 정상 응답 (시장 마감 중일 수 있음)
                return {
                    'symbol': symbol,
                    'timeframe': timeframe,
                    'chart_data': [],
                    'data_points': 0,
                    'market_status': self.market_checker.get_market_status(),
                    'last_updated': datetime.now(pytz.UTC).isoformat(),
                    'message': f'No recent data for {timeframe} timeframe. Market may be closed.'
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
            logger.error(f"❌ {symbol} ETF 차트 데이터 조회 실패: {e}")
            self.stats["errors"] += 1
            return {
                'symbol': symbol,
                'timeframe': timeframe,
                'chart_data': [],
                'error': str(e)
            }
        finally:
            db.close()
    
    def _format_timestamp_by_timeframe(self, timestamp: datetime, timeframe: str) -> str:
        """시간대별로 적절한 타임스탬프 포맷 반환"""
        try:
            if timeframe == '1D':
                return timestamp.strftime('%H:%M')
            elif timeframe == '1W':
                return timestamp.strftime('%m/%d')
            elif timeframe == '1M':
                return timestamp.strftime('%m/%d')
            else:
                return timestamp.strftime('%H:%M')
        except Exception as e:
            logger.error(f"❌ 타임스탬프 포맷 오류: {e}")
            return timestamp.isoformat()

    # =========================
    # 검색 API
    # =========================
    
    def search_etfs(self, query: str, limit: int = 20) -> Dict[str, Any]:
        """
        ETF 검색 (심볼 기준)
        
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
            logger.error(f"❌ ETF 검색 실패 ({query}): {e}")
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
        """전체 ETF 시장 개요 조회"""
        try:
            self.stats["api_requests"] += 1
            
            db = next(get_db())
            
            # 기본 시장 요약 정보
            total_etfs = db.query(ETFBasicInfo).count()
            
            # 최근 거래 데이터가 있는 ETF 수
            active_etfs = db.query(ETFRealtimePrices.symbol).distinct().count()
            
            market_summary = {
                'total_etfs': total_etfs,
                'active_etfs': active_etfs,
                'last_updated': datetime.now(pytz.UTC).isoformat()
            }
            
            return {
                'market_summary': market_summary,
                'market_status': self.market_checker.get_market_status(),
                'last_updated': datetime.now(pytz.UTC).isoformat()
            }
            
        except Exception as e:
            logger.error(f"❌ ETF 시장 개요 조회 실패: {e}")
            self.stats["errors"] += 1
            return {
                'market_summary': {},
                'market_status': self.market_checker.get_market_status(),
                'error': str(e)
            }
        finally:
            if 'db' in locals():
                db.close()
    
    # =========================
    # 배치 조회 함수들 (동기 버전)
    # =========================
    
    def get_batch_previous_close_prices_sync(self, symbols: List[str]) -> Dict[str, float]:
        """
        여러 심볼의 전일 종가를 일괄 조회 (동기 방식, WebSocket용)
        
        Args:
            symbols: ETF 심볼 리스트
            
        Returns:
            Dict[str, float]: {symbol: previous_close_price}
        """
        try:
            db = next(get_db())
            previous_close_prices = {}
            
            current_time = datetime.now(pytz.UTC)
            
            for symbol in symbols:
                prev_close = self._get_robust_previous_close_price(db, symbol, current_time)
                if prev_close:
                    previous_close_prices[symbol] = prev_close
            
            logger.debug(f"📊 ETF 전일 종가 조회 완료: {len(previous_close_prices)}개 / {len(symbols)}개")
            return previous_close_prices
            
        except Exception as e:
            logger.error(f"❌ ETF 전일 종가 일괄 조회 실패: {e}")
            return {}
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
            # get_etf_list를 사용하여 데이터 조회
            result = self.get_etf_list(limit)
            return result.get('etfs', [])
        except Exception as e:
            logger.error(f"❌ WebSocket 실시간 데이터 조회 실패: {e}")
            return []
    
    async def get_symbol_data(self, symbol: str) -> Optional[dict]:
        """
        WebSocket용 특정 심볼 데이터 조회 (비동기)
        
        Args:
            symbol: ETF 심볼
            
        Returns:
            Optional[dict]: 심볼 데이터
        """
        try:
            result = self.get_etf_details_by_symbol(symbol)
            if result:
                # ETFDetailResponse를 dict로 변환
                return {
                    'symbol': result.basic_info.symbol,
                    'name': result.basic_info.name,
                    'current_price': result.basic_info.current_price,
                    'change_amount': result.basic_info.change_amount,
                    'change_percentage': result.basic_info.change_percentage,
                    'volume': result.basic_info.volume,
                    'previous_close': result.basic_info.previous_close,
                    'is_positive': result.basic_info.is_positive,
                    'last_updated': result.basic_info.last_updated
                }
            return None
        except Exception as e:
            logger.error(f"❌ WebSocket 심볼 {symbol} 조회 실패: {e}")
            return None
    
    # =========================
    # 서비스 상태 및 헬스 체크
    # =========================
    
    def get_service_stats(self) -> Dict[str, Any]:
        """서비스 통계 정보 반환"""
        return {
            "service": "ETFService",
            "stats": self.stats,
            "market_status": self.market_checker.get_market_status()
        }
    
    def health_check(self) -> Dict[str, Any]:
        """서비스 헬스 체크"""
        try:
            is_healthy = self.stats["errors"] < 100
            
            return {
                "status": "healthy" if is_healthy else "degraded",
                "service": "ETFService",
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

def get_etf_data_from_redis(redis_client: redis.Redis, limit: int = 500) -> List[dict]:
    """
    동기 방식으로 Redis에서 ETF 데이터 조회 및 병합
    (WebSocket 핸들러에서 사용)
    
    Redis 키 구조:
    - etf_realtime_data (Consumer): {symbol: {"symbol": "SPY", "price": 450.5, "volume": 1000}}
    - etf_market_data (Airflow): {symbol: {"etf_name": "SPDR S&P 500", "change_percentage": 1.5, ...}}
    
    Args:
        redis_client: Redis 클라이언트
        limit: 최대 반환 개수
        
    Returns:
        List[dict]: 병합된 ETF 데이터 리스트
    """
    try:
        realtime_key = "etf_realtime_data"
        market_key = "etf_market_data"
        
        realtime_data_raw = redis_client.hgetall(realtime_key)
        market_data_raw = redis_client.hgetall(market_key)
        
        if not realtime_data_raw:
            logger.warning("Redis에 ETF 실시간 데이터 없음")
            return []
        
        merged_data = []
        
        # 실시간 데이터 기준으로만 병합
        for symbol_bytes, json_str_bytes in realtime_data_raw.items():
            symbol = symbol_bytes.decode('utf-8') if isinstance(symbol_bytes, bytes) else symbol_bytes
            json_str = json_str_bytes.decode('utf-8') if isinstance(json_str_bytes, bytes) else json_str_bytes
            
            try:
                realtime_data = json.loads(json_str)
            except json.JSONDecodeError:
                logger.warning(f"⚠️ ETF 실시간 데이터 파싱 실패: {symbol}")
                continue
            
            # 시장 데이터 조회 (없으면 빈 dict)
            market_json_bytes = market_data_raw.get(symbol_bytes)
            market_data = {}
            if market_json_bytes:
                market_json_str = market_json_bytes.decode('utf-8') if isinstance(market_json_bytes, bytes) else market_json_bytes
                try:
                    market_data = json.loads(market_json_str)
                except json.JSONDecodeError:
                    logger.warning(f"⚠️ ETF 시장 데이터 파싱 실패: {symbol}")
            
            # 병합 (SP500과 동일한 패턴)
            etf_item = {
                'symbol': realtime_data.get('symbol', symbol),
                'price': realtime_data.get('price', 0),
                'current_price': realtime_data.get('price', 0),
                'timestamp': realtime_data.get('timestamp'),
                
                # market_data 없으면 기본값 사용
                'name': market_data.get('etf_name', symbol),  # 프론트엔드 호환
                'etf_name': market_data.get('etf_name', symbol),
                'change_amount': market_data.get('change_amount', 0),
                'change_percentage': market_data.get('change_percentage', 0),
                'volume': realtime_data.get('volume', 0),
                'volume_24h': market_data.get('volume_24h', 0),
                'last_updated': market_data.get('last_updated'),
                'is_positive': market_data.get('change_amount', 0) > 0 if market_data.get('change_amount') is not None else None
            }
            
            merged_data.append(etf_item)
        
        # 변화율 기준 정렬
        merged_data.sort(key=lambda x: x.get('change_percentage', 0), reverse=True)
        
        logger.debug(f"✅ Redis ETF 데이터 병합 완료: {len(merged_data)}개")
        return merged_data[:limit]
        
    except Exception as e:
        logger.error(f"❌ ETF Redis 조회 실패: {e}")
        return []