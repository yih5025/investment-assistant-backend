# app/services/websocket_service.py
import asyncio
import json
import logging
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, timedelta
import hashlib
import pytz

from app.database import get_db
from app.config import settings
from app.models.topgainers_model import TopGainers
from app.models.bithumb_ticker_model import BithumbTicker
from app.models.sp500_model import SP500WebsocketTrades
from app.schemas.websocket_schema import (
    TopGainerData, CryptoData, SP500Data,
    db_to_topgainer_data, db_to_crypto_data, db_to_sp500_data
)

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
            return False  # 안전하게 장 마감으로 처리
    
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

class WebSocketService:
    """
    WebSocket 실시간 데이터 서비스 - 장 마감 시 DB fallback 강화
    
    이 클래스는 데이터베이스와 Redis에서 실시간 데이터를 조회하고,
    미국 장 마감 시간에는 Redis 대신 PostgreSQL에서 최신 데이터를 제공합니다.
    """
    
    def __init__(self):
        """WebSocketService 초기화"""
        self.redis_client = None
        self.last_data_cache: Dict[str, Any] = {}
        self.data_hashes: Dict[str, str] = {}
        self.market_checker = MarketTimeChecker()
        
        # 성능 통계
        self.stats = {
            "db_queries": 0,
            "redis_queries": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "changes_detected": 0,
            "last_update": None,
            "errors": 0,
            "market_closed_fallbacks": 0,
            "db_fallback_count": 0
        }
        
        logger.info("✅ WebSocketService 초기화 완료 (장 마감 시 DB fallback 지원)")
    
    async def init_redis(self) -> bool:
        """
        Redis 연결 초기화
        
        Returns:
            bool: Redis 연결 성공 여부
        """
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
            
            # 연결 테스트
            await self.redis_client.ping()
            logger.info("✅ Redis 연결 성공")
            return True
            
        except Exception as e:
            logger.warning(f"⚠️ Redis 연결 실패: {e} (DB fallback 모드)")
            self.redis_client = None
            return False
    
    # =========================
    # 🎯 장 마감 시 DB fallback 로직
    # =========================
    
    def should_use_db_fallback(self) -> bool:
        """
        DB fallback을 사용해야 하는지 판단
        
        Returns:
            bool: DB fallback 사용 여부
        """
        # Redis가 없으면 항상 DB 사용
        if not self.redis_client:
            return True
        
        # 미국 장이 마감된 경우 DB 사용 (최신 가격 유지)
        market_status = self.market_checker.get_market_status()
        if not market_status['is_open']:
            logger.debug("🕐 미국 장 마감 시간 - DB fallback 모드")
            self.stats["market_closed_fallbacks"] += 1
            return True
        
        return False
    
    # =========================
    # TopGainers 데이터 처리 (강화)
    # =========================
    
    async def get_topgainers_from_db(self, category: str = None, limit: int = 50) -> List[TopGainerData]:
        """
        데이터베이스에서 TopGainers 데이터 조회 (장 마감 시 최신 데이터 보장)
        
        Args:
            category: 카테고리 필터 (top_gainers, top_losers, most_actively_traded)
            limit: 반환할 최대 개수
            
        Returns:
            List[TopGainerData]: TopGainers 데이터 리스트
        """
        try:
            db = next(get_db())
            
            # 🎯 시장 상태에 따른 데이터 조회 전략
            if self.should_use_db_fallback():
                # 장 마감 시: 각 심볼의 최신 데이터 조회
                db_objects = TopGainers.get_latest_data_by_symbols(db, category, limit)
                logger.debug(f"📊 장 마감 시 TopGainers 최신 데이터 조회: {len(db_objects)}개")
            else:
                # 장 개장 시: 최신 batch_id 기준 조회
                latest_batch = TopGainers.get_latest_batch_id(db)
                if not latest_batch:
                    logger.warning("📊 TopGainers 데이터 없음")
                    return []
                
                batch_id = latest_batch[0]
                
                if category:
                    db_objects = TopGainers.get_by_category(db, category, batch_id, limit)
                else:
                    db_objects = db.query(TopGainers).filter(
                        TopGainers.batch_id == batch_id
                    ).order_by(TopGainers.rank_position).limit(limit).all()
            
            # Pydantic 모델로 변환
            data = [db_to_topgainer_data(obj) for obj in db_objects]
            
            self.stats["db_queries"] += 1
            self.stats["last_update"] = datetime.now(pytz.UTC)
            
            logger.debug(f"📊 TopGainers 데이터 조회 완료: {len(data)}개")
            return data
            
        except Exception as e:
            logger.error(f"❌ TopGainers DB 조회 실패: {e}")
            self.stats["errors"] += 1
            return []
        finally:
            db.close()
    
    async def get_topgainers_from_redis(self, category: str = None, limit: int = 50) -> List[TopGainerData]:
        """
        Redis에서 TopGainers 데이터 조회 (장 마감 시 DB fallback)
        
        Args:
            category: 카테고리 필터
            limit: 반환할 최대 개수
            
        Returns:
            List[TopGainerData]: TopGainers 데이터 리스트
        """
        # 🎯 장 마감 시 또는 Redis 없으면 DB fallback
        if self.should_use_db_fallback():
            self.stats["db_fallback_count"] += 1
            return await self.get_topgainers_from_db(category, limit)
        
        try:
            # Redis 키 패턴: latest:stocks:topgainers:{symbol}
            pattern = "latest:stocks:topgainers:*"
            keys = await self.redis_client.keys(pattern)
            
            if not keys:
                logger.debug("📊 Redis TopGainers 데이터 없음, DB fallback")
                return await self.get_topgainers_from_db(category, limit)
            
            # 모든 키의 데이터 가져오기
            pipeline = self.redis_client.pipeline()
            for key in keys:
                pipeline.get(key)
            
            results = await pipeline.execute()
            
            # JSON 파싱 및 필터링
            data = []
            for result in results:
                if result:
                    try:
                        json_data = json.loads(result)
                        
                        # 카테고리 필터링
                        if category and json_data.get('category') != category:
                            continue
                        
                        # TopGainerData 생성
                        data.append(TopGainerData(**json_data))
                        
                    except (json.JSONDecodeError, ValueError) as e:
                        logger.warning(f"⚠️ Redis 데이터 파싱 실패: {e}")
                        continue
            
            # 순위별 정렬 및 제한
            data.sort(key=lambda x: x.rank_position or 999)
            data = data[:limit]
            
            self.stats["redis_queries"] += 1
            self.stats["last_update"] = datetime.now(pytz.UTC)
            
            logger.debug(f"📊 Redis TopGainers 데이터 조회 완료: {len(data)}개")
            return data
            
        except Exception as e:
            logger.error(f"❌ Redis TopGainers 조회 실패: {e}, DB fallback")
            self.stats["errors"] += 1
            return await self.get_topgainers_from_db(category, limit)
    
    # =========================
    # SP500 데이터 처리 (강화)
    # =========================
    
    async def get_sp500_from_db(self, category: str = None, limit: int = 100) -> List[SP500Data]:
        """
        데이터베이스에서 SP500 데이터 조회 (장 마감 시 최신 데이터 보장)
        
        Args:
            category: 카테고리 필터 (top_gainers, most_actively_traded, top_losers)
            limit: 반환할 최대 개수
            
        Returns:
            List[SP500Data]: SP500 데이터 리스트
        """
        try:
            db = next(get_db())
            
            # 🎯 시장 상태에 따른 조회 전략
            if self.should_use_db_fallback():
                # 장 마감 시: 각 심볼의 최신 가격 조회 (기존 메서드 사용)
                db_objects = SP500WebsocketTrades.get_all_current_prices(db, limit)
                logger.debug(f"📊 장 마감 시 SP500 최신 데이터 조회: {len(db_objects)}개")
            else:
                # 장 개장 시: 동일한 메서드 사용 (Redis 우선, DB fallback)
                db_objects = SP500WebsocketTrades.get_all_current_prices(db, limit)
                logger.debug(f"📊 장 개장 시 SP500 데이터 조회: {len(db_objects)}개")
            
            # Pydantic 모델로 변환
            data = [db_to_sp500_data(obj) for obj in db_objects]
            
            self.stats["db_queries"] += 1
            self.stats["last_update"] = datetime.now(pytz.UTC)
            
            logger.debug(f"📊 SP500 데이터 조회 완료: {len(data)}개")
            return data
            
        except Exception as e:
            logger.error(f"❌ SP500 DB 조회 실패: {e}")
            self.stats["errors"] += 1
            return []
        finally:
            db.close()
    
    async def get_sp500_from_redis(self, category: str = None, limit: int = 100) -> List[SP500Data]:
        """
        Redis에서 SP500 데이터 조회 (장 마감 시 DB fallback)
        
        Args:
            category: 카테고리 필터
            limit: 반환할 최대 개수
            
        Returns:
            List[SP500Data]: SP500 데이터 리스트
        """
        # 🎯 장 마감 시 또는 Redis 없으면 DB fallback
        if self.should_use_db_fallback():
            self.stats["db_fallback_count"] += 1
            return await self.get_sp500_from_db(category, limit)
        
        try:
            # Redis 키 패턴: latest:stocks:sp500:{symbol}
            pattern = "latest:stocks:sp500:*"
            keys = await self.redis_client.keys(pattern)
            
            if not keys:
                logger.debug("📊 Redis SP500 데이터 없음, DB fallback")
                return await self.get_sp500_from_db(category, limit)
            
            # 모든 키의 데이터 가져오기
            pipeline = self.redis_client.pipeline()
            for key in keys:
                pipeline.get(key)
            
            results = await pipeline.execute()
            
            # JSON 파싱 및 필터링
            data = []
            for i, result in enumerate(results):
                if result:
                    try:
                        json_data = json.loads(result)
                        
                        # 카테고리 필터링
                        if category and json_data.get('category') != category:
                            continue
                        
                        # SP500Data 생성
                        sp500_data = SP500Data(
                            symbol=json_data.get('symbol', keys[i].split(':')[-1]),
                            price=json_data.get('price'),
                            volume=json_data.get('volume'),
                            timestamp_ms=json_data.get('timestamp'),
                            category=json_data.get('category'),
                            source=json_data.get('source', 'finnhub_websocket')
                        )
                        data.append(sp500_data)
                        
                    except (json.JSONDecodeError, ValueError) as e:
                        logger.warning(f"⚠️ Redis SP500 데이터 파싱 실패: {e}")
                        continue
            
            # 거래량별 정렬 및 제한
            data.sort(key=lambda x: x.volume or 0, reverse=True)
            data = data[:limit]
            
            self.stats["redis_queries"] += 1
            self.stats["last_update"] = datetime.now(pytz.UTC)
            
            logger.debug(f"📊 Redis SP500 데이터 조회 완료: {len(data)}개")
            return data
            
        except Exception as e:
            logger.error(f"❌ Redis SP500 조회 실패: {e}, DB fallback")
            self.stats["errors"] += 1
            return await self.get_sp500_from_db(category, limit)
    
    # =========================
    # 암호화폐 데이터 처리 (기존 유지 - 항상 데이터 있음)
    # =========================
    
    async def get_crypto_from_db(self, limit: int = 100) -> List[CryptoData]:
        """
        데이터베이스에서 암호화폐 데이터 조회
        
        Args:
            limit: 반환할 최대 개수
            
        Returns:
            List[CryptoData]: 암호화폐 데이터 리스트
        """
        try:
            db = next(get_db())
            
            # 모든 마켓의 최신 가격 조회
            db_objects = BithumbTicker.get_all_latest_prices(db, limit)
            
            # Pydantic 모델로 변환
            data = [db_to_crypto_data(obj) for obj in db_objects]
            
            self.stats["db_queries"] += 1
            self.stats["last_update"] = datetime.now(pytz.UTC)
            
            logger.debug(f"📊 암호화폐 데이터 조회 완료: {len(data)}개")
            return data
            
        except Exception as e:
            logger.error(f"❌ 암호화폐 DB 조회 실패: {e}")
            self.stats["errors"] += 1
            return []
        finally:
            db.close()
    
    async def get_crypto_from_redis(self, limit: int = 100) -> List[CryptoData]:
        """
        Redis에서 암호화폐 데이터 조회 (암호화폐는 24시간 거래이므로 Redis 우선)
        
        Args:
            limit: 반환할 최대 개수
            
        Returns:
            List[CryptoData]: 암호화폐 데이터 리스트
        """
        if not self.redis_client:
            return await self.get_crypto_from_db(limit)
        
        try:
            # Redis 키 패턴: latest:crypto:{market}
            pattern = "latest:crypto:*"
            keys = await self.redis_client.keys(pattern)
            
            if not keys:
                logger.debug("📊 Redis 암호화폐 데이터 없음, DB fallback")
                return await self.get_crypto_from_db(limit)
            
            # 모든 키의 데이터 가져오기
            pipeline = self.redis_client.pipeline()
            for key in keys:
                pipeline.get(key)
            
            results = await pipeline.execute()
            
            # JSON 파싱
            data = []
            for i, result in enumerate(results):
                if result:
                    try:
                        json_data = json.loads(result)
                        
                        # CryptoData 생성 (Redis 형태 → DB 형태 변환)
                        crypto_data = CryptoData(
                            market=json_data.get('symbol', keys[i].split(':')[-1]),
                            trade_price=json_data.get('price'),
                            signed_change_rate=json_data.get('change_rate'),
                            signed_change_price=json_data.get('change_price'),
                            trade_volume=json_data.get('volume'),
                            timestamp_field=json_data.get('timestamp'),
                            source=json_data.get('source', 'bithumb')
                        )
                        data.append(crypto_data)
                        
                    except (json.JSONDecodeError, ValueError) as e:
                        logger.warning(f"⚠️ Redis 암호화폐 데이터 파싱 실패: {e}")
                        continue
            
            # 거래량별 정렬 및 제한
            data.sort(key=lambda x: x.trade_volume or 0, reverse=True)
            data = data[:limit]
            
            self.stats["redis_queries"] += 1
            self.stats["last_update"] = datetime.now(pytz.UTC)
            
            logger.debug(f"📊 Redis 암호화폐 데이터 조회 완료: {len(data)}개")
            return data
            
        except Exception as e:
            logger.error(f"❌ Redis 암호화폐 조회 실패: {e}, DB fallback")
            self.stats["errors"] += 1
            return await self.get_crypto_from_db(limit)
    
    # =========================
    # 🎯 시장별 최적화된 데이터 조회
    # =========================
    
    async def get_market_optimized_data(self, data_type: str, limit: int = 50) -> List[Any]:
        """
        시장 상태에 따른 최적화된 데이터 조회
        
        Args:
            data_type: 데이터 타입 (topgainers, sp500, crypto)
            limit: 반환할 최대 개수
            
        Returns:
            List[Any]: 데이터 리스트
        """
        market_status = self.market_checker.get_market_status()
        
        logger.debug(f"🕐 시장 상태: {market_status['status']} - {data_type} 데이터 조회")
        
        if data_type == "topgainers":
            if market_status['is_open']:
                # 개장 시: Redis 우선
                return await self.get_topgainers_from_redis(limit=limit)
            else:
                # 장 마감 시: DB에서 최신 데이터
                return await self.get_topgainers_from_db(limit=limit)
                
        elif data_type == "sp500":
            if market_status['is_open']:
                # 개장 시: Redis 우선
                return await self.get_sp500_from_redis(limit=limit)
            else:
                # 장 마감 시: DB에서 최신 데이터
                return await self.get_sp500_from_db(limit=limit)
                
        elif data_type == "crypto":
            # 암호화폐는 24시간 거래이므로 항상 Redis 우선
            return await self.get_crypto_from_redis(limit=limit)
        
        else:
            logger.warning(f"⚠️ 알 수 없는 데이터 타입: {data_type}")
            return []
    
    # =========================
    # 대시보드 데이터 처리 (장 마감 대응)
    # =========================
    
    async def get_dashboard_data(self) -> Dict[str, Any]:
        """
        대시보드용 통합 데이터 조회 (시장 상태 대응)
        
        Returns:
            Dict[str, Any]: 대시보드 데이터
        """
        try:
            market_status = self.market_checker.get_market_status()
            
            # 병렬로 데이터 조회 (시장 상태에 따라 최적화)
            tasks = [
                self.get_market_optimized_data("topgainers", 10),  # 상위 10개 상승 주식
                self.get_market_optimized_data("crypto", 20),      # 상위 20개 암호화폐
                self.get_market_optimized_data("sp500", 15)        # SP500 15개
            ]
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            top_gainers = results[0] if not isinstance(results[0], Exception) else []
            top_crypto = results[1] if not isinstance(results[1], Exception) else []
            sp500_highlights = results[2] if not isinstance(results[2], Exception) else []
            
            # 요약 통계 계산
            summary = {
                "top_gainers_count": len(top_gainers),
                "crypto_count": len(top_crypto),
                "sp500_count": len(sp500_highlights),
                "last_updated": datetime.now(pytz.UTC).isoformat(),
                "data_sources": ["topgainers", "crypto", "sp500"],
                "market_status": market_status,  # 🎯 시장 상태 추가
                "db_fallback_used": self.should_use_db_fallback()
            }
            
            return {
                "top_gainers": top_gainers,
                "top_crypto": top_crypto,
                "sp500_highlights": sp500_highlights,
                "summary": summary
            }
            
        except Exception as e:
            logger.error(f"❌ 대시보드 데이터 조회 실패: {e}")
            return {
                "top_gainers": [],
                "top_crypto": [],
                "sp500_highlights": [],
                "summary": {"error": str(e)}
            }
    

    async def get_sp500_from_redis_with_changes(self, limit: int = 100) -> List[Any]:
        """
        Redis에서 SP500 데이터 조회 + 전날 종가 기반 변화율 계산
        
        Args:
            limit: 반환할 최대 개수
            
        Returns:
            List[Any]: 변화율이 계산된 SP500 데이터
        """
        try:
            # 🎯 1. Redis에서 현재가 조회 (기존 로직)
            current_data = await self.get_sp500_from_redis(limit)
            if not current_data:
                return []
            
            # 🎯 2. 심볼 리스트 추출
            symbols = [item.symbol for item in current_data if hasattr(item, 'symbol')]
            
            # 🎯 3. 전날 종가 일괄 조회 (캐싱)
            previous_close_prices = await self._get_cached_previous_close_prices(symbols, 'sp500')
            
            # 🎯 4. 변화율 계산 및 추가
            enhanced_data = []
            for item in current_data:
                if hasattr(item, 'symbol') and item.symbol in previous_close_prices:
                    current_price = float(item.price) if item.price else 0
                    previous_close = previous_close_prices[item.symbol]
                    
                    # 변화 계산
                    change_amount = current_price - previous_close
                    change_percentage = (change_amount / previous_close) * 100 if previous_close > 0 else 0
                    
                    # 기존 데이터에 변화 정보 추가
                    if hasattr(item, 'dict'):
                        enhanced_item = item.dict()
                    else:
                        enhanced_item = item
                    
                    enhanced_item.update({
                        'current_price': current_price,
                        'previous_close': previous_close,
                        'change_amount': round(change_amount, 2),
                        'change_percentage': round(change_percentage, 2),
                        'is_positive': change_amount > 0,
                        'change_color': 'green' if change_amount > 0 else 'red' if change_amount < 0 else 'gray'
                    })
                    
                    enhanced_data.append(enhanced_item)
                else:
                    # 전날 종가 없는 경우 기본값
                    if hasattr(item, 'dict'):
                        enhanced_item = item.dict()
                    else:
                        enhanced_item = item
                    
                    enhanced_item.update({
                        'current_price': float(item.price) if item.price else 0,
                        'previous_close': None,
                        'change_amount': None,
                        'change_percentage': None,
                        'is_positive': None,
                        'change_color': 'gray'
                    })
                    
                    enhanced_data.append(enhanced_item)
            
            return enhanced_data
            
        except Exception as e:
            logger.error(f"❌ SP500 변화율 계산 실패: {e}")
            return await self.get_sp500_from_redis(limit)  # fallback

    async def get_topgainers_from_redis_with_changes(self, category: str = None, limit: int = 50) -> List[Any]:
        """
        Redis에서 TopGainers 데이터 조회 + 전날 종가 기반 변화율 계산
        
        Args:
            category: 카테고리 필터
            limit: 반환할 최대 개수
            
        Returns:
            List[Any]: 변화율이 계산된 TopGainers 데이터
        """
        try:
            # 🎯 1. Redis에서 현재가 조회 (기존 로직)
            current_data = await self.get_topgainers_from_redis(category, limit)
            if not current_data:
                return []
            
            # 🎯 2. 심볼 리스트 추출
            symbols = [item.symbol for item in current_data if hasattr(item, 'symbol')]
            
            # 🎯 3. 전날 종가 일괄 조회 (캐싱)
            previous_close_prices = await self._get_cached_previous_close_prices(symbols, 'topgainers')
            
            # 🎯 4. 변화율 계산 및 추가
            enhanced_data = []
            for item in current_data:
                if hasattr(item, 'symbol') and item.symbol in previous_close_prices:
                    current_price = float(item.price) if item.price else 0
                    previous_close = previous_close_prices[item.symbol]
                    
                    # 변화 계산
                    change_amount = current_price - previous_close
                    change_percentage = (change_amount / previous_close) * 100 if previous_close > 0 else 0
                    
                    # 기존 데이터에 변화 정보 추가
                    if hasattr(item, 'dict'):
                        enhanced_item = item.dict()
                    else:
                        enhanced_item = item
                    
                    enhanced_item.update({
                        'current_price': current_price,
                        'previous_close': previous_close,
                        'change_amount': round(change_amount, 2),
                        'change_percentage': round(change_percentage, 2),
                        'is_positive': change_amount > 0,
                        'change_color': 'green' if change_amount > 0 else 'red' if change_amount < 0 else 'gray'
                    })
                    
                    enhanced_data.append(enhanced_item)
                else:
                    # 전날 종가 없는 경우 기본값
                    if hasattr(item, 'dict'):
                        enhanced_item = item.dict()
                    else:
                        enhanced_item = item
                    
                    enhanced_item.update({
                        'current_price': float(item.price) if item.price else 0,
                        'previous_close': None,
                        'change_amount': None,
                        'change_percentage': None,
                        'is_positive': None,
                        'change_color': 'gray'
                    })
                    
                    enhanced_data.append(enhanced_item)
            
            return enhanced_data
            
        except Exception as e:
            logger.error(f"❌ TopGainers 변화율 계산 실패: {e}")
            return await self.get_topgainers_from_redis(category, limit)  # fallback
    # =========================
    # 전날 종가 캐싱 및 조회 메서드
    # =========================
    
    async def _get_cached_previous_close_prices(self, symbols: List[str], data_type: str) -> Dict[str, float]:
        """
        여러 심볼의 전날 종가를 캐싱하여 조회 (성능 최적화)
        
        Args:
            symbols: 주식 심볼 리스트
            data_type: 데이터 타입 ('sp500', 'topgainers')
            
        Returns:
            Dict[str, float]: {symbol: previous_close_price}
        """
        try:
            cache_key = f"previous_close_{data_type}"
            
            # 캐시에서 먼저 확인
            if cache_key in self.last_data_cache:
                cached_data = self.last_data_cache[cache_key]
                # 캐시 유효성 확인 (1시간 이내)
                if isinstance(cached_data, dict) and cached_data.get('timestamp'):
                    cache_time = datetime.fromisoformat(cached_data['timestamp'].replace('Z', '+00:00'))
                    if datetime.now(pytz.UTC) - cache_time < timedelta(hours=1):
                        logger.debug(f"📊 전날 종가 캐시 히트: {data_type}")
                        return cached_data.get('data', {})
            
            # 캐시 미스 또는 만료된 경우 DB에서 조회
            db = next(get_db())
            
            if data_type == 'sp500':
                # SP500 모델 사용
                from app.models.sp500_model import SP500WebsocketTrades
                previous_close_prices = {}
                
                for symbol in symbols:
                    prev_close = SP500WebsocketTrades.get_previous_close_price(db, symbol)
                    if prev_close:
                        previous_close_prices[symbol] = prev_close
                        
            elif data_type == 'topgainers':
                # TopGainers 모델 사용
                from app.models.topgainers_model import TopGainers
                previous_close_prices = TopGainers.get_batch_previous_close_prices(db, symbols)
            else:
                previous_close_prices = {}
            
            # 캐시에 저장
            self.last_data_cache[cache_key] = {
                'data': previous_close_prices,
                'timestamp': datetime.now(pytz.UTC).isoformat()
            }
            
            logger.debug(f"📊 전날 종가 DB 조회 완료: {data_type}, {len(previous_close_prices)}개")
            return previous_close_prices
            
        except Exception as e:
            logger.error(f"❌ 전날 종가 조회 실패 ({data_type}): {e}")
            return {}
        finally:
            if 'db' in locals():
                db.close()

    # app/services/websocket_service.py에 추가할 메서드들

    def getStockName(self, symbol: str) -> str:
        """주식 이름 조회 (sp500_companies 테이블 사용) - 기존 하드코딩 대체"""
        try:
            db = next(get_db())
            
            result = db.execute(
                "SELECT company_name FROM sp500_companies WHERE symbol = %s",
                (symbol,)
            ).fetchone()
            
            if result and result[0]:
                return result[0]
            else:
                return f"{symbol} Inc."
                
        except Exception as e:
            logger.warning(f"⚠️ {symbol} 주식 이름 조회 실패: {e}")
            return f"{symbol} Inc."
        finally:
            if 'db' in locals():
                db.close()

    def getCryptoName(self, market_code: str) -> str:
        """암호화폐 이름 조회 (market_code_bithumb 테이블 사용)"""
        try:
            db = next(get_db())
            
            result = db.execute(
                """SELECT korean_name, english_name 
                FROM market_code_bithumb 
                WHERE market_code = %s""",
                (market_code,)
            ).fetchone()
            
            if result:
                korean_name, english_name = result
                if korean_name and english_name:
                    return f"{korean_name} ({english_name})"
                elif korean_name:
                    return korean_name
                elif english_name:
                    return english_name
                else:
                    return market_code.replace('KRW-', '')
            else:
                return market_code.replace('KRW-', '')
                
        except Exception as e:
            logger.warning(f"⚠️ {market_code} 암호화폐 이름 조회 실패: {e}")
            return market_code.replace('KRW-', '')
        finally:
            if 'db' in locals():
                db.close()
    # =========================
    # 변경 감지 및 캐싱 (기존 유지)
    # =========================
    
    def detect_changes(self, new_data: List[Any], data_type: str = "topgainers") -> Tuple[List[Any], int]:
        """
        데이터 변경 감지
        
        Args:
            new_data: 새로운 데이터 리스트
            data_type: 데이터 타입
            
        Returns:
            Tuple[List[Any], int]: (변경된 데이터, 변경 개수)
        """
        cache_key = f"{data_type}_last_data"
        hash_key = f"{data_type}_hash"
        
        # 새 데이터 해시 계산
        new_hash = self._calculate_data_hash(new_data)
        
        # 이전 해시와 비교
        previous_hash = self.data_hashes.get(hash_key)
        
        if previous_hash == new_hash:
            # 변경 없음
            self.stats["cache_hits"] += 1
            return [], 0
        
        # 변경 감지됨
        self.data_hashes[hash_key] = new_hash
        self.last_data_cache[cache_key] = new_data
        self.stats["cache_misses"] += 1
        self.stats["changes_detected"] += 1
        
        logger.debug(f"📊 {data_type} 데이터 변경 감지: {len(new_data)}개")
        return new_data, len(new_data)
    
    def _calculate_data_hash(self, data: List[Any]) -> str:
        """
        데이터 리스트의 해시 계산
        
        Args:
            data: 데이터 리스트
            
        Returns:
            str: MD5 해시 값
        """
        try:
            # 데이터를 JSON 문자열로 변환 (정렬하여 일관성 보장)
            json_str = json.dumps([
                item.dict() if hasattr(item, 'dict') else str(item) 
                for item in data
            ], sort_keys=True)
            
            # MD5 해시 계산
            return hashlib.md5(json_str.encode()).hexdigest()
            
        except Exception as e:
            logger.warning(f"⚠️ 데이터 해시 계산 실패: {e}")
            return str(hash(str(data)))
    

    
    # =========================
    # 헬스 체크 및 통계
    # =========================
    
    async def health_check(self) -> Dict[str, Any]:
        """
        서비스 헬스 체크
        
        Returns:
            Dict[str, Any]: 헬스 체크 결과
        """
        health_info = {
            "timestamp": datetime.now(pytz.UTC).isoformat(),
            "status": "healthy",
            "services": {}
        }
        
        # Redis 연결 상태 확인
        if self.redis_client:
            try:
                await asyncio.wait_for(self.redis_client.ping(), timeout=3.0)
                health_info["services"]["redis"] = {"status": "connected", "mode": "primary"}
            except Exception as e:
                health_info["services"]["redis"] = {"status": "disconnected", "error": str(e), "mode": "fallback"}
                health_info["status"] = "degraded"
        else:
            health_info["services"]["redis"] = {"status": "not_configured", "mode": "db_only"}
            health_info["status"] = "degraded"
        
        # 데이터베이스 연결 상태 확인
        try:
            db = next(get_db())
            # 간단한 쿼리로 DB 연결 테스트
            result = db.execute("SELECT 1").fetchone()
            health_info["services"]["database"] = {"status": "connected"}
            db.close()
        except Exception as e:
            health_info["services"]["database"] = {"status": "disconnected", "error": str(e)}
            health_info["status"] = "unhealthy"
        
        # 최근 데이터 업데이트 확인
        last_update = self.stats.get("last_update")
        if last_update:
            time_since_update = (datetime.now(pytz.UTC) - last_update).total_seconds()
            if time_since_update > 300:  # 5분 이상 업데이트 없음
                health_info["data_freshness"] = "stale"
                health_info["status"] = "degraded"
            else:
                health_info["data_freshness"] = "fresh"
        else:
            health_info["data_freshness"] = "unknown"
        
        return health_info
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        서비스 통계 정보 반환
        
        Returns:
            Dict[str, Any]: 통계 정보
        """
        total_queries = self.stats["db_queries"] + self.stats["redis_queries"]
        cache_hit_rate = (
            self.stats["cache_hits"] / max(self.stats["cache_hits"] + self.stats["cache_misses"], 1) * 100
        )
        
        return {
            "performance": {
                "total_queries": total_queries,
                "db_queries": self.stats["db_queries"],
                "redis_queries": self.stats["redis_queries"],
                "cache_hit_rate": f"{cache_hit_rate:.1f}%",
                "changes_detected": self.stats["changes_detected"],
                "errors": self.stats["errors"]
            },
            "data_status": {
                "last_update": self.stats["last_update"].isoformat() if self.stats["last_update"] else None,
                "cached_datasets": len(self.last_data_cache),
                "data_hashes": len(self.data_hashes)
            },
            "health": {
                "redis_available": self.redis_client is not None,
                "error_rate": self.stats["errors"] / max(total_queries, 1) * 100
            }
        }
    
    async def cleanup_cache(self):
        """캐시 정리 (주기적으로 실행)"""
        try:
            # 1시간 이상 된 캐시 데이터 정리
            cutoff_time = datetime.now(pytz.UTC) - timedelta(hours=24)
            
            if self.stats.get("last_update") and self.stats["last_update"] < cutoff_time:
                self.last_data_cache.clear()
                self.data_hashes.clear()
                logger.info("🧹 WebSocket 서비스 캐시 정리 완료")
                
        except Exception as e:
            logger.error(f"❌ 캐시 정리 실패: {e}")
    
    async def shutdown(self):
        """서비스 종료 처리"""
        try:
            if self.redis_client:
                await self.redis_client.close()
                logger.info("✅ Redis 연결 종료")
            
            # 캐시 정리
            self.last_data_cache.clear()
            self.data_hashes.clear()
            
            logger.info("✅ WebSocketService 종료 완료")
            
        except Exception as e:
            logger.error(f"❌ WebSocketService 종료 실패: {e}")