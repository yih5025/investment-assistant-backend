# app/services/websocket_service.py
import asyncio
import json
import logging
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, timedelta
import hashlib

from app.database import get_db
from app.config import settings
from app.models.top_gainers_model import TopGainers
from app.models.bithumb_ticker_model import BithumbTicker
from app.models.finnhub_trades_model import FinnhubTrades
from app.schemas.websocket_schema import (
    TopGainerData, CryptoData, SP500Data,
    db_to_topgainer_data, db_to_crypto_data, db_to_sp500_data
)

logger = logging.getLogger(__name__)

class WebSocketService:
    """
    WebSocket 실시간 데이터 서비스
    
    이 클래스는 데이터베이스와 Redis에서 실시간 데이터를 조회하고,
    변경 감지 및 캐싱 기능을 제공합니다.
    """
    
    def __init__(self):
        """WebSocketService 초기화"""
        self.redis_client = None
        self.last_data_cache: Dict[str, Any] = {}
        self.data_hashes: Dict[str, str] = {}
        
        # 성능 통계
        self.stats = {
            "db_queries": 0,
            "redis_queries": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "changes_detected": 0,
            "last_update": None,
            "errors": 0
        }
        
        logger.info("✅ WebSocketService 초기화 완료")
    
    async def init_redis(self) -> bool:
        """
        Redis 연결 초기화
        
        Returns:
            bool: Redis 연결 성공 여부
        """
        try:
            import redis.asyncio as redis
            
            self.redis_client = redis.Redis(
                host=settings.REDIS_HOST,
                port=settings.REDIS_PORT,
                db=settings.REDIS_DB,
                password=settings.REDIS_PASSWORD,
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
    # Top Gainers 데이터 처리
    # =========================
    
    async def get_topgainers_from_db(self, category: str = None, limit: int = 50) -> List[TopGainerData]:
        """
        데이터베이스에서 TopGainers 데이터 조회
        
        Args:
            category: 카테고리 필터 (top_gainers, top_losers, most_actively_traded)
            limit: 반환할 최대 개수
            
        Returns:
            List[TopGainerData]: TopGainers 데이터 리스트
        """
        try:
            db = next(get_db())
            
            # 최신 batch_id 조회
            latest_batch = TopGainers.get_latest_batch_id(db)
            if not latest_batch:
                logger.warning("📊 TopGainers 데이터 없음")
                return []
            
            batch_id = latest_batch[0]
            
            if category:
                # 특정 카테고리만 조회
                db_objects = TopGainers.get_by_category(db, category, batch_id, limit)
            else:
                # 모든 카테고리 조회
                db_objects = db.query(TopGainers).filter(
                    TopGainers.batch_id == batch_id
                ).order_by(TopGainers.rank_position).limit(limit).all()
            
            # Pydantic 모델로 변환
            data = [db_to_topgainer_data(obj) for obj in db_objects]
            
            self.stats["db_queries"] += 1
            self.stats["last_update"] = datetime.utcnow()
            
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
        Redis에서 TopGainers 데이터 조회
        
        Args:
            category: 카테고리 필터
            limit: 반환할 최대 개수
            
        Returns:
            List[TopGainerData]: TopGainers 데이터 리스트
        """
        if not self.redis_client:
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
            self.stats["last_update"] = datetime.utcnow()
            
            logger.debug(f"📊 Redis TopGainers 데이터 조회 완료: {len(data)}개")
            return data
            
        except Exception as e:
            logger.error(f"❌ Redis TopGainers 조회 실패: {e}, DB fallback")
            self.stats["errors"] += 1
            return await self.get_topgainers_from_db(category, limit)
    
    # =========================
    # 암호화폐 데이터 처리
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
            self.stats["last_update"] = datetime.utcnow()
            
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
        Redis에서 암호화폐 데이터 조회
        
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
            self.stats["last_update"] = datetime.utcnow()
            
            logger.debug(f"📊 Redis 암호화폐 데이터 조회 완료: {len(data)}개")
            return data
            
        except Exception as e:
            logger.error(f"❌ Redis 암호화폐 조회 실패: {e}, DB fallback")
            self.stats["errors"] += 1
            return await self.get_crypto_from_db(limit)
    
    # =========================
    # SP500 데이터 처리
    # =========================
    
    async def get_sp500_from_db(self, category: str = None, limit: int = 100) -> List[SP500Data]:
        """
        데이터베이스에서 SP500 데이터 조회
        
        Args:
            category: 카테고리 필터 (top_gainers, most_actively_traded, top_losers)
            limit: 반환할 최대 개수
            
        Returns:
            List[SP500Data]: SP500 데이터 리스트
        """
        try:
            db = next(get_db())
            
            if category:
                # 특정 카테고리 최신 가격들 조회
                db_objects = FinnhubTrades.get_latest_prices(db, category=category)
            else:
                # 모든 심볼의 최신 가격들 조회
                db_objects = FinnhubTrades.get_latest_prices(db)
            
            # 결과 제한
            db_objects = db_objects[:limit]
            
            # Pydantic 모델로 변환
            data = [db_to_sp500_data(obj) for obj in db_objects]
            
            self.stats["db_queries"] += 1
            self.stats["last_update"] = datetime.utcnow()
            
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
        Redis에서 SP500 데이터 조회
        
        Args:
            category: 카테고리 필터
            limit: 반환할 최대 개수
            
        Returns:
            List[SP500Data]: SP500 데이터 리스트
        """
        if not self.redis_client:
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
            self.stats["last_update"] = datetime.utcnow()
            
            logger.debug(f"📊 Redis SP500 데이터 조회 완료: {len(data)}개")
            return data
            
        except Exception as e:
            logger.error(f"❌ Redis SP500 조회 실패: {e}, DB fallback")
            self.stats["errors"] += 1
            return await self.get_sp500_from_db(category, limit)
    
    # =========================
    # 특정 심볼 데이터 처리
    # =========================
    
    async def get_symbol_realtime_data(self, symbol: str, data_type: str = "topgainers") -> Optional[Any]:
        """
        특정 심볼의 실시간 데이터 조회
        
        Args:
            symbol: 조회할 심볼
            data_type: 데이터 타입 (topgainers, crypto, sp500)
            
        Returns:
            Optional[Any]: 심볼 데이터 (데이터 타입에 따라 다름)
        """
        symbol = symbol.upper()
        
        try:
            if data_type == "topgainers":
                return await self._get_topgainer_symbol_data(symbol)
            elif data_type == "crypto":
                return await self._get_crypto_symbol_data(symbol)
            elif data_type == "sp500":
                return await self._get_sp500_symbol_data(symbol)
            else:
                logger.warning(f"⚠️ 알 수 없는 데이터 타입: {data_type}")
                return None
                
        except Exception as e:
            logger.error(f"❌ 심볼 {symbol} ({data_type}) 데이터 조회 실패: {e}")
            return None
    
    async def _get_topgainer_symbol_data(self, symbol: str) -> Optional[TopGainerData]:
        """TopGainers 특정 심볼 데이터 조회"""
        if self.redis_client:
            try:
                key = f"latest:stocks:topgainers:{symbol}"
                result = await self.redis_client.get(key)
                
                if result:
                    json_data = json.loads(result)
                    return TopGainerData(**json_data)
            except Exception as e:
                logger.warning(f"⚠️ Redis TopGainer 심볼 조회 실패: {e}")
        
        # DB fallback
        try:
            db = next(get_db())
            db_obj = TopGainers.get_symbol_data(db, symbol)
            return db_to_topgainer_data(db_obj) if db_obj else None
        except Exception as e:
            logger.error(f"❌ DB TopGainer 심볼 조회 실패: {e}")
            return None
        finally:
            db.close()
    
    async def _get_crypto_symbol_data(self, symbol: str) -> Optional[CryptoData]:
        """암호화폐 특정 심볼 데이터 조회"""
        if self.redis_client:
            try:
                key = f"latest:crypto:{symbol}"
                result = await self.redis_client.get(key)
                
                if result:
                    json_data = json.loads(result)
                    return CryptoData(
                        market=symbol,
                        trade_price=json_data.get('price'),
                        signed_change_rate=json_data.get('change_rate'),
                        signed_change_price=json_data.get('change_price'),
                        trade_volume=json_data.get('volume'),
                        timestamp_field=json_data.get('timestamp'),
                        source=json_data.get('source', 'bithumb')
                    )
            except Exception as e:
                logger.warning(f"⚠️ Redis 암호화폐 심볼 조회 실패: {e}")
        
        # DB fallback
        try:
            db = next(get_db())
            db_objects = BithumbTicker.get_latest_by_market(db, symbol, limit=1)
            return db_to_crypto_data(db_objects[0]) if db_objects else None
        except Exception as e:
            logger.error(f"❌ DB 암호화폐 심볼 조회 실패: {e}")
            return None
        finally:
            db.close()
    
    async def _get_sp500_symbol_data(self, symbol: str) -> Optional[SP500Data]:
        """SP500 특정 심볼 데이터 조회"""
        if self.redis_client:
            try:
                key = f"latest:stocks:sp500:{symbol}"
                result = await self.redis_client.get(key)
                
                if result:
                    json_data = json.loads(result)
                    return SP500Data(
                        symbol=symbol,
                        price=json_data.get('price'),
                        volume=json_data.get('volume'),
                        timestamp_ms=json_data.get('timestamp'),
                        category=json_data.get('category'),
                        source=json_data.get('source', 'finnhub_websocket')
                    )
            except Exception as e:
                logger.warning(f"⚠️ Redis SP500 심볼 조회 실패: {e}")
        
        # DB fallback
        try:
            db = next(get_db())
            db_objects = FinnhubTrades.get_latest_by_symbol(db, symbol, limit=1)
            return db_to_sp500_data(db_objects[0]) if db_objects else None
        except Exception as e:
            logger.error(f"❌ DB SP500 심볼 조회 실패: {e}")
            return None
        finally:
            db.close()
    
    # =========================
    # 변경 감지 및 캐싱
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
    # 대시보드 데이터 처리
    # =========================
    
    async def get_dashboard_data(self) -> Dict[str, Any]:
        """
        대시보드용 통합 데이터 조회
        
        Returns:
            Dict[str, Any]: 대시보드 데이터
        """
        try:
            # 병렬로 데이터 조회
            tasks = [
                self.get_topgainers_from_redis("top_gainers", 10),  # 상위 10개 상승 주식
                self.get_crypto_from_redis(20),                     # 상위 20개 암호화폐
                self.get_sp500_from_redis(None, 15)                 # SP500 15개
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
                "last_updated": datetime.utcnow().isoformat(),
                "data_sources": ["topgainers", "crypto", "sp500"]
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
            "timestamp": datetime.utcnow().isoformat(),
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
            time_since_update = (datetime.utcnow() - last_update).total_seconds()
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
            cutoff_time = datetime.utcnow() - timedelta(hours=1)
            
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