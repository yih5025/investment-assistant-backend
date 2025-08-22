# app/services/topgainers_service.py
import asyncio
import json
import logging
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, timedelta
import pytz

from app.database import get_db
from app.config import settings
from app.models.topgainers_model import TopGainers
from app.schemas.websocket_schema import TopGainerData, db_to_topgainer_data

logger = logging.getLogger(__name__)

class MarketTimeChecker:
    """미국 주식 시장 시간 체크 클래스 (간소화)"""
    
    def __init__(self):
        self.us_eastern = pytz.timezone('US/Eastern')
        # 주요 공휴일만 포함 (간소화)
        self.market_holidays = {
            '2024-12-25', '2025-01-01', '2025-01-20', '2025-07-04', '2025-12-25'
        }
    
    def is_market_open(self) -> bool:
        """현재 미국 주식 시장이 열려있는지 확인"""
        try:
            now_utc = datetime.utcnow().replace(tzinfo=pytz.UTC)
            now_et = now_utc.astimezone(self.us_eastern)
            
            # 주말 체크
            if now_et.weekday() >= 5:
                return False
            
            # 공휴일 체크
            if now_et.strftime('%Y-%m-%d') in self.market_holidays:
                return False
            
            # 정규 거래시간: 9:30 AM - 4:00 PM ET
            market_open = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
            market_close = now_et.replace(hour=16, minute=0, second=0, microsecond=0)
            
            return market_open <= now_et <= market_close
            
        except Exception as e:
            logger.error(f"❌ 시장 시간 확인 오류: {e}")
            return False

class TopGainersService:
    """
    🎯 TopGainers 전용 서비스 클래스
    
    top_gainers 테이블 중심의 카테고리 기반 데이터 처리를 담당합니다.
    장중에는 Redis, 장마감 시에는 PostgreSQL에서 최신 데이터를 제공합니다.
    """
    
    def __init__(self):
        """TopGainersService 초기화"""
        self.redis_client = None
        self.market_checker = MarketTimeChecker()
        
        # 캐시
        self.symbol_category_cache: Dict[str, str] = {}
        self.last_cache_update = 0
        self.cache_ttl = 300  # 5분
        
        # 통계
        self.stats = {
            "api_calls": 0,
            "redis_calls": 0,
            "db_calls": 0,
            "cache_hits": 0,
            "market_closed_calls": 0,
            "errors": 0,
            "last_update": None
        }
        
        logger.info("✅ TopGainersService 초기화 완료")
    
    async def init_redis(self) -> bool:
        """Redis 연결 초기화"""
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
            
            await self.redis_client.ping()
            logger.info("✅ TopGainers Redis 연결 성공")
            return True
            
        except Exception as e:
            logger.warning(f"⚠️ TopGainers Redis 연결 실패: {e}")
            self.redis_client = None
            return False
    
    # =========================
    # 🎯 핵심 비즈니스 로직
    # =========================
    
    def get_latest_batch_symbols_with_categories(self) -> Dict[str, List[str]]:
        """
        최신 배치의 50개 심볼을 카테고리별로 반환
        
        Returns:
            Dict[str, List[str]]: 카테고리별 심볼 리스트
            {
                "top_gainers": ["GXAI", "PRFX", ...],
                "top_losers": ["BNAIW", "VELO", ...], 
                "most_actively_traded": ["ADD", "OPEN", ...]
            }
        """
        try:
            db = next(get_db())
            
            # 최신 batch_id 조회
            latest_batch = TopGainers.get_latest_batch_id(db)
            if not latest_batch:
                logger.warning("📊 top_gainers 테이블에 데이터 없음")
                return {}
            
            batch_id = latest_batch[0]
            
            # 배치의 모든 데이터 조회
            db_objects = db.query(TopGainers).filter(
                TopGainers.batch_id == batch_id
            ).order_by(TopGainers.category, TopGainers.rank_position).all()
            
            # 카테고리별로 분류
            result = {}
            for obj in db_objects:
                category = obj.category
                if category not in result:
                    result[category] = []
                result[category].append(obj.symbol)
            
            # 캐시 업데이트
            self.symbol_category_cache.clear()
            for obj in db_objects:
                self.symbol_category_cache[obj.symbol] = obj.category
            self.last_cache_update = datetime.utcnow().timestamp()
            
            logger.debug(f"📊 최신 배치 심볼 조회: {sum(len(symbols) for symbols in result.values())}개")
            return result
            
        except Exception as e:
            logger.error(f"❌ 최신 배치 심볼 조회 실패: {e}")
            self.stats["errors"] += 1
            return {}
        finally:
            db.close()
    
    async def get_market_data_with_categories(self, category: str = None, limit: int = 50) -> List[TopGainerData]:
        """
        🎯 시장 상태에 따른 카테고리별 데이터 조회
        
        Args:
            category: 카테고리 필터 (None=전체, "top_gainers", "top_losers", "most_actively_traded")
            limit: 반환할 최대 개수
            
        Returns:
            List[TopGainerData]: 카테고리 정보가 포함된 데이터 리스트
        """
        self.stats["api_calls"] += 1
        
        try:
            is_market_open = self.market_checker.is_market_open()
            
            if is_market_open and self.redis_client:
                # 🔄 장중: Redis에서 실시간 데이터
                data = await self._get_data_from_redis(category, limit)
                logger.debug(f"📊 Redis에서 TopGainers 데이터 조회: {len(data)}개")
            else:
                # 💾 장마감: PostgreSQL에서 최신 데이터
                data = await self._get_data_from_db(category, limit)
                logger.debug(f"📊 PostgreSQL에서 TopGainers 데이터 조회: {len(data)}개")
                self.stats["market_closed_calls"] += 1
            
            self.stats["last_update"] = datetime.utcnow()
            return data
            
        except Exception as e:
            logger.error(f"❌ TopGainers 시장 데이터 조회 실패: {e}")
            self.stats["errors"] += 1
            return []
    
    async def _get_data_from_redis(self, category: str = None, limit: int = 50) -> List[TopGainerData]:
        """Redis에서 TopGainers 데이터 조회"""
        try:
            self.stats["redis_calls"] += 1
            
            # 최신 배치 심볼들 가져오기 (카테고리 매핑 포함)
            symbol_categories = self.get_latest_batch_symbols_with_categories()
            
            # 카테고리 필터링
            target_symbols = []
            if category:
                target_symbols = symbol_categories.get(category, [])
            else:
                # 전체 심볼
                for symbols in symbol_categories.values():
                    target_symbols.extend(symbols)
            
            if not target_symbols:
                logger.warning(f"📊 Redis: {category} 카테고리에 심볼 없음")
                return []
            
            # Redis에서 병렬 조회
            pipeline = self.redis_client.pipeline()
            redis_keys = [f"latest:stocks:topgainers:{symbol}" for symbol in target_symbols]
            
            for key in redis_keys:
                pipeline.get(key)
            
            results = await pipeline.execute()
            
            # JSON 파싱 및 TopGainerData 변환
            data = []
            for i, result in enumerate(results):
                if result:
                    try:
                        json_data = json.loads(result)
                        symbol = target_symbols[i]
                        
                        # 카테고리 정보 추가 (캐시에서)
                        redis_category = json_data.get('category', 'unknown')
                        cached_category = self.symbol_category_cache.get(symbol, redis_category)
                        
                        # TopGainerData 생성
                        topgainer_data = TopGainerData(
                            batch_id=0,  # Redis에서는 batch_id 없음
                            symbol=symbol,
                            category=cached_category,
                            last_updated=datetime.utcnow().isoformat(),
                            price=json_data.get('price'),
                            volume=json_data.get('volume'),
                            change_amount=json_data.get('change_amount'),
                            change_percentage=json_data.get('change_percentage'),
                            rank_position=None,  # Redis에는 순위 정보 없음
                            created_at=datetime.utcnow().isoformat()
                        )
                        data.append(topgainer_data)
                        
                    except (json.JSONDecodeError, ValueError) as e:
                        logger.warning(f"⚠️ Redis 데이터 파싱 실패 ({target_symbols[i]}): {e}")
                        continue
            
            # 가격 기준 정렬 및 제한
            data.sort(key=lambda x: x.price or 0, reverse=True)
            return data[:limit]
            
        except Exception as e:
            logger.error(f"❌ Redis TopGainers 조회 실패: {e}")
            # Redis 실패 시 DB fallback
            return await self._get_data_from_db(category, limit)
    
    async def _get_data_from_db(self, category: str = None, limit: int = 50) -> List[TopGainerData]:
        """PostgreSQL에서 TopGainers 데이터 조회"""
        try:
            self.stats["db_calls"] += 1
            
            db = next(get_db())
            
            # 최신 batch_id 조회
            latest_batch = TopGainers.get_latest_batch_id(db)
            if not latest_batch:
                logger.warning("📊 DB: top_gainers 테이블에 데이터 없음")
                return []
            
            batch_id = latest_batch[0]
            
            # 카테고리 필터링
            if category:
                db_objects = TopGainers.get_by_category(db, category, batch_id, limit)
            else:
                db_objects = db.query(TopGainers).filter(
                    TopGainers.batch_id == batch_id
                ).order_by(TopGainers.rank_position.asc().nulls_last()).limit(limit).all()
            
            # Pydantic 모델로 변환
            data = [db_to_topgainer_data(obj) for obj in db_objects]
            return data
            
        except Exception as e:
            logger.error(f"❌ DB TopGainers 조회 실패: {e}")
            return []
        finally:
            db.close()
    
    def get_category_statistics(self) -> Dict[str, Any]:
        """
        🎯 카테고리별 통계 정보 조회
        
        Returns:
            Dict[str, Any]: 카테고리 통계
            {
                "categories": {
                    "top_gainers": 20,
                    "top_losers": 10,
                    "most_actively_traded": 20
                },
                "total": 50,
                "last_updated": "2025-08-21T10:30:00Z",
                "market_status": "OPEN"
            }
        """
        try:
            db = next(get_db())
            
            # 최신 batch_id 조회
            latest_batch = TopGainers.get_latest_batch_id(db)
            if not latest_batch:
                return {
                    "categories": {},
                    "total": 0,
                    "last_updated": None,
                    "market_status": "UNKNOWN"
                }
            
            batch_id = latest_batch[0]
            
            # 카테고리별 개수 조회
            from sqlalchemy import func
            category_counts = {}
            
            categories = db.query(
                TopGainers.category,
                func.count(TopGainers.symbol)
            ).filter(
                TopGainers.batch_id == batch_id
            ).group_by(TopGainers.category).all()
            
            total = 0
            for category, count in categories:
                if category:
                    category_counts[category] = count
                    total += count
            
            # 마지막 업데이트 시간
            last_updated_obj = db.query(TopGainers).filter(
                TopGainers.batch_id == batch_id
            ).order_by(TopGainers.last_updated.desc()).first()
            
            last_updated = None
            if last_updated_obj:
                last_updated = last_updated_obj.last_updated.isoformat()
            
            # 시장 상태
            market_status = "OPEN" if self.market_checker.is_market_open() else "CLOSED"
            
            return {
                "categories": category_counts,
                "total": total,
                "batch_id": batch_id,
                "last_updated": last_updated,
                "market_status": market_status,
                "data_source": "redis" if market_status == "OPEN" and self.redis_client else "database"
            }
            
        except Exception as e:
            logger.error(f"❌ TopGainers 통계 조회 실패: {e}")
            self.stats["errors"] += 1
            return {"categories": {}, "total": 0, "error": str(e)}
        finally:
            db.close()
    # =========================
# 5. TopGainers 서비스 확장
# =========================

# app/services/topgainers_service.py (기존 파일에 추가)

    async def get_realtime_polling_data(self, limit: int, category: Optional[str] = None):
        """
        TopGainers 실시간 폴링 데이터 ("더보기" 방식)
        
        Args:
            limit: 반환할 항목 수 (1번부터 limit번까지)
            category: 카테고리 필터 (None이면 전체)
        
        Returns:
            dict: 폴링 응답 데이터
        """
        try:
            # 🎯 WebSocket과 동일한 데이터 소스 사용
            if category:
                # 특정 카테고리만 조회
                all_data = await self.get_category_data_for_websocket(category, limit=200)
            else:
                # 전체 카테고리 조회
                all_data = await self.get_market_data_with_categories(limit=200)
            
            if not all_data:
                logger.warning("📊 TopGainers 실시간 데이터 없음")
                return {
                    "data": [],
                    "metadata": {
                        "current_count": 0,
                        "total_available": 0,
                        "has_more": False,
                        "next_limit": limit,
                        "timestamp": datetime.utcnow().isoformat(),
                        "data_source": "no_data",
                        "message": "데이터를 찾을 수 없습니다"
                    }
                }
            
            # 순위별 정렬 (rank_position 기준)
            all_data.sort(key=lambda x: x.rank_position or 999)
            
            # 순위 재부여 (1부터 시작)
            for i, item in enumerate(all_data):
                item.rank_position = i + 1
            
            # limit만큼 자르기
            limited_data = all_data[:limit]
            total_available = len(all_data)
            
            # 카테고리 통계 계산
            category_stats = {}
            if not category:
                # 전체 조회인 경우 카테고리별 개수 제공
                for item in all_data:
                    cat = item.category or "unknown"
                    category_stats[cat] = category_stats.get(cat, 0) + 1
            
            return {
                "data": [item.model_dump() for item in limited_data],
                "metadata": {
                    "current_count": len(limited_data),
                    "total_available": total_available,
                    "has_more": limit < total_available,
                    "next_limit": min(limit + 50, total_available),
                    "timestamp": datetime.utcnow().isoformat(),
                    "data_source": "redis_realtime",
                    "category_filter": category,
                    "category_stats": category_stats if category_stats else None,
                    "market_status": self._get_market_status()
                }
            }
        
        except Exception as e:
            logger.error(f"❌ TopGainers 실시간 폴링 데이터 조회 실패: {e}")
            return {"error": str(e)}

    def _get_market_status(self):
        """시장 상태 조회 (TopGainers용)"""
        try:
            from app.services.websocket_service import MarketTimeChecker
            market_checker = MarketTimeChecker()
            status = market_checker.get_market_status()
            return {
                "is_open": status["is_open"],
                "status": status["status"]
            }
        except Exception as e:
            return {"is_open": False, "status": "UNKNOWN"}
    
    async def get_symbol_data(self, symbol: str, category: str = None) -> Optional[TopGainerData]:
        """
        특정 심볼의 TopGainers 데이터 조회
        
        Args:
            symbol: 주식 심볼
            category: 카테고리 필터 (선택사항)
            
        Returns:
            Optional[TopGainerData]: 심볼 데이터 (없으면 None)
        """
        try:
            symbol = symbol.upper().strip()
            is_market_open = self.market_checker.is_market_open()
            
            if is_market_open and self.redis_client:
                # Redis에서 조회
                redis_key = f"latest:stocks:topgainers:{symbol}"
                result = await self.redis_client.get(redis_key)
                
                if result:
                    json_data = json.loads(result)
                    redis_category = json_data.get('category', 'unknown')
                    
                    # 카테고리 필터링
                    if category and redis_category != category:
                        return None
                    
                    return TopGainerData(
                        batch_id=0,
                        symbol=symbol,
                        category=redis_category,
                        last_updated=datetime.utcnow().isoformat(),
                        price=json_data.get('price'),
                        volume=json_data.get('volume'),
                        change_amount=json_data.get('change_amount'),
                        change_percentage=json_data.get('change_percentage'),
                        rank_position=None,
                        created_at=datetime.utcnow().isoformat()
                    )
            
            # DB에서 조회 (Redis 실패 또는 장 마감)
            db = next(get_db())
            db_object = TopGainers.get_symbol_data(db, symbol, category)
            
            if db_object:
                return db_to_topgainer_data(db_object)
            
            return None
            
        except Exception as e:
            logger.error(f"❌ 심볼 {symbol} 데이터 조회 실패: {e}")
            return None
        finally:
            if 'db' in locals():
                db.close()
    
    # =========================
    # 🎯 WebSocket 지원 메서드
    # =========================
    
    async def get_realtime_updates(self, last_update_time: datetime = None) -> List[TopGainerData]:
        """
        실시간 업데이트용 데이터 조회 (WebSocket에서 사용)
        
        Args:
            last_update_time: 마지막 업데이트 시간 (변경 감지용)
            
        Returns:
            List[TopGainerData]: 변경된 데이터만 반환
        """
        try:
            # 전체 데이터 조회
            all_data = await self.get_market_data_with_categories()
            
            # 변경 감지 로직 (간단히 전체 반환 - 상세한 diff는 WebSocket 매니저에서)
            return all_data
            
        except Exception as e:
            logger.error(f"❌ 실시간 업데이트 데이터 조회 실패: {e}")
            return []
    
    async def get_category_data_for_websocket(self, category: str, limit: int = 20) -> List[TopGainerData]:
        """
        WebSocket용 카테고리별 데이터 조회
        
        Args:
            category: 카테고리 (top_gainers, top_losers, most_actively_traded)
            limit: 반환할 최대 개수
            
        Returns:
            List[TopGainerData]: 카테고리별 데이터
        """
        return await self.get_market_data_with_categories(category, limit)
    
    # =========================
    # 통계 및 헬스체크
    # =========================
    
    def get_service_stats(self) -> Dict[str, Any]:
        """서비스 통계 정보 반환"""
        return {
            "performance": {
                "api_calls": self.stats["api_calls"],
                "redis_calls": self.stats["redis_calls"],
                "db_calls": self.stats["db_calls"],
                "cache_hits": self.stats["cache_hits"],
                "market_closed_calls": self.stats["market_closed_calls"],
                "errors": self.stats["errors"]
            },
            "data_status": {
                "last_update": self.stats["last_update"].isoformat() if self.stats["last_update"] else None,
                "cached_symbols": len(self.symbol_category_cache),
                "cache_age_seconds": datetime.utcnow().timestamp() - self.last_cache_update
            },
            "health": {
                "redis_available": self.redis_client is not None,
                "market_status": "OPEN" if self.market_checker.is_market_open() else "CLOSED",
                "error_rate": self.stats["errors"] / max(self.stats["api_calls"], 1) * 100
            }
        }
    
    async def health_check(self) -> Dict[str, Any]:
        """서비스 헬스체크"""
        health_info = {
            "timestamp": datetime.utcnow().isoformat(),
            "status": "healthy",
            "services": {}
        }
        
        # Redis 상태 확인
        if self.redis_client:
            try:
                await asyncio.wait_for(self.redis_client.ping(), timeout=3.0)
                health_info["services"]["redis"] = {"status": "connected"}
            except Exception as e:
                health_info["services"]["redis"] = {"status": "disconnected", "error": str(e)}
                health_info["status"] = "degraded"
        else:
            health_info["services"]["redis"] = {"status": "not_configured"}
            health_info["status"] = "degraded"
        
        # 데이터베이스 상태 확인
        try:
            db = next(get_db())
            latest_batch = TopGainers.get_latest_batch_id(db)
            
            if latest_batch:
                health_info["services"]["database"] = {"status": "connected", "latest_batch": latest_batch[0]}
            else:
                health_info["services"]["database"] = {"status": "connected", "data": "empty"}
                health_info["status"] = "degraded"
            
            db.close()
        except Exception as e:
            health_info["services"]["database"] = {"status": "disconnected", "error": str(e)}
            health_info["status"] = "unhealthy"
        
        return health_info
    
    async def shutdown(self):
        """서비스 종료 처리"""
        try:
            if self.redis_client:
                await self.redis_client.close()
                logger.info("✅ TopGainers Redis 연결 종료")
            
            self.symbol_category_cache.clear()
            logger.info("✅ TopGainersService 종료 완료")
            
        except Exception as e:
            logger.error(f"❌ TopGainersService 종료 실패: {e}")