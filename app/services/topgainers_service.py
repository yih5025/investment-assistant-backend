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
from app.schemas.topgainers_schema import TopGainerData, db_to_topgainer_data

logger = logging.getLogger(__name__)

class MarketTimeChecker:
    """미국 주식 시장 시간 체크 클래스 (간소화)"""
    
    def __init__(self):
        self.us_eastern = pytz.timezone('US/Eastern')
        # 주요 공휴일만 포함 (간소화)
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
        self.cache_ttl = 60   # 1분 (실시간성 향상)
        
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
                host=settings.redis_host,
                port=settings.redis_port,
                db=settings.redis_db,
                password=settings.redis_password,
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
            # 🎯 한국 시간 기준으로 캐시 업데이트 시간 기록
            korea_tz = pytz.timezone('Asia/Seoul')
            self.last_cache_update = datetime.now(korea_tz).timestamp()
            
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
            
            self.stats["last_update"] = datetime.now(pytz.UTC)
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
                            last_updated=datetime.now(pytz.UTC).isoformat(),
                            price=json_data.get('price'),
                            volume=json_data.get('volume'),
                            change_amount=json_data.get('change_amount'),
                            change_percentage=json_data.get('change_percentage'),
                            rank_position=None,  # Redis에는 순위 정보 없음
                            created_at=datetime.now(pytz.UTC).isoformat()
                        )
                        data.append(topgainer_data)
                        
                    except (json.JSONDecodeError, ValueError) as e:
                        logger.warning(f"⚠️ Redis 데이터 파싱 실패 ({target_symbols[i]}): {e}")
                        continue
            
            # 가격 기준 정렬 및 제한 (None 값 안전 처리)
            data.sort(key=lambda x: (x.price if x.price is not None else 0), reverse=True)
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
        TopGainers 실시간 폴링 데이터 ("더보기" 방식) + 변화율 계산
        
        Args:
            limit: 반환할 항목 수 (1번부터 limit번까지)
            category: 카테고리 필터 (None이면 전체)
        
        Returns:
            dict: 변화율이 포함된 폴링 응답 데이터
        """
        try:
            # 🎯 기존 로직으로 현재가 데이터 조회
            if category:
                all_data = await self.get_category_data_for_websocket(category, limit=200)
            else:
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
                        "timestamp": datetime.now(pytz.UTC).isoformat(),
                        "data_source": "no_data",
                        "message": "데이터를 찾을 수 없습니다"
                    }
                }
            
            # 🎯 변화율 계산 추가
            enhanced_data = await self._add_change_calculations(all_data)
            
            # 순위별 정렬 (변화율 기준 또는 기존 rank_position 기준) - None 값 안전 처리
            if category == 'top_gainers':
                enhanced_data.sort(key=lambda x: (x.get('change_percentage') if x.get('change_percentage') is not None else -999), reverse=True)
            elif category == 'top_losers':
                enhanced_data.sort(key=lambda x: (x.get('change_percentage') if x.get('change_percentage') is not None else 999))
            else:
                enhanced_data.sort(key=lambda x: (x.get('rank_position') if x.get('rank_position') is not None else 999))
            
            # 순위 재부여
            for i, item in enumerate(enhanced_data):
                item['rank_position'] = i + 1
            
            # limit만큼 자르기
            limited_data = enhanced_data[:limit]
            total_available = len(enhanced_data)
            
            # 카테고리 통계 계산
            category_stats = {}
            if not category:
                for item in enhanced_data:
                    cat = item.get('category', 'unknown')
                    category_stats[cat] = category_stats.get(cat, 0) + 1
            
            return {
                "data": limited_data,
                "metadata": {
                    "current_count": len(limited_data),
                    "total_available": total_available,
                    "has_more": limit < total_available,
                    "next_limit": min(limit + 50, total_available),
                    "timestamp": datetime.now(pytz.UTC).isoformat(),
                    "data_source": "redis_realtime_with_changes",
                    "category_filter": category,
                    "category_stats": category_stats if category_stats else None,
                    "market_status": self._get_market_status(),
                    "features": ["real_time_prices", "change_calculation", "previous_close_comparison"]
                }
            }
            
        except Exception as e:
            logger.error(f"❌ TopGainers 실시간 폴링 데이터 조회 실패: {e}")
            return {"error": str(e)}

    async def _add_change_calculations(self, data_list: List) -> List[Dict]:
        """
        데이터 리스트에 변화율 계산 추가
        
        Args:
            data_list: 기존 TopGainers 데이터 리스트
            
        Returns:
            List[Dict]: 변화율이 추가된 데이터 리스트
        """
        try:
            # 심볼 리스트 추출
            symbols = []
            for item in data_list:
                if hasattr(item, 'symbol'):
                    symbols.append(item.symbol)
                elif isinstance(item, dict) and 'symbol' in item:
                    symbols.append(item['symbol'])
            
            # 전날 종가 일괄 조회
            db = next(get_db())
            try:
                previous_close_prices = TopGainers.get_batch_previous_close_prices(db, symbols)
            finally:
                db.close()
            
            # 변화율 계산 및 추가
            enhanced_data = []
            for item in data_list:
                # 기존 데이터를 딕셔너리로 변환
                if hasattr(item, 'dict'):
                    item_dict = item.dict()
                elif hasattr(item, 'model_dump'):
                    item_dict = item.model_dump()
                elif isinstance(item, dict):
                    item_dict = item.copy()
                else:
                    item_dict = {}
                
                symbol = item_dict.get('symbol')
                # None 값 안전 처리를 위한 가격 변환
                price_value = item_dict.get('price')
                current_price = float(price_value) if price_value is not None else 0.0
                
                if symbol and symbol in previous_close_prices and current_price > 0:
                    previous_close = previous_close_prices[symbol]
                    
                    # 변화 계산
                    change_amount = current_price - previous_close
                    change_percentage = (change_amount / previous_close) * 100 if previous_close > 0 else 0
                    
                    # 변화 정보 추가
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
                
                enhanced_data.append(item_dict)
            
            return enhanced_data
            
        except Exception as e:
            logger.error(f"❌ 변화율 계산 추가 실패: {e}")
            # 실패 시 기존 데이터 반환
            return [item.dict() if hasattr(item, 'dict') else item for item in data_list]
    def _get_market_status(self):
        """시장 상태 조회 (TopGainers용)"""
        try:
            from app.services.topgainers_service import MarketTimeChecker
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
                        last_updated=datetime.now(pytz.UTC).isoformat(),
                        price=json_data.get('price'),
                        volume=json_data.get('volume'),
                        change_amount=json_data.get('change_amount'),
                        change_percentage=json_data.get('change_percentage'),
                        rank_position=None,
                        created_at=datetime.now(pytz.UTC).isoformat()
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
                "cache_age_seconds": datetime.now(pytz.UTC).timestamp() - self.last_cache_update
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
            "timestamp": datetime.now(pytz.UTC).isoformat(),
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

    # =========================
    # WebSocket 전용 메서드 추가
    # =========================

    async def get_websocket_updates(self, category: str = None, limit: int = 50) -> List[TopGainerData]:
        """
        WebSocket용 실시간 TopGainers 데이터
        
        Args:
            category: 카테고리 필터 (None=전체, "top_gainers", "top_losers", "most_actively_traded")
            limit: 반환할 최대 개수
            
        Returns:
            List[TopGainerData]: WebSocket 전송용 데이터
        """
        self.stats["api_calls"] += 1
        return await self.get_market_data_with_categories(category, limit)

    async def get_realtime_streaming_data(self, category: str = None, limit: int = 50) -> Dict[str, Any]:
        """
        WebSocket 스트리밍용 TopGainers 데이터 (변화율 포함)
        
        Args:
            category: 카테고리 필터
            limit: 반환할 항목 수
            
        Returns:
            Dict[str, Any]: WebSocket 스트리밍 응답 데이터
        """
        try:
            # 기존 변화율 계산 로직 활용
            enhanced_data = await self._get_enhanced_data_for_streaming(category, limit)
            
            return {
                "type": "topgainers_update",
                "data": enhanced_data,
                "timestamp": datetime.now(pytz.UTC).isoformat(),
                "data_count": len(enhanced_data),
                "category_filter": category,
                "market_status": self._get_market_status(),
                "data_source": "redis_realtime" if self.market_checker.is_market_open() else "database"
            }
            
        except Exception as e:
            logger.error(f"TopGainers 스트리밍 데이터 조회 실패: {e}")
            return {
                "type": "topgainers_error",
                "error": str(e),
                "timestamp": datetime.now(pytz.UTC).isoformat()
            }

    async def _get_enhanced_data_for_streaming(self, category: str = None, limit: int = 50) -> List[Dict]:
        """
        WebSocket 스트리밍용 데이터 가공 (변화율 포함)
        
        Args:
            category: 카테고리 필터
            limit: 반환할 최대 개수
            
        Returns:
            List[Dict]: 변화율이 포함된 스트리밍용 데이터
        """
        try:
            # 기본 데이터 조회
            raw_data = await self.get_market_data_with_categories(category, limit * 2)
            
            if not raw_data:
                return []
            
            # 변화율 계산 추가 (기존 로직 재사용)
            enhanced_data = await self._add_change_calculations(raw_data)
            
            # WebSocket 스트리밍에 최적화된 형태로 변환
            streaming_data = []
            for item in enhanced_data[:limit]:
                if isinstance(item, dict):
                    streaming_item = item.copy()
                else:
                    streaming_item = item.dict() if hasattr(item, 'dict') else {}
                
                # 회사명 추가
                symbol = streaming_item.get('symbol', '')
                if symbol:
                    streaming_item['company_name'] = self._get_company_name(symbol)
                
                # WebSocket 전송에 필요한 추가 필드
                streaming_item.update({
                    'last_updated': datetime.now(pytz.UTC).isoformat(),
                    'data_type': 'topgainers',
                    'websocket_timestamp': datetime.now(pytz.UTC).timestamp()
                })
                
                streaming_data.append(streaming_item)
            
            return streaming_data
            
        except Exception as e:
            logger.error(f"TopGainers 스트리밍 데이터 가공 실패: {e}")
            return []

    async def get_category_streaming_data(self, category: str, limit: int = 20) -> Dict[str, Any]:
        """
        특정 카테고리 WebSocket 스트리밍 데이터
        
        Args:
            category: 카테고리 (top_gainers, top_losers, most_actively_traded)
            limit: 반환할 최대 개수
            
        Returns:
            Dict[str, Any]: 카테고리별 스트리밍 데이터
        """
        try:
            # 카테고리별 데이터 조회
            category_data = await self.get_category_data_for_websocket(category, limit)
            
            if not category_data:
                return {
                    "type": "topgainers_category_update",
                    "category": category,
                    "data": [],
                    "data_count": 0,
                    "timestamp": datetime.now(pytz.UTC).isoformat(),
                    "message": f"No data available for category {category}"
                }
            
            # 딕셔너리 형태로 변환
            formatted_data = []
            for item in category_data:
                if hasattr(item, 'dict'):
                    item_dict = item.dict()
                else:
                    item_dict = item
                
                # 회사명 추가
                symbol = item_dict.get('symbol', '')
                if symbol:
                    item_dict['company_name'] = self._get_company_name(symbol)
                
                formatted_data.append(item_dict)
            
            return {
                "type": "topgainers_category_update",
                "category": category,
                "data": formatted_data,
                "data_count": len(formatted_data),
                "timestamp": datetime.now(pytz.UTC).isoformat(),
                "market_status": self._get_market_status(),
                "data_source": "redis_realtime" if self.market_checker.is_market_open() else "database"
            }
            
        except Exception as e:
            logger.error(f"TopGainers 카테고리 {category} 스트리밍 데이터 실패: {e}")
            return {
                "type": "topgainers_error",
                "category": category,
                "error": str(e),
                "timestamp": datetime.now(pytz.UTC).isoformat()
            }

    def _get_company_name(self, symbol: str) -> str:
        """
        심볼로 회사명 조회 (sp500_companies 테이블 사용)
        기존 WebSocketService의 getStockName 로직 이동
        
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
            
            if result and result[0]:
                return result[0]
            else:
                # 기본값: 심볼을 회사명으로 사용
                return f"{symbol} Inc."
                    
        except Exception as e:
            logger.warning(f"{symbol} 회사명 조회 실패: {e}")
            return f"{symbol} Inc."
        finally:
            if 'db' in locals():
                db.close()

    # =========================
    # 변화 감지 및 업데이트 확인
    # =========================

    async def detect_data_changes(self, last_check_time: Optional[datetime] = None) -> Dict[str, Any]:
        """
        데이터 변화 감지 (WebSocket에서 변경 사항 확인용)
        
        Args:
            last_check_time: 마지막 확인 시간
            
        Returns:
            Dict[str, Any]: 변화 감지 결과
        """
        try:
            # 현재 데이터 조회
            current_data = await self.get_market_data_with_categories()
            
            # 변화 감지 로직 (간단 버전)
            has_changes = True  # 실제로는 이전 데이터와 비교
            
            if has_changes:
                return {
                    "has_changes": True,
                    "change_count": len(current_data),
                    "last_update": datetime.now(pytz.UTC).isoformat(),
                    "data": current_data[:10]  # 변화된 데이터의 일부만
                }
            else:
                return {
                    "has_changes": False,
                    "last_check": last_check_time.isoformat() if last_check_time else None,
                    "message": "No changes detected"
                }
                
        except Exception as e:
            logger.error(f"TopGainers 변화 감지 실패: {e}")
            return {
                "has_changes": False,
                "error": str(e)
            }