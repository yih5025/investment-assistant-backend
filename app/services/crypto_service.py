# app/services/crypto_service.py
import asyncio
import json
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
import pytz

from app.database import get_db
from app.config import settings
from app.models.crypto_model import Crypto
from app.schemas.crypto_schema import CryptoData, db_to_crypto_data

logger = logging.getLogger(__name__)

class CryptoService:
    """
    암호화폐 WebSocket 전용 서비스
    
    24시간 거래하는 암호화폐 특성상 항상 실시간 데이터를 제공합니다.
    Redis 우선 조회, DB fallback 지원
    """
    
    def __init__(self):
        """CryptoService 초기화"""
        self.redis_client = None
        
        # 통계
        self.stats = {
            "api_calls": 0,
            "redis_calls": 0,
            "db_calls": 0,
            "cache_hits": 0,
            "errors": 0,
            "last_update": None
        }
        
        logger.info("✅ CryptoService 초기화 완료")
    
    async def init_redis(self) -> bool:
        """Redis 연결 초기화 (연결 풀 및 재시도 로직 개선)"""
        try:
            import redis.asyncio as redis
            
            # 기존 연결이 있으면 정리
            if self.redis_client:
                try:
                    await self.redis_client.aclose()
                except:
                    pass
            
            # 연결 풀 설정으로 안정성 향상
            self.redis_client = redis.Redis(
                host=settings.redis_host,
                port=settings.redis_port,
                db=settings.redis_db,
                password=settings.redis_password,
                decode_responses=True,
                socket_connect_timeout=10,  # 연결 timeout 증가
                socket_timeout=10,          # 읽기 timeout 증가
                socket_keepalive=True,      # keepalive 활성화
                socket_keepalive_options={},
                health_check_interval=30,   # 30초마다 연결 상태 확인
                max_connections=20,         # 연결 풀 크기 증가
                retry_on_timeout=True,      # timeout 시 재시도
                retry_on_error=[ConnectionError, TimeoutError]  # 특정 에러 시 재시도
            )
            
            # 연결 테스트 (재시도 로직)
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    await asyncio.wait_for(self.redis_client.ping(), timeout=5.0)
                    logger.info(f"✅ Crypto Redis 연결 성공 (시도 {attempt + 1}/{max_retries})")
                    return True
                except Exception as e:
                    if attempt < max_retries - 1:
                        logger.warning(f"⚠️ Crypto Redis 연결 재시도 {attempt + 1}/{max_retries}: {e}")
                        await asyncio.sleep(1)
                    else:
                        raise e
            
        except Exception as e:
            logger.warning(f"⚠️ Crypto Redis 연결 실패: {e}")
            self.redis_client = None
            return False
    
    # =========================
    # 핵심 데이터 조회 메서드
    # =========================
    
    async def get_crypto_from_redis(self, limit: int = 415) -> List[CryptoData]:
        """
        Redis에서 암호화폐 데이터 조회 (24시간 거래이므로 Redis 우선)
        
        Args:
            limit: 반환할 최대 개수
            
        Returns:
            List[CryptoData]: 암호화폐 데이터 리스트
        """
        # Redis 클라이언트가 없으면 DB로 fallback
        if not self.redis_client:
            logger.debug("📊 Redis 클라이언트 없음, DB fallback")
            return await self.get_crypto_from_db(limit)
        
        try:
            self.stats["redis_calls"] += 1
            
            # Redis 키 패턴: latest:crypto:{market}
            pattern = "latest:crypto:*"
            keys = await asyncio.wait_for(self.redis_client.keys(pattern), timeout=8.0)
            
            if not keys:
                logger.debug("📊 Redis 암호화폐 데이터 없음, DB fallback")
                return await self.get_crypto_from_db(limit)
            
            # 모든 키의 데이터 가져오기 (timeout 추가)
            pipeline = self.redis_client.pipeline()
            for key in keys:
                pipeline.get(key)
            
            results = await asyncio.wait_for(pipeline.execute(), timeout=8.0)
            
            # JSON 파싱
            data = []
            for i, result in enumerate(results):
                if result:
                    try:
                        json_data = json.loads(result)
                        
                        # 새로운 스키마에 맞춘 CryptoData 생성
                        market_code = json_data.get('symbol', keys[i].split(':')[-1])
                        symbol = market_code.replace('KRW-', '') if market_code and 'KRW-' in market_code else market_code
                        
                        crypto_data = CryptoData(
                            # 새로운 필수 필드들
                            market_code=market_code,
                            symbol=symbol,
                            price=json_data.get('price'),
                            change_24h=json_data.get('change_price'),
                            change_rate_24h=f"{json_data.get('change_rate', 0):.2f}%" if json_data.get('change_rate') else "0.00%",
                            volume=json_data.get('volume'),
                            acc_trade_value_24h=json_data.get('volume_24h'),
                            timestamp=json_data.get('timestamp'),
                            
                            # 기존 호환성 필드들 (deprecated)
                            market=market_code,
                            trade_price=json_data.get('price'),
                            signed_change_rate=json_data.get('change_rate'),
                            signed_change_price=json_data.get('change_price'),
                            trade_volume=json_data.get('volume'),
                            acc_trade_volume_24h=json_data.get('volume_24h'),
                            timestamp_field=json_data.get('timestamp'),
                            source='bithumb',
                            crypto_name=self._get_crypto_name(market_code)
                        )
                        data.append(crypto_data)
                        
                    except (json.JSONDecodeError, ValueError) as e:
                        logger.warning(f"⚠️ Redis 암호화폐 데이터 파싱 실패: {e}")
                        continue
            
            # 현재가별 정렬 및 제한
            data.sort(key=lambda x: x.trade_price or 0, reverse=True)
            data = data[:limit]
            
            self.stats["last_update"] = datetime.now(pytz.UTC)
            logger.debug(f"📊 Redis 암호화폐 데이터 조회 완료: {len(data)}개")
            return data
            
        except (asyncio.TimeoutError, ConnectionError, TimeoutError) as e:
            logger.error(f"❌ Redis 암호화폐 조회 실패: {e}, DB fallback")
            self.stats["errors"] += 1
            
            # Redis 연결 문제 시 재연결 시도
            if "timeout" in str(e).lower() or "connection" in str(e).lower():
                logger.info("🔄 Redis 재연결 시도...")
                try:
                    await self.init_redis()
                except:
                    pass
            
            return await self.get_crypto_from_db(limit)
        
        except Exception as e:
            logger.error(f"❌ Redis 암호화폐 조회 실패 (기타): {e}, DB fallback")
            self.stats["errors"] += 1
            return await self.get_crypto_from_db(limit)
    
    async def get_crypto_from_db(self, limit: int = 415) -> List[CryptoData]:
        """
        데이터베이스에서 암호화폐 데이터 조회
        
        Args:
            limit: 반환할 최대 개수
            
        Returns:
            List[CryptoData]: 암호화폐 데이터 리스트
        """
        try:
            self.stats["db_calls"] += 1
            
            db = next(get_db())
            
            # 모든 마켓의 최신 가격 조회
            db_objects = Crypto.get_all_latest_prices(db, limit)
            
            # Pydantic 모델로 변환
            data = []
            for obj in db_objects:
                crypto_name = self._get_crypto_name(obj.market)
                crypto_data = db_to_crypto_data(obj, crypto_name)
                data.append(crypto_data)
            
            self.stats["last_update"] = datetime.now(pytz.UTC)
            logger.debug(f"📊 암호화폐 DB 데이터 조회 완료: {len(data)}개")
            return data
            
        except Exception as e:
            logger.error(f"❌ 암호화폐 DB 조회 실패: {e}")
            self.stats["errors"] += 1
            return []
        finally:
            if 'db' in locals():
                db.close()
    
    # =========================
    # WebSocket 전용 메서드
    # =========================
    
    async def get_websocket_updates(self, limit: int = 415) -> List[CryptoData]:
        """
        WebSocket용 실시간 암호화폐 데이터
        
        Args:
            limit: 반환할 최대 개수
            
        Returns:
            List[CryptoData]: WebSocket 전송용 데이터
        """
        self.stats["api_calls"] += 1
        return await self.get_crypto_from_redis(limit)
    
    async def get_realtime_polling_data(self, limit: int = 415) -> Dict[str, Any]:
        """
        암호화폐 실시간 폴링 데이터 ("더보기" 방식)
        
        Args:
            limit: 반환할 항목 수
            
        Returns:
            Dict[str, Any]: 폴링 응답 데이터
        """
        try:
            # 전체 데이터 조회
            all_data = await self.get_crypto_from_redis(limit * 2)
            
            if not all_data:
                return {
                    "data": [],
                    "metadata": {
                        "current_count": 0,
                        "total_available": 0,
                        "has_more": False,
                        "timestamp": datetime.now(pytz.UTC).isoformat(),
                        "data_source": "no_data"
                    }
                }
            
            # 딕셔너리 형태로 변환
            formatted_data = []
            for item in all_data:
                if hasattr(item, 'dict'):
                    item_dict = item.dict()
                else:
                    item_dict = {
                        'symbol': item.market.replace('KRW-', '') if item.market else '',
                        'market': item.market,
                        'name': item.crypto_name or item.market,
                        'price': item.trade_price,
                        'change_rate': item.signed_change_rate,
                        'change_price': item.signed_change_price,
                        'volume': item.trade_volume,
                        'volume_24h': item.acc_trade_volume_24h,
                        'source': item.source
                    }
                formatted_data.append(item_dict)
            
            # 순위 추가
            for i, item in enumerate(formatted_data):
                item['rank'] = i + 1
            
            # limit만큼 자르기
            limited_data = formatted_data[:limit]
            total_available = len(formatted_data)
            
            return {
                "data": limited_data,
                "metadata": {
                    "current_count": len(limited_data),
                    "total_available": total_available,
                    "has_more": limit < total_available,
                    "next_limit": min(limit + 50, total_available),
                    "timestamp": datetime.now(pytz.UTC).isoformat(),
                    "data_source": "redis_realtime",
                    "market_type": "crypto_24h",
                    "features": ["24h_trading", "real_time_prices", "volume_ranking"]
                }
            }
            
        except Exception as e:
            logger.error(f"❌ 암호화폐 실시간 폴링 데이터 조회 실패: {e}")
            return {"error": str(e)}
    
    async def get_symbol_data(self, market: str) -> Optional[CryptoData]:
        """
        특정 마켓의 암호화폐 데이터 조회
        
        Args:
            market: 마켓 코드 (예: 'KRW-BTC')
            
        Returns:
            Optional[CryptoData]: 마켓 데이터 (없으면 None)
        """
        try:
            market = market.upper()
            
            if self.redis_client:
                # Redis에서 조회
                redis_key = f"latest:crypto:{market}"
                result = await self.redis_client.get(redis_key)
                
                if result:
                    json_data = json.loads(result)
                    return CryptoData(
                        market=market,
                        trade_price=json_data.get('price'),
                        signed_change_rate=json_data.get('change_rate'),
                        signed_change_price=json_data.get('change_price'),
                        trade_volume=json_data.get('volume'),
                        acc_trade_volume_24h=json_data.get('volume_24h'),
                        timestamp_field=json_data.get('timestamp'),
                        source='bithumb',
                        crypto_name=self._get_crypto_name(market)
                    )
            
            # DB에서 조회
            db = next(get_db())
            db_object = Crypto.get_latest_by_market(db, market)
            
            if db_object:
                crypto_name = self._get_crypto_name(market)
                return db_to_crypto_data(db_object, crypto_name)
            
            return None
            
        except Exception as e:
            logger.error(f"❌ 마켓 {market} 데이터 조회 실패: {e}")
            return None
        finally:
            if 'db' in locals():
                db.close()
    
    # =========================
    # 유틸리티 메서드
    # =========================
    
    def _get_crypto_name(self, market_code: str) -> str:
        """
        암호화폐 이름 조회 (market_code_bithumb 테이블 사용)
        
        Args:
            market_code: 마켓 코드 (예: 'KRW-BTC')
            
        Returns:
            str: 암호화폐 이름
        """
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
            
            # 기본값: 심볼만 반환
            return market_code.replace('KRW-', '') if market_code else ''
                
        except Exception as e:
            #logger.warning(f"⚠️ {market_code} 암호화폐 이름 조회 실패: {e}")
            return market_code.replace('KRW-', '') if market_code else ''
        finally:
            if 'db' in locals():
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
                "errors": self.stats["errors"]
            },
            "data_status": {
                "last_update": self.stats["last_update"].isoformat() if self.stats["last_update"] else None,
                "market_type": "crypto_24h"
            },
            "health": {
                "redis_available": self.redis_client is not None,
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
            latest_data = db.execute(
                "SELECT COUNT(*) FROM bithumb_ticker WHERE created_at > NOW() - INTERVAL '1 hour'"
            ).fetchone()
            
            count = latest_data[0] if latest_data else 0
            if count > 0:
                health_info["services"]["database"] = {"status": "connected", "recent_data": count}
            else:
                health_info["services"]["database"] = {"status": "connected", "data": "stale"}
                health_info["status"] = "degraded"
            
            db.close()
        except Exception as e:
            health_info["services"]["database"] = {"status": "disconnected", "error": str(e)}
            health_info["status"] = "unhealthy"
        
        return health_info
    
    async def get_realtime_crypto_data(self, limit: int = 415) -> List[Dict[str, Any]]:
        """
        WebSocket용 암호화폐 실시간 데이터 조회
        
        Args:
            limit: 반환할 항목 수
            
        Returns:
            List[Dict]: WebSocket 전송용 암호화폐 데이터 리스트
        """
        try:
            # Redis에서 데이터 조회
            redis_data = await self.get_crypto_from_redis(limit)
            
            if not redis_data:
                # Redis 데이터가 없으면 DB에서 직접 조회
                return await self._get_crypto_from_db_with_names(limit)
            
            # 딕셔너리 형태로 변환 (한국명, 영어명 포함)
            formatted_data = []
            for item in redis_data:
                if hasattr(item, 'dict'):
                    item_dict = item.dict()
                elif hasattr(item, '__dict__'):
                    item_dict = item.__dict__.copy()
                else:
                    item_dict = item
                
                # Redis 데이터에서 마켓 코드 추출 (symbol 또는 market 필드)
                market_code = item_dict.get('symbol') or item_dict.get('market', '')
                if not market_code:
                    logger.warning(f"마켓 코드가 없는 데이터 스킵: {item_dict}")
                    continue
                
                # 심볼 추출 (KRW- 제거)
                symbol = market_code.replace('KRW-', '') if market_code and 'KRW-' in market_code else market_code
                
                # 한국명, 영어명 조회
                korean_name, english_name = await self._get_crypto_names(market_code)
                
                # 안전한 float 변환 함수
                def safe_float(value, default=0.0):
                    try:
                        return float(value) if value is not None else default
                    except (ValueError, TypeError):
                        return default
                
                formatted_item = {
                    "market_code": market_code,
                    "symbol": symbol,
                    "korean_name": korean_name,
                    "english_name": english_name,
                    "price": safe_float(item_dict.get('price')),
                    "change_24h": safe_float(item_dict.get('change_24h')),
                    "change_rate_24h": f"{safe_float(item_dict.get('change_rate')):.2f}%" if item_dict.get('change_rate') is not None else "0.00%",
                    "volume": safe_float(item_dict.get('volume')),
                    "acc_trade_value_24h": safe_float(item_dict.get('acc_trade_value_24h')),
                    "timestamp": item_dict.get('timestamp')
                }
                formatted_data.append(formatted_item)
            
            self.stats["api_calls"] += 1
            return formatted_data
            
        except Exception as e:
            logger.error(f"❌ 암호화폐 실시간 데이터 조회 실패: {e}")
            self.stats["errors"] += 1
            return []

    async def _get_crypto_names(self, market_code: str) -> tuple:
        """
        마켓 코드로 한국명, 영어명 조회
        
        Args:
            market_code: 마켓 코드 (예: 'KRW-BTC')
            
        Returns:
            tuple: (korean_name, english_name)
        """
        try:
            from sqlalchemy import text
            db = next(get_db())
            
            result = db.execute(
                text("""SELECT korean_name, english_name 
                FROM market_code_bithumb 
                WHERE market_code = :market_code"""),
                {"market_code": market_code}
            ).fetchone()
            
            if result:
                korean_name, english_name = result
                return korean_name or '', english_name or ''
            
            # 기본값
            symbol = market_code.replace('KRW-', '') if market_code else ''
            return symbol, symbol
                
        except Exception as e:
            # DB에 데이터가 없는 경우는 정상이므로 DEBUG 레벨로 변경
            #logger.debug(f"🔍 {market_code} 암호화폐 이름 DB에 없음: {e}")
            symbol = market_code.replace('KRW-', '') if market_code else ''
            return symbol, symbol
        finally:
            if 'db' in locals():
                db.close()


    async def _get_crypto_from_db_with_names(self, limit: int = 415) -> List[Dict[str, Any]]:
        """
        DB에서 암호화폐 데이터 조회 (이름 포함)
        
        Args:
            limit: 반환할 항목 수
            
        Returns:
            List[Dict]: 암호화폐 데이터 리스트
        """
        try:
            from app.models.crypto_model import Crypto
            db = next(get_db())
            
            # 최신 암호화폐 데이터 조회
            crypto_data = db.query(Crypto).order_by(
                Crypto.timestamp_field.desc()
            ).limit(limit).all()
            
            # 딕셔너리 형태로 변환 (이름 포함)
            result = []
            for crypto in crypto_data:
                # 한국명, 영어명 조회
                korean_name, english_name = await self._get_crypto_names(crypto.market)
                
                crypto_dict = {
                    "market_code": crypto.market,
                    "symbol": crypto.market.replace('KRW-', '') if crypto.market and 'KRW-' in crypto.market else crypto.market,
                    "korean_name": korean_name,
                    "english_name": english_name,
                    "price": float(crypto.trade_price) if crypto.trade_price else 0,
                    "change_24h": float(crypto.change_price) if crypto.change_price else 0,
                    "change_rate_24h": f"{float(crypto.signed_change_rate):.2f}%" if crypto.signed_change_rate else "0.00%",
                    "volume": float(crypto.acc_trade_volume_24h) if crypto.acc_trade_volume_24h else 0,
                    "acc_trade_value_24h": float(crypto.acc_trade_price_24h) if crypto.acc_trade_price_24h else 0,
                    "timestamp": crypto.timestamp_field
                }
                result.append(crypto_dict)
            
            db.close()
            return result
            
        except Exception as e:
            logger.error(f"❌ Crypto DB 데이터 조회 실패: {e}")
            return []

    async def get_crypto_symbol_data(self, symbol: str) -> Dict[str, Any]:
        """
        특정 암호화폐 심볼 데이터 조회
        
        Args:
            symbol: 심볼 (예: 'BTC', 'KRW-BTC')
            
        Returns:
            Dict: 심볼 데이터 또는 None
        """
        try:
            # 전체 데이터에서 특정 심볼 필터링
            all_data = await self.get_realtime_crypto_data(limit=500)
            
            # 심볼 정규화
            target_symbol = symbol.upper()
            
            for item in all_data:
                if (item.get('symbol', '').upper() == target_symbol or 
                    item.get('market_code', '').upper() == target_symbol or
                    item.get('market_code', '').upper() == f"KRW-{target_symbol}"):
                    return item
            
            return None
            
        except Exception as e:
            logger.error(f"❌ Crypto 심볼 {symbol} 데이터 조회 실패: {e}")
            return None

    async def shutdown(self):
        """서비스 종료 처리"""
        try:
            if self.redis_client:
                await self.redis_client.close()
                logger.info("✅ Crypto Redis 연결 종료")
            
            logger.info("✅ CryptoService 종료 완료")
            
        except Exception as e:
            logger.error(f"❌ CryptoService 종료 실패: {e}")