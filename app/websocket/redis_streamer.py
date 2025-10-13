# app/websocket/redis_streamer.py - 통합 버전
import asyncio
import json
import logging
from typing import Dict, Optional, Any
from datetime import datetime
import pytz
import redis.asyncio as aioredis

from app.services.crypto_service import CryptoService
from app.services.sp500_service import SP500Service, get_sp500_data_from_redis
from app.services.etf_service import get_etf_data_from_redis

from app.schemas.crypto_schema import create_crypto_update_message
from app.schemas.sp500_schema import create_sp500_update_message

logger = logging.getLogger(__name__)

class RedisStreamer:
    """
    통합 Redis 실시간 데이터 스트리머
    
    Redis Pub/Sub을 통해 다음 채널을 감시:
    - crypto_updates (기존)
    - sp500_updates (신규)
    - etf_updates (신규)
    """
    
    def __init__(self, 
                 crypto_service: Optional[CryptoService], 
                 sp500_service: Optional[SP500Service],
                 redis_url: str):
        """
        RedisStreamer 초기화
        
        Args:
            crypto_service: CryptoService 인스턴스
            sp500_service: SP500Service 인스턴스
            redis_url: Redis 연결 URL
        """
        self.crypto_service = crypto_service
        self.sp500_service = sp500_service
        self.redis_url = redis_url
        
        # Redis Pub/Sub 관련
        self.pubsub = None
        self.redis_client = None
        self.sync_redis_client = None  # 동기 클라이언트 (데이터 조회용)
        
        # 채널 매핑: Redis 채널 → 데이터 타입
        self.channels_to_types = {
            "crypto_updates": "crypto",
            "sp500_updates": "sp500",
            "etf_updates": "etf",
        }
        
        # 스트리밍 상태 관리
        self.is_streaming = False
        self.listen_task: Optional[asyncio.Task] = None
        
        # 성능 통계
        self.stats = {
            "crypto_updates": 0,
            "sp500_updates": 0,
            "etf_updates": 0,
            "total_messages": 0,
            "errors": 0,
            "start_time": datetime.now(pytz.UTC),
            "last_crypto_update": None,
            "last_sp500_update": None,
            "last_etf_update": None,
        }
        
        # WebSocket 매니저 (나중에 설정됨)
        self.websocket_manager = None
        
        logger.info(f"✅ RedisStreamer 초기화 완료 (Crypto + SP500 + ETF)")
    
    async def initialize(self):
        """RedisStreamer 초기화 및 Redis 연결"""
        try:
            logger.info("🔧 RedisStreamer Redis 연결 시작")
            
            # 비동기 Redis 클라이언트 (Pub/Sub용)
            self.redis_client = await aioredis.from_url(
                self.redis_url, 
                decode_responses=True,
                encoding="utf-8"
            )
            
            # 동기 Redis 클라이언트 (데이터 조회용)
            import redis
            self.sync_redis_client = redis.Redis.from_url(
                self.redis_url,
                decode_responses=True
            )
            
            # Pub/Sub 초기화
            self.pubsub = self.redis_client.pubsub()
            await self.pubsub.subscribe(*self.channels_to_types.keys())
            
            logger.info(f"✅ Redis Pub/Sub 구독 완료: {list(self.channels_to_types.keys())}")
            
        except Exception as e:
            logger.error(f"❌ RedisStreamer 초기화 실패: {e}")
            raise
    
    def set_websocket_manager(self, websocket_manager):
        """
        WebSocket 매니저 설정
        
        Args:
            websocket_manager: WebSocketManager 인스턴스
        """
        self.websocket_manager = websocket_manager
        logger.info("✅ WebSocket 매니저 연결 완료")
    
    # =========================
    # 메인 스트리밍 로직
    # =========================
    
    async def start_streaming(self):
        """Redis Pub/Sub 스트리밍 시작"""
        if self.is_streaming:
            logger.warning("⚠️ 스트리밍이 이미 실행 중입니다")
            return
        
        if not self.pubsub:
            logger.error("❌ Redis Pub/Sub이 초기화되지 않았습니다")
            return
        
        self.is_streaming = True
        logger.info("🚀 Redis Pub/Sub 스트리밍 시작")
        
        self.listen_task = asyncio.create_task(self._listen_loop())
    
    async def _listen_loop(self):
        """
        Redis Pub/Sub 메시지 수신 루프
        
        각 채널에서 업데이트 신호를 받으면:
        1. Redis에서 최신 데이터 조회
        2. WebSocket 매니저를 통해 구독자들에게 브로드캐스트
        """
        logger.info("👂 Redis Pub/Sub 리스닝 시작...")
        
        try:
            while self.is_streaming:
                try:
                    # Pub/Sub 메시지 대기
                    message = await self.pubsub.get_message(
                        ignore_subscribe_messages=True,
                        timeout=1.0
                    )
                    
                    if not message:
                        await asyncio.sleep(0.1)
                        continue
                    
                    # 채널 이름 확인
                    channel = message.get('channel')
                    if not channel:
                        continue
                    
                    # 채널에 해당하는 데이터 타입 확인
                    data_type = self.channels_to_types.get(channel)
                    if not data_type:
                        logger.warning(f"⚠️ 알 수 없는 채널: {channel}")
                        continue
                    
                    # logger.info(f"📬 '{data_type}' 업데이트 신호 수신!")
                    
                    # 데이터 타입별 처리
                    await self._handle_update(data_type)
                    
                    # 통계 업데이트
                    self.stats["total_messages"] += 1
                    self.stats[f"{data_type}_updates"] += 1
                    
                except asyncio.CancelledError:
                    logger.info("🛑 리스닝 루프 취소됨")
                    break
                    
                except Exception as e:
                    logger.error(f"❌ 리스닝 중 오류: {e}")
                    self.stats["errors"] += 1
                    await asyncio.sleep(2)
                    
        except Exception as e:
            logger.error(f"❌ 리스닝 루프 치명적 오류: {e}")
            
        finally:
            self.is_streaming = False
            logger.info("🏁 Redis Pub/Sub 리스닝 종료")
    
    async def _handle_update(self, data_type: str):
        """
        데이터 타입별 업데이트 처리
        
        Args:
            data_type: 'crypto', 'sp500', 'etf' 중 하나
        """
        try:
            if not self.websocket_manager:
                logger.warning("⚠️ WebSocket 매니저가 설정되지 않았습니다")
                return
            
            # 데이터 타입별 처리
            if data_type == "crypto":
                await self._handle_crypto_update()
            elif data_type == "sp500":
                await self._handle_sp500_update()
            elif data_type == "etf":
                await self._handle_etf_update()
            else:
                logger.warning(f"⚠️ 지원하지 않는 데이터 타입: {data_type}")
                
        except Exception as e:
            logger.error(f"❌ {data_type} 업데이트 처리 실패: {e}")
    
    async def _handle_crypto_update(self):
        """Crypto 업데이트 처리"""
        try:
            if not self.crypto_service:
                logger.debug("Crypto 서비스 없음")
                return
            
            # Redis에서 최신 Crypto 데이터 조회
            crypto_data = await self.crypto_service.get_realtime_data(limit=100)
            
            if not crypto_data:
                logger.debug("📊 Crypto 데이터 없음")
                return
            
            # WebSocket 브로드캐스트
            update_message = create_crypto_update_message(crypto_data)
            await self.websocket_manager.broadcast_crypto_update(update_message)
            
            # 통계 업데이트
            self.stats["last_crypto_update"] = datetime.now(pytz.UTC)
            logger.debug(f"📤 Crypto 업데이트 전송 완료: {len(crypto_data)}개")
            
        except Exception as e:
            logger.error(f"❌ Crypto 업데이트 처리 실패: {e}")
    
    async def _handle_sp500_update(self):
        """SP500 업데이트 처리"""
        try:
            if not self.sync_redis_client:
                logger.debug("Redis 클라이언트 없음")
                return
            
            # Redis에서 최신 SP500 데이터 조회 (동기 함수 사용)
            sp500_data = await asyncio.to_thread(
                get_sp500_data_from_redis,
                self.sync_redis_client,
                100
            )
            
            if not sp500_data:
                logger.debug("📊 SP500 데이터 없음")
                return
            
            # WebSocket 브로드캐스트
            update_message = create_sp500_update_message(sp500_data)
            await self.websocket_manager.broadcast_sp500_update(update_message)
            
            # 통계 업데이트
            self.stats["last_sp500_update"] = datetime.now(pytz.UTC)
            logger.debug(f"📤 SP500 업데이트 전송 완료: {len(sp500_data)}개")
            
        except Exception as e:
            logger.error(f"❌ SP500 업데이트 처리 실패: {e}")
    
    async def _handle_etf_update(self):
        """ETF 업데이트 처리"""
        try:
            if not self.sync_redis_client:
                logger.debug("Redis 클라이언트 없음")
                return
            
            # Redis에서 최신 ETF 데이터 조회 (동기 함수 사용)
            etf_data = await asyncio.to_thread(
                get_etf_data_from_redis,
                self.sync_redis_client,
                100
            )
            
            if not etf_data:
                logger.debug("📊 ETF 데이터 없음")
                return
            
            # WebSocket 브로드캐스트
            await self.websocket_manager.broadcast_etf_update(etf_data)
            
            # 통계 업데이트
            self.stats["last_etf_update"] = datetime.now(pytz.UTC)
            logger.debug(f"📤 ETF 업데이트 전송 완료: {len(etf_data)}개")
            
        except Exception as e:
            logger.error(f"❌ ETF 업데이트 처리 실패: {e}")
    
    # =========================
    # 제어 메서드
    # =========================
    
    async def stop_streaming(self):
        """Redis Pub/Sub 스트리밍 중단"""
        if not self.is_streaming:
            return
        
        logger.info("🛑 Redis Pub/Sub 스트리밍 중단 시작")
        
        self.is_streaming = False
        
        # 리스닝 태스크 취소
        if self.listen_task and not self.listen_task.done():
            self.listen_task.cancel()
            try:
                await self.listen_task
            except asyncio.CancelledError:
                pass
        
        logger.info("✅ Redis Pub/Sub 스트리밍 중단 완료")
    
    async def shutdown(self):
        """스트리머 종료 처리"""
        try:
            logger.info("🛑 RedisStreamer 종료 시작")
            
            # 스트리밍 중단
            await self.stop_streaming()
            
            # Pub/Sub 연결 해제
            if self.pubsub:
                await self.pubsub.unsubscribe()
                await self.pubsub.close()
            
            # Redis 클라이언트 종료
            if self.redis_client:
                await self.redis_client.close()
            
            if self.sync_redis_client:
                self.sync_redis_client.close()
            
            # WebSocket 매니저 연결 해제
            self.websocket_manager = None
            
            # 최종 통계 로깅
            logger.info(f"📊 최종 통계: {self.get_stats()}")
            
            logger.info("✅ RedisStreamer 종료 완료")
            
        except Exception as e:
            logger.error(f"❌ RedisStreamer 종료 실패: {e}")
    
    # =========================
    # 상태 조회 및 통계
    # =========================
    
    def get_status(self) -> Dict[str, Any]:
        """스트리머 상태 반환"""
        uptime = datetime.now(pytz.UTC) - self.stats["start_time"]
        
        return {
            "is_streaming": self.is_streaming,
            "subscribed_channels": list(self.channels_to_types.keys()),
            "websocket_manager_connected": self.websocket_manager is not None,
            "uptime_seconds": uptime.total_seconds(),
            "performance": {
                "total_messages": self.stats["total_messages"],
                "crypto_updates": self.stats["crypto_updates"],
                "sp500_updates": self.stats["sp500_updates"],
                "etf_updates": self.stats["etf_updates"],
                "errors": self.stats["errors"],
                "last_updates": {
                    "crypto": self.stats["last_crypto_update"].isoformat() if self.stats["last_crypto_update"] else None,
                    "sp500": self.stats["last_sp500_update"].isoformat() if self.stats["last_sp500_update"] else None,
                    "etf": self.stats["last_etf_update"].isoformat() if self.stats["last_etf_update"] else None,
                }
            }
        }
    
    def get_stats(self) -> Dict[str, Any]:
        """상세 통계 정보 반환"""
        return {
            **self.stats,
            "uptime": str(datetime.now(pytz.UTC) - self.stats["start_time"]),
            "error_rate": self.stats["errors"] / max(self.stats["total_messages"], 1) * 100,
        }
    
    async def health_check(self) -> Dict[str, Any]:
        """헬스 체크"""
        try:
            is_healthy = (
                self.is_streaming and
                self.websocket_manager is not None and
                self.redis_client is not None and
                self.stats["errors"] < 100
            )
            
            return {
                "status": "healthy" if is_healthy else "degraded",
                "streaming": self.is_streaming,
                "redis_connected": self.redis_client is not None,
                "websocket_manager_available": self.websocket_manager is not None,
                "stats": self.get_status(),
                "timestamp": datetime.now(pytz.UTC).isoformat()
            }
            
        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e),
                "timestamp": datetime.now(pytz.UTC).isoformat()
            }