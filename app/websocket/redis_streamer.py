# app/websocket/redis_streamer.py
import asyncio
import logging
from typing import Dict, Set, Optional, List, Any
from datetime import datetime
import pytz

# 새로운 서비스 import 구조
from app.services.topgainers_service import TopGainersService
from app.services.crypto_service import CryptoService
from app.services.sp500_service import SP500Service

# 새로운 스키마 import 구조
from app.schemas.base_websocket_schema import (
    create_symbol_update_message, create_dashboard_update_message
)
from app.schemas.topgainers_schema import (
    TopGainerData, create_topgainers_update_message
)
from app.schemas.crypto_schema import (
    CryptoData, create_crypto_update_message
)
from app.schemas.sp500_schema import (
    StockInfo, create_sp500_update_message
)

logger = logging.getLogger(__name__)

class RedisStreamer:
    """
    Redis 실시간 데이터 스트리머
    
    이 클래스는 Redis 또는 DB에서 주기적으로 데이터를 조회하고,
    변경된 데이터만 WebSocket 클라이언트들에게 전송하는 역할을 담당합니다.
    
    **지원하는 스트리밍 타입:**
    - TopGainers 전체 스트리밍
    - 암호화폐 전체 스트리밍  
    - SP500 전체 스트리밍
    - 특정 심볼별 스트리밍
    - 대시보드 통합 스트리밍
    """
    
    def __init__(self, 
                 topgainers_service: Optional[TopGainersService],
                 crypto_service: Optional[CryptoService], 
                 sp500_service: Optional[SP500Service],
                 polling_interval: float = 0.5):
        """
        RedisStreamer 초기화
        
        Args:
            topgainers_service: TopGainersService 인스턴스
            crypto_service: CryptoService 인스턴스
            sp500_service: SP500Service 인스턴스
            polling_interval: 폴링 간격 (초) - 기본값 500ms
        """
        self.topgainers_service = topgainers_service
        self.crypto_service = crypto_service
        self.sp500_service = sp500_service
        self.polling_interval = polling_interval
        
        # 스트리밍 상태 관리
        self.is_streaming_topgainers = False
        self.is_streaming_crypto = False
        self.is_streaming_sp500 = False
        self.is_streaming_dashboard = False
        
        # 심볼별 스트리밍 상태 {data_type:symbol: is_streaming}
        self.symbol_streams: Dict[str, bool] = {}
        
        # 스트리밍 작업들
        self.topgainers_task: Optional[asyncio.Task] = None
        self.crypto_task: Optional[asyncio.Task] = None
        self.sp500_task: Optional[asyncio.Task] = None
        self.dashboard_task: Optional[asyncio.Task] = None
        self.symbol_tasks: Dict[str, asyncio.Task] = {}
        
        # 성능 통계
        self.stats = {
            "topgainers_cycles": 0,
            "crypto_cycles": 0,
            "sp500_cycles": 0,
            "dashboard_cycles": 0,
            "symbol_cycles": 0,
            "total_data_fetched": 0,
            "total_changes_detected": 0,
            "last_topgainers_update": None,
            "last_crypto_update": None,
            "last_sp500_update": None,
            "last_dashboard_update": None,
            "errors": 0,
            "start_time": datetime.now(pytz.UTC)
        }
        
        # WebSocket 매니저 (나중에 설정됨)
        self.websocket_manager = None
        
        logger.info(f"✅ RedisStreamer 초기화 완료 (폴링 간격: {polling_interval}초)")
    
    async def initialize(self):
        """RedisStreamer 초기화"""
        try:
            # 각 서비스 초기화 (필요한 경우)
            logger.info("🔧 RedisStreamer 서비스 초기화 시작")
            
            # 각 서비스가 Redis 연결을 가지고 있는지 확인하고 초기화
            services = [
                ("TopGainers", self.topgainers_service),
                ("Crypto", self.crypto_service), 
                ("SP500", self.sp500_service)
            ]
            
            for service_name, service in services:
                try:
                    # 서비스가 Redis 클라이언트나 초기화 메소드를 가지고 있다면 호출
                    if hasattr(service, 'initialize'):
                        await service.initialize()
                        logger.info(f"✅ {service_name} 서비스 초기화 완료")
                    elif hasattr(service, 'redis_client') and not service.redis_client:
                        logger.warning(f"⚠️ {service_name} 서비스 Redis 연결 없음")
                except Exception as e:
                    logger.warning(f"⚠️ {service_name} 서비스 초기화 실패: {e}")
            
            logger.info("✅ RedisStreamer 초기화 완료")
            
        except Exception as e:
            logger.error(f"❌ RedisStreamer 초기화 실패: {e}")
    
    def set_websocket_manager(self, websocket_manager):
        """
        WebSocket 매니저 설정 (순환 참조 방지)
        
        Args:
            websocket_manager: WebSocketManager 인스턴스
        """
        self.websocket_manager = websocket_manager
        logger.info("✅ WebSocket 매니저 연결 완료")
    
    # =========================
    # TopGainers 스트리밍
    # =========================
    
    async def start_topgainers_stream(self):
        """TopGainers 실시간 스트리밍 시작"""
        if self.is_streaming_topgainers:
            logger.warning("⚠️ TopGainers 스트리밍이 이미 실행 중입니다")
            return
        
        self.is_streaming_topgainers = True
        logger.info("🚀 TopGainers 실시간 스트리밍 시작")
        
        self.topgainers_task = asyncio.create_task(self._topgainers_stream_loop())
    
    async def _topgainers_stream_loop(self):
        """TopGainers 스트리밍 루프"""
        try:
            while self.is_streaming_topgainers:
                try:
                    # TopGainers 서비스에서 데이터 조회
                    new_data = await self.topgainers_service.get_realtime_data(limit=50)
                    
                    if new_data:
                        # 변경 감지 (간단한 구현)
                        changed_data, changed_count = self._detect_topgainers_changes(new_data)
                        
                        # 변경된 데이터가 있으면 브로드캐스트
                        if changed_count > 0 and self.websocket_manager:
                            # 최신 batch_id 추출
                            batch_id = new_data[0].batch_id if new_data else None
                            
                            update_message = create_topgainers_update_message(changed_data, batch_id)
                            await self.websocket_manager.broadcast_topgainers_update(update_message)
                            
                            logger.debug(f"📤 TopGainers 업데이트 전송: {changed_count}개 변경")
                        
                        # 통계 업데이트
                        self.stats["topgainers_cycles"] += 1
                        self.stats["total_data_fetched"] += len(new_data)
                        self.stats["total_changes_detected"] += changed_count
                        self.stats["last_topgainers_update"] = datetime.now(pytz.UTC)
                    
                    else:
                        logger.debug("📊 TopGainers 데이터 없음")
                    
                    # 폴링 간격 대기
                    await asyncio.sleep(self.polling_interval)
                    
                except Exception as e:
                    logger.error(f"❌ TopGainers 스트리밍 오류: {e}")
                    self.stats["errors"] += 1
                    
                    # 오류 발생 시 더 긴 대기 (5초)
                    await asyncio.sleep(5)
                    
        except asyncio.CancelledError:
            logger.info("🛑 TopGainers 스트리밍 중단됨")
            
        finally:
            self.is_streaming_topgainers = False
            logger.info("🏁 TopGainers 스트리밍 종료")
    
    async def stop_topgainers_stream(self):
        """TopGainers 스트리밍 중단"""
        if not self.is_streaming_topgainers:
            return
        
        self.is_streaming_topgainers = False
        
        if self.topgainers_task and not self.topgainers_task.done():
            self.topgainers_task.cancel()
            try:
                await self.topgainers_task
            except asyncio.CancelledError:
                pass
        
        logger.info("🛑 TopGainers 스트리밍 중단 완료")
    
    # =========================
    # 암호화폐 스트리밍
    # =========================
    
    async def start_crypto_stream(self):
        """암호화폐 실시간 스트리밍 시작"""
        if self.is_streaming_crypto:
            logger.warning("⚠️ 암호화폐 스트리밍이 이미 실행 중입니다")
            return
        
        self.is_streaming_crypto = True
        logger.info("🚀 암호화폐 실시간 스트리밍 시작")
        
        self.crypto_task = asyncio.create_task(self._crypto_stream_loop())
    
    async def _crypto_stream_loop(self):
        """암호화폐 스트리밍 루프"""
        try:
            while self.is_streaming_crypto:
                try:
                    # Crypto 서비스에서 데이터 조회
                    new_data = await self.crypto_service.get_realtime_data(limit=100)
                    
                    if new_data:
                        # 변경 감지 (간단한 구현)
                        changed_data, changed_count = self._detect_crypto_changes(new_data)
                        
                        # 변경된 데이터가 있으면 브로드캐스트
                        if changed_count > 0 and self.websocket_manager:
                            update_message = create_crypto_update_message(changed_data)
                            await self.websocket_manager.broadcast_crypto_update(update_message)
                            
                            logger.debug(f"📤 암호화폐 업데이트 전송: {changed_count}개 변경")
                        
                        # 통계 업데이트
                        self.stats["crypto_cycles"] += 1
                        self.stats["total_data_fetched"] += len(new_data)
                        self.stats["total_changes_detected"] += changed_count
                        self.stats["last_crypto_update"] = datetime.now(pytz.UTC)
                    
                    else:
                        logger.debug("📊 암호화폐 데이터 없음")
                    
                    # 폴링 간격 대기
                    await asyncio.sleep(self.polling_interval)
                    
                except Exception as e:
                    logger.error(f"❌ 암호화폐 스트리밍 오류: {e}")
                    self.stats["errors"] += 1
                    
                    # 오류 발생 시 더 긴 대기 (5초)
                    await asyncio.sleep(5)
                    
        except asyncio.CancelledError:
            logger.info("🛑 암호화폐 스트리밍 중단됨")
            
        finally:
            self.is_streaming_crypto = False
            logger.info("🏁 암호화폐 스트리밍 종료")
    
    async def stop_crypto_stream(self):
        """암호화폐 스트리밍 중단"""
        if not self.is_streaming_crypto:
            return
        
        self.is_streaming_crypto = False
        
        if self.crypto_task and not self.crypto_task.done():
            self.crypto_task.cancel()
            try:
                await self.crypto_task
            except asyncio.CancelledError:
                pass
        
        logger.info("🛑 암호화폐 스트리밍 중단 완료")
    
    # =========================
    # SP500 스트리밍
    # =========================
    
    async def start_sp500_stream(self):
        """SP500 실시간 스트리밍 시작"""
        if self.is_streaming_sp500:
            logger.warning("⚠️ SP500 스트리밍이 이미 실행 중입니다")
            return
        
        self.is_streaming_sp500 = True
        logger.info("🚀 SP500 실시간 스트리밍 시작")
        
        self.sp500_task = asyncio.create_task(self._sp500_stream_loop())
    
    async def _sp500_stream_loop(self):
        """SP500 스트리밍 루프"""
        try:
            while self.is_streaming_sp500:
                try:
                    # SP500 서비스에서 데이터 조회
                    new_data = await self.sp500_service.get_realtime_data(limit=100)
                    
                    if new_data:
                        # 변경 감지 (간단한 구현)
                        changed_data, changed_count = self._detect_sp500_changes(new_data)
                        
                        # 변경된 데이터가 있으면 브로드캐스트
                        if changed_count > 0 and self.websocket_manager:
                            update_message = create_sp500_update_message(changed_data)
                            await self.websocket_manager.broadcast_sp500_update(update_message)
                            
                            logger.debug(f"📤 SP500 업데이트 전송: {changed_count}개 변경")
                        
                        # 통계 업데이트
                        self.stats["sp500_cycles"] += 1
                        self.stats["total_data_fetched"] += len(new_data)
                        self.stats["total_changes_detected"] += changed_count
                        self.stats["last_sp500_update"] = datetime.now(pytz.UTC)
                    
                    else:
                        logger.debug("📊 SP500 데이터 없음")
                    
                    # 폴링 간격 대기
                    await asyncio.sleep(self.polling_interval)
                    
                except Exception as e:
                    logger.error(f"❌ SP500 스트리밍 오류: {e}")
                    self.stats["errors"] += 1
                    
                    # 오류 발생 시 더 긴 대기 (5초)
                    await asyncio.sleep(5)
                    
        except asyncio.CancelledError:
            logger.info("🛑 SP500 스트리밍 중단됨")
            
        finally:
            self.is_streaming_sp500 = False
            logger.info("🏁 SP500 스트리밍 종료")
    
    async def stop_sp500_stream(self):
        """SP500 스트리밍 중단"""
        if not self.is_streaming_sp500:
            return
        
        self.is_streaming_sp500 = False
        
        if self.sp500_task and not self.sp500_task.done():
            self.sp500_task.cancel()
            try:
                await self.sp500_task
            except asyncio.CancelledError:
                pass
        
        logger.info("🛑 SP500 스트리밍 중단 완료")
    
    # =========================
    # 대시보드 스트리밍
    # =========================
    
    async def start_dashboard_stream(self):
        """대시보드 실시간 스트리밍 시작"""
        if self.is_streaming_dashboard:
            logger.warning("⚠️ 대시보드 스트리밍이 이미 실행 중입니다")
            return
        
        self.is_streaming_dashboard = True
        logger.info("🚀 대시보드 실시간 스트리밍 시작")
        
        self.dashboard_task = asyncio.create_task(self._dashboard_stream_loop())
    
    async def _dashboard_stream_loop(self):
        """대시보드 스트리밍 루프"""
        try:
            while self.is_streaming_dashboard:
                try:
                    # 대시보드 데이터 조회 (통합)
                    dashboard_data = await self._get_dashboard_data()
                    
                    if any(dashboard_data.values()):
                        # 변경 감지 (간단한 구현)
                        changed_data, changed_count = self._detect_dashboard_changes(dashboard_data)
                        
                        # 변경된 데이터가 있으면 브로드캐스트
                        if changed_count > 0 and self.websocket_manager:
                            update_message = create_dashboard_update_message(
                                top_gainers=dashboard_data.get("top_gainers", []),
                                top_crypto=dashboard_data.get("top_crypto", []),
                                sp500_highlights=dashboard_data.get("sp500_highlights", []),
                                summary=dashboard_data.get("summary", {})
                            )
                            await self.websocket_manager.broadcast_dashboard_update(update_message)
                            
                            logger.debug(f"📤 대시보드 업데이트 전송: 통합 데이터")
                        
                        # 통계 업데이트
                        self.stats["dashboard_cycles"] += 1
                        self.stats["last_dashboard_update"] = datetime.now(pytz.UTC)
                    
                    else:
                        logger.debug("📊 대시보드 데이터 없음")
                    
                    # 폴링 간격 대기 (대시보드는 좀 더 긴 간격)
                    await asyncio.sleep(self.polling_interval * 2)  # 1초 간격
                    
                except Exception as e:
                    logger.error(f"❌ 대시보드 스트리밍 오류: {e}")
                    self.stats["errors"] += 1
                    
                    # 오류 발생 시 더 긴 대기 (10초)
                    await asyncio.sleep(10)
                    
        except asyncio.CancelledError:
            logger.info("🛑 대시보드 스트리밍 중단됨")
            
        finally:
            self.is_streaming_dashboard = False
            logger.info("🏁 대시보드 스트리밍 종료")
    
    async def stop_dashboard_stream(self):
        """대시보드 스트리밍 중단"""
        if not self.is_streaming_dashboard:
            return
        
        self.is_streaming_dashboard = False
        
        if self.dashboard_task and not self.dashboard_task.done():
            self.dashboard_task.cancel()
            try:
                await self.dashboard_task
            except asyncio.CancelledError:
                pass
        
        logger.info("🛑 대시보드 스트리밍 중단 완료")
    
    # =========================
    # 특정 심볼 스트리밍
    # =========================
    
    async def start_symbol_stream(self, symbol: str, data_type: str):
        """
        특정 심볼 실시간 스트리밍 시작
        
        Args:
            symbol: 스트리밍할 심볼
            data_type: 데이터 타입 (topgainers, crypto, sp500)
        """
        symbol = symbol.upper()
        stream_key = f"{data_type}:{symbol}"
        
        if stream_key in self.symbol_streams and self.symbol_streams[stream_key]:
            logger.warning(f"⚠️ 심볼 {stream_key} 스트리밍이 이미 실행 중입니다")
            return
        
        self.symbol_streams[stream_key] = True
        logger.info(f"🚀 심볼 {stream_key} 실시간 스트리밍 시작")
        
        # 비동기 작업 생성
        task = asyncio.create_task(self._symbol_stream_loop(symbol, data_type))
        self.symbol_tasks[stream_key] = task
    
    async def _symbol_stream_loop(self, symbol: str, data_type: str):
        """
        특정 심볼 스트리밍 루프
        
        Args:
            symbol: 스트리밍할 심볼
            data_type: 데이터 타입
        """
        stream_key = f"{data_type}:{symbol}"
        cache_key = f"symbol_{stream_key}"
        previous_hash = None
        
        try:
            while self.symbol_streams.get(stream_key, False):
                try:
                    # 심볼 데이터 조회
                    new_data = await self._get_symbol_data(symbol, data_type)
                    
                    if new_data:
                        # 간단한 해시 기반 변경 감지
                        import json
                        data_str = json.dumps(new_data.dict() if hasattr(new_data, 'dict') else str(new_data), sort_keys=True)
                        current_hash = hash(data_str)
                        
                        # 변경 감지 (이전 해시와 비교)
                        if previous_hash is None or previous_hash != current_hash:
                            # 변경된 데이터 브로드캐스트
                            if self.websocket_manager:
                                update_message = create_symbol_update_message(symbol, data_type, new_data)
                                await self.websocket_manager.broadcast_symbol_update(symbol, data_type, update_message)
                                
                                logger.debug(f"📤 심볼 {stream_key} 업데이트 전송")
                            
                            # 이전 해시 저장
                            previous_hash = current_hash
                        
                        # 통계 업데이트
                        self.stats["symbol_cycles"] += 1
                    
                    else:
                        logger.debug(f"📊 심볼 {stream_key} 데이터 없음")
                    
                    # 폴링 간격 대기
                    await asyncio.sleep(self.polling_interval)
                    
                except Exception as e:
                    logger.error(f"❌ 심볼 {stream_key} 스트리밍 오류: {e}")
                    self.stats["errors"] += 1
                    
                    # 오류 발생 시 더 긴 대기
                    await asyncio.sleep(5)
                    
        except asyncio.CancelledError:
            logger.info(f"🛑 심볼 {stream_key} 스트리밍 중단됨")
            
        finally:
            # 정리
            self.symbol_streams[stream_key] = False
            if stream_key in self.symbol_tasks:
                del self.symbol_tasks[stream_key]
            logger.info(f"🏁 심볼 {stream_key} 스트리밍 종료")
    
    async def stop_symbol_stream(self, symbol: str, data_type: str):
        """
        특정 심볼 스트리밍 중단
        
        Args:
            symbol: 중단할 심볼
            data_type: 데이터 타입
        """
        symbol = symbol.upper()
        stream_key = f"{data_type}:{symbol}"
        
        if stream_key not in self.symbol_streams:
            return
        
        # 스트리밍 플래그 해제
        self.symbol_streams[stream_key] = False
        
        # 작업 취소
        if stream_key in self.symbol_tasks:
            task = self.symbol_tasks[stream_key]
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        
        logger.info(f"🛑 심볼 {stream_key} 스트리밍 중단 완료")
    
    # =========================
    # 전체 제어 메서드들
    # =========================
    
    async def stop_all_streams(self):
        """모든 스트리밍 중단"""
        logger.info("🛑 모든 스트리밍 중단 시작")
        
        # 전체 데이터 스트리밍 중단
        await self.stop_topgainers_stream()
        await self.stop_crypto_stream()
        await self.stop_sp500_stream()
        await self.stop_dashboard_stream()
        
        # 모든 심볼 스트리밍 중단
        symbol_list = list(self.symbol_streams.keys())
        for stream_key in symbol_list:
            if ":" in stream_key:
                data_type, symbol = stream_key.split(":", 1)
                await self.stop_symbol_stream(symbol, data_type)
        
        logger.info("🏁 모든 스트리밍 중단 완료")
    
    async def start_all_basic_streams(self):
        """기본 스트리밍들 모두 시작"""
        logger.info("🚀 기본 스트리밍들 시작")
        
        # 순서대로 시작 (부하 분산)
        await asyncio.sleep(0.1)
        asyncio.create_task(self.start_topgainers_stream())
        
        await asyncio.sleep(0.2)
        asyncio.create_task(self.start_crypto_stream())
        
        await asyncio.sleep(0.3)
        asyncio.create_task(self.start_sp500_stream())
        
        await asyncio.sleep(0.5)
        asyncio.create_task(self.start_dashboard_stream())
        
        logger.info("✅ 기본 스트리밍들 시작 완료")
    
    # =========================
    # 상태 조회 및 통계
    # =========================
    
    def get_status(self) -> Dict[str, Any]:
        """
        스트리머 상태 반환
        
        Returns:
            Dict[str, Any]: 상태 정보
        """
        uptime = datetime.now(pytz.UTC) - self.stats["start_time"]
        
        active_symbol_streams = [k for k, v in self.symbol_streams.items() if v]
        
        return {
            "polling_interval": self.polling_interval,
            "streaming_status": {
                "topgainers": self.is_streaming_topgainers,
                "crypto": self.is_streaming_crypto,
                "sp500": self.is_streaming_sp500,
                "dashboard": self.is_streaming_dashboard
            },
            "symbol_streams": {
                "active_count": len(active_symbol_streams),
                "active_streams": active_symbol_streams,
                "total_registered": len(self.symbol_streams)
            },
            "performance": {
                "uptime_seconds": uptime.total_seconds(),
                "total_cycles": (
                    self.stats["topgainers_cycles"] + 
                    self.stats["crypto_cycles"] + 
                    self.stats["sp500_cycles"] + 
                    self.stats["dashboard_cycles"] + 
                    self.stats["symbol_cycles"]
                ),
                "error_count": self.stats["errors"],
                "last_updates": {
                    "topgainers": self.stats["last_topgainers_update"].isoformat() if self.stats["last_topgainers_update"] else None,
                    "crypto": self.stats["last_crypto_update"].isoformat() if self.stats["last_crypto_update"] else None,
                    "sp500": self.stats["last_sp500_update"].isoformat() if self.stats["last_sp500_update"] else None,
                    "dashboard": self.stats["last_dashboard_update"].isoformat() if self.stats["last_dashboard_update"] else None
                }
            }
        }
    
    def get_detailed_stats(self) -> Dict[str, Any]:
        """
        상세 통계 정보 반환
        
        Returns:
            Dict[str, Any]: 상세 통계
        """
        total_cycles = (
            self.stats["topgainers_cycles"] + 
            self.stats["crypto_cycles"] + 
            self.stats["sp500_cycles"] + 
            self.stats["dashboard_cycles"] + 
            self.stats["symbol_cycles"]
        )
        
        return {
            "streaming_breakdown": {
                "topgainers_cycles": self.stats["topgainers_cycles"],
                "crypto_cycles": self.stats["crypto_cycles"],
                "sp500_cycles": self.stats["sp500_cycles"],
                "dashboard_cycles": self.stats["dashboard_cycles"],
                "symbol_cycles": self.stats["symbol_cycles"]
            },
            "performance_metrics": {
                "total_cycles": total_cycles,
                "total_data_fetched": self.stats["total_data_fetched"],
                "total_changes_detected": self.stats["total_changes_detected"],
                "average_data_per_cycle": self.stats["total_data_fetched"] / max(total_cycles, 1),
                "change_detection_rate": self.stats["total_changes_detected"] / max(self.stats["total_data_fetched"], 1) * 100,
                "error_rate": self.stats["errors"] / max(total_cycles, 1) * 100
            },
            "health_indicators": {
                "websocket_manager_connected": self.websocket_manager is not None,
                "recent_activity": any([
                    self.stats["last_topgainers_update"],
                    self.stats["last_crypto_update"],
                    self.stats["last_sp500_update"],
                    self.stats["last_dashboard_update"]
                ]),
                "errors": self.stats["errors"]
            }
        }
    
    async def health_check(self) -> Dict[str, Any]:
        """
        헬스 체크
        
        Returns:
            Dict[str, Any]: 헬스 체크 결과
        """
        try:
            # 각 서비스 헬스 체크
            realtime_health = await self._check_services_health()
            
            # 스트리머 자체 상태 확인
            active_streams = sum([
                self.is_streaming_topgainers,
                self.is_streaming_crypto,
                self.is_streaming_sp500,
                self.is_streaming_dashboard
            ]) + len([v for v in self.symbol_streams.values() if v])
            
            is_healthy = (
                realtime_health.get("status") in ["healthy", "degraded"] and
                active_streams > 0 and
                self.websocket_manager is not None and
                self.stats["errors"] < 100  # 에러가 100개 미만
            )
            
            return {
                "status": "healthy" if is_healthy else "degraded",
                "active_streams": active_streams,
                "streamer_status": self.get_status(),
                "realtime_service": realtime_health,
                "timestamp": datetime.now(pytz.UTC).isoformat()
            }
            
        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e),
                "timestamp": datetime.now(pytz.UTC).isoformat()
            }
    
    async def cleanup_completed_tasks(self):
        """완료된 비동기 작업들 정리"""
        try:
            # 완료된 심볼 스트리밍 작업들 정리
            completed_tasks = []
            for stream_key, task in self.symbol_tasks.items():
                if task.done():
                    completed_tasks.append(stream_key)
            
            for stream_key in completed_tasks:
                del self.symbol_tasks[stream_key]
                self.symbol_streams[stream_key] = False
            
            if completed_tasks:
                logger.info(f"🧹 완료된 스트리밍 작업 정리: {len(completed_tasks)}개")
                
        except Exception as e:
            logger.error(f"❌ 작업 정리 실패: {e}")
    
    async def shutdown(self):
        """스트리머 종료 처리"""
        try:
            logger.info("🛑 RedisStreamer 종료 시작")
            
            # 모든 스트리밍 중단
            await self.stop_all_streams()
            
            # 작업 정리
            await self.cleanup_completed_tasks()
            
            # WebSocket 매니저 연결 해제
            self.websocket_manager = None
            
            # 통계 정리
            logger.info(f"📊 최종 통계: {self.get_detailed_stats()}")
            
            logger.info("✅ RedisStreamer 종료 완료")
            
        except Exception as e:
            logger.error(f"❌ RedisStreamer 종료 실패: {e}")
    
    # =========================
    # 헬퍼 메소드들
    # =========================
    
    def _detect_topgainers_changes(self, new_data: List[TopGainerData]) -> tuple[List[TopGainerData], int]:
        """TopGainers 데이터 변경 감지 (간단한 구현)"""
        # 실제 구현에서는 이전 데이터와 비교하여 변경된 항목만 반환
        # 지금은 모든 데이터를 변경된 것으로 간주
        return new_data, len(new_data) if new_data else 0
    
    def _detect_crypto_changes(self, new_data: List[CryptoData]) -> tuple[List[CryptoData], int]:
        """Crypto 데이터 변경 감지 (간단한 구현)"""
        # 실제 구현에서는 이전 데이터와 비교하여 변경된 항목만 반환
        # 지금은 모든 데이터를 변경된 것으로 간주
        return new_data, len(new_data) if new_data else 0
    
    def _detect_sp500_changes(self, new_data: List[StockInfo]) -> tuple[List[StockInfo], int]:
        """SP500 데이터 변경 감지 (간단한 구현)"""
        # 실제 구현에서는 이전 데이터와 비교하여 변경된 항목만 반환
        # 지금은 모든 데이터를 변경된 것으로 간주
        return new_data, len(new_data) if new_data else 0
    
    def _detect_dashboard_changes(self, new_data: Dict[str, Any]) -> tuple[Dict[str, Any], int]:
        """Dashboard 데이터 변경 감지 (간단한 구현)"""
        # 실제 구현에서는 이전 데이터와 비교하여 변경된 항목만 반환
        # 지금은 모든 데이터를 변경된 것으로 간주
        total_items = sum(len(v) if isinstance(v, list) else 1 for v in new_data.values())
        return new_data, total_items
    
    async def _get_dashboard_data(self) -> Dict[str, Any]:
        """대시보드용 통합 데이터 조회"""
        try:
            # 각 서비스에서 요약 데이터 조회
            top_gainers = await self.topgainers_service.get_realtime_data(limit=10)
            top_crypto = await self.crypto_service.get_realtime_data(limit=10) 
            sp500_highlights = await self.sp500_service.get_realtime_data(limit=10)
            
            return {
                "top_gainers": [item.dict() if hasattr(item, 'dict') else item for item in (top_gainers or [])],
                "top_crypto": [item.dict() if hasattr(item, 'dict') else item for item in (top_crypto or [])],
                "sp500_highlights": [item.dict() if hasattr(item, 'dict') else item for item in (sp500_highlights or [])],
                "summary": {
                    "topgainers_count": len(top_gainers) if top_gainers else 0,
                    "crypto_count": len(top_crypto) if top_crypto else 0,
                    "sp500_count": len(sp500_highlights) if sp500_highlights else 0,
                    "last_update": datetime.now(pytz.UTC).isoformat()
                }
            }
        except Exception as e:
            logger.error(f"❌ 대시보드 데이터 조회 실패: {e}")
            return {}
    
    async def _get_symbol_data(self, symbol: str, data_type: str):
        """특정 심볼 데이터 조회"""
        try:
            if data_type == "topgainers":
                return await self.topgainers_service.get_symbol_data(symbol)
            elif data_type == "crypto":
                return await self.crypto_service.get_symbol_data(symbol)
            elif data_type == "sp500":
                return await self.sp500_service.get_symbol_data(symbol)
            else:
                logger.warning(f"⚠️ 지원하지 않는 데이터 타입: {data_type}")
                return None
        except Exception as e:
            logger.error(f"❌ 심볼 {symbol} ({data_type}) 데이터 조회 실패: {e}")
            return None
    
    async def _check_services_health(self) -> Dict[str, Any]:
        """각 서비스의 헬스 체크"""
        health_status = {
            "status": "healthy",
            "services": {}
        }
        
        services = [
            ("topgainers", self.topgainers_service),
            ("crypto", self.crypto_service),
            ("sp500", self.sp500_service)
        ]
        
        for service_name, service in services:
            try:
                if hasattr(service, 'health_check'):
                    service_health = await service.health_check()
                    health_status["services"][service_name] = service_health
                else:
                    health_status["services"][service_name] = {"status": "unknown", "message": "No health check method"}
            except Exception as e:
                health_status["services"][service_name] = {"status": "error", "message": str(e)}
                health_status["status"] = "degraded"
        
        return health_status