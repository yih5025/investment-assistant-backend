# app/websocket/manager.py - 통합 버전
import asyncio
import json
import logging
from typing import List, Dict, Set, Optional, Any
from datetime import datetime
import pytz
from fastapi import WebSocket

# 스키마 import (기존 유지)
from app.schemas.base_websocket_schema import (
    WebSocketMessageType, SymbolUpdateMessage, DashboardUpdateMessage, BaseErrorMessage,
    create_symbol_update_message, create_dashboard_update_message, create_error_message
)
from app.schemas.crypto_schema import (
    CryptoUpdateMessage, create_crypto_update_message
)
from app.schemas.sp500_schema import (
    SP500UpdateMessage, create_sp500_update_message
)

logger = logging.getLogger(__name__)

class WebSocketManager:
    """
    통합 WebSocket 연결 관리 클래스
    
    지원 타입:
    - Crypto (기존 유지)
    - SP500 (신규 추가)
    - ETF (신규 추가)
    """
    
    def __init__(self):
        """WebSocketManager 초기화"""
        
        # ✅ 기존 crypto 구독자 (유지)
        self.crypto_subscribers: List[WebSocket] = []
        
        # 🆕 신규 추가: SP500, ETF 구독자
        self.sp500_subscribers: List[WebSocket] = []
        self.etf_subscribers: List[WebSocket] = []
        
        # 대시보드 구독자 (선택적)
        self.dashboard_subscribers: List[WebSocket] = []
        
        # 심볼별 구독자들 {data_type:symbol: [websocket1, websocket2, ...]}
        self.symbol_subscribers: Dict[str, List[WebSocket]] = {}
        
        # 클라이언트 메타데이터 {websocket_id: metadata}
        self.client_metadata: Dict[int, Dict[str, Any]] = {}
        
        # 통계 정보
        self.stats = {
            "total_connections": 0,
            "total_disconnections": 0,
            "total_messages_sent": 0,
            "total_errors": 0,
            "start_time": datetime.now(pytz.UTC)
        }
        
        # 활성 연결들 추적
        self.active_connections: Set[int] = set()
        
        logger.info("✅ WebSocketManager 초기화 완료 (Crypto + SP500 + ETF)")
    
    # =========================
    # ✅ Crypto 연결 관리 (기존 유지)
    # =========================
    
    async def connect_crypto(self, websocket: WebSocket) -> bool:
        """암호화폐 전체 구독자로 연결"""
        try:
            client_id = id(websocket)
            client_ip = websocket.client.host if websocket.client else "unknown"
            
            self.crypto_subscribers.append(websocket)
            self.active_connections.add(client_id)
            
            self.client_metadata[client_id] = {
                "type": "crypto",
                "subscription": "all",
                "ip": client_ip,
                "connected_at": datetime.now(pytz.UTC),
                "last_heartbeat": datetime.now(pytz.UTC),
                "messages_received": 0
            }
            
            self.stats["total_connections"] += 1
            
            logger.info(f"🔗 Crypto 구독자 연결: {client_id} ({client_ip})")
            return True
            
        except Exception as e:
            logger.error(f"❌ Crypto 연결 실패: {e}")
            return False
    
    async def disconnect_crypto(self, websocket: WebSocket):
        """Crypto 구독자 연결 해제"""
        try:
            client_id = id(websocket)
            
            if websocket in self.crypto_subscribers:
                self.crypto_subscribers.remove(websocket)
            
            await self._cleanup_client(client_id, "Crypto")
            
        except Exception as e:
            logger.error(f"❌ Crypto 연결 해제 오류: {e}")
    
    async def broadcast_crypto_update(self, message: CryptoUpdateMessage):
        """모든 Crypto 구독자에게 업데이트 브로드캐스트"""
        if not self.crypto_subscribers:
            return
        
        successful_sends = await self._broadcast_to_subscribers(
            self.crypto_subscribers, message, "Crypto"
        )
        
        if successful_sends > 0:
            logger.debug(f"📤 Crypto 업데이트 전송 완료: {successful_sends}명")
    
    # =========================
    # 🆕 SP500 연결 관리 (신규)
    # =========================
    
    async def connect_sp500(self, websocket: WebSocket) -> bool:
        """SP500 전체 구독자로 연결"""
        try:
            client_id = id(websocket)
            client_ip = websocket.client.host if websocket.client else "unknown"
            
            self.sp500_subscribers.append(websocket)
            self.active_connections.add(client_id)
            
            self.client_metadata[client_id] = {
                "type": "sp500",
                "subscription": "all",
                "ip": client_ip,
                "connected_at": datetime.now(pytz.UTC),
                "last_heartbeat": datetime.now(pytz.UTC),
                "messages_received": 0
            }
            
            self.stats["total_connections"] += 1
            
            logger.info(f"🔗 SP500 구독자 연결: {client_id} ({client_ip})")
            return True
            
        except Exception as e:
            logger.error(f"❌ SP500 연결 실패: {e}")
            return False
    
    async def disconnect_sp500(self, websocket: WebSocket):
        """SP500 구독자 연결 해제"""
        try:
            client_id = id(websocket)
            
            if websocket in self.sp500_subscribers:
                self.sp500_subscribers.remove(websocket)
            
            await self._cleanup_client(client_id, "SP500")
            
        except Exception as e:
            logger.error(f"❌ SP500 연결 해제 오류: {e}")
    
    async def broadcast_sp500_update(self, message: SP500UpdateMessage):
        """모든 SP500 구독자에게 업데이트 브로드캐스트"""
        if not self.sp500_subscribers:
            return
        
        successful_sends = await self._broadcast_to_subscribers(
            self.sp500_subscribers, message, "SP500"
        )
        
        if successful_sends > 0:
            logger.debug(f"📤 SP500 업데이트 전송 완료: {successful_sends}명")
    
    # =========================
    # 🆕 ETF 연결 관리 (신규)
    # =========================
    
    async def connect_etf(self, websocket: WebSocket) -> bool:
        """ETF 전체 구독자로 연결"""
        try:
            client_id = id(websocket)
            client_ip = websocket.client.host if websocket.client else "unknown"
            
            self.etf_subscribers.append(websocket)
            self.active_connections.add(client_id)
            
            self.client_metadata[client_id] = {
                "type": "etf",
                "subscription": "all",
                "ip": client_ip,
                "connected_at": datetime.now(pytz.UTC),
                "last_heartbeat": datetime.now(pytz.UTC),
                "messages_received": 0
            }
            
            self.stats["total_connections"] += 1
            
            logger.info(f"🔗 ETF 구독자 연결: {client_id} ({client_ip})")
            return True
            
        except Exception as e:
            logger.error(f"❌ ETF 연결 실패: {e}")
            return False
    
    async def disconnect_etf(self, websocket: WebSocket):
        """ETF 구독자 연결 해제"""
        try:
            client_id = id(websocket)
            
            if websocket in self.etf_subscribers:
                self.etf_subscribers.remove(websocket)
            
            await self._cleanup_client(client_id, "ETF")
            
        except Exception as e:
            logger.error(f"❌ ETF 연결 해제 오류: {e}")
    
    async def broadcast_etf_update(self, data: List[dict]):
        """
        모든 ETF 구독자에게 업데이트 브로드캐스트
        
        Args:
            data: ETF 데이터 리스트
        """
        if not self.etf_subscribers:
            return
        
        try:
            # ETF 메시지 포맷 생성
            message = {
                "type": "etf",
                "data": data,
                "timestamp": datetime.now(pytz.UTC).isoformat()
            }
            
            successful_sends = await self._broadcast_to_subscribers(
                self.etf_subscribers, message, "ETF"
            )
            
            if successful_sends > 0:
                logger.debug(f"📤 ETF 업데이트 전송 완료: {successful_sends}명")
                
        except Exception as e:
            logger.error(f"❌ ETF 브로드캐스트 실패: {e}")
    
    # =========================
    # 공통 유틸리티 메서드들
    # =========================
    
    async def _broadcast_to_subscribers(self, subscribers: List[WebSocket], message: Any, context: str) -> int:
        """
        구독자 리스트에 메시지 브로드캐스트
        
        Args:
            subscribers: WebSocket 구독자 리스트
            message: 전송할 메시지 (dict 또는 Pydantic 모델)
            context: 로깅용 컨텍스트 정보
            
        Returns:
            int: 성공적으로 전송된 메시지 수
        """
        if not subscribers:
            return 0
        
        # 메시지 JSON 변환
        if isinstance(message, dict):
            message_json = json.dumps(message, default=str)
        elif hasattr(message, 'json'):
            message_json = message.json()
        elif hasattr(message, 'model_dump_json'):
            message_json = message.model_dump_json()
        else:
            message_json = json.dumps(str(message))
        
        disconnected_clients = []
        successful_sends = 0
        
        # 모든 구독자에게 메시지 전송
        for websocket in subscribers[:]:  # 복사본으로 순회 (안전)
            try:
                await websocket.send_text(message_json)
                successful_sends += 1
                
                # 클라이언트 메타데이터 업데이트
                client_id = id(websocket)
                if client_id in self.client_metadata:
                    self.client_metadata[client_id]["messages_received"] += 1
                    self.client_metadata[client_id]["last_heartbeat"] = datetime.now(pytz.UTC)
                
            except Exception as e:
                logger.warning(f"⚠️ {context} 메시지 전송 실패: {id(websocket)} - {e}")
                disconnected_clients.append(websocket)
                self.stats["total_errors"] += 1
        
        # 연결 끊어진 클라이언트들 정리
        for websocket in disconnected_clients:
            await self._remove_disconnected_client(websocket)
        
        # 통계 업데이트
        self.stats["total_messages_sent"] += successful_sends
        
        return successful_sends
    
    async def _remove_disconnected_client(self, websocket: WebSocket):
        """연결 끊어진 클라이언트 모든 리스트에서 제거"""
        try:
            # 모든 구독자 리스트에서 제거
            if websocket in self.crypto_subscribers:
                self.crypto_subscribers.remove(websocket)
            
            if websocket in self.sp500_subscribers:
                self.sp500_subscribers.remove(websocket)
            
            if websocket in self.etf_subscribers:
                self.etf_subscribers.remove(websocket)
            
            if websocket in self.dashboard_subscribers:
                self.dashboard_subscribers.remove(websocket)
            
            # 심볼별 구독자 리스트에서도 제거
            for subscription_key, subscriber_list in list(self.symbol_subscribers.items()):
                if websocket in subscriber_list:
                    subscriber_list.remove(websocket)
                    if not subscriber_list:  # 빈 리스트면 키 삭제
                        del self.symbol_subscribers[subscription_key]
            
            # 메타데이터 정리
            client_id = id(websocket)
            await self._cleanup_client(client_id, "연결 끊어진 클라이언트")
            
        except Exception as e:
            logger.error(f"❌ 끊어진 클라이언트 정리 실패: {e}")
    
    async def _cleanup_client(self, client_id: int, context: str):
        """클라이언트 메타데이터 정리"""
        try:
            # 활성 연결에서 제거
            self.active_connections.discard(client_id)
            
            # 메타데이터 정리
            if client_id in self.client_metadata:
                metadata = self.client_metadata.pop(client_id)
                connect_duration = datetime.now(pytz.UTC) - metadata["connected_at"]
                logger.info(f"🔌 {context} 구독자 해제: {client_id} (연결 시간: {connect_duration})")
            
            # 통계 업데이트
            self.stats["total_disconnections"] += 1
            
        except Exception as e:
            logger.error(f"❌ 클라이언트 정리 실패: {e}")
    
    # =========================
    # 상태 조회 및 통계
    # =========================
    
    def get_status(self) -> Dict[str, Any]:
        """WebSocket 매니저 상태 반환"""
        return {
            "total_connections": len(self.active_connections),
            "crypto_subscribers": len(self.crypto_subscribers),
            "sp500_subscribers": len(self.sp500_subscribers),
            "etf_subscribers": len(self.etf_subscribers),
            "dashboard_subscribers": len(self.dashboard_subscribers),
        }
    
    async def shutdown_all_connections(self):
        """모든 WebSocket 연결 종료"""
        try:
            logger.info("🛑 모든 WebSocket 연결 종료 시작")
            
            # 종료 메시지 생성
            shutdown_message = create_error_message(
                error_code="SERVER_SHUTDOWN",
                message="서버가 종료됩니다. 연결이 곧 끊어집니다."
            )
            
            # 모든 구독자 리스트 정리
            self.crypto_subscribers.clear()
            self.sp500_subscribers.clear()
            self.etf_subscribers.clear()
            self.dashboard_subscribers.clear()
            self.symbol_subscribers.clear()
            self.client_metadata.clear()
            self.active_connections.clear()
            
            logger.info("✅ 모든 WebSocket 연결 종료 완료")
            
        except Exception as e:
            logger.error(f"❌ WebSocket 연결 종료 실패: {e}")