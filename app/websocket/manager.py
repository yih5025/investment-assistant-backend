# app/websocket/manager.py
import asyncio
import json
import logging
from typing import List, Dict, Set, Optional, Any
from datetime import datetime
from fastapi import WebSocket

from app.schemas.websocket_schema import (
    TopGainersUpdateMessage, CryptoUpdateMessage, SP500UpdateMessage,
    SymbolUpdateMessage, DashboardUpdateMessage, ErrorMessage,
    WebSocketMessageType, create_error_message
)

logger = logging.getLogger(__name__)

class WebSocketManager:
    """
    WebSocket 연결 관리 클래스
    
    이 클래스는 모든 WebSocket 연결을 관리하고, 데이터 타입별로 
    클라이언트 그룹을 분리하여 메시지를 브로드캐스트합니다.
    
    **지원하는 구독 타입:**
    - TopGainers 전체 구독
    - 암호화폐 전체 구독  
    - SP500 전체 구독
    - 특정 심볼 구독 (데이터 타입별)
    - 대시보드 통합 구독
    """
    
    def __init__(self):
        """WebSocketManager 초기화"""
        
        # 전체 데이터 구독자들
        self.topgainers_subscribers: List[WebSocket] = []
        self.crypto_subscribers: List[WebSocket] = []
        self.sp500_subscribers: List[WebSocket] = []
        self.dashboard_subscribers: List[WebSocket] = []
        
        # 심볼별 구독자들 {data_type:symbol: [websocket1, websocket2, ...]}
        # 예: {"topgainers:AAPL": [ws1, ws2], "crypto:KRW-BTC": [ws3]}
        self.symbol_subscribers: Dict[str, List[WebSocket]] = {}
        
        # 클라이언트 메타데이터 {websocket_id: metadata}
        self.client_metadata: Dict[int, Dict[str, Any]] = {}
        
        # 통계 정보
        self.stats = {
            "total_connections": 0,
            "total_disconnections": 0,
            "total_messages_sent": 0,
            "total_errors": 0,
            "start_time": datetime.utcnow()
        }
        
        # 활성 연결들 추적
        self.active_connections: Set[int] = set()
        
        logger.info("✅ WebSocketManager 초기화 완료")
    
    # =========================
    # TopGainers 연결 관리
    # =========================
    
    async def connect_topgainers(self, websocket: WebSocket) -> bool:
        """
        TopGainers 전체 구독자로 연결
        
        Args:
            websocket: WebSocket 연결 객체
            
        Returns:
            bool: 연결 성공 여부
        """
        try:
            client_id = id(websocket)
            client_ip = websocket.client.host if websocket.client else "unknown"
            
            # 연결 리스트에 추가
            self.topgainers_subscribers.append(websocket)
            self.active_connections.add(client_id)
            
            # 클라이언트 메타데이터 저장
            self.client_metadata[client_id] = {
                "type": "topgainers",
                "subscription": "all",
                "ip": client_ip,
                "connected_at": datetime.utcnow(),
                "last_heartbeat": datetime.utcnow(),
                "messages_received": 0
            }
            
            # 통계 업데이트
            self.stats["total_connections"] += 1
            
            logger.info(f"🔗 TopGainers 구독자 연결: {client_id} ({client_ip})")
            logger.info(f"📊 현재 TopGainers 구독자 수: {len(self.topgainers_subscribers)}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ TopGainers 연결 실패: {e}")
            return False
    
    async def disconnect_topgainers(self, websocket: WebSocket):
        """TopGainers 구독자 연결 해제"""
        try:
            client_id = id(websocket)
            
            # 연결 리스트에서 제거
            if websocket in self.topgainers_subscribers:
                self.topgainers_subscribers.remove(websocket)
            
            await self._cleanup_client(client_id, "TopGainers")
            
        except Exception as e:
            logger.error(f"❌ TopGainers 연결 해제 오류: {e}")
    
    async def broadcast_topgainers_update(self, message: TopGainersUpdateMessage):
        """모든 TopGainers 구독자에게 업데이트 브로드캐스트"""
        if not self.topgainers_subscribers:
            return
        
        successful_sends = await self._broadcast_to_subscribers(
            self.topgainers_subscribers, message, "TopGainers"
        )
        
        if successful_sends > 0:
            logger.debug(f"📤 TopGainers 업데이트 전송 완료: {successful_sends}명")
    
    # =========================
    # 암호화폐 연결 관리
    # =========================
    
    async def connect_crypto(self, websocket: WebSocket) -> bool:
        """
        암호화폐 전체 구독자로 연결
        
        Args:
            websocket: WebSocket 연결 객체
            
        Returns:
            bool: 연결 성공 여부
        """
        try:
            client_id = id(websocket)
            client_ip = websocket.client.host if websocket.client else "unknown"
            
            # 연결 리스트에 추가
            self.crypto_subscribers.append(websocket)
            self.active_connections.add(client_id)
            
            # 클라이언트 메타데이터 저장
            self.client_metadata[client_id] = {
                "type": "crypto",
                "subscription": "all",
                "ip": client_ip,
                "connected_at": datetime.utcnow(),
                "last_heartbeat": datetime.utcnow(),
                "messages_received": 0
            }
            
            # 통계 업데이트
            self.stats["total_connections"] += 1
            
            logger.info(f"🔗 암호화폐 구독자 연결: {client_id} ({client_ip})")
            logger.info(f"📊 현재 암호화폐 구독자 수: {len(self.crypto_subscribers)}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ 암호화폐 연결 실패: {e}")
            return False
    
    async def disconnect_crypto(self, websocket: WebSocket):
        """암호화폐 구독자 연결 해제"""
        try:
            client_id = id(websocket)
            
            # 연결 리스트에서 제거
            if websocket in self.crypto_subscribers:
                self.crypto_subscribers.remove(websocket)
            
            await self._cleanup_client(client_id, "암호화폐")
            
        except Exception as e:
            logger.error(f"❌ 암호화폐 연결 해제 오류: {e}")
    
    async def broadcast_crypto_update(self, message: CryptoUpdateMessage):
        """모든 암호화폐 구독자에게 업데이트 브로드캐스트"""
        if not self.crypto_subscribers:
            return
        
        successful_sends = await self._broadcast_to_subscribers(
            self.crypto_subscribers, message, "암호화폐"
        )
        
        if successful_sends > 0:
            logger.debug(f"📤 암호화폐 업데이트 전송 완료: {successful_sends}명")
    
    # =========================
    # SP500 연결 관리
    # =========================
    
    async def connect_sp500(self, websocket: WebSocket) -> bool:
        """
        SP500 전체 구독자로 연결
        
        Args:
            websocket: WebSocket 연결 객체
            
        Returns:
            bool: 연결 성공 여부
        """
        try:
            client_id = id(websocket)
            client_ip = websocket.client.host if websocket.client else "unknown"
            
            # 연결 리스트에 추가
            self.sp500_subscribers.append(websocket)
            self.active_connections.add(client_id)
            
            # 클라이언트 메타데이터 저장
            self.client_metadata[client_id] = {
                "type": "sp500",
                "subscription": "all",
                "ip": client_ip,
                "connected_at": datetime.utcnow(),
                "last_heartbeat": datetime.utcnow(),
                "messages_received": 0
            }
            
            # 통계 업데이트
            self.stats["total_connections"] += 1
            
            logger.info(f"🔗 SP500 구독자 연결: {client_id} ({client_ip})")
            logger.info(f"📊 현재 SP500 구독자 수: {len(self.sp500_subscribers)}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ SP500 연결 실패: {e}")
            return False
    
    async def disconnect_sp500(self, websocket: WebSocket):
        """SP500 구독자 연결 해제"""
        try:
            client_id = id(websocket)
            
            # 연결 리스트에서 제거
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
    # 대시보드 연결 관리
    # =========================
    
    async def connect_dashboard(self, websocket: WebSocket) -> bool:
        """
        대시보드 구독자로 연결
        
        Args:
            websocket: WebSocket 연결 객체
            
        Returns:
            bool: 연결 성공 여부
        """
        try:
            client_id = id(websocket)
            client_ip = websocket.client.host if websocket.client else "unknown"
            
            # 연결 리스트에 추가
            self.dashboard_subscribers.append(websocket)
            self.active_connections.add(client_id)
            
            # 클라이언트 메타데이터 저장
            self.client_metadata[client_id] = {
                "type": "dashboard",
                "subscription": "integrated",
                "ip": client_ip,
                "connected_at": datetime.utcnow(),
                "last_heartbeat": datetime.utcnow(),
                "messages_received": 0
            }
            
            # 통계 업데이트
            self.stats["total_connections"] += 1
            
            logger.info(f"🔗 대시보드 구독자 연결: {client_id} ({client_ip})")
            logger.info(f"📊 현재 대시보드 구독자 수: {len(self.dashboard_subscribers)}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ 대시보드 연결 실패: {e}")
            return False
    
    async def disconnect_dashboard(self, websocket: WebSocket):
        """대시보드 구독자 연결 해제"""
        try:
            client_id = id(websocket)
            
            # 연결 리스트에서 제거
            if websocket in self.dashboard_subscribers:
                self.dashboard_subscribers.remove(websocket)
            
            await self._cleanup_client(client_id, "대시보드")
            
        except Exception as e:
            logger.error(f"❌ 대시보드 연결 해제 오류: {e}")
    
    async def broadcast_dashboard_update(self, message: DashboardUpdateMessage):
        """모든 대시보드 구독자에게 업데이트 브로드캐스트"""
        if not self.dashboard_subscribers:
            return
        
        successful_sends = await self._broadcast_to_subscribers(
            self.dashboard_subscribers, message, "대시보드"
        )
        
        if successful_sends > 0:
            logger.debug(f"📤 대시보드 업데이트 전송 완료: {successful_sends}명")
    
    # =========================
    # 심볼별 구독 관리
    # =========================
    
    async def connect_symbol_subscriber(self, websocket: WebSocket, symbol: str, data_type: str) -> bool:
        """
        특정 심볼 구독자로 연결
        
        Args:
            websocket: WebSocket 연결 객체
            symbol: 구독할 심볼
            data_type: 데이터 타입 (topgainers, crypto, sp500)
            
        Returns:
            bool: 연결 성공 여부
        """
        try:
            client_id = id(websocket)
            client_ip = websocket.client.host if websocket.client else "unknown"
            symbol = symbol.upper()
            
            # 구독 키 생성 (data_type:symbol)
            subscription_key = f"{data_type}:{symbol}"
            
            # 심볼별 구독자 리스트 초기화 (필요한 경우)
            if subscription_key not in self.symbol_subscribers:
                self.symbol_subscribers[subscription_key] = []
            
            # 구독자 리스트에 추가
            self.symbol_subscribers[subscription_key].append(websocket)
            self.active_connections.add(client_id)
            
            # 클라이언트 메타데이터 저장
            self.client_metadata[client_id] = {
                "type": "symbol",
                "data_type": data_type,
                "symbol": symbol,
                "subscription_key": subscription_key,
                "ip": client_ip,
                "connected_at": datetime.utcnow(),
                "last_heartbeat": datetime.utcnow(),
                "messages_received": 0
            }
            
            # 통계 업데이트
            self.stats["total_connections"] += 1
            
            logger.info(f"🔗 심볼 구독자 연결: {client_id} ({client_ip}) - {subscription_key}")
            logger.info(f"📊 {subscription_key} 구독자 수: {len(self.symbol_subscribers[subscription_key])}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ 심볼 구독자 연결 실패: {e}")
            return False
    
    async def disconnect_symbol_subscriber(self, websocket: WebSocket, symbol: str, data_type: str):
        """
        특정 심볼 구독자 연결 해제
        
        Args:
            websocket: WebSocket 연결 객체
            symbol: 구독 중인 심볼
            data_type: 데이터 타입
        """
        try:
            client_id = id(websocket)
            symbol = symbol.upper()
            subscription_key = f"{data_type}:{symbol}"
            
            # 심볼별 구독자 리스트에서 제거
            if subscription_key in self.symbol_subscribers and websocket in self.symbol_subscribers[subscription_key]:
                self.symbol_subscribers[subscription_key].remove(websocket)
                
                # 구독자가 없으면 심볼 키 자체를 제거
                if not self.symbol_subscribers[subscription_key]:
                    del self.symbol_subscribers[subscription_key]
                    logger.info(f"🧹 심볼 {subscription_key} 구독자 리스트 정리 완료")
            
            await self._cleanup_client(client_id, f"심볼 {subscription_key}")
            
            remaining_count = len(self.symbol_subscribers.get(subscription_key, []))
            logger.info(f"📊 {subscription_key} 남은 구독자 수: {remaining_count}")
            
        except Exception as e:
            logger.error(f"❌ 심볼 구독자 연결 해제 오류: {e}")
    
    async def broadcast_symbol_update(self, symbol: str, data_type: str, message: SymbolUpdateMessage):
        """
        특정 심볼 구독자들에게 업데이트 브로드캐스트
        
        Args:
            symbol: 업데이트할 심볼
            data_type: 데이터 타입
            message: 전송할 SymbolUpdateMessage
        """
        symbol = symbol.upper()
        subscription_key = f"{data_type}:{symbol}"
        
        if subscription_key not in self.symbol_subscribers or not self.symbol_subscribers[subscription_key]:
            return
        
        successful_sends = await self._broadcast_to_subscribers(
            self.symbol_subscribers[subscription_key], message, f"심볼 {subscription_key}"
        )
        
        if successful_sends > 0:
            logger.debug(f"📤 심볼 {subscription_key} 업데이트 전송 완료: {successful_sends}명")
    
    # =========================
    # 공통 유틸리티 메서드들
    # =========================
    
    async def _broadcast_to_subscribers(self, subscribers: List[WebSocket], message: Any, context: str) -> int:
        """
        구독자 리스트에 메시지 브로드캐스트
        
        Args:
            subscribers: WebSocket 구독자 리스트
            message: 전송할 메시지
            context: 로깅용 컨텍스트 정보
            
        Returns:
            int: 성공적으로 전송된 메시지 수
        """
        if not subscribers:
            return 0
        
        message_json = message.json()
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
                    self.client_metadata[client_id]["last_heartbeat"] = datetime.utcnow()
                
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
            if websocket in self.topgainers_subscribers:
                self.topgainers_subscribers.remove(websocket)
            
            if websocket in self.crypto_subscribers:
                self.crypto_subscribers.remove(websocket)
            
            if websocket in self.sp500_subscribers:
                self.sp500_subscribers.remove(websocket)
            
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
                connect_duration = datetime.utcnow() - metadata["connected_at"]
                logger.info(f"🔌 {context} 구독자 해제: {client_id} (연결 시간: {connect_duration})")
            
            # 통계 업데이트
            self.stats["total_disconnections"] += 1
            
        except Exception as e:
            logger.error(f"❌ 클라이언트 정리 실패: {e}")
    
    async def broadcast_error(self, error_message: ErrorMessage, target_clients: Optional[List[WebSocket]] = None):
        """
        에러 메시지 브로드캐스트
        
        Args:
            error_message: 전송할 ErrorMessage
            target_clients: 특정 클라이언트들 (None이면 모든 클라이언트)
        """
        if target_clients is None:
            # 모든 클라이언트에게 전송
            all_clients = []
            all_clients.extend(self.topgainers_subscribers)
            all_clients.extend(self.crypto_subscribers)
            all_clients.extend(self.sp500_subscribers)
            all_clients.extend(self.dashboard_subscribers)
            
            for symbol_clients in self.symbol_subscribers.values():
                all_clients.extend(symbol_clients)
            
            target_clients = list(set(all_clients))  # 중복 제거
        
        message_json = error_message.json()
        
        for websocket in target_clients:
            try:
                await websocket.send_text(message_json)
            except Exception as e:
                logger.warning(f"⚠️ 에러 메시지 전송 실패: {id(websocket)} - {e}")
    
    # =========================
    # 상태 조회 및 통계
    # =========================
    
    def get_status(self) -> Dict[str, Any]:
        """
        WebSocket 매니저 상태 반환
        
        Returns:
            Dict[str, Any]: 상태 정보
        """
        total_symbol_subscribers = sum(len(clients) for clients in self.symbol_subscribers.values())
        
        return {
            "total_connections": len(self.active_connections),
            "topgainers_subscribers": len(self.topgainers_subscribers),
            "crypto_subscribers": len(self.crypto_subscribers),
            "sp500_subscribers": len(self.sp500_subscribers),
            "dashboard_subscribers": len(self.dashboard_subscribers),
            "symbol_subscribers": total_symbol_subscribers,
            "unique_symbols": len(self.symbol_subscribers),
            "active_symbols": list(self.symbol_subscribers.keys())
        }
    
    def get_detailed_stats(self) -> Dict[str, Any]:
        """
        상세 통계 정보 반환
        
        Returns:
            Dict[str, Any]: 상세 통계
        """
        uptime = datetime.utcnow() - self.stats["start_time"]
        
        return {
            **self.stats,
            "uptime_seconds": uptime.total_seconds(),
            "uptime_str": str(uptime),
            "subscription_breakdown": {
                "topgainers": len(self.topgainers_subscribers),
                "crypto": len(self.crypto_subscribers),
                "sp500": len(self.sp500_subscribers),
                "dashboard": len(self.dashboard_subscribers),
                "symbols": {
                    symbol: len(clients) 
                    for symbol, clients in self.symbol_subscribers.items()
                }
            },
            "error_rate": (
                self.stats["total_errors"] / max(self.stats["total_messages_sent"], 1) * 100
            )
        }
    
    def get_client_info(self, client_id: int) -> Optional[Dict[str, Any]]:
        """
        특정 클라이언트 정보 조회
        
        Args:
            client_id: 클라이언트 ID
            
        Returns:
            Optional[Dict[str, Any]]: 클라이언트 정보
        """
        return self.client_metadata.get(client_id)
    
    def get_subscription_summary(self) -> Dict[str, Any]:
        """
        구독 현황 요약 반환
        
        Returns:
            Dict[str, Any]: 구독 현황
        """
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "total_active_connections": len(self.active_connections),
            "subscriptions": {
                "all_data": {
                    "topgainers": len(self.topgainers_subscribers),
                    "crypto": len(self.crypto_subscribers),
                    "sp500": len(self.sp500_subscribers),
                    "dashboard": len(self.dashboard_subscribers)
                },
                "symbol_specific": {
                    "total_symbols": len(self.symbol_subscribers),
                    "total_subscribers": sum(len(clients) for clients in self.symbol_subscribers.values()),
                    "by_data_type": self._get_symbol_breakdown_by_type()
                }
            }
        }
    
    def _get_symbol_breakdown_by_type(self) -> Dict[str, Dict[str, int]]:
        """데이터 타입별 심볼 구독 현황 분석"""
        breakdown = {"topgainers": {}, "crypto": {}, "sp500": {}}
        
        for subscription_key, clients in self.symbol_subscribers.items():
            if ":" in subscription_key:
                data_type, symbol = subscription_key.split(":", 1)
                if data_type in breakdown:
                    breakdown[data_type][symbol] = len(clients)
        
        return breakdown
    
    async def cleanup_inactive_connections(self):
        """
        비활성 연결들 정리 (주기적으로 실행)
        """
        current_time = datetime.utcnow()
        inactive_threshold = 300  # 5분
        
        inactive_clients = []
        
        for client_id, metadata in self.client_metadata.items():
            last_heartbeat = metadata.get("last_heartbeat")
            if last_heartbeat and (current_time - last_heartbeat).total_seconds() > inactive_threshold:
                inactive_clients.append(client_id)
        
        if inactive_clients:
            logger.info(f"🧹 비활성 연결 정리: {len(inactive_clients)}개")
            
            # 실제 정리는 각 연결의 disconnect 메서드에서 처리됨
            # 여기서는 로깅 및 통계만 수행
    
    async def shutdown_all_connections(self):
        """모든 WebSocket 연결 종료"""
        try:
            logger.info("🛑 모든 WebSocket 연결 종료 시작")
            
            # 종료 메시지 생성
            shutdown_message = create_error_message(
                error_code="SERVER_SHUTDOWN",
                message="서버가 종료됩니다. 연결이 곧 끊어집니다."
            )
            
            # 모든 클라이언트에게 종료 알림
            await self.broadcast_error(shutdown_message)
            
            # 짧은 대기 (메시지 전송 완료)
            await asyncio.sleep(1)
            
            # 통계 정리
            total_connections = len(self.active_connections)
            
            # 연결 리스트 정리
            self.topgainers_subscribers.clear()
            self.crypto_subscribers.clear()
            self.sp500_subscribers.clear()
            self.dashboard_subscribers.clear()
            self.symbol_subscribers.clear()
            self.client_metadata.clear()
            self.active_connections.clear()
            
            logger.info(f"✅ 모든 WebSocket 연결 종료 완료: {total_connections}개")
            
        except Exception as e:
            logger.error(f"❌ WebSocket 연결 종료 실패: {e}")