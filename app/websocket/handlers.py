# app/websocket/handlers.py
import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime
from fastapi import WebSocket

from app.schemas.websocket_schema import (
    WebSocketMessageType, SubscriptionType,
    create_error_message, StatusMessage
)

logger = logging.getLogger(__name__)

class WebSocketHandlers:
    """
    WebSocket 메시지 처리 핸들러 클래스
    
    이 클래스는 클라이언트로부터 받은 WebSocket 메시지를 처리하고,
    적절한 응답을 생성하는 역할을 담당합니다.
    """
    
    def __init__(self, websocket_manager=None):
        """
        WebSocketHandlers 초기화
        
        Args:
            websocket_manager: WebSocketManager 인스턴스
        """
        self.websocket_manager = websocket_manager
        self.message_handlers = {
            WebSocketMessageType.SUBSCRIBE: self.handle_subscribe,
            WebSocketMessageType.UNSUBSCRIBE: self.handle_unsubscribe,
            WebSocketMessageType.HEARTBEAT: self.handle_heartbeat,
            WebSocketMessageType.STATUS: self.handle_status_request
        }
        
        logger.info("✅ WebSocketHandlers 초기화 완료")
    
    async def handle_client_message(self, websocket: WebSocket, message_data: str) -> bool:
        """
        클라이언트 메시지 처리 메인 함수
        
        Args:
            websocket: WebSocket 연결 객체
            message_data: 클라이언트로부터 받은 JSON 문자열
            
        Returns:
            bool: 처리 성공 여부
        """
        client_id = id(websocket)
        
        try:
            # JSON 파싱
            raw_message = json.loads(message_data)
            
            # 메시지 타입 확인
            message_type = raw_message.get("type")
            if not message_type:
                await self.send_error(websocket, "INVALID_MESSAGE", "메시지 타입이 없습니다")
                return False
            
            # 메시지 타입을 enum으로 변환
            try:
                msg_type_enum = WebSocketMessageType(message_type)
            except ValueError:
                await self.send_error(websocket, "UNKNOWN_MESSAGE_TYPE", f"알 수 없는 메시지 타입: {message_type}")
                return False
            
            # 메시지 처리
            if msg_type_enum in self.message_handlers:
                handler = self.message_handlers[msg_type_enum]
                return await handler(websocket, raw_message)
            else:
                await self.send_error(websocket, "UNSUPPORTED_MESSAGE", f"지원하지 않는 메시지 타입: {message_type}")
                return False
                
        except json.JSONDecodeError as e:
            logger.warning(f"⚠️ JSON 파싱 오류: {client_id} - {e}")
            await self.send_error(websocket, "INVALID_JSON", "유효하지 않은 JSON 형식입니다")
            return False
            
        except Exception as e:
            logger.error(f"❌ 메시지 처리 오류: {client_id} - {e}")
            await self.send_error(websocket, "INTERNAL_ERROR", f"서버 내부 오류: {str(e)}")
            return False
    
    async def handle_subscribe(self, websocket: WebSocket, message: Dict[str, Any]) -> bool:
        """
        구독 요청 처리
        
        Args:
            websocket: WebSocket 연결 객체
            message: 구독 메시지
            
        Returns:
            bool: 처리 성공 여부
        """
        try:
            subscription_type = message.get("subscription_type")
            symbol = message.get("symbol")
            
            if not subscription_type:
                await self.send_error(websocket, "MISSING_SUBSCRIPTION_TYPE", "구독 타입이 필요합니다")
                return False
            
            # 구독 타입별 처리
            if subscription_type == SubscriptionType.ALL_TOPGAINERS:
                success = await self.websocket_manager.connect_topgainers(websocket)
                if success:
                    await self.send_subscribe_success(websocket, "all_topgainers", None)
                    return True
                else:
                    await self.send_error(websocket, "SUBSCRIPTION_FAILED", "TopGainers 구독 실패")
                    return False
                    
            elif subscription_type == SubscriptionType.SINGLE_SYMBOL:
                if not symbol:
                    await self.send_error(websocket, "MISSING_SYMBOL", "심볼이 필요합니다")
                    return False
                
                symbol = symbol.upper().strip()
                success = await self.websocket_manager.connect_symbol_subscriber(websocket, symbol)
                if success:
                    await self.send_subscribe_success(websocket, "single_symbol", symbol)
                    return True
                else:
                    await self.send_error(websocket, "SUBSCRIPTION_FAILED", f"심볼 {symbol} 구독 실패")
                    return False
            
            else:
                await self.send_error(websocket, "INVALID_SUBSCRIPTION_TYPE", f"지원하지 않는 구독 타입: {subscription_type}")
                return False
                
        except Exception as e:
            logger.error(f"❌ 구독 처리 오류: {e}")
            await self.send_error(websocket, "SUBSCRIPTION_ERROR", f"구독 처리 중 오류: {str(e)}")
            return False
    
    async def handle_unsubscribe(self, websocket: WebSocket, message: Dict[str, Any]) -> bool:
        """
        구독 해제 요청 처리
        
        Args:
            websocket: WebSocket 연결 객체
            message: 구독 해제 메시지
            
        Returns:
            bool: 처리 성공 여부
        """
        try:
            subscription_type = message.get("subscription_type")
            symbol = message.get("symbol")
            
            if subscription_type == SubscriptionType.ALL_TOPGAINERS:
                await self.websocket_manager.disconnect_topgainers(websocket)
                await self.send_unsubscribe_success(websocket, "all_topgainers", None)
                return True
                
            elif subscription_type == SubscriptionType.SINGLE_SYMBOL:
                if not symbol:
                    await self.send_error(websocket, "MISSING_SYMBOL", "심볼이 필요합니다")
                    return False
                
                symbol = symbol.upper().strip()
                await self.websocket_manager.disconnect_symbol_subscriber(websocket, symbol)
                await self.send_unsubscribe_success(websocket, "single_symbol", symbol)
                return True
            
            else:
                await self.send_error(websocket, "INVALID_SUBSCRIPTION_TYPE", f"지원하지 않는 구독 타입: {subscription_type}")
                return False
                
        except Exception as e:
            logger.error(f"❌ 구독 해제 처리 오류: {e}")
            await self.send_error(websocket, "UNSUBSCRIBE_ERROR", f"구독 해제 처리 중 오류: {str(e)}")
            return False
    
    async def handle_heartbeat(self, websocket: WebSocket, message: Dict[str, Any]) -> bool:
        """
        하트비트 요청 처리
        
        Args:
            websocket: WebSocket 연결 객체
            message: 하트비트 메시지
            
        Returns:
            bool: 처리 성공 여부
        """
        try:
            # 하트비트 응답
            heartbeat_response = {
                "type": WebSocketMessageType.HEARTBEAT,
                "timestamp": datetime.utcnow().isoformat(),
                "server_time": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
                "status": "alive"
            }
            
            await websocket.send_text(json.dumps(heartbeat_response))
            
            # 클라이언트 메타데이터 업데이트 (last_heartbeat)
            client_id = id(websocket)
            if self.websocket_manager and client_id in self.websocket_manager.client_metadata:
                self.websocket_manager.client_metadata[client_id]["last_heartbeat"] = datetime.utcnow()
            
            logger.debug(f"💓 하트비트 응답: {client_id}")
            return True
            
        except Exception as e:
            logger.error(f"❌ 하트비트 처리 오류: {e}")
            return False
    
    async def handle_status_request(self, websocket: WebSocket, message: Dict[str, Any]) -> bool:
        """
        상태 요청 처리
        
        Args:
            websocket: WebSocket 연결 객체
            message: 상태 요청 메시지
            
        Returns:
            bool: 처리 성공 여부
        """
        try:
            if not self.websocket_manager:
                await self.send_error(websocket, "SERVICE_UNAVAILABLE", "WebSocket 매니저를 사용할 수 없습니다")
                return False
            
            # 현재 상태 정보 생성
            status_info = self.websocket_manager.get_status()
            
            client_id = id(websocket)
            client_metadata = self.websocket_manager.client_metadata.get(client_id, {})
            
            status_response = StatusMessage(
                status="connected",
                connected_clients=status_info["total_connections"],
                subscription_info={
                    "client_type": client_metadata.get("type", "unknown"),
                    "symbol": client_metadata.get("symbol"),
                    "connected_at": client_metadata.get("connected_at").isoformat() if client_metadata.get("connected_at") else None,
                    "messages_received": client_metadata.get("messages_received", 0),
                    "server_stats": status_info
                }
            )
            
            await websocket.send_text(status_response.json())
            logger.debug(f"📊 상태 정보 전송: {client_id}")
            return True
            
        except Exception as e:
            logger.error(f"❌ 상태 요청 처리 오류: {e}")
            await self.send_error(websocket, "STATUS_ERROR", f"상태 정보 조회 중 오류: {str(e)}")
            return False
    
    async def send_error(self, websocket: WebSocket, error_code: str, message: str, details: Dict[str, Any] = None):
        """
        에러 메시지 전송
        
        Args:
            websocket: WebSocket 연결 객체
            error_code: 에러 코드
            message: 에러 메시지
            details: 추가 상세 정보
        """
        try:
            error_msg = create_error_message(error_code, message, details)
            await websocket.send_text(error_msg.json())
            
            client_id = id(websocket)
            logger.warning(f"⚠️ 에러 메시지 전송: {client_id} - {error_code}: {message}")
            
        except Exception as e:
            logger.error(f"❌ 에러 메시지 전송 실패: {e}")
    
    async def send_subscribe_success(self, websocket: WebSocket, subscription_type: str, symbol: Optional[str]):
        """
        구독 성공 메시지 전송
        
        Args:
            websocket: WebSocket 연결 객체
            subscription_type: 구독 타입
            symbol: 심볼 (있는 경우)
        """
        try:
            success_message = {
                "type": WebSocketMessageType.STATUS,
                "timestamp": datetime.utcnow().isoformat(),
                "status": "subscribed",
                "subscription_type": subscription_type,
                "symbol": symbol,
                "message": f"구독 성공: {subscription_type}" + (f" ({symbol})" if symbol else "")
            }
            
            await websocket.send_text(json.dumps(success_message))
            
            client_id = id(websocket)
            logger.info(f"✅ 구독 성공 알림 전송: {client_id} - {subscription_type}" + (f" ({symbol})" if symbol else ""))
            
        except Exception as e:
            logger.error(f"❌ 구독 성공 메시지 전송 실패: {e}")
    
    async def send_unsubscribe_success(self, websocket: WebSocket, subscription_type: str, symbol: Optional[str]):
        """
        구독 해제 성공 메시지 전송
        
        Args:
            websocket: WebSocket 연결 객체
            subscription_type: 구독 타입
            symbol: 심볼 (있는 경우)
        """
        try:
            success_message = {
                "type": WebSocketMessageType.STATUS,
                "timestamp": datetime.utcnow().isoformat(),
                "status": "unsubscribed",
                "subscription_type": subscription_type,
                "symbol": symbol,
                "message": f"구독 해제 성공: {subscription_type}" + (f" ({symbol})" if symbol else "")
            }
            
            await websocket.send_text(json.dumps(success_message))
            
            client_id = id(websocket)
            logger.info(f"✅ 구독 해제 성공 알림 전송: {client_id} - {subscription_type}" + (f" ({symbol})" if symbol else ""))
            
        except Exception as e:
            logger.error(f"❌ 구독 해제 성공 메시지 전송 실패: {e}")
    
    async def send_notification(self, websocket: WebSocket, notification_type: str, message: str, data: Dict[str, Any] = None):
        """
        일반 알림 메시지 전송
        
        Args:
            websocket: WebSocket 연결 객체
            notification_type: 알림 타입
            message: 알림 메시지
            data: 추가 데이터
        """
        try:
            notification = {
                "type": "notification",
                "timestamp": datetime.utcnow().isoformat(),
                "notification_type": notification_type,
                "message": message,
                "data": data or {}
            }
            
            await websocket.send_text(json.dumps(notification))
            
            client_id = id(websocket)
            logger.debug(f"📢 알림 전송: {client_id} - {notification_type}: {message}")
            
        except Exception as e:
            logger.error(f"❌ 알림 전송 실패: {e}")
    
    def validate_message_format(self, message: Dict[str, Any]) -> tuple[bool, str]:
        """
        메시지 형식 유효성 검사
        
        Args:
            message: 검사할 메시지
            
        Returns:
            tuple[bool, str]: (유효성, 오류 메시지)
        """
        # 필수 필드 확인
        if "type" not in message:
            return False, "메시지 타입이 필요합니다"
        
        # 타입별 추가 검증
        msg_type = message.get("type")
        
        if msg_type == WebSocketMessageType.SUBSCRIBE:
            if "subscription_type" not in message:
                return False, "구독 타입이 필요합니다"
            
            subscription_type = message.get("subscription_type")
            if subscription_type == SubscriptionType.SINGLE_SYMBOL and not message.get("symbol"):
                return False, "단일 심볼 구독에는 심볼이 필요합니다"
        
        elif msg_type == WebSocketMessageType.UNSUBSCRIBE:
            if "subscription_type" not in message:
                return False, "구독 해제할 타입이 필요합니다"
        
        return True, ""
    
    def get_handler_stats(self) -> Dict[str, Any]:
        """
        핸들러 통계 정보 반환
        
        Returns:
            Dict[str, Any]: 핸들러 통계
        """
        return {
            "supported_message_types": list(self.message_handlers.keys()),
            "handler_count": len(self.message_handlers),
            "websocket_manager_available": self.websocket_manager is not None
        }