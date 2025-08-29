# app/api/endpoints/crypto_websocket_endpoint.py
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Query, Path
import asyncio
import json
import logging
from datetime import datetime, timedelta
import pytz
from typing import Optional
import hashlib

from app.websocket.manager import WebSocketManager
from app.websocket.redis_streamer import RedisStreamer
from app.services.crypto_service import CryptoService
from app.schemas.crypto_schema import (
    CryptoData, CryptoChangeType, CryptoExchange, 
    CryptoUpdateMessage, CryptoStatusMessage, CryptoErrorMessage,
    create_crypto_update_message, create_crypto_error_message, 
    db_to_crypto_data, validate_crypto_market, 
    extract_crypto_symbol, format_crypto_price
)

# 로거 설정
logger = logging.getLogger(__name__)

# 라우터 생성 - 기존 경로와 동일하게 수정
router = APIRouter(prefix="/ws", tags=["Crypto WebSocket"])

# 서비스 인스턴스들
websocket_manager = WebSocketManager()
crypto_service = CryptoService()
redis_streamer = None  # 첫 연결 시 초기화

# =========================
# Crypto WebSocket 메시지 스키마 (내장)
# =========================

class CryptoWebSocketMessage:
    """Crypto WebSocket 메시지 생성 헬퍼"""
    
    @staticmethod
    def create_update_message(data, timestamp=None):
        """업데이트 메시지 생성"""
        return {
            "type": "crypto_update",
            "data": data,
            "timestamp": timestamp or datetime.now(pytz.UTC).isoformat(),
            "data_count": len(data),
            "exchange": "bithumb",
            "features": ["real_time_prices", "24h_trading"]
        }
    
    @staticmethod
    def create_status_message(status, **kwargs):
        """상태 메시지 생성"""
        return {
            "type": "crypto_status",
            "status": status,
            "timestamp": datetime.now(pytz.UTC).isoformat(),
            **kwargs
        }
    
    @staticmethod
    def create_error_message(error_code, message, symbol=None):
        """에러 메시지 생성"""
        return {
            "type": "crypto_error",
            "error_code": error_code,
            "message": message,
            "symbol": symbol,
            "timestamp": datetime.now(pytz.UTC).isoformat()
        }

async def initialize_crypto_websocket_services():
    """Crypto WebSocket 서비스 초기화"""
    global redis_streamer
    
    logger.info("Crypto WebSocket 서비스 초기화 시작...")
    
    # CryptoService Redis 연결 초기화
    redis_connected = await crypto_service.init_redis() if hasattr(crypto_service, 'init_redis') else False
    if redis_connected:
        logger.info("Crypto Service Redis 연결 성공")
    else:
        logger.warning("Crypto Service Redis 연결 실패 (DB로 fallback)")
    
    # RedisStreamer 초기화 (Crypto만 사용하므로 다른 서비스는 None)
    redis_streamer = RedisStreamer(
        topgainers_service=None,
        crypto_service=crypto_service,
        sp500_service=None
    )
    await redis_streamer.initialize()
    redis_streamer.set_websocket_manager(websocket_manager)
    
    logger.info("Crypto WebSocket 서비스 초기화 완료")

# =========================
# 암호화폐 WebSocket 엔드포인트
# =========================

@router.websocket("/crypto")
async def websocket_crypto_all(websocket: WebSocket):
    """
    모든 암호화폐 실시간 데이터 WebSocket
    
    주요 기능:
    - 빗썸 거래소의 모든 암호화폐 실시간 시세를 500ms마다 전송
    - 암호화폐는 24시간 거래이므로 별도 변화율 계산 없이 기존 로직 유지
    - Redis를 통한 실시간 데이터 스트리밍
    - 변경된 데이터만 전송으로 효율성 향상
    
    데이터 형태:
    {
        "type": "crypto_update",
        "data": [
            {
                "market_code": "KRW-BTC",
                "symbol": "BTC",
                "korean_name": "비트코인",
                "english_name": "Bitcoin",
                "price": 50000000,
                "change_24h": 1250000,
                "change_rate_24h": "2.56%",
                "volume": 125000000,
                "acc_trade_value_24h": 6250000000000
            }
        ],
        "timestamp": "2025-08-28T10:30:00Z",
        "data_count": len(data),
        "exchange": "bithumb",
        "features": ["real_time_prices", "24h_trading"]
    }
    """
    await websocket.accept()
    client_id = id(websocket)
    client_ip = websocket.client.host if websocket.client else "unknown"
    
    logger.info(f"Crypto WebSocket 연결: {client_id} ({client_ip})")
    
    try:
        # WebSocket 매니저에 클라이언트 등록
        await websocket_manager.connect_crypto(websocket)
        
        # 연결 성공 메시지 전송
        status_msg = CryptoWebSocketMessage.create_status_message(
            status="connected",
            connected_clients=len(websocket_manager.crypto_subscribers),
            data_source="redis+database",
            exchange="bithumb",
            update_interval=500
        )
        await websocket.send_text(json.dumps(status_msg))
        
        # Crypto 스트리밍 시작
        asyncio.create_task(_start_crypto_streaming(websocket, client_id))
        
        # 클라이언트 메시지 대기
        while True:
            try:
                data = await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
                logger.debug(f"Crypto 클라이언트 메시지 수신: {client_id} - {data}")
                
            except asyncio.TimeoutError:
                # 하트비트 전송
                heartbeat = {
                    "type": "heartbeat",
                    "timestamp": datetime.now(pytz.UTC).isoformat(),
                    "server_time": datetime.now(pytz.UTC).strftime("%Y-%m-%d %H:%M:%S UTC"),
                    "data_type": "crypto",
                    "connected_clients": len(websocket_manager.crypto_subscribers)
                }
                await websocket.send_text(json.dumps(heartbeat))
                
    except WebSocketDisconnect:
        logger.info(f"Crypto WebSocket 연결 해제: {client_id}")
        
    except Exception as e:
        logger.error(f"Crypto WebSocket 오류: {client_id} - {e}")
        
        try:
            error_msg = CryptoWebSocketMessage.create_error_message(
                error_code="CRYPTO_ERROR",
                message=f"암호화폐 데이터 처리 오류: {str(e)}"
            )
            await websocket.send_text(json.dumps(error_msg))
        except:
            pass
            
    finally:
        await websocket_manager.disconnect_crypto(websocket)
        logger.info(f"Crypto WebSocket 정리 완료: {client_id}")

@router.websocket("/crypto/{symbol}")
async def websocket_crypto_symbol(websocket: WebSocket, symbol: str = Path(..., max_length=15)):
    """
    특정 암호화폐 실시간 데이터 WebSocket
    
    Args:
        symbol: 암호화폐 심볼 (예: KRW-BTC, KRW-ETH, BTC)
    """
    # 심볼 유효성 검사
    symbol = symbol.upper().strip()
    if not symbol or len(symbol) > 15:
        await websocket.close(code=1008, reason="Invalid crypto symbol")
        return
    
    await websocket.accept()
    client_id = id(websocket)
    client_ip = websocket.client.host if websocket.client else "unknown"
    
    logger.info(f"Crypto Symbol WebSocket 연결: {client_id} ({client_ip}) - {symbol}")
    
    try:
        # WebSocket 매니저에 클라이언트 등록
        await websocket_manager.connect_symbol_subscriber(websocket, symbol, "crypto")
        
        # 연결 성공 메시지 전송
        status_msg = CryptoWebSocketMessage.create_status_message(
            status="connected",
            connected_clients=len(websocket_manager.symbol_subscribers.get(f"crypto:{symbol}", [])),
            symbol=symbol,
            data_source="redis+database",
            exchange="bithumb"
        )
        await websocket.send_text(json.dumps(status_msg))
        
        # 개별 심볼 스트리밍 시작
        asyncio.create_task(_start_crypto_symbol_streaming(websocket, client_id, symbol))
        
        # 클라이언트 메시지 대기
        while True:
            try:
                data = await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
                logger.debug(f"Crypto Symbol 클라이언트 메시지: {client_id} ({symbol}) - {data}")
                
            except asyncio.TimeoutError:
                heartbeat = {
                    "type": "heartbeat",
                    "timestamp": datetime.now(pytz.UTC).isoformat(),
                    "symbol": symbol,
                    "data_type": "crypto"
                }
                await websocket.send_text(json.dumps(heartbeat))
                
    except WebSocketDisconnect:
        logger.info(f"Crypto Symbol WebSocket 연결 해제: {client_id} ({symbol})")
        
    except Exception as e:
        logger.error(f"Crypto Symbol WebSocket 오류: {client_id} ({symbol}) - {e}")
        
        try:
            error_msg = CryptoWebSocketMessage.create_error_message(
                error_code="CRYPTO_SYMBOL_ERROR",
                message=f"암호화폐 '{symbol}' 처리 오류: {str(e)}",
                symbol=symbol
            )
            await websocket.send_text(json.dumps(error_msg))
        except:
            pass
            
    finally:
        await websocket_manager.disconnect_symbol_subscriber(websocket, symbol, "crypto")
        logger.info(f"Crypto Symbol WebSocket 정리 완료: {client_id} ({symbol})")

# =========================
# Crypto 스트리밍 함수들
# =========================

async def _start_crypto_streaming(websocket: WebSocket, client_id: int):
    """암호화폐 전체 데이터 스트리밍"""
    last_data_hash = None
    error_count = 0
    max_errors = 5
    
    logger.info(f"Crypto 전체 스트리밍 시작: {client_id}")
    
    while True:
        try:
            # CryptoService를 통한 데이터 조회 (한국명, 영어명 포함)
            data = await crypto_service.get_realtime_crypto_data(limit=415)
            
            if not data:
                logger.warning("Crypto 데이터 없음, 재시도...")
                await asyncio.sleep(2.0)
                continue
            
            # 데이터 변경 감지
            data_str = json.dumps(data, sort_keys=True, default=str)
            current_hash = hashlib.md5(data_str.encode()).hexdigest()
            
            # 변경된 경우만 전송
            if current_hash != last_data_hash:
                # WebSocket 연결 상태 확인
                if websocket.client_state.name != 'CONNECTED':
                    logger.info(f"Crypto WebSocket 연결 종료됨, 스트리밍 중단: {client_id}")
                    break
                
                # 업데이트 메시지 생성
                update_msg = CryptoWebSocketMessage.create_update_message(data)
                
                # WebSocket으로 전송
                try:
                    await websocket.send_text(json.dumps(update_msg))
                except Exception as send_error:
                    logger.warning(f"Crypto WebSocket 전송 실패: {client_id} - {send_error}")
                    break
                
                last_data_hash = current_hash
                error_count = 0
                
                logger.debug(f"Crypto 데이터 전송: {len(data)}개 ({client_id})")
            
            # 500ms 대기
            await asyncio.sleep(0.5)
            
        except Exception as e:
            error_count += 1
            logger.error(f"Crypto 스트리밍 오류: {client_id} - {e} (에러 {error_count}/{max_errors})")
            
            if error_count >= max_errors:
                logger.error(f"Crypto 스트리밍 최대 에러 도달, 중단: {client_id}")
                break
            
            await asyncio.sleep(1.0)

async def _start_crypto_symbol_streaming(websocket: WebSocket, client_id: int, symbol: str):
    """암호화폐 개별 심볼 데이터 스트리밍"""
    last_data_hash = None
    error_count = 0
    max_errors = 5
    
    logger.info(f"Crypto 심볼 스트리밍 시작: {client_id} - {symbol}")
    
    while True:
        try:
            # CryptoService를 통한 특정 심볼 데이터 조회
            symbol_data = await crypto_service.get_crypto_symbol_data(symbol)
            
            if symbol_data:
                # 변경 감지
                data_str = json.dumps(symbol_data, sort_keys=True, default=str)
                current_hash = hashlib.md5(data_str.encode()).hexdigest()
                
                if current_hash != last_data_hash:
                    # 심볼별 업데이트 메시지 생성
                    update_msg = {
                        "type": "crypto_symbol_update",
                        "data": symbol_data,
                        "symbol": symbol,
                        "timestamp": datetime.now(pytz.UTC).isoformat(),
                        "exchange": "bithumb"
                    }
                    
                    await websocket.send_text(json.dumps(update_msg))
                    
                    last_data_hash = current_hash
                    error_count = 0
                    
                    logger.debug(f"Crypto 심볼 데이터 전송: {symbol} ({client_id})")
            else:
                # 심볼 데이터가 없는 경우 경고 로그
                if error_count == 0:  # 최초 한번만 로그
                    logger.warning(f"Crypto 심볼 {symbol} 데이터 없음")
            
            await asyncio.sleep(0.5)
            
        except Exception as e:
            error_count += 1
            logger.error(f"Crypto 심볼 스트리밍 오류: {client_id} ({symbol}) - {e}")
            
            if error_count >= max_errors:
                logger.error(f"Crypto 심볼 스트리밍 중단: {client_id} ({symbol})")
                break
            
            await asyncio.sleep(1.0)

# =========================
# 헬퍼 함수들 (CryptoService 사용으로 대부분 제거됨)
# =========================

# =========================
# 서버 종료 시 정리
# =========================

async def shutdown_crypto_websocket_services():
    """Crypto WebSocket 서비스 종료 시 정리"""
    global redis_streamer
    
    logger.info("Crypto WebSocket 서비스 종료 시작...")
    
    try:
        # Redis 스트리머 종료
        if redis_streamer:
            await redis_streamer.shutdown()
        
        # CryptoService 종료 (있는 경우)
        if hasattr(crypto_service, 'shutdown'):
            await crypto_service.shutdown()
        
        # WebSocket 매니저 Crypto 연결 종료
        await websocket_manager.disconnect_all_crypto()
        
        logger.info("Crypto WebSocket 서비스 종료 완료")
        
    except Exception as e:
        logger.error(f"Crypto WebSocket 서비스 종료 실패: {e}")