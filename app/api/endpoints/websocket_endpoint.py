import json
import logging
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Depends
from typing import Optional
import asyncio
from datetime import datetime
import pytz

from app.services.crypto_service import get_crypto_data_from_redis
from app.services.sp500_service import get_sp500_data_from_redis
from app.services.etf_service import get_etf_data_from_redis

logger = logging.getLogger(__name__)

router = APIRouter()

# WebSocket 매니저와 Redis Streamer는 app.py에서 주입받음
websocket_manager = None
redis_streamer = None
sync_redis_client = None
sp500_service = None
etf_service = None

def set_websocket_dependencies(manager, streamer, redis_client, sp500_svc=None, etf_svc=None):
    """WebSocket 의존성 설정"""
    global websocket_manager, redis_streamer, sync_redis_client, sp500_service, etf_service
    websocket_manager = manager
    redis_streamer = streamer
    sync_redis_client = redis_client
    sp500_service = sp500_svc
    etf_service = etf_svc
    logger.info("✅ WebSocket 의존성 설정 완료")


@router.websocket("/ws/crypto")
async def websocket_crypto_endpoint(websocket: WebSocket):
    """
    Crypto 실시간 데이터 WebSocket 엔드포인트
    
    클라이언트는 연결 후 자동으로 crypto 업데이트를 받습니다.
    """
    client_id = id(websocket)
    client_ip = websocket.client.host if websocket.client else "unknown"
    
    try:
        # WebSocket 연결 수락
        await websocket.accept()
        logger.info(f"🔗 Crypto WebSocket 연결: {client_id} ({client_ip})")
        
        # 매니저에 구독자 등록
        if websocket_manager:
            await websocket_manager.connect_crypto(websocket)
        
        # 🎁 초기 데이터 전송 (연결 즉시)
        if sync_redis_client:
            initial_data = await asyncio.to_thread(
                get_crypto_data_from_redis,
                sync_redis_client,
                500
            )
            if initial_data:
                response = {
                    "type": "crypto",
                    "data": initial_data,
                    "timestamp": datetime.now(pytz.UTC).isoformat()
                }
                await websocket.send_text(json.dumps(response, default=str))
                logger.info(f"📦 Crypto 초기 데이터 전송: {len(initial_data)}개")
        
        # 연결 유지 (클라이언트로부터 메시지 대기)
        while True:
            try:
                # 클라이언트 메시지 수신 (heartbeat, 구독 해제 등)
                data = await websocket.receive_text()
                
                # 간단한 메시지 처리 (필요시 handlers.py로 확장 가능)
                try:
                    message = json.loads(data)
                    action = message.get("action")
                    
                    if action == "heartbeat":
                        await websocket.send_text(json.dumps({
                            "type": "heartbeat_response",
                            "timestamp": datetime.now(pytz.UTC).isoformat()
                        }))
                    elif action == "unsubscribe":
                        logger.info(f"👋 Crypto 구독 해제 요청: {client_id}")
                        break
                        
                except json.JSONDecodeError:
                    logger.warning(f"⚠️ 잘못된 JSON 메시지: {client_id}")
                    
            except WebSocketDisconnect:
                logger.info(f"🔌 Crypto 클라이언트 연결 해제: {client_id}")
                break
                
    except Exception as e:
        logger.error(f"❌ Crypto WebSocket 오류 ({client_id}): {e}")
        
    finally:
        # 연결 해제 처리
        if websocket_manager:
            await websocket_manager.disconnect_crypto(websocket)
        
        logger.info(f"🏁 Crypto WebSocket 종료: {client_id}")


@router.websocket("/ws/sp500")
async def websocket_sp500_endpoint(websocket: WebSocket):
    """
    SP500 실시간 데이터 WebSocket 엔드포인트
    
    클라이언트는 연결 후 자동으로 SP500 업데이트를 받습니다.
    """
    client_id = id(websocket)
    client_ip = websocket.client.host if websocket.client else "unknown"
    
    try:
        # WebSocket 연결 수락
        await websocket.accept()
        logger.info(f"🔗 SP500 WebSocket 연결: {client_id} ({client_ip})")
        
        # 매니저에 구독자 등록
        if websocket_manager:
            await websocket_manager.connect_sp500(websocket)
        
        # 🎁 초기 데이터 전송 (Redis에서 빠르게 조회)
        if sync_redis_client:
            initial_data = await asyncio.to_thread(
                get_sp500_data_from_redis,
                sync_redis_client,
                500
            )
            if initial_data:
                response = {
                    "type": "sp500",
                    "data": initial_data,
                    "timestamp": datetime.now(pytz.UTC).isoformat()
                }
                await websocket.send_text(json.dumps(response, default=str))
                logger.info(f"📦 SP500 초기 데이터 전송 (Redis): {len(initial_data)}개")
            else:
                # Redis에 데이터 없으면 DB fallback
                logger.warning("⚠️ Redis에 SP500 데이터 없음, DB fallback")
                if sp500_service:
                    initial_result = await asyncio.to_thread(
                        sp500_service.get_stock_list,
                        500
                    )
                    initial_data_db = initial_result.get('stocks', [])
                    if initial_data_db:
                        response = {
                            "type": "sp500",
                            "data": initial_data_db,
                            "timestamp": datetime.now(pytz.UTC).isoformat()
                        }
                        await websocket.send_text(json.dumps(response, default=str))
                        logger.info(f"📦 SP500 초기 데이터 전송 (DB fallback): {len(initial_data_db)}개")
        
        # 연결 유지
        while True:
            try:
                data = await websocket.receive_text()
                
                try:
                    message = json.loads(data)
                    action = message.get("action")
                    
                    if action == "heartbeat":
                        await websocket.send_text(json.dumps({
                            "type": "heartbeat_response",
                            "timestamp": datetime.now(pytz.UTC).isoformat()
                        }))
                    elif action == "unsubscribe":
                        logger.info(f"👋 SP500 구독 해제 요청: {client_id}")
                        break
                        
                except json.JSONDecodeError:
                    logger.warning(f"⚠️ 잘못된 JSON 메시지: {client_id}")
                    
            except WebSocketDisconnect:
                logger.info(f"🔌 SP500 클라이언트 연결 해제: {client_id}")
                break
                
    except Exception as e:
        logger.error(f"❌ SP500 WebSocket 오류 ({client_id}): {e}")
        
    finally:
        if websocket_manager:
            await websocket_manager.disconnect_sp500(websocket)
        
        logger.info(f"🏁 SP500 WebSocket 종료: {client_id}")


@router.websocket("/ws/etf")
async def websocket_etf_endpoint(websocket: WebSocket):
    """
    ETF 실시간 데이터 WebSocket 엔드포인트
    
    클라이언트는 연결 후 자동으로 ETF 업데이트를 받습니다.
    """
    client_id = id(websocket)
    client_ip = websocket.client.host if websocket.client else "unknown"
    
    try:
        # WebSocket 연결 수락
        await websocket.accept()
        logger.info(f"🔗 ETF WebSocket 연결: {client_id} ({client_ip})")
        
        # 매니저에 구독자 등록
        if websocket_manager:
            await websocket_manager.connect_etf(websocket)
        
        # 🎁 초기 데이터 전송 (Service를 통해 변화량과 거래량 계산 포함)
        if etf_service:
            initial_result = await asyncio.to_thread(
                etf_service.get_etf_list,
                500
            )
            initial_data = initial_result.get('etfs', [])
            if initial_data:
                response = {
                    "type": "etf",
                    "data": initial_data,
                    "timestamp": datetime.now(pytz.UTC).isoformat()
                }
                await websocket.send_text(json.dumps(response, default=str))
                logger.info(f"📦 ETF 초기 데이터 전송: {len(initial_data)}개 (변화량 및 거래량 계산 포함)")
        
        # 연결 유지
        while True:
            try:
                data = await websocket.receive_text()
                
                try:
                    message = json.loads(data)
                    action = message.get("action")
                    
                    if action == "heartbeat":
                        await websocket.send_text(json.dumps({
                            "type": "heartbeat_response",
                            "timestamp": datetime.now(pytz.UTC).isoformat()
                        }))
                    elif action == "unsubscribe":
                        logger.info(f"👋 ETF 구독 해제 요청: {client_id}")
                        break
                        
                except json.JSONDecodeError:
                    logger.warning(f"⚠️ 잘못된 JSON 메시지: {client_id}")
                    
            except WebSocketDisconnect:
                logger.info(f"🔌 ETF 클라이언트 연결 해제: {client_id}")
                break
                
    except Exception as e:
        logger.error(f"❌ ETF WebSocket 오류 ({client_id}): {e}")
        
    finally:
        if websocket_manager:
            await websocket_manager.disconnect_etf(websocket)
        
        logger.info(f"🏁 ETF WebSocket 종료: {client_id}")


# 🔍 헬스 체크 엔드포인트
@router.get("/ws/health")
async def websocket_health():
    """WebSocket 시스템 헬스 체크"""
    try:
        manager_status = websocket_manager.get_status() if websocket_manager else {}
        streamer_status = redis_streamer.get_status() if redis_streamer else {}
        
        return {
            "status": "healthy",
            "websocket_manager": manager_status,
            "redis_streamer": streamer_status,
            "timestamp": datetime.now(pytz.UTC).isoformat()
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.now(pytz.UTC).isoformat()
        }
