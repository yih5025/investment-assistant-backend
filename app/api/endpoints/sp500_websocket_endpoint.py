# app/api/endpoints/sp500_websocket_endpoint.py
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
from app.services.sp500_service import SP500Service
from app.schemas.sp500_schema import (
    TimeframeEnum, MarketStatus, StockInfo, 
    SP500UpdateMessage, SP500StatusMessage, SP500ErrorMessage,
    create_error_response
)

# 로거 설정
logger = logging.getLogger(__name__)

# 라우터 생성 - 기존 경로와 동일하게 수정
router = APIRouter(prefix="/ws", tags=["SP500 WebSocket"])

# SP500 전용 인스턴스들
websocket_manager = WebSocketManager()
sp500_service = SP500Service()
redis_streamer = None  # 첫 연결 시 초기화

async def initialize_sp500_websocket_services():
    """SP500 WebSocket 서비스 초기화"""
    global redis_streamer
    
    logger.info("🚀 SP500 WebSocket 서비스 초기화 시작...")
    
    # SP500Service Redis 연결 초기화
    redis_connected = await sp500_service.init_redis()
    if redis_connected:
        logger.info("✅ SP500 Service Redis 연결 성공")
    else:
        logger.warning("⚠️ SP500 Service Redis 연결 실패 (DB로 fallback)")
    
    # RedisStreamer 초기화 (기존 WebSocketService 대신 SP500Service 전달)
    # RedisStreamer 초기화 (SP500만 사용하므로 다른 서비스는 None)
    redis_streamer = RedisStreamer(
        topgainers_service=None,
        crypto_service=None,
        sp500_service=sp500_service
    )
    await redis_streamer.initialize()
    redis_streamer.set_websocket_manager(websocket_manager)
    
    logger.info("✅ SP500 WebSocket 서비스 초기화 완료")

# =========================
# SP500 전체 WebSocket (변화율 포함)
# =========================

@router.websocket("/stocks/sp500")
async def websocket_sp500_all(websocket: WebSocket):
    """
    SP500 전체 실시간 데이터 WebSocket (변화율 포함)
    
    주요 기능:
    - S&P 500 전체 기업의 실시간 주식 데이터를 500ms마다 전송
    - 전날 종가 기준 변화율 계산 및 포함
    - 장중: Redis 실시간 / 장마감: DB 최신 데이터
    - 변경된 데이터만 전송으로 효율성 향상
    
    데이터 형태:
    {
        "type": "sp500_update",
        "data": [
            {
                "symbol": "AAPL",
                "current_price": 150.25,
                "previous_close": 148.50,
                "change_amount": 1.75,
                "change_percentage": 1.18,
                "is_positive": true,
                "change_color": "green",
                "volume": 45280000,
                "sector": "Technology"
            }
        ],
        "timestamp": "2025-08-28T10:30:00Z",
        "data_count": 500,
        "market": "sp500",
        "features": ["real_time_prices", "change_calculation"]
    }
    """
    await websocket.accept()
    client_id = id(websocket)
    client_ip = websocket.client.host if websocket.client else "unknown"
    
    logger.info(f"🔗 SP500 전체 WebSocket 연결 (변화율 포함): {client_id} ({client_ip})")
    
    try:
        # WebSocket 매니저에 클라이언트 등록
        await websocket_manager.connect_sp500(websocket)
        
        # 연결 성공 메시지 전송
        status_msg = SP500StatusMessage(
            status="connected",
            market_status=MarketStatus(**sp500_service.market_checker.get_market_status())
        )
        await websocket.send_text(status_msg.model_dump_json())
        
        # SP500 실시간 스트리밍 시작 (변화율 포함)
        asyncio.create_task(_start_sp500_streaming_with_changes(websocket, client_id))
        
        # 클라이언트 메시지 대기
        while True:
            try:
                data = await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
                logger.debug(f"📨 SP500 클라이언트 메시지 수신: {client_id} - {data}")
                
            except asyncio.TimeoutError:
                heartbeat = {
                    "type": "heartbeat",
                    "timestamp": datetime.now(pytz.UTC).isoformat(),
                    "server_time": datetime.now(pytz.UTC).strftime("%Y-%m-%d %H:%M:%S UTC"),
                    "data_type": "sp500_with_changes",
                    "connected_clients": len(websocket_manager.sp500_subscribers)
                }
                await websocket.send_text(json.dumps(heartbeat))
                
    except WebSocketDisconnect:
        logger.info(f"🔌 SP500 WebSocket 연결 해제: {client_id}")
        
    except Exception as e:
        logger.error(f"❌ SP500 WebSocket 오류: {client_id} - {e}")
        
        try:
            error_msg = SP500ErrorMessage(
                error_code="SP500_ERROR",
                message=f"SP500 데이터 처리 오류: {str(e)}"
            )
            await websocket.send_text(error_msg.model_dump_json())
        except:
            pass
            
    finally:
        await websocket_manager.disconnect_sp500(websocket)
        logger.info(f"🧹 SP500 WebSocket 정리 완료: {client_id}")

@router.websocket("/stocks/sp500/{symbol}")
async def websocket_sp500_symbol(websocket: WebSocket, symbol: str = Path(..., regex=r"^[A-Z]{1,5}$")):
    """
    특정 SP500 주식 실시간 데이터 WebSocket (변화율 포함)
    
    Args:
        symbol: 주식 심볼 (예: AAPL, TSLA, GOOGL)
        
    개별 주식의 실시간 가격과 전날 종가 기준 변화율을 포함합니다.
    """
    # 심볼 유효성 검사
    symbol = symbol.upper().strip()
    
    await websocket.accept()
    client_id = id(websocket)
    client_ip = websocket.client.host if websocket.client else "unknown"
    
    logger.info(f"🔗 SP500 Symbol WebSocket 연결 (변화율 포함): {client_id} ({client_ip}) - {symbol}")
    
    try:
        # WebSocket 매니저에 클라이언트 등록
        await websocket_manager.connect_symbol_subscriber(websocket, symbol, "sp500")
        
        # 연결 성공 메시지 전송
        status_msg = SP500StatusMessage(
            status="connected",
            market_status=MarketStatus(**sp500_service.market_checker.get_market_status())
        )
        await websocket.send_text(status_msg.model_dump_json())
        
        # 개별 심볼 스트리밍 시작 (변화율 포함)
        asyncio.create_task(_start_sp500_symbol_streaming_with_changes(websocket, client_id, symbol))
        
        # 클라이언트 메시지 대기
        while True:
            try:
                data = await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
                logger.debug(f"📨 SP500 Symbol 클라이언트 메시지: {client_id} ({symbol}) - {data}")
                
            except asyncio.TimeoutError:
                heartbeat = {
                    "type": "heartbeat",
                    "timestamp": datetime.now(pytz.UTC).isoformat(),
                    "symbol": symbol,
                    "data_type": "sp500_symbol_with_changes"
                }
                await websocket.send_text(json.dumps(heartbeat))
                
    except WebSocketDisconnect:
        logger.info(f"🔌 SP500 Symbol WebSocket 연결 해제: {client_id} ({symbol})")
        
    except Exception as e:
        logger.error(f"❌ SP500 Symbol WebSocket 오류: {client_id} ({symbol}) - {e}")
        
        try:
            error_msg = SP500ErrorMessage(
                error_code="SP500_SYMBOL_ERROR",
                message=f"SP500 주식 '{symbol}' 처리 오류: {str(e)}",
                symbol=symbol
            )
            await websocket.send_text(error_msg.model_dump_json())
        except:
            pass
            
    finally:
        await websocket_manager.disconnect_symbol_subscriber(websocket, symbol, "sp500")
        logger.info(f"🧹 SP500 Symbol WebSocket 정리 완료: {client_id} ({symbol})")

# =========================
# SP500 스트리밍 함수들 (변화율 포함)
# =========================

async def _start_sp500_streaming_with_changes(websocket: WebSocket, client_id: int):
    """SP500 전체 데이터 스트리밍 (변화율 포함)"""
    last_data_hash = None
    error_count = 0
    max_errors = 5
    
    logger.info(f"📡 SP500 전체 스트리밍 시작 (변화율 포함): {client_id}")
    
    while True:
        try:
            # SP500Service를 통한 실시간 데이터 조회 (변화율 계산 포함)
            streaming_result = await sp500_service.get_realtime_streaming_data(limit=500)
            
            if streaming_result.get("type") == "sp500_error":
                error_count += 1
                logger.error(f"❌ SP500 데이터 조회 오류: {streaming_result.get('error')}")
                if error_count >= max_errors:
                    break
                await asyncio.sleep(2.0)
                continue
            
            data = streaming_result.get("data", [])
            
            # 데이터 변경 감지
            data_str = json.dumps(data, sort_keys=True, default=str)
            current_hash = hashlib.md5(data_str.encode()).hexdigest()
            
            # 변경된 경우만 전송
            if current_hash != last_data_hash:
                # SP500WebSocketMessage 스키마 사용
                websocket_message = SP500UpdateMessage(
                    data=[StockInfo(**item) for item in data if isinstance(item, dict)],
                    data_count=len(data),
                    market_status=MarketStatus(**sp500_service.market_checker.get_market_status())
                )
                
                # WebSocket으로 전송
                await websocket.send_text(websocket_message.model_dump_json())
                
                last_data_hash = current_hash
                error_count = 0
                
                logger.debug(f"📊 SP500 변화율 데이터 전송: {len(data)}개 ({client_id})")
            
            # 500ms 대기
            await asyncio.sleep(0.5)
            
        except Exception as e:
            error_count += 1
            logger.error(f"❌ SP500 스트리밍 오류: {client_id} - {e} (에러 {error_count}/{max_errors})")
            
            if error_count >= max_errors:
                logger.error(f"💀 SP500 스트리밍 최대 에러 도달, 중단: {client_id}")
                break
            
            await asyncio.sleep(1.0)

async def _start_sp500_symbol_streaming_with_changes(websocket: WebSocket, client_id: int, symbol: str):
    """SP500 개별 심볼 데이터 스트리밍 (변화율 포함)"""
    last_data_hash = None
    error_count = 0
    max_errors = 5
    
    logger.info(f"📡 SP500 심볼 스트리밍 시작 (변화율 포함): {client_id} - {symbol}")
    
    while True:
        try:
            # SP500Service를 통한 개별 심볼 데이터 조회
            symbol_result = await sp500_service.get_symbol_streaming_data(symbol)
            
            if symbol_result.get("type") == "sp500_symbol_error":
                error_count += 1
                logger.error(f"❌ SP500 심볼 {symbol} 데이터 조회 오류: {symbol_result.get('error')}")
                if error_count >= max_errors:
                    break
                await asyncio.sleep(2.0)
                continue
            
            symbol_data = symbol_result.get("data")
            
            if symbol_data:
                # 변경 감지
                data_str = json.dumps(symbol_data, sort_keys=True, default=str)
                current_hash = hashlib.md5(data_str.encode()).hexdigest()
                
                if current_hash != last_data_hash:
                    # SP500 개별 심볼 업데이트 메시지
                    update_msg = {
                        "type": "sp500_symbol_update",
                        "symbol": symbol,
                        "data": symbol_data,
                        "timestamp": datetime.now(pytz.UTC).isoformat()
                    }
                    
                    await websocket.send_text(json.dumps(update_msg))
                    
                    last_data_hash = current_hash
                    error_count = 0
                    
                    logger.debug(f"📊 SP500 심볼 변화율 데이터 전송: {symbol} ({client_id})")
            
            await asyncio.sleep(0.5)
            
        except Exception as e:
            error_count += 1
            logger.error(f"❌ SP500 심볼 스트리밍 오류: {client_id} ({symbol}) - {e}")
            
            if error_count >= max_errors:
                logger.error(f"💀 SP500 심볼 스트리밍 중단: {client_id} ({symbol})")
                break
            
            await asyncio.sleep(1.0)

# =========================
# 서버 종료 시 정리
# =========================

async def shutdown_sp500_websocket_services():
    """SP500 WebSocket 서비스 종료 시 정리"""
    global redis_streamer
    
    logger.info("🛑 SP500 WebSocket 서비스 종료 시작...")
    
    try:
        # Redis 스트리머 종료
        if redis_streamer:
            await redis_streamer.shutdown()
        
        # SP500Service 종료
        await sp500_service.shutdown_websocket()
        
        logger.info("✅ SP500 WebSocket 서비스 종료 완료")
        
    except Exception as e:
        logger.error(f"❌ SP500 WebSocket 서비스 종료 실패: {e}")