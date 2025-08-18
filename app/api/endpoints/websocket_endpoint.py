# app/api/endpoints/websocket_endpoint.py
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Depends, HTTPException
from fastapi.responses import JSONResponse
import asyncio
import json
import logging
from datetime import datetime
from typing import Optional

from app.websocket.manager import WebSocketManager
from app.websocket.redis_streamer import RedisStreamer
from app.services.websocket_service import WebSocketService
from app.schemas.websocket_schema import (
    WebSocketMessageType, SubscriptionType, ErrorMessage, StatusMessage,
    create_error_message
)

# 로거 설정
logger = logging.getLogger(__name__)

# 라우터 생성
router = APIRouter(prefix="/ws", tags=["WebSocket"])

# 전역 인스턴스들 (서버 시작 시 초기화됨)
websocket_manager = WebSocketManager()
websocket_service = WebSocketService()
redis_streamer = None  # 첫 연결 시 초기화

async def initialize_websocket_services():
    """WebSocket 서비스 초기화 (main.py에서 호출)"""
    global redis_streamer
    
    logger.info("🚀 WebSocket 서비스 초기화 시작...")
    
    # RealtimeService Redis 연결 초기화
    redis_connected = await websocket_service.init_redis()
    if redis_connected:
        logger.info("✅ RealtimeService Redis 연결 성공")
    else:
        logger.warning("⚠️ RealtimeService Redis 연결 실패 (DB로 fallback)")
    
    # RedisStreamer 초기화
    redis_streamer = RedisStreamer(websocket_service)
    await redis_streamer.initialize()
    redis_streamer.set_websocket_manager(websocket_manager)
    
    logger.info("✅ WebSocket 서비스 초기화 완료")

# =========================
# Top Gainers WebSocket 엔드포인트
# =========================

@router.websocket("/stocks/topgainers")
async def websocket_topgainers(websocket: WebSocket):
    """
    Top Gainers 전체 실시간 데이터 WebSocket
    
    이 엔드포인트는 모든 상승 주식의 실시간 데이터를 500ms마다 전송합니다.
    변경된 데이터만 필터링하여 전송하므로 효율적입니다.
    """
    await websocket.accept()
    client_id = id(websocket)
    client_ip = websocket.client.host if websocket.client else "unknown"
    
    logger.info(f"🔗 TopGainers WebSocket 연결: {client_id} ({client_ip})")
    
    try:
        # WebSocket 매니저에 클라이언트 등록
        await websocket_manager.connect_topgainers(websocket)
        
        # 연결 성공 메시지 전송
        status_msg = StatusMessage(
            status="connected",
            connected_clients=len(websocket_manager.topgainers_subscribers),
            subscription_info={
                "type": "all_topgainers",
                "update_interval": "500ms",
                "data_source": "redis + database"
            }
        )
        await websocket.send_text(status_msg.model_dump_json())
        
        # Redis 스트리머 시작 (아직 시작되지 않은 경우)
        if redis_streamer and not redis_streamer.is_streaming_topgainers:
            asyncio.create_task(redis_streamer.start_topgainers_stream())
        
        # 클라이언트 메시지 대기 (현재는 단순히 연결 유지용)
        while True:
            try:
                # 클라이언트로부터 메시지 대기 (타임아웃 설정)
                data = await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
                
                # 메시지 처리 (현재는 단순 로깅)
                logger.debug(f"📨 클라이언트 메시지 수신: {client_id} - {data}")
                
            except asyncio.TimeoutError:
                # 30초마다 하트비트 전송
                heartbeat = {
                    "type": "heartbeat",
                    "timestamp": datetime.utcnow().isoformat(),
                    "server_time": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
                }
                await websocket.send_text(json.dumps(heartbeat))
                
    except WebSocketDisconnect:
        logger.info(f"🔌 TopGainers WebSocket 연결 해제: {client_id}")
        
    except Exception as e:
        logger.error(f"❌ TopGainers WebSocket 오류: {client_id} - {e}")
        
        # 에러 메시지 전송 (연결이 유효한 경우)
        try:
            error_msg = create_error_message(
                error_code="INTERNAL_ERROR",
                message=f"서버 내부 오류: {str(e)}"
            )
            await websocket.send_text(error_msg.model_dump_json())
        except:
            pass
            
    finally:
        # 클라이언트 연결 해제 처리
        await websocket_manager.disconnect_topgainers(websocket)
        logger.info(f"🧹 TopGainers WebSocket 정리 완료: {client_id}")

@router.websocket("/stocks/topgainers/{symbol}")
async def websocket_topgainers_symbol(websocket: WebSocket, symbol: str):
    """
    특정 심볼의 Top Gainers 실시간 데이터 WebSocket
    
    Args:
        symbol: 주식 심볼 (예: AAPL, TSLA)
    
    이 엔드포인트는 특정 심볼의 실시간 데이터만 전송합니다.
    """
    # 심볼 유효성 검사
    symbol = symbol.upper().strip()
    if not symbol or len(symbol) > 10:
        await websocket.close(code=1008, reason="Invalid symbol")
        return
    
    await websocket.accept()
    client_id = id(websocket)
    client_ip = websocket.client.host if websocket.client else "unknown"
    
    logger.info(f"🔗 TopGainers Symbol WebSocket 연결: {client_id} ({client_ip}) - {symbol}")
    
    try:
        # WebSocket 매니저에 클라이언트 등록
        await websocket_manager.connect_symbol_subscriber(websocket, symbol, "topgainers")
        
        # 연결 성공 메시지 전송
        status_msg = StatusMessage(
            status="connected",
            connected_clients=len(websocket_manager.symbol_subscribers.get(f"topgainers:{symbol}", [])),
            subscription_info={
                "type": "single_symbol",
                "symbol": symbol,
                "data_type": "topgainers",
                "update_interval": "500ms",
                "data_source": "redis + database"
            }
        )
        await websocket.send_text(status_msg.model_dump_json())
        
        # 특정 심볼 스트리머 시작
        stream_key = f"topgainers:{symbol}"
        if redis_streamer and stream_key not in redis_streamer.symbol_streams:
            asyncio.create_task(redis_streamer.start_symbol_stream(symbol, "topgainers"))
        
        # 클라이언트 메시지 대기
        while True:
            try:
                data = await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
                logger.debug(f"📨 TopGainers Symbol 클라이언트 메시지: {client_id} ({symbol}) - {data}")
                
            except asyncio.TimeoutError:
                # 하트비트 전송
                heartbeat = {
                    "type": "heartbeat",
                    "timestamp": datetime.utcnow().isoformat(),
                    "symbol": symbol,
                    "data_type": "topgainers"
                }
                await websocket.send_text(json.dumps(heartbeat))
                
    except WebSocketDisconnect:
        logger.info(f"🔌 TopGainers Symbol WebSocket 연결 해제: {client_id} ({symbol})")
        
    except Exception as e:
        logger.error(f"❌ TopGainers Symbol WebSocket 오류: {client_id} ({symbol}) - {e}")
        
        try:
            error_msg = create_error_message(
                error_code="SYMBOL_ERROR",
                message=f"심볼 '{symbol}' 처리 오류: {str(e)}"
            )
            await websocket.send_text(error_msg.model_dump_json())
        except:
            pass
            
    finally:
        # 클라이언트 연결 해제 처리
        await websocket_manager.disconnect_symbol_subscriber(websocket, symbol, "topgainers")
        logger.info(f"🧹 TopGainers Symbol WebSocket 정리 완료: {client_id} ({symbol})")

# =========================
# 암호화폐 WebSocket 엔드포인트
# =========================

@router.websocket("/crypto")
async def websocket_crypto(websocket: WebSocket):
    """
    모든 암호화폐 실시간 데이터 WebSocket
    
    빗썸 거래소의 모든 암호화폐 실시간 시세를 500ms마다 전송합니다.
    """
    await websocket.accept()
    client_id = id(websocket)
    client_ip = websocket.client.host if websocket.client else "unknown"
    
    logger.info(f"🔗 Crypto WebSocket 연결: {client_id} ({client_ip})")
    
    try:
        # WebSocket 매니저에 클라이언트 등록
        await websocket_manager.connect_crypto(websocket)
        
        # 연결 성공 메시지 전송
        status_msg = StatusMessage(
            status="connected",
            connected_clients=len(websocket_manager.crypto_subscribers),
            subscription_info={
                "type": "all_crypto",
                "update_interval": "500ms",
                "data_source": "redis + database",
                "exchange": "bithumb"
            }
        )
        await websocket.send_text(status_msg.model_dump_json())
        
        # Redis 스트리머 시작
        if redis_streamer and not redis_streamer.is_streaming_crypto:
            asyncio.create_task(redis_streamer.start_crypto_stream())
        
        # 클라이언트 메시지 대기
        while True:
            try:
                data = await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
                logger.debug(f"📨 Crypto 클라이언트 메시지 수신: {client_id} - {data}")
                
            except asyncio.TimeoutError:
                # 하트비트 전송
                heartbeat = {
                    "type": "heartbeat",
                    "timestamp": datetime.utcnow().isoformat(),
                    "server_time": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
                    "data_type": "crypto"
                }
                await websocket.send_text(json.dumps(heartbeat))
                
    except WebSocketDisconnect:
        logger.info(f"🔌 Crypto WebSocket 연결 해제: {client_id}")
        
    except Exception as e:
        logger.error(f"❌ Crypto WebSocket 오류: {client_id} - {e}")
        
        try:
            error_msg = create_error_message(
                error_code="CRYPTO_ERROR",
                message=f"암호화폐 데이터 처리 오류: {str(e)}"
            )
            await websocket.send_text(error_msg.model_dump_json())
        except:
            pass
            
    finally:
        await websocket_manager.disconnect_crypto(websocket)
        logger.info(f"🧹 Crypto WebSocket 정리 완료: {client_id}")

@router.websocket("/crypto/{symbol}")
async def websocket_crypto_symbol(websocket: WebSocket, symbol: str):
    """
    특정 암호화폐 실시간 데이터 WebSocket
    
    Args:
        symbol: 암호화폐 심볼 (예: KRW-BTC, KRW-ETH)
    """
    # 심볼 유효성 검사
    symbol = symbol.upper().strip()
    if not symbol or len(symbol) > 15:
        await websocket.close(code=1008, reason="Invalid crypto symbol")
        return
    
    await websocket.accept()
    client_id = id(websocket)
    client_ip = websocket.client.host if websocket.client else "unknown"
    
    logger.info(f"🔗 Crypto Symbol WebSocket 연결: {client_id} ({client_ip}) - {symbol}")
    
    try:
        # WebSocket 매니저에 클라이언트 등록
        await websocket_manager.connect_symbol_subscriber(websocket, symbol, "crypto")
        
        # 연결 성공 메시지 전송
        status_msg = StatusMessage(
            status="connected",
            connected_clients=len(websocket_manager.symbol_subscribers.get(f"crypto:{symbol}", [])),
            subscription_info={
                "type": "single_symbol",
                "symbol": symbol,
                "data_type": "crypto",
                "update_interval": "500ms",
                "exchange": "bithumb"
            }
        )
        await websocket.send_text(status_msg.model_dump_json())
        
        # 특정 심볼 스트리머 시작
        stream_key = f"crypto:{symbol}"
        if redis_streamer and stream_key not in redis_streamer.symbol_streams:
            asyncio.create_task(redis_streamer.start_symbol_stream(symbol, "crypto"))
        
        # 클라이언트 메시지 대기
        while True:
            try:
                data = await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
                logger.debug(f"📨 Crypto Symbol 클라이언트 메시지: {client_id} ({symbol}) - {data}")
                
            except asyncio.TimeoutError:
                heartbeat = {
                    "type": "heartbeat",
                    "timestamp": datetime.utcnow().isoformat(),
                    "symbol": symbol,
                    "data_type": "crypto"
                }
                await websocket.send_text(json.dumps(heartbeat))
                
    except WebSocketDisconnect:
        logger.info(f"🔌 Crypto Symbol WebSocket 연결 해제: {client_id} ({symbol})")
        
    except Exception as e:
        logger.error(f"❌ Crypto Symbol WebSocket 오류: {client_id} ({symbol}) - {e}")
        
        try:
            error_msg = create_error_message(
                error_code="CRYPTO_SYMBOL_ERROR",
                message=f"암호화폐 '{symbol}' 처리 오류: {str(e)}"
            )
            await websocket.send_text(error_msg.model_dump_json())
        except:
            pass
            
    finally:
        await websocket_manager.disconnect_symbol_subscriber(websocket, symbol, "crypto")
        logger.info(f"🧹 Crypto Symbol WebSocket 정리 완료: {client_id} ({symbol})")

# =========================
# SP500 WebSocket 엔드포인트
# =========================

@router.websocket("/stocks/sp500")
async def websocket_sp500(websocket: WebSocket):
    """
    SP500 전체 실시간 데이터 WebSocket
    
    S&P 500 전체 기업의 실시간 주식 데이터를 500ms마다 전송합니다.
    """
    await websocket.accept()
    client_id = id(websocket)
    client_ip = websocket.client.host if websocket.client else "unknown"
    
    logger.info(f"🔗 SP500 WebSocket 연결: {client_id} ({client_ip})")
    
    try:
        # WebSocket 매니저에 클라이언트 등록
        await websocket_manager.connect_sp500(websocket)
        
        # 연결 성공 메시지 전송
        status_msg = StatusMessage(
            status="connected",
            connected_clients=len(websocket_manager.sp500_subscribers),
            subscription_info={
                "type": "all_sp500",
                "update_interval": "500ms",
                "data_source": "redis + database",
                "market": "sp500"
            }
        )
        await websocket.send_text(status_msg.model_dump_json())
        
        # Redis 스트리머 시작
        if redis_streamer and not redis_streamer.is_streaming_sp500:
            asyncio.create_task(redis_streamer.start_sp500_stream())
        
        # 클라이언트 메시지 대기
        while True:
            try:
                data = await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
                logger.debug(f"📨 SP500 클라이언트 메시지 수신: {client_id} - {data}")
                
            except asyncio.TimeoutError:
                heartbeat = {
                    "type": "heartbeat",
                    "timestamp": datetime.utcnow().isoformat(),
                    "server_time": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
                    "data_type": "sp500"
                }
                await websocket.send_text(json.dumps(heartbeat))
                
    except WebSocketDisconnect:
        logger.info(f"🔌 SP500 WebSocket 연결 해제: {client_id}")
        
    except Exception as e:
        logger.error(f"❌ SP500 WebSocket 오류: {client_id} - {e}")
        
        try:
            error_msg = create_error_message(
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
async def websocket_sp500_symbol(websocket: WebSocket, symbol: str):
    """
    특정 SP500 주식 실시간 데이터 WebSocket
    
    Args:
        symbol: 주식 심볼 (예: AAPL, TSLA, GOOGL)
    """
    # 심볼 유효성 검사
    symbol = symbol.upper().strip()
    if not symbol or len(symbol) > 10:
        await websocket.close(code=1008, reason="Invalid SP500 symbol")
        return
    
    await websocket.accept()
    client_id = id(websocket)
    client_ip = websocket.client.host if websocket.client else "unknown"
    
    logger.info(f"🔗 SP500 Symbol WebSocket 연결: {client_id} ({client_ip}) - {symbol}")
    
    try:
        # WebSocket 매니저에 클라이언트 등록
        await websocket_manager.connect_symbol_subscriber(websocket, symbol, "sp500")
        
        # 연결 성공 메시지 전송
        status_msg = StatusMessage(
            status="connected",
            connected_clients=len(websocket_manager.symbol_subscribers.get(f"sp500:{symbol}", [])),
            subscription_info={
                "type": "single_symbol",
                "symbol": symbol,
                "data_type": "sp500",
                "update_interval": "500ms"
            }
        )
        await websocket.send_text(status_msg.model_dump_json())
        
        # 특정 심볼 스트리머 시작
        stream_key = f"sp500:{symbol}"
        if redis_streamer and stream_key not in redis_streamer.symbol_streams:
            asyncio.create_task(redis_streamer.start_symbol_stream(symbol, "sp500"))
        
        # 클라이언트 메시지 대기
        while True:
            try:
                data = await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
                logger.debug(f"📨 SP500 Symbol 클라이언트 메시지: {client_id} ({symbol}) - {data}")
                
            except asyncio.TimeoutError:
                heartbeat = {
                    "type": "heartbeat",
                    "timestamp": datetime.utcnow().isoformat(),
                    "symbol": symbol,
                    "data_type": "sp500"
                }
                await websocket.send_text(json.dumps(heartbeat))
                
    except WebSocketDisconnect:
        logger.info(f"🔌 SP500 Symbol WebSocket 연결 해제: {client_id} ({symbol})")
        
    except Exception as e:
        logger.error(f"❌ SP500 Symbol WebSocket 오류: {client_id} ({symbol}) - {e}")
        
        try:
            error_msg = create_error_message(
                error_code="SP500_SYMBOL_ERROR",
                message=f"SP500 주식 '{symbol}' 처리 오류: {str(e)}"
            )
            await websocket.send_text(error_msg.model_dump_json())
        except:
            pass
            
    finally:
        await websocket_manager.disconnect_symbol_subscriber(websocket, symbol, "sp500")
        logger.info(f"🧹 SP500 Symbol WebSocket 정리 완료: {client_id} ({symbol})")

# =========================
# 대시보드 WebSocket 엔드포인트
# =========================

@router.websocket("/dashboard")
async def websocket_dashboard(websocket: WebSocket):
    """
    대시보드용 통합 실시간 데이터 WebSocket
    
    주요 지표들의 요약 데이터를 전송합니다:
    - Top 10 상승 주식
    - Top 10 상승 암호화폐
    - 주요 SP500 주식 (AAPL, TSLA, GOOGL 등)
    """
    await websocket.accept()
    client_id = id(websocket)
    client_ip = websocket.client.host if websocket.client else "unknown"
    
    logger.info(f"🔗 Dashboard WebSocket 연결: {client_id} ({client_ip})")
    
    try:
        # WebSocket 매니저에 클라이언트 등록
        await websocket_manager.connect_dashboard(websocket)
        
        # 연결 성공 메시지 전송
        status_msg = StatusMessage(
            status="connected",
            connected_clients=len(websocket_manager.dashboard_subscribers),
            subscription_info={
                "type": "dashboard",
                "update_interval": "500ms",
                "data_includes": ["top_gainers", "crypto", "major_sp500"]
            }
        )
        await websocket.send_text(status_msg.model_dump_json())
        
        # 대시보드 스트리머 시작
        if redis_streamer and not redis_streamer.is_streaming_dashboard:
            asyncio.create_task(redis_streamer.start_dashboard_stream())
        
        # 클라이언트 메시지 대기
        while True:
            try:
                data = await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
                logger.debug(f"📨 Dashboard 클라이언트 메시지 수신: {client_id} - {data}")
                
            except asyncio.TimeoutError:
                heartbeat = {
                    "type": "heartbeat",
                    "timestamp": datetime.utcnow().isoformat(),
                    "server_time": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
                    "data_type": "dashboard"
                }
                await websocket.send_text(json.dumps(heartbeat))
                
    except WebSocketDisconnect:
        logger.info(f"🔌 Dashboard WebSocket 연결 해제: {client_id}")
        
    except Exception as e:
        logger.error(f"❌ Dashboard WebSocket 오류: {client_id} - {e}")
        
        try:
            error_msg = create_error_message(
                error_code="DASHBOARD_ERROR",
                message=f"대시보드 데이터 처리 오류: {str(e)}"
            )
            await websocket.send_text(error_msg.model_dump_json())
        except:
            pass
            
    finally:
        await websocket_manager.disconnect_dashboard(websocket)
        logger.info(f"🧹 Dashboard WebSocket 정리 완료: {client_id}")

# =========================
# HTTP 엔드포인트 (상태 조회용)
# =========================

@router.get("/status", response_model=dict)
async def get_websocket_status():
    """
    WebSocket 서비스 상태 조회
    
    Returns:
        dict: 서버 상태, 연결된 클라이언트 수, 통계 정보
    """
    try:
        # 헬스 체크 수행
        health_info = await websocket_service.health_check()
        
        # WebSocket 매니저 상태
        manager_status = websocket_manager.get_status()
        
        # Redis 스트리머 상태
        streamer_status = redis_streamer.get_status() if redis_streamer else {"status": "not_initialized"}
        
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "websocket_server": {
                "status": "running",
                "total_connections": manager_status["total_connections"],
                "topgainers_subscribers": manager_status["topgainers_subscribers"],
                "crypto_subscribers": manager_status["crypto_subscribers"],
                "sp500_subscribers": manager_status["sp500_subscribers"],
                "dashboard_subscribers": manager_status["dashboard_subscribers"],
                "symbol_subscribers": manager_status["symbol_subscribers"]
            },
            "realtime_service": health_info,
            "redis_streamer": streamer_status
        }
        
    except Exception as e:
        logger.error(f"❌ WebSocket 상태 조회 실패: {e}")
        raise HTTPException(status_code=500, detail=f"상태 조회 실패: {str(e)}")

@router.get("/stats", response_model=dict)
async def get_websocket_stats():
    """
    WebSocket 서비스 상세 통계
    
    Returns:
        dict: 상세 통계 정보
    """
    try:
        stats = websocket_service.get_statistics() if hasattr(websocket_service, 'get_statistics') else {}
        manager_stats = websocket_manager.get_detailed_stats()
        
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "service_stats": stats,
            "connection_stats": manager_stats,
            "performance": {
                "average_response_time": "< 500ms",
                "data_freshness": "500ms",
                "uptime": "running"
            }
        }
        
    except Exception as e:
        logger.error(f"❌ WebSocket 통계 조회 실패: {e}")
        raise HTTPException(status_code=500, detail=f"통계 조회 실패: {str(e)}")

# =========================
# 서버 종료 시 정리
# =========================

async def shutdown_websocket_services():
    """WebSocket 서비스 종료 시 정리 (main.py에서 호출)"""
    global redis_streamer
    
    logger.info("🛑 WebSocket 서비스 종료 시작...")
    
    try:
        # Redis 스트리머 종료
        if redis_streamer:
            await redis_streamer.shutdown()
        
        # WebSocket 매니저 종료
        await websocket_manager.shutdown_all_connections()
        
        # WebSocket 서비스 종료
        if hasattr(websocket_service, 'shutdown'):
            await websocket_service.shutdown()
        
        logger.info("✅ WebSocket 서비스 종료 완료")
        
    except Exception as e:
        logger.error(f"❌ WebSocket 서비스 종료 실패: {e}")