# app/api/endpoints/topgainers_websocket_endpoint.py
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
from app.services.topgainers_service import TopGainersService
from app.schemas.topgainers_schema import (
    TopGainerData, TopGainersUpdateMessage, TopGainersStatusMessage, TopGainersErrorMessage,
    TopGainersCategory, create_topgainers_update_message, create_topgainers_error_message
)

# 로거 설정
logger = logging.getLogger(__name__)

# 라우터 생성 - 기존 경로와 동일하게 수정
router = APIRouter(prefix="/ws", tags=["TopGainers WebSocket"])

# TopGainers 전용 인스턴스들
websocket_manager = WebSocketManager()
topgainers_service = TopGainersService()
redis_streamer = None  # 첫 연결 시 초기화

async def initialize_topgainers_websocket_services():
    """TopGainers WebSocket 서비스 초기화"""
    global redis_streamer
    
    logger.info("TopGainers WebSocket 서비스 초기화 시작...")
    
    # TopGainers 서비스 Redis 연결
    redis_connected = await topgainers_service.init_redis()
    if redis_connected:
        logger.info("TopGainers Redis 연결 성공")
    else:
        logger.warning("TopGainers Redis 연결 실패 (DB fallback)")
    
    # RedisStreamer 초기화 (TopGainers만 사용하므로 다른 서비스는 None)
    redis_streamer = RedisStreamer(
        topgainers_service=topgainers_service,
        crypto_service=None,
        sp500_service=None
    )
    await redis_streamer.initialize()
    redis_streamer.set_websocket_manager(websocket_manager)
    
    logger.info("TopGainers WebSocket 서비스 초기화 완료")

# =========================
# TopGainers 전용 WebSocket (변화율 포함)
# =========================

@router.websocket("/stocks/topgainers")
async def websocket_topgainers_all(websocket: WebSocket):
    """
    TopGainers 실시간 데이터 WebSocket (카테고리 + 변화율 포함)
    
    기능:
    - 최신 batch_id의 50개 심볼 실시간 데이터 전송
    - 각 데이터에 카테고리 정보 포함 (top_gainers, top_losers, most_actively_traded)
    - 전날 종가 기준 변화율 계산 (change_amount, change_percentage)
    - 장중: Redis 실시간 / 장마감: DB 최신 데이터
    - 500ms 간격 업데이트, 변경된 데이터만 전송
    """
    await websocket.accept()
    client_id = id(websocket)
    client_ip = websocket.client.host if websocket.client else "unknown"
    
    logger.info(f"TopGainers WebSocket 연결 (변화율 포함): {client_id} ({client_ip})")
    
    try:
        # WebSocket 매니저에 클라이언트 등록
        await websocket_manager.connect_topgainers(websocket)
        
        # 연결 성공 메시지 전송
        status_msg = TopGainersStatusMessage(
            status="connected",
            connected_clients=len(websocket_manager.topgainers_subscribers),
            data_source="redis+database",
            polling_interval=500,
            last_data_update=datetime.now(pytz.UTC).isoformat()
        )
        await websocket.send_text(status_msg.model_dump_json())
        
        # TopGainers 실시간 스트리밍 시작 (변화율 포함)
        asyncio.create_task(_start_topgainers_streaming_with_changes(websocket, client_id))
        
        # 클라이언트 메시지 대기 (연결 유지용)
        while True:
            try:
                # 클라이언트 메시지 대기 (30초 타임아웃)
                data = await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
                logger.debug(f"TopGainers 클라이언트 메시지: {client_id} - {data}")
                
            except asyncio.TimeoutError:
                # 하트비트 전송
                heartbeat = {
                    "type": "heartbeat",
                    "timestamp": datetime.now(pytz.UTC).isoformat(),
                    "server_time": datetime.now(pytz.UTC).strftime("%Y-%m-%d %H:%M:%S UTC"),
                    "data_type": "topgainers_with_changes",
                    "connected_clients": len(websocket_manager.topgainers_subscribers)
                }
                await websocket.send_text(json.dumps(heartbeat))
                
    except WebSocketDisconnect:
        logger.info(f"TopGainers WebSocket 연결 해제: {client_id}")
        
    except Exception as e:
        logger.error(f"TopGainers WebSocket 오류: {client_id} - {e}")
        
        # 에러 메시지 전송
        try:
            error_msg = create_topgainers_error_message(
                error_code="TOPGAINERS_ERROR",
                message=f"TopGainers 데이터 처리 오류: {str(e)}"
            )
            await websocket.send_text(error_msg.model_dump_json())
        except:
            pass
            
    finally:
        # 클라이언트 연결 해제 처리
        await websocket_manager.disconnect_topgainers(websocket)
        logger.info(f"TopGainers WebSocket 정리 완료: {client_id}")

@router.websocket("/stocks/topgainers/{category}")
async def websocket_topgainers_category(websocket: WebSocket, category: TopGainersCategory):
    """
    TopGainers 카테고리별 실시간 데이터 WebSocket (변화율 포함)
    
    지원 카테고리:
    - top_gainers: 상승 주식 (~20개)
    - top_losers: 하락 주식 (~10개)  
    - most_actively_traded: 활발히 거래되는 주식 (~20개)
    """
    await websocket.accept()
    client_id = id(websocket)
    client_ip = websocket.client.host if websocket.client else "unknown"
    
    logger.info(f"TopGainers Category WebSocket 연결 (변화율 포함): {client_id} ({client_ip}) - {category}")
    
    try:
        # WebSocket 매니저에 카테고리별 클라이언트 등록
        await websocket_manager.connect_symbol_subscriber(websocket, category, "topgainers_category")
        
        # 연결 성공 메시지 전송
        status_msg = TopGainersStatusMessage(
            status="connected",
            connected_clients=len(websocket_manager.symbol_subscribers.get(f"topgainers_category:{category}", [])),
            data_source="redis+database",
            polling_interval=500
        )
        await websocket.send_text(status_msg.model_dump_json())
        
        # 카테고리별 스트리밍 시작 (변화율 포함)
        asyncio.create_task(_start_topgainers_category_streaming_with_changes(websocket, client_id, category))
        
        # 클라이언트 메시지 대기
        while True:
            try:
                data = await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
                logger.debug(f"TopGainers Category 클라이언트 메시지: {client_id} ({category}) - {data}")
                
            except asyncio.TimeoutError:
                # 하트비트 전송
                heartbeat = {
                    "type": "heartbeat",
                    "timestamp": datetime.now(pytz.UTC).isoformat(),
                    "category": category,
                    "data_type": "topgainers_category_with_changes"
                }
                await websocket.send_text(json.dumps(heartbeat))
                
    except WebSocketDisconnect:
        logger.info(f"TopGainers Category WebSocket 연결 해제: {client_id} ({category})")
        
    except Exception as e:
        logger.error(f"TopGainers Category WebSocket 오류: {client_id} ({category}) - {e}")
        
        try:
            error_msg = create_topgainers_error_message(
                error_code="TOPGAINERS_CATEGORY_ERROR",
                message=f"TopGainers 카테고리 '{category}' 처리 오류: {str(e)}",
                category=category
            )
            await websocket.send_text(error_msg.model_dump_json())
        except:
            pass
            
    finally:
        # 클라이언트 연결 해제 처리
        await websocket_manager.disconnect_symbol_subscriber(websocket, category, "topgainers_category")
        logger.info(f"TopGainers Category WebSocket 정리 완료: {client_id} ({category})")

# =========================
# TopGainers 스트리밍 함수들 (변화율 포함)
# =========================

async def _start_topgainers_streaming_with_changes(websocket: WebSocket, client_id: int):
    """TopGainers 전체 데이터 스트리밍 (변화율 포함)"""
    last_data_hash = None
    error_count = 0
    max_errors = 5
    
    logger.info(f"TopGainers 전체 스트리밍 시작 (변화율 포함): {client_id}")
    
    while True:
        try:
            # TopGainersService를 통한 실시간 스트리밍 데이터 조회
            streaming_result = await topgainers_service.get_realtime_streaming_data(limit=50)
            
            if streaming_result.get("type") == "topgainers_error":
                error_count += 1
                logger.error(f"TopGainers 데이터 조회 오류: {streaming_result.get('error')}")
                if error_count >= max_errors:
                    logger.error(f"TopGainers 스트리밍 최대 에러 도달, 중단: {client_id}")
                    break
                await asyncio.sleep(2.0)
                continue
            
            data = streaming_result.get("data", [])
            
            # 데이터 변경 감지
            data_str = json.dumps(data, sort_keys=True, default=str)
            current_hash = hashlib.md5(data_str.encode()).hexdigest()
            
            # 변경된 경우만 전송
            if current_hash != last_data_hash:
                # TopGainersUpdateMessage 스키마 사용
                categories = list(set(item.get('category', '') for item in data if item.get('category')))
                
                update_msg = {
                    "type": "topgainers_update",
                    "data": data,
                    "timestamp": datetime.now(pytz.UTC).isoformat(),
                    "data_count": len(data),
                    "categories": categories,
                    "market_status": streaming_result.get("market_status"),
                    "data_source": streaming_result.get("data_source", "unknown")
                }
                
                # WebSocket으로 전송
                await websocket.send_text(json.dumps(update_msg))
                
                last_data_hash = current_hash
                error_count = 0
                
                logger.debug(f"TopGainers 변화율 데이터 전송: {len(data)}개 ({client_id})")
            
            # 500ms 대기
            await asyncio.sleep(0.5)
            
        except Exception as e:
            error_count += 1
            logger.error(f"TopGainers 스트리밍 오류: {client_id} - {e} (에러 {error_count}/{max_errors})")
            
            if error_count >= max_errors:
                logger.error(f"TopGainers 스트리밍 최대 에러 도달, 중단: {client_id}")
                break
            
            await asyncio.sleep(1.0)

async def _start_topgainers_category_streaming_with_changes(websocket: WebSocket, client_id: int, category: str):
    """TopGainers 카테고리별 데이터 스트리밍 (변화율 포함)"""
    last_data_hash = None
    error_count = 0
    max_errors = 5
    
    logger.info(f"TopGainers 카테고리 스트리밍 시작 (변화율 포함): {client_id} - {category}")
    
    while True:
        try:
            # TopGainersService를 통한 카테고리별 스트리밍 데이터 조회
            streaming_result = await topgainers_service.get_category_streaming_data(category, limit=25)
            
            if streaming_result.get("type") == "topgainers_error":
                error_count += 1
                logger.error(f"TopGainers 카테고리 {category} 데이터 조회 오류: {streaming_result.get('error')}")
                if error_count >= max_errors:
                    logger.error(f"TopGainers 카테고리 스트리밍 중단: {client_id} ({category})")
                    break
                await asyncio.sleep(2.0)
                continue
            
            data = streaming_result.get("data", [])
            
            # 변경 감지
            data_str = json.dumps(data, sort_keys=True, default=str)
            current_hash = hashlib.md5(data_str.encode()).hexdigest()
            
            if current_hash != last_data_hash:
                # 카테고리별 업데이트 메시지 생성
                update_msg = {
                    "type": "topgainers_category_update",
                    "data": data,
                    "timestamp": datetime.now(pytz.UTC).isoformat(),
                    "category": category,
                    "data_count": len(data),
                    "market_status": streaming_result.get("market_status"),
                    "data_source": streaming_result.get("data_source", "unknown")
                }
                
                await websocket.send_text(json.dumps(update_msg))
                
                last_data_hash = current_hash
                error_count = 0
                
                logger.debug(f"TopGainers 카테고리 변화율 데이터 전송: {len(data)}개 ({category}, {client_id})")
            
            await asyncio.sleep(0.5)
            
        except Exception as e:
            error_count += 1
            logger.error(f"TopGainers 카테고리 스트리밍 오류: {client_id} ({category}) - {e}")
            
            if error_count >= max_errors:
                logger.error(f"TopGainers 카테고리 스트리밍 중단: {client_id} ({category})")
                break
            
            await asyncio.sleep(1.0)

# =========================
# 서버 종료 시 정리
# =========================

async def shutdown_topgainers_websocket_services():
    """TopGainers WebSocket 서비스 종료 시 정리"""
    global redis_streamer
    
    logger.info("TopGainers WebSocket 서비스 종료 시작...")
    
    try:
        # TopGainers 서비스 종료
        if hasattr(topgainers_service, 'shutdown'):
            await topgainers_service.shutdown()
            logger.info("TopGainersService 종료 완료")
        else:
            logger.info("TopGainersService shutdown 메서드 없음")
        
        # Redis 스트리머 종료
        if redis_streamer:
            if hasattr(redis_streamer, 'shutdown'):
                await redis_streamer.shutdown()
                logger.info("RedisStreamer 종료 완료")
            else:
                logger.info("RedisStreamer shutdown 메서드 없음")
        
        # WebSocket 매니저 TopGainers 연결 종료
        if hasattr(websocket_manager, 'disconnect_all_topgainers'):
            await websocket_manager.disconnect_all_topgainers()
            logger.info("TopGainers WebSocket 연결 전체 해제 완료")
        else:
            # 대체 방법: 개별적으로 TopGainers 연결 해제
            if hasattr(websocket_manager, 'topgainers_subscribers'):
                subscribers = list(websocket_manager.topgainers_subscribers)
                for websocket_conn in subscribers:
                    try:
                        await websocket_manager.disconnect_topgainers(websocket_conn)
                    except Exception as disconnect_error:
                        logger.warning(f"TopGainers WebSocket 개별 연결 해제 실패: {disconnect_error}")
                logger.info(f"TopGainers WebSocket 개별 연결 {len(subscribers)}개 해제 완료")
        
        # Redis 클라이언트 종료 (TopGainersService)
        if hasattr(topgainers_service, 'redis_client') and topgainers_service.redis_client:
            try:
                await topgainers_service.redis_client.close()
                logger.info("TopGainers Redis 클라이언트 종료 완료")
            except Exception as redis_error:
                logger.warning(f"TopGainers Redis 클라이언트 종료 실패: {redis_error}")
        
        # 캐시 정리
        if hasattr(topgainers_service, 'symbol_category_cache'):
            topgainers_service.symbol_category_cache.clear()
            logger.info("TopGainers 캐시 정리 완료")
        
        logger.info("TopGainers WebSocket 서비스 종료 완료")
        
    except Exception as e:
        logger.error(f"TopGainers WebSocket 서비스 종료 실패: {e}")
        # 강제로라도 기본 정리 수행
        try:
            if topgainers_service and hasattr(topgainers_service, 'redis_client'):
                if topgainers_service.redis_client:
                    await topgainers_service.redis_client.close()
                    logger.info("TopGainers 강제 Redis 클라이언트 종료 완료")
        except Exception as force_error:
            logger.error(f"TopGainers 강제 정리도 실패: {force_error}")