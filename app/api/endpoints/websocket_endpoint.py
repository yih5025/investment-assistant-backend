# # app/api/endpoints/websocket_endpoint.py
# from fastapi import APIRouter, WebSocket, WebSocketDisconnect, HTTPException
# from fastapi.responses import JSONResponse
# import asyncio
# import json
# import logging
# from datetime import datetime, timedelta
# import pytz
# from typing import Optional
# import hashlib

# from app.websocket.manager import WebSocketManager
# from app.websocket.redis_streamer import RedisStreamer
# from app.services.websocket_service import WebSocketService
# from app.services.topgainers_service import TopGainersService
# from app.schemas.websocket_schema import (
#     WebSocketMessageType, SubscriptionType, ErrorMessage, StatusMessage,
#     create_error_message, create_topgainers_update_message
# )

# # 로거 설정
# logger = logging.getLogger(__name__)

# # 라우터 생성
# router = APIRouter(prefix="/ws", tags=["WebSocket"])

# # 🎯 전용 인스턴스들
# websocket_manager = WebSocketManager()
# websocket_service = WebSocketService()
# topgainers_service = TopGainersService()
# redis_streamer = None  # 첫 연결 시 초기화

# async def initialize_websocket_services():
#     """WebSocket 서비스 초기화 (main.py에서 호출)"""
#     global redis_streamer
    
#     logger.info("🚀 WebSocket 서비스 초기화 시작...")
    
#     # TopGainers 서비스 Redis 연결
#     redis_connected = await topgainers_service.init_redis()
#     if redis_connected:
#         logger.info("✅ TopGainers Redis 연결 성공")
#     else:
#         logger.warning("⚠️ TopGainers Redis 연결 실패 (DB fallback)")
    
#     # RealtimeService Redis 연결 초기화
#     redis_connected = await websocket_service.init_redis()
#     if redis_connected:
#         logger.info("✅ RealtimeService Redis 연결 성공")
#     else:
#         logger.warning("⚠️ RealtimeService Redis 연결 실패 (DB로 fallback)")
    
#     # RedisStreamer 초기화
#     redis_streamer = RedisStreamer(websocket_service)
#     await redis_streamer.initialize()
#     redis_streamer.set_websocket_manager(websocket_manager)
    
#     logger.info("✅ WebSocket 서비스 초기화 완료")

# # =========================
# # 🎯 TopGainers 전용 WebSocket (변화율 포함)
# # =========================

# @router.websocket("/stocks/topgainers")
# async def websocket_topgainers(websocket: WebSocket):
#     """
#     🎯 TopGainers 실시간 데이터 WebSocket (카테고리 + 변화율 포함)
    
#     **기능:**
#     - 최신 batch_id의 50개 심볼 실시간 데이터 전송
#     - 각 데이터에 카테고리 정보 포함 (top_gainers, top_losers, most_actively_traded)
#     - 🆕 전날 종가 기준 변화율 계산 (change_amount, change_percentage)
#     - 장중: Redis 실시간 / 장마감: DB 최신 데이터
#     - 500ms 간격 업데이트, 변경된 데이터만 전송
    
#     **데이터 형태:**
#     ```json
#     {
#         "type": "topgainers_update",
#         "data": [
#             {
#                 "symbol": "GXAI",
#                 "category": "top_gainers", 
#                 "current_price": 2.06,
#                 "previous_close": 1.10,
#                 "change_amount": 0.96,
#                 "change_percentage": 87.27,
#                 "is_positive": true,
#                 "change_color": "green",
#                 "volume": 246928896
#             }
#         ],
#         "timestamp": "2025-08-22T10:30:00Z",
#         "data_count": 50,
#         "categories": ["top_gainers", "top_losers", "most_actively_traded"],
#         "features": ["real_time_prices", "change_calculation"]
#     }
#     ```
#     """
#     await websocket.accept()
#     client_id = id(websocket)
#     client_ip = websocket.client.host if websocket.client else "unknown"
    
#     logger.info(f"🔗 TopGainers WebSocket 연결 (변화율 포함): {client_id} ({client_ip})")
    
#     try:
#         # WebSocket 매니저에 클라이언트 등록
#         await websocket_manager.connect_topgainers(websocket)
        
#         # 연결 성공 메시지 전송
#         status_msg = StatusMessage(
#             status="connected",
#             connected_clients=len(websocket_manager.topgainers_subscribers),
#             subscription_info={
#                 "type": "topgainers_all",
#                 "update_interval": "500ms",
#                 "data_source": "redis + database",
#                 "categories_included": ["top_gainers", "top_losers", "most_actively_traded"],
#                 "max_symbols": 50,
#                 "features": ["real_time_prices", "change_calculation", "previous_close_comparison"]
#             }
#         )
#         await websocket.send_text(status_msg.model_dump_json())
        
#         # 🎯 TopGainers 실시간 스트리밍 시작 (변화율 포함)
#         asyncio.create_task(_start_topgainers_streaming_with_changes(websocket, client_id))
        
#         # 클라이언트 메시지 대기 (연결 유지용)
#         while True:
#             try:
#                 # 클라이언트 메시지 대기 (30초 타임아웃)
#                 data = await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
#                 logger.debug(f"📨 TopGainers 클라이언트 메시지: {client_id} - {data}")
                
#             except asyncio.TimeoutError:
#                 # 하트비트 전송
#                 heartbeat = {
#                     "type": "heartbeat",
#                     "timestamp": datetime.now(pytz.UTC).isoformat(),
#                     "server_time": datetime.now(pytz.UTC).strftime("%Y-%m-%d %H:%M:%S UTC"),
#                     "data_type": "topgainers_with_changes",
#                     "connected_clients": len(websocket_manager.topgainers_subscribers)
#                 }
#                 await websocket.send_text(json.dumps(heartbeat))
                
#     except WebSocketDisconnect:
#         logger.info(f"🔌 TopGainers WebSocket 연결 해제: {client_id}")
        
#     except Exception as e:
#         logger.error(f"❌ TopGainers WebSocket 오류: {client_id} - {e}")
        
#         # 에러 메시지 전송
#         try:
#             error_msg = create_error_message(
#                 error_code="TOPGAINERS_ERROR",
#                 message=f"TopGainers 데이터 처리 오류: {str(e)}"
#             )
#             await websocket.send_text(error_msg.model_dump_json())
#         except:
#             pass
            
#     finally:
#         # 클라이언트 연결 해제 처리
#         await websocket_manager.disconnect_topgainers(websocket)
#         logger.info(f"🧹 TopGainers WebSocket 정리 완료: {client_id}")

# @router.websocket("/stocks/topgainers/{category}")
# async def websocket_topgainers_category(websocket: WebSocket, category: str):
#     """
#     🎯 TopGainers 카테고리별 실시간 데이터 WebSocket (변화율 포함)
    
#     **지원 카테고리:**
#     - `top_gainers`: 상승 주식 (~20개)
#     - `top_losers`: 하락 주식 (~10개)  
#     - `most_actively_traded`: 활발히 거래되는 주식 (~20개)
    
#     **용도:**
#     - 프론트엔드에서 카테고리별 배너 데이터 실시간 업데이트
#     - 특정 카테고리만 관심 있는 클라이언트용
#     - 🆕 전날 종가 기준 변화율 포함
#     """
#     # 카테고리 유효성 검사
#     valid_categories = ["top_gainers", "top_losers", "most_actively_traded"]
#     if category not in valid_categories:
#         await websocket.close(code=1008, reason=f"Invalid category. Valid: {valid_categories}")
#         return
    
#     await websocket.accept()
#     client_id = id(websocket)
#     client_ip = websocket.client.host if websocket.client else "unknown"
    
#     logger.info(f"🔗 TopGainers Category WebSocket 연결 (변화율 포함): {client_id} ({client_ip}) - {category}")
    
#     try:
#         # WebSocket 매니저에 카테고리별 클라이언트 등록
#         await websocket_manager.connect_symbol_subscriber(websocket, category, "topgainers_category")
        
#         # 연결 성공 메시지 전송
#         status_msg = StatusMessage(
#             status="connected",
#             connected_clients=len(websocket_manager.symbol_subscribers.get(f"topgainers_category:{category}", [])),
#             subscription_info={
#                 "type": "topgainers_category",
#                 "category": category,
#                 "update_interval": "500ms",
#                 "data_source": "redis + database",
#                 "features": ["real_time_prices", "change_calculation"]
#             }
#         )
#         await websocket.send_text(status_msg.model_dump_json())
        
#         # 🎯 카테고리별 스트리밍 시작 (변화율 포함)
#         asyncio.create_task(_start_topgainers_category_streaming_with_changes(websocket, client_id, category))
        
#         # 클라이언트 메시지 대기
#         while True:
#             try:
#                 data = await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
#                 logger.debug(f"📨 TopGainers Category 클라이언트 메시지: {client_id} ({category}) - {data}")
                
#             except asyncio.TimeoutError:
#                 # 하트비트 전송
#                 heartbeat = {
#                     "type": "heartbeat",
#                     "timestamp": datetime.now(pytz.UTC).isoformat(),
#                     "category": category,
#                     "data_type": "topgainers_category_with_changes"
#                 }
#                 await websocket.send_text(json.dumps(heartbeat))
                
#     except WebSocketDisconnect:
#         logger.info(f"🔌 TopGainers Category WebSocket 연결 해제: {client_id} ({category})")
        
#     except Exception as e:
#         logger.error(f"❌ TopGainers Category WebSocket 오류: {client_id} ({category}) - {e}")
        
#         try:
#             error_msg = create_error_message(
#                 error_code="TOPGAINERS_CATEGORY_ERROR",
#                 message=f"TopGainers 카테고리 '{category}' 처리 오류: {str(e)}"
#             )
#             await websocket.send_text(error_msg.model_dump_json())
#         except:
#             pass
            
#     finally:
#         # 클라이언트 연결 해제 처리
#         await websocket_manager.disconnect_symbol_subscriber(websocket, category, "topgainers_category")
#         logger.info(f"🧹 TopGainers Category WebSocket 정리 완료: {client_id} ({category})")

# # =========================
# # 🎯 SP500 전용 WebSocket (변화율 포함)
# # =========================

# @router.websocket("/stocks/sp500")
# async def websocket_sp500(websocket: WebSocket):
#     """
#     SP500 전체 실시간 데이터 WebSocket (변화율 포함)
    
#     🆕 S&P 500 전체 기업의 실시간 주식 데이터를 500ms마다 전송하며,
#     전날 종가 기준 변화율을 포함합니다.
#     """
#     await websocket.accept()
#     client_id = id(websocket)
#     client_ip = websocket.client.host if websocket.client else "unknown"
    
#     logger.info(f"🔗 SP500 WebSocket 연결 (변화율 포함): {client_id} ({client_ip})")
    
#     try:
#         # WebSocket 매니저에 클라이언트 등록
#         await websocket_manager.connect_sp500(websocket)
        
#         # 연결 성공 메시지 전송
#         status_msg = StatusMessage(
#             status="connected",
#             connected_clients=len(websocket_manager.sp500_subscribers),
#             subscription_info={
#                 "type": "all_sp500",
#                 "update_interval": "500ms",
#                 "data_source": "redis + database",
#                 "market": "sp500",
#                 "features": ["real_time_prices", "change_calculation", "previous_close_comparison"]
#             }
#         )
#         await websocket.send_text(status_msg.model_dump_json())
        
#         # 🎯 SP500 실시간 스트리밍 시작 (변화율 포함)
#         asyncio.create_task(_start_sp500_streaming_with_changes(websocket, client_id))
        
#         # 클라이언트 메시지 대기
#         while True:
#             try:
#                 data = await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
#                 logger.debug(f"📨 SP500 클라이언트 메시지 수신: {client_id} - {data}")
                
#             except asyncio.TimeoutError:
#                 heartbeat = {
#                     "type": "heartbeat",
#                     "timestamp": datetime.now(pytz.UTC).isoformat(),
#                     "server_time": datetime.now(pytz.UTC).strftime("%Y-%m-%d %H:%M:%S UTC"),
#                     "data_type": "sp500_with_changes"
#                 }
#                 await websocket.send_text(json.dumps(heartbeat))
                
#     except WebSocketDisconnect:
#         logger.info(f"🔌 SP500 WebSocket 연결 해제: {client_id}")
        
#     except Exception as e:
#         logger.error(f"❌ SP500 WebSocket 오류: {client_id} - {e}")
        
#         try:
#             error_msg = create_error_message(
#                 error_code="SP500_ERROR",
#                 message=f"SP500 데이터 처리 오류: {str(e)}"
#             )
#             await websocket.send_text(error_msg.model_dump_json())
#         except:
#             pass
            
#     finally:
#         await websocket_manager.disconnect_sp500(websocket)
#         logger.info(f"🧹 SP500 WebSocket 정리 완료: {client_id}")

# @router.websocket("/stocks/sp500/{symbol}")
# async def websocket_sp500_symbol(websocket: WebSocket, symbol: str):
#     """
#     특정 SP500 주식 실시간 데이터 WebSocket (변화율 포함)
    
#     Args:
#         symbol: 주식 심볼 (예: AAPL, TSLA, GOOGL)
        
#     🆕 개별 주식의 실시간 가격과 전날 종가 기준 변화율을 포함합니다.
#     """
#     # 심볼 유효성 검사
#     symbol = symbol.upper().strip()
#     if not symbol or len(symbol) > 10:
#         await websocket.close(code=1008, reason="Invalid SP500 symbol")
#         return
    
#     await websocket.accept()
#     client_id = id(websocket)
#     client_ip = websocket.client.host if websocket.client else "unknown"
    
#     logger.info(f"🔗 SP500 Symbol WebSocket 연결 (변화율 포함): {client_id} ({client_ip}) - {symbol}")
    
#     try:
#         # WebSocket 매니저에 클라이언트 등록
#         await websocket_manager.connect_symbol_subscriber(websocket, symbol, "sp500")
        
#         # 연결 성공 메시지 전송
#         status_msg = StatusMessage(
#             status="connected",
#             connected_clients=len(websocket_manager.symbol_subscribers.get(f"sp500:{symbol}", [])),
#             subscription_info={
#                 "type": "single_symbol",
#                 "symbol": symbol,
#                 "data_type": "sp500",
#                 "update_interval": "500ms",
#                 "features": ["real_time_prices", "change_calculation"]
#             }
#         )
#         await websocket.send_text(status_msg.model_dump_json())
        
#         # 🎯 개별 심볼 스트리밍 시작 (변화율 포함)
#         asyncio.create_task(_start_sp500_symbol_streaming_with_changes(websocket, client_id, symbol))
        
#         # 클라이언트 메시지 대기
#         while True:
#             try:
#                 data = await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
#                 logger.debug(f"📨 SP500 Symbol 클라이언트 메시지: {client_id} ({symbol}) - {data}")
                
#             except asyncio.TimeoutError:
#                 heartbeat = {
#                     "type": "heartbeat",
#                     "timestamp": datetime.now(pytz.UTC).isoformat(),
#                     "symbol": symbol,
#                     "data_type": "sp500_symbol_with_changes"
#                 }
#                 await websocket.send_text(json.dumps(heartbeat))
                
#     except WebSocketDisconnect:
#         logger.info(f"🔌 SP500 Symbol WebSocket 연결 해제: {client_id} ({symbol})")
        
#     except Exception as e:
#         logger.error(f"❌ SP500 Symbol WebSocket 오류: {client_id} ({symbol}) - {e}")
        
#         try:
#             error_msg = create_error_message(
#                 error_code="SP500_SYMBOL_ERROR",
#                 message=f"SP500 주식 '{symbol}' 처리 오류: {str(e)}"
#             )
#             await websocket.send_text(error_msg.model_dump_json())
#         except:
#             pass
            
#     finally:
#         await websocket_manager.disconnect_symbol_subscriber(websocket, symbol, "sp500")
#         logger.info(f"🧹 SP500 Symbol WebSocket 정리 완료: {client_id} ({symbol})")

# # =========================
# # 🎯 향상된 스트리밍 함수들 (변화율 포함)
# # =========================

# async def _start_topgainers_streaming_with_changes(websocket: WebSocket, client_id: int):
#     """TopGainers 전체 데이터 스트리밍 (변화율 포함)"""
#     last_data_hash = None
#     error_count = 0
#     max_errors = 5
    
#     logger.info(f"📡 TopGainers 전체 스트리밍 시작 (변화율 포함): {client_id}")
    
#     while True:
#         try:
#             # 🎯 변화율이 계산된 데이터 조회
#             data = await websocket_service.get_topgainers_from_redis_with_changes()
            
#             if not data:
#                 # 데이터가 없으면 기존 방식으로 fallback
#                 data = await topgainers_service.get_market_data_with_categories()
#                 data = [item.dict() if hasattr(item, 'dict') else item for item in data]
            
#             # 데이터 변경 감지
#             data_str = json.dumps(data, sort_keys=True, default=str)
#             current_hash = hashlib.md5(data_str.encode()).hexdigest()
            
#             # 변경된 경우만 전송
#             if current_hash != last_data_hash:
#                 # 카테고리별 분류
#                 categories = list(set(item.get('category') for item in data if item.get('category')))
                
#                 # 업데이트 메시지 생성
#                 update_msg = {
#                     "type": "topgainers_update",
#                     "data": data,
#                     "timestamp": datetime.now(pytz.UTC).isoformat(),
#                     "data_count": len(data),
#                     "categories": categories,
#                     "features": ["real_time_prices", "change_calculation", "previous_close_comparison"]
#                 }
                
#                 # WebSocket으로 전송
#                 await websocket.send_text(json.dumps(update_msg))
                
#                 last_data_hash = current_hash
#                 error_count = 0
                
#                 logger.debug(f"📊 TopGainers 변화율 데이터 전송: {len(data)}개 ({client_id})")
            
#             # 500ms 대기
#             await asyncio.sleep(0.5)
            
#         except Exception as e:
#             error_count += 1
#             logger.error(f"❌ TopGainers 스트리밍 오류: {client_id} - {e} (에러 {error_count}/{max_errors})")
            
#             if error_count >= max_errors:
#                 logger.error(f"💀 TopGainers 스트리밍 최대 에러 도달, 중단: {client_id}")
#                 break
            
#             await asyncio.sleep(1.0)

# async def _start_topgainers_category_streaming_with_changes(websocket: WebSocket, client_id: int, category: str):
#     """TopGainers 카테고리별 데이터 스트리밍 (변화율 포함)"""
#     last_data_hash = None
#     error_count = 0
#     max_errors = 5
    
#     logger.info(f"📡 TopGainers 카테고리 스트리밍 시작 (변화율 포함): {client_id} - {category}")
    
#     while True:
#         try:
#             # 🎯 변화율이 계산된 카테고리별 데이터 조회
#             data = await websocket_service.get_topgainers_from_redis_with_changes(category=category, limit=25)
            
#             if not data:
#                 # 데이터가 없으면 기존 방식으로 fallback
#                 data = await topgainers_service.get_category_data_for_websocket(category, 25)
#                 data = [item.dict() if hasattr(item, 'dict') else item for item in data]
            
#             # 변경 감지
#             data_str = json.dumps(data, sort_keys=True, default=str)
#             current_hash = hashlib.md5(data_str.encode()).hexdigest()
            
#             if current_hash != last_data_hash:
#                 # 카테고리별 업데이트 메시지 생성
#                 update_msg = {
#                     "type": "topgainers_category_update",
#                     "data": data,
#                     "timestamp": datetime.now(pytz.UTC).isoformat(),
#                     "category": category,
#                     "data_count": len(data),
#                     "features": ["real_time_prices", "change_calculation"]
#                 }
                
#                 await websocket.send_text(json.dumps(update_msg))
                
#                 last_data_hash = current_hash
#                 error_count = 0
                
#                 logger.debug(f"📊 TopGainers 카테고리 변화율 데이터 전송: {len(data)}개 ({category}, {client_id})")
            
#             await asyncio.sleep(0.5)
            
#         except Exception as e:
#             error_count += 1
#             logger.error(f"❌ TopGainers 카테고리 스트리밍 오류: {client_id} ({category}) - {e}")
            
#             if error_count >= max_errors:
#                 logger.error(f"💀 TopGainers 카테고리 스트리밍 중단: {client_id} ({category})")
#                 break
            
#             await asyncio.sleep(1.0)

# async def _start_sp500_streaming_with_changes(websocket: WebSocket, client_id: int):
#     """SP500 전체 데이터 스트리밍 (변화율 포함)"""
#     last_data_hash = None
#     error_count = 0
#     max_errors = 5
    
#     logger.info(f"📡 SP500 전체 스트리밍 시작 (변화율 포함): {client_id}")
    
#     while True:
#         try:
#             # 🎯 변화율이 계산된 SP500 데이터 조회
#             data = await websocket_service.get_sp500_from_redis_with_changes(limit=100)
            
#             if not data:
#                 # 데이터가 없으면 기존 방식으로 fallback
#                 data = await websocket_service.get_sp500_from_redis(limit=100)
#                 data = [item.dict() if hasattr(item, 'dict') else item for item in data]
            
#             # 데이터 변경 감지
#             data_str = json.dumps(data, sort_keys=True, default=str)
#             current_hash = hashlib.md5(data_str.encode()).hexdigest()
            
#             # 변경된 경우만 전송
#             if current_hash != last_data_hash:
#                 # 업데이트 메시지 생성
#                 update_msg = {
#                     "type": "sp500_update",
#                     "data": data,
#                     "timestamp": datetime.now(pytz.UTC).isoformat(),
#                     "data_count": len(data),
#                     "features": ["real_time_prices", "change_calculation", "previous_close_comparison"]
#                 }
                
#                 # WebSocket으로 전송
#                 await websocket.send_text(json.dumps(update_msg))
                
#                 last_data_hash = current_hash
#                 error_count = 0
                
#                 logger.debug(f"📊 SP500 변화율 데이터 전송: {len(data)}개 ({client_id})")
            
#             # 500ms 대기
#             await asyncio.sleep(0.5)
            
#         except Exception as e:
#             error_count += 1
#             logger.error(f"❌ SP500 스트리밍 오류: {client_id} - {e} (에러 {error_count}/{max_errors})")
            
#             if error_count >= max_errors:
#                 logger.error(f"💀 SP500 스트리밍 최대 에러 도달, 중단: {client_id}")
#                 break
            
#             await asyncio.sleep(1.0)

# async def _start_sp500_symbol_streaming_with_changes(websocket: WebSocket, client_id: int, symbol: str):
#     """SP500 개별 심볼 데이터 스트리밍 (변화율 포함)"""
#     last_data_hash = None
#     error_count = 0
#     max_errors = 5
    
#     logger.info(f"📡 SP500 심볼 스트리밍 시작 (변화율 포함): {client_id} - {symbol}")
    
#     while True:
#         try:
#             # 🎯 변화율이 계산된 개별 심볼 데이터 조회
#             all_data = await websocket_service.get_sp500_from_redis_with_changes(limit=500)
            
#             # 특정 심볼 필터링
#             symbol_data = None
#             for item in all_data:
#                 if item.get('symbol') == symbol:
#                     symbol_data = item
#                     break
            
#             if symbol_data:
#                 # 변경 감지
#                 data_str = json.dumps(symbol_data, sort_keys=True, default=str)
#                 current_hash = hashlib.md5(data_str.encode()).hexdigest()
                
#                 if current_hash != last_data_hash:
#                     # 심볼별 업데이트 메시지 생성
#                     update_msg = {
#                         "type": "sp500_symbol_update",
#                         "data": symbol_data,
#                         "symbol": symbol,
#                         "timestamp": datetime.now(pytz.UTC).isoformat(),
#                         "features": ["real_time_prices", "change_calculation"]
#                     }
                    
#                     await websocket.send_text(json.dumps(update_msg))
                    
#                     last_data_hash = current_hash
#                     error_count = 0
                    
#                     logger.debug(f"📊 SP500 심볼 변화율 데이터 전송: {symbol} ({client_id})")
            
#             await asyncio.sleep(0.5)
            
#         except Exception as e:
#             error_count += 1
#             logger.error(f"❌ SP500 심볼 스트리밍 오류: {client_id} ({symbol}) - {e}")
            
#             if error_count >= max_errors:
#                 logger.error(f"💀 SP500 심볼 스트리밍 중단: {client_id} ({symbol})")
#                 break
            
#             await asyncio.sleep(1.0)

# # =========================
# # 암호화폐 WebSocket 엔드포인트 (기존 유지)
# # =========================

# @router.websocket("/crypto")
# async def websocket_crypto(websocket: WebSocket):
#     """
#     모든 암호화폐 실시간 데이터 WebSocket
    
#     빗썸 거래소의 모든 암호화폐 실시간 시세를 500ms마다 전송합니다.
#     암호화폐는 24시간 거래이므로 별도 변화율 계산 없이 기존 로직 유지합니다.
#     """
#     await websocket.accept()
#     client_id = id(websocket)
#     client_ip = websocket.client.host if websocket.client else "unknown"
    
#     logger.info(f"🔗 Crypto WebSocket 연결: {client_id} ({client_ip})")
    
#     try:
#         # WebSocket 매니저에 클라이언트 등록
#         await websocket_manager.connect_crypto(websocket)
        
#         # 연결 성공 메시지 전송
#         status_msg = StatusMessage(
#             status="connected",
#             connected_clients=len(websocket_manager.crypto_subscribers),
#             subscription_info={
#                 "type": "all_crypto",
#                 "update_interval": "500ms",
#                 "data_source": "redis + database",
#                 "exchange": "bithumb",
#                 "features": ["real_time_prices", "24h_trading"]
#             }
#         )
#         await websocket.send_text(status_msg.model_dump_json())
        
#         # Redis 스트리머 시작
#         if redis_streamer and not redis_streamer.is_streaming_crypto:
#             asyncio.create_task(redis_streamer.start_crypto_stream())
        
#         # 클라이언트 메시지 대기
#         while True:
#             try:
#                 data = await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
#                 logger.debug(f"📨 Crypto 클라이언트 메시지 수신: {client_id} - {data}")
                
#             except asyncio.TimeoutError:
#                 # 하트비트 전송
#                 heartbeat = {
#                     "type": "heartbeat",
#                     "timestamp": datetime.now(pytz.UTC).isoformat(),
#                     "server_time": datetime.now(pytz.UTC).strftime("%Y-%m-%d %H:%M:%S UTC"),
#                     "data_type": "crypto"
#                 }
#                 await websocket.send_text(json.dumps(heartbeat))
                
#     except WebSocketDisconnect:
#         logger.info(f"🔌 Crypto WebSocket 연결 해제: {client_id}")
        
#     except Exception as e:
#         logger.error(f"❌ Crypto WebSocket 오류: {client_id} - {e}")
        
#         try:
#             error_msg = create_error_message(
#                 error_code="CRYPTO_ERROR",
#                 message=f"암호화폐 데이터 처리 오류: {str(e)}"
#             )
#             await websocket.send_text(error_msg.model_dump_json())
#         except:
#             pass
            
#     finally:
#         await websocket_manager.disconnect_crypto(websocket)
#         logger.info(f"🧹 Crypto WebSocket 정리 완료: {client_id}")

# @router.websocket("/crypto/{symbol}")
# async def websocket_crypto_symbol(websocket: WebSocket, symbol: str):
#     """
#     특정 암호화폐 실시간 데이터 WebSocket
    
#     Args:
#         symbol: 암호화폐 심볼 (예: KRW-BTC, KRW-ETH)
#     """
#     # 심볼 유효성 검사
#     symbol = symbol.upper().strip()
#     if not symbol or len(symbol) > 15:
#         await websocket.close(code=1008, reason="Invalid crypto symbol")
#         return
    
#     await websocket.accept()
#     client_id = id(websocket)
#     client_ip = websocket.client.host if websocket.client else "unknown"
    
#     logger.info(f"🔗 Crypto Symbol WebSocket 연결: {client_id} ({client_ip}) - {symbol}")
    
#     try:
#         # WebSocket 매니저에 클라이언트 등록
#         await websocket_manager.connect_symbol_subscriber(websocket, symbol, "crypto")
        
#         # 연결 성공 메시지 전송
#         status_msg = StatusMessage(
#             status="connected",
#             connected_clients=len(websocket_manager.symbol_subscribers.get(f"crypto:{symbol}", [])),
#             subscription_info={
#                 "type": "single_symbol",
#                 "symbol": symbol,
#                 "data_type": "crypto",
#                 "update_interval": "500ms",
#                 "exchange": "bithumb"
#             }
#         )
#         await websocket.send_text(status_msg.model_dump_json())
        
#         # 특정 심볼 스트리머 시작
#         stream_key = f"crypto:{symbol}"
#         if redis_streamer and stream_key not in redis_streamer.symbol_streams:
#             asyncio.create_task(redis_streamer.start_symbol_stream(symbol, "crypto"))
        
#         # 클라이언트 메시지 대기
#         while True:
#             try:
#                 data = await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
#                 logger.debug(f"📨 Crypto Symbol 클라이언트 메시지: {client_id} ({symbol}) - {data}")
                
#             except asyncio.TimeoutError:
#                 heartbeat = {
#                     "type": "heartbeat",
#                     "timestamp": datetime.now(pytz.UTC).isoformat(),
#                     "symbol": symbol,
#                     "data_type": "crypto"
#                 }
#                 await websocket.send_text(json.dumps(heartbeat))
                
#     except WebSocketDisconnect:
#         logger.info(f"🔌 Crypto Symbol WebSocket 연결 해제: {client_id} ({symbol})")
        
#     except Exception as e:
#         logger.error(f"❌ Crypto Symbol WebSocket 오류: {client_id} ({symbol}) - {e}")
        
#         try:
#             error_msg = create_error_message(
#                 error_code="CRYPTO_SYMBOL_ERROR",
#                 message=f"암호화폐 '{symbol}' 처리 오류: {str(e)}"
#             )
#             await websocket.send_text(error_msg.model_dump_json())
#         except:
#             pass
            
#     finally:
#         await websocket_manager.disconnect_symbol_subscriber(websocket, symbol, "crypto")
#         logger.info(f"🧹 Crypto Symbol WebSocket 정리 완료: {client_id} ({symbol})")

# # =========================
# # 🎯 대시보드 WebSocket 엔드포인트 (변화율 포함)
# # =========================

# @router.websocket("/dashboard")
# async def websocket_dashboard(websocket: WebSocket):
#     """
#     대시보드용 통합 실시간 데이터 WebSocket (변화율 포함)
    
#     주요 지표들의 요약 데이터를 전송합니다:
#     - Top 10 상승 주식 (변화율 포함)
#     - Top 10 상승 암호화폐
#     - 주요 SP500 주식 (AAPL, TSLA, GOOGL 등, 변화율 포함)
    
#     🆕 모든 주식 데이터에 전날 종가 기준 변화율이 포함됩니다.
#     """
#     await websocket.accept()
#     client_id = id(websocket)
#     client_ip = websocket.client.host if websocket.client else "unknown"
    
#     logger.info(f"🔗 Dashboard WebSocket 연결 (변화율 포함): {client_id} ({client_ip})")
    
#     try:
#         # WebSocket 매니저에 클라이언트 등록
#         await websocket_manager.connect_dashboard(websocket)
        
#         # 연결 성공 메시지 전송
#         status_msg = StatusMessage(
#             status="connected",
#             connected_clients=len(websocket_manager.dashboard_subscribers),
#             subscription_info={
#                 "type": "dashboard",
#                 "update_interval": "500ms",
#                 "data_includes": ["top_gainers", "crypto", "major_sp500"],
#                 "features": ["real_time_prices", "change_calculation", "dashboard_summary"]
#             }
#         )
#         await websocket.send_text(status_msg.model_dump_json())
        
#         # 🎯 대시보드 스트리머 시작 (변화율 포함)
#         asyncio.create_task(_start_dashboard_streaming_with_changes(websocket, client_id))
        
#         # 클라이언트 메시지 대기
#         while True:
#             try:
#                 data = await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
#                 logger.debug(f"📨 Dashboard 클라이언트 메시지 수신: {client_id} - {data}")
                
#             except asyncio.TimeoutError:
#                 heartbeat = {
#                     "type": "heartbeat",
#                     "timestamp": datetime.now(pytz.UTC).isoformat(),
#                     "server_time": datetime.now(pytz.UTC).strftime("%Y-%m-%d %H:%M:%S UTC"),
#                     "data_type": "dashboard_with_changes"
#                 }
#                 await websocket.send_text(json.dumps(heartbeat))
                
#     except WebSocketDisconnect:
#         logger.info(f"🔌 Dashboard WebSocket 연결 해제: {client_id}")
        
#     except Exception as e:
#         logger.error(f"❌ Dashboard WebSocket 오류: {client_id} - {e}")
        
#         try:
#             error_msg = create_error_message(
#                 error_code="DASHBOARD_ERROR",
#                 message=f"대시보드 데이터 처리 오류: {str(e)}"
#             )
#             await websocket.send_text(error_msg.model_dump_json())
#         except:
#             pass
            
#     finally:
#         await websocket_manager.disconnect_dashboard(websocket)
#         logger.info(f"🧹 Dashboard WebSocket 정리 완료: {client_id}")

# async def _start_dashboard_streaming_with_changes(websocket: WebSocket, client_id: int):
#     """대시보드 통합 데이터 스트리밍 (변화율 포함)"""
#     last_data_hash = None
#     error_count = 0
#     max_errors = 5
    
#     logger.info(f"📡 Dashboard 통합 스트리밍 시작 (변화율 포함): {client_id}")
    
#     while True:
#         try:
#             # 🎯 변화율이 포함된 대시보드 데이터 조합
#             dashboard_data = await _get_dashboard_data_with_changes()
            
#             # 데이터 변경 감지
#             data_str = json.dumps(dashboard_data, sort_keys=True, default=str)
#             current_hash = hashlib.md5(data_str.encode()).hexdigest()
            
#             # 변경된 경우만 전송
#             if current_hash != last_data_hash:
#                 # 대시보드 업데이트 메시지 생성
#                 update_msg = {
#                     "type": "dashboard_update",
#                     "data": dashboard_data,
#                     "timestamp": datetime.now(pytz.UTC).isoformat(),
#                     "features": ["real_time_prices", "change_calculation", "integrated_dashboard"]
#                 }
                
#                 # WebSocket으로 전송
#                 await websocket.send_text(json.dumps(update_msg))
                
#                 last_data_hash = current_hash
#                 error_count = 0
                
#                 logger.debug(f"📊 Dashboard 변화율 데이터 전송: ({client_id})")
            
#             # 1초 대기 (대시보드는 조금 더 느리게)
#             await asyncio.sleep(1.0)
            
#         except Exception as e:
#             error_count += 1
#             logger.error(f"❌ Dashboard 스트리밍 오류: {client_id} - {e} (에러 {error_count}/{max_errors})")
            
#             if error_count >= max_errors:
#                 logger.error(f"💀 Dashboard 스트리밍 최대 에러 도달, 중단: {client_id}")
#                 break
            
#             await asyncio.sleep(2.0)

# async def _get_dashboard_data_with_changes():
#     """대시보드용 통합 데이터 조회 (변화율 포함)"""
#     try:
#         # 병렬로 데이터 조회
#         tasks = [
#             websocket_service.get_topgainers_from_redis_with_changes(limit=10),  # 상위 10개 상승주 (변화율 포함)
#             websocket_service.get_crypto_from_redis(limit=10),                    # 상위 10개 암호화폐
#             websocket_service.get_sp500_from_redis_with_changes(limit=15)         # SP500 15개 (변화율 포함)
#         ]
        
#         results = await asyncio.gather(*tasks, return_exceptions=True)
        
#         top_gainers = results[0] if not isinstance(results[0], Exception) else []
#         crypto_data = results[1] if not isinstance(results[1], Exception) else []
#         sp500_data = results[2] if not isinstance(results[2], Exception) else []
        
#         # 변화율 기준으로 정렬
#         if top_gainers:
#             top_gainers.sort(key=lambda x: x.get('change_percentage', 0), reverse=True)
        
#         # 대시보드 요약 통계
#         dashboard_summary = {
#             "top_gainers_count": len(top_gainers),
#             "crypto_count": len(crypto_data),
#             "sp500_count": len(sp500_data),
#             "market_status": websocket_service._get_market_status() if hasattr(websocket_service, '_get_market_status') else {"is_open": False},
#             "last_updated": datetime.now(pytz.UTC).isoformat()
#         }
        
#         return {
#             "summary": dashboard_summary,
#             "top_gainers": top_gainers[:10],
#             "crypto": crypto_data[:10],
#             "sp500_highlights": sp500_data[:15]
#         }
        
#     except Exception as e:
#         logger.error(f"❌ 대시보드 데이터 조회 실패: {e}")
#         return {
#             "summary": {"error": str(e)},
#             "top_gainers": [],
#             "crypto": [],
#             "sp500_highlights": []
#         }

# # =========================
# # HTTP 상태 조회 엔드포인트 (기존 유지)
# # =========================

# @router.get("/status", response_model=dict)
# async def get_websocket_status():
#     """
#     WebSocket 서비스 상태 조회
    
#     Returns:
#         dict: 서버 상태, 연결된 클라이언트 수, 통계 정보
#     """
#     try:
#         # 헬스 체크 수행
#         health_info = await websocket_service.health_check()
        
#         # TopGainers 서비스 헬스체크
#         topgainers_health = await topgainers_service.health_check()
        
#         # WebSocket 매니저 상태
#         manager_status = websocket_manager.get_status()
        
#         # Redis 스트리머 상태
#         streamer_status = redis_streamer.get_status() if redis_streamer else {"status": "not_initialized"}
        
#         return {
#             "timestamp": datetime.now(pytz.UTC).isoformat(),
#             "websocket_server": {
#                 "status": "running",
#                 "total_connections": manager_status["total_connections"],
#                 "topgainers_subscribers": manager_status["topgainers_subscribers"],
#                 "crypto_subscribers": manager_status["crypto_subscribers"],
#                 "sp500_subscribers": manager_status["sp500_subscribers"],
#                 "dashboard_subscribers": manager_status["dashboard_subscribers"],
#                 "symbol_subscribers": manager_status["symbol_subscribers"]
#             },
#             "realtime_service": health_info,
#             "topgainers_service": topgainers_health,
#             "redis_streamer": streamer_status,
#             "features": ["real_time_prices", "change_calculation", "previous_close_comparison"]
#         }
        
#     except Exception as e:
#         logger.error(f"❌ WebSocket 상태 조회 실패: {e}")
#         raise HTTPException(status_code=500, detail=f"상태 조회 실패: {str(e)}")

# @router.get("/topgainers/status", response_model=dict)
# async def get_topgainers_websocket_status():
#     """
#     TopGainers WebSocket 서비스 상태 조회 (변화율 기능 포함)
    
#     Returns:
#         dict: 서비스 상태, 연결된 클라이언트 수, 통계 정보
#     """
#     try:
#         # TopGainers 서비스 헬스체크
#         health_info = await topgainers_service.health_check()
        
#         # WebSocket 매니저 상태
#         manager_status = websocket_manager.get_status()
        
#         return {
#             "timestamp": datetime.now(pytz.UTC).isoformat(),
#             "websocket_server": {
#                 "status": "running",
#                 "topgainers_subscribers": manager_status.get("topgainers_subscribers", 0),
#                 "category_subscribers": len([k for k in manager_status.get("symbol_subscribers", {}) if "topgainers_category:" in k]),
#                 "symbol_subscribers": len([k for k in manager_status.get("symbol_subscribers", {}) if "topgainers_symbol:" in k])
#             },
#             "topgainers_service": health_info,
#             "performance": topgainers_service.get_service_stats(),
#             "features": ["real_time_prices", "change_calculation", "previous_close_comparison", "category_filtering"]
#         }
        
#     except Exception as e:
#         logger.error(f"❌ TopGainers WebSocket 상태 조회 실패: {e}")
#         raise HTTPException(status_code=500, detail=f"상태 조회 실패: {str(e)}")

# @router.get("/stats", response_model=dict)
# async def get_websocket_stats():
#     """
#     WebSocket 서비스 상세 통계 (변화율 기능 포함)
    
#     Returns:
#         dict: 상세 통계 정보
#     """
#     try:
#         stats = websocket_service.get_statistics() if hasattr(websocket_service, 'get_statistics') else {}
#         manager_stats = websocket_manager.get_detailed_stats()
        
#         return {
#             "timestamp": datetime.now(pytz.UTC).isoformat(),
#             "service_stats": stats,
#             "connection_stats": manager_stats,
#             "performance": {
#                 "average_response_time": "< 500ms",
#                 "data_freshness": "500ms",
#                 "uptime": "running",
#                 "change_calculation_enabled": True
#             },
#             "features": {
#                 "real_time_prices": True,
#                 "change_calculation": True,
#                 "previous_close_comparison": True,
#                 "category_filtering": True,
#                 "dashboard_integration": True
#             }
#         }
        
#     except Exception as e:
#         logger.error(f"❌ WebSocket 통계 조회 실패: {e}")
#         raise HTTPException(status_code=500, detail=f"통계 조회 실패: {str(e)}")

# # =========================
# # 서버 종료 시 정리 (기존 유지)
# # =========================

# async def shutdown_websocket_services():
#     """WebSocket 서비스 종료 시 정리 (main.py에서 호출)"""
#     global redis_streamer
    
#     logger.info("🛑 WebSocket 서비스 종료 시작...")
    
#     try:
#         # TopGainers 서비스 종료
#         await topgainers_service.shutdown()
        
#         # Redis 스트리머 종료
#         if redis_streamer:
#             await redis_streamer.shutdown()
        
#         # WebSocket 매니저 종료
#         await websocket_manager.shutdown_all_connections()
        
#         # WebSocket 서비스 종료
#         if hasattr(websocket_service, 'shutdown'):
#             await websocket_service.shutdown()
        
#         logger.info("✅ WebSocket 서비스 종료 완료")
        
#     except Exception as e:
#         logger.error(f"❌ WebSocket 서비스 종료 실패: {e}")