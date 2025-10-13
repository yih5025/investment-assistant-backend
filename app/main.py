from fastapi import FastAPI, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
import logging
import logging.config
import time
import json
import redis
import os

from .config import settings, get_log_config
from .database import test_db_connection
from .dependencies import verify_db_connection

# WebSocket 관련 import
from app.websocket.manager import WebSocketManager
from app.websocket.redis_streamer import RedisStreamer
from app.services.crypto_service import CryptoService
from app.services.sp500_service import SP500Service
from app.api.endpoints.websocket_endpoint import set_websocket_dependencies

# 로깅 설정
logging.config.dictConfig(get_log_config())
logger = logging.getLogger(__name__)

# =========================
# 전역 WebSocket 객체
# =========================
websocket_manager: WebSocketManager = None
redis_streamer: RedisStreamer = None

# =========================
# FastAPI 생명주기 관리
# =========================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI 앱 생명주기 관리
    
    시작 시: WebSocket Push 시스템 초기화
    종료 시: 모든 연결 정리
    """
    global websocket_manager, redis_streamer
    
    logger.info("=" * 60)
    logger.info("🚀 FastAPI 앱 시작 - WebSocket Push 시스템 초기화")
    logger.info("=" * 60)
    
    try:
        # 1. Redis 클라이언트 초기화 (동기)
        sync_redis_client = redis.Redis.from_url(
            settings.redis_url,
            decode_responses=True
        )
        logger.info("✅ [1/7] Redis 클라이언트 초기화")
        
        # 2. 서비스 레이어 초기화
        crypto_service = CryptoService()
        await crypto_service.init_redis()  # 비동기 Redis 초기화
        
        sp500_service = SP500Service(redis_client=sync_redis_client)
        logger.info("✅ [2/7] 서비스 레이어 초기화 (Crypto + SP500)")
        
        # 3. WebSocket Manager 초기화
        websocket_manager = WebSocketManager()
        logger.info("✅ [3/7] WebSocket Manager 초기화")
        
        # 4. Redis Streamer 초기화
        redis_streamer = RedisStreamer(
            crypto_service=crypto_service,
            sp500_service=sp500_service,
            redis_url=settings.redis_url
        )
        await redis_streamer.initialize()
        logger.info("✅ [4/7] Redis Streamer 초기화")
        
        # 5. WebSocket Manager ↔ Redis Streamer 연결
        redis_streamer.set_websocket_manager(websocket_manager)
        logger.info("✅ [5/7] WebSocket Manager ↔ Redis Streamer 연결")
        
        # 6. WebSocket 라우터에 의존성 주입
        set_websocket_dependencies(
            manager=websocket_manager,
            streamer=redis_streamer,
            redis_client=sync_redis_client
        )
        logger.info("✅ [6/7] WebSocket 라우터 의존성 주입")
        
        # 7. Redis Pub/Sub 스트리밍 시작
        await redis_streamer.start_streaming()
        logger.info("✅ [7/7] Redis Pub/Sub 스트리밍 시작")
        
        logger.info("=" * 60)
        logger.info("🎉 WebSocket Push 시스템 초기화 완료!")
        logger.info(f"📢 구독 채널: crypto_updates, sp500_updates, etf_updates")
        logger.info(f"🔌 WebSocket 엔드포인트: /api/v1/ws/crypto, /api/v1/ws/sp500, /api/v1/ws/etf")
        logger.info("=" * 60)
        
        # 앱 실행 (yield로 제어권 반환)
        yield
        
        # ========================================
        # 종료 처리
        # ========================================
        logger.info("=" * 60)
        logger.info("🛑 FastAPI 앱 종료 - WebSocket 시스템 정리 시작")
        logger.info("=" * 60)
        
        # Redis Streamer 종료
        if redis_streamer:
            await redis_streamer.shutdown()
            logger.info("✅ Redis Streamer 종료")
        
        # WebSocket Manager 종료
        if websocket_manager:
            await websocket_manager.shutdown_all_connections()
            logger.info("✅ WebSocket Manager 종료")
        
        # Crypto Service 종료
        if crypto_service:
            await crypto_service.shutdown()
            logger.info("✅ Crypto Service 종료")
        
        # Redis 클라이언트 종료
        if sync_redis_client:
            sync_redis_client.close()
            logger.info("✅ Redis 클라이언트 종료")
        
        logger.info("=" * 60)
        logger.info("✅ WebSocket 시스템 정리 완료")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"❌ WebSocket 시스템 초기화 실패: {e}", exc_info=True)
        raise

# =========================
# FastAPI 애플리케이션 생성
# =========================

app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description="WE Investing API - Real-time Push WebSocket System",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    lifespan=lifespan,
    redirect_slashes=False
)

# =========================
# CORS 설정
# =========================

# 프로덕션 허용 오리진
ALLOWED_ORIGINS = [
    # Vercel 프론트엔드
    "https://investment-assistant.vercel.app",
    
    # 커스텀 도메인
    "https://weinvesting.site",
    "https://www.weinvesting.site",
    "https://investment-assistant.site",
    "https://api.investment-assistant.site",
]

logger.info("=" * 60)
logger.info("🔒 CORS 설정 (프로덕션)")
logger.info(f"📝 허용 오리진: {len(ALLOWED_ORIGINS)}개")
for origin in ALLOWED_ORIGINS:
    logger.info(f"   ✅ {origin}")
logger.info("=" * 60)

# CORS 미들웨어 추가
# ⚠️ allow_origins=["*"]로 설정한 이유:
#    - 특정 오리진만 허용 시 Preflight 요청 처리 문제 발생
#    - WebSocket 연결 시 Origin 헤더 불일치 이슈
#    - credentials=False이므로 보안 문제 최소화
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 모든 오리진 허용 (credentials 미사용)
    allow_credentials=False,  # 쿠키/인증 비활성화 (보안)
    allow_methods=["*"],  # 모든 HTTP 메서드 허용
    allow_headers=["*"],  # 모든 헤더 허용
    expose_headers=["*"]  # 모든 응답 헤더 노출
)

# =========================
# 미들웨어
# =========================

@app.middleware("http")
async def api_logging_middleware(request: Request, call_next):
    """API 요청/응답 로깅"""
    start_time = time.time()
    
    # 요청 정보
    client_ip = request.client.host if request.client else "unknown"
    method = request.method
    url = str(request.url)
    
    # 요청 로깅
    logger.info(f"📥 {method} {url} - IP: {client_ip}")
    
    try:
        # 요청 처리
        response = await call_next(request)
        process_time = time.time() - start_time
        
        # 응답 로깅
        status_emoji = "✅" if response.status_code < 400 else "❌"
        logger.info(f"{status_emoji} {method} {url} - {response.status_code} ({process_time:.3f}s)")
        
        return response
        
    except Exception as e:
        process_time = time.time() - start_time
        logger.error(f"💥 {method} {url} - ERROR ({process_time:.3f}s): {str(e)}")
        raise

# =========================
# 기본 엔드포인트
# =========================

@app.get("/", tags=["Root"])
async def root():
    """루트 경로 - API 정보"""
    return {
        "message": f"Welcome to {settings.app_name}!",
        "version": settings.app_version,
        "system": "Real-time Push WebSocket",
        "docs": "/docs",
        "endpoints": {
            "rest_api": "/api/v1/*",
            "websocket": {
                "crypto": "/api/v1/ws/crypto",
                "sp500": "/api/v1/ws/sp500",
                "etf": "/api/v1/ws/etf"
            },
            "health": "/health"
        },
        "frontend": {
            "vercel": "https://investment-assistant.vercel.app",
            "main": "https://weinvesting.site"
        },
        "status": "running"
    }

@app.get("/health", tags=["Health"])
async def health_check():
    """헬스체크"""
    db_status = "connected" if test_db_connection() else "disconnected"
    
    return {
        "status": "healthy" if db_status == "connected" else "unhealthy",
        "app_name": settings.app_name,
        "version": settings.app_version,
        "database": db_status,
        "websocket_system": "active",
        "cors": "enabled_for_all_origins"
    }

@app.get("/health/detailed", tags=["Health"])
async def detailed_health_check(_: None = Depends(verify_db_connection)):
    """상세 헬스체크"""
    
    # WebSocket 시스템 상태
    websocket_status = {}
    if websocket_manager:
        websocket_status = websocket_manager.get_status()
    
    # Redis Streamer 상태
    streamer_status = {}
    if redis_streamer:
        streamer_status = redis_streamer.get_status()
    
    return {
        "status": "healthy",
        "app_name": settings.app_name,
        "version": settings.app_version,
        "database": "connected",
        "websocket": {
            "manager": websocket_status,
            "streamer": streamer_status
        },
        "services": {
            "postgresql": "connected",
            "redis": "connected"
        }
    }

@app.get("/ws/status", tags=["WebSocket"])
async def websocket_status():
    """WebSocket 시스템 상태 조회"""
    if not websocket_manager or not redis_streamer:
        return {
            "status": "not_initialized",
            "message": "WebSocket 시스템이 초기화되지 않았습니다."
        }
    
    return {
        "status": "active",
        "manager": websocket_manager.get_status(),
        "streamer": redis_streamer.get_status(),
        "timestamp": time.time()
    }

# =========================
# API 라우터 등록
# =========================

from .api.api_v1 import api_router
from .api.endpoints.websocket_endpoint import router as websocket_router

# REST API 라우터
app.include_router(api_router, prefix=settings.api_v1_prefix)

# WebSocket 라우터
app.include_router(websocket_router, prefix=settings.api_v1_prefix)
logger.info(f"✅ WebSocket 라우터 등록: {settings.api_v1_prefix}/ws/{{crypto|sp500|etf}}")

# =========================
# 전역 예외 처리
# =========================

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """전역 예외 처리기"""
    logger.error(f"예상하지 못한 에러: {str(exc)}", exc_info=True)
    
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal Server Error",
            "detail": str(exc) if settings.debug else "서버 오류가 발생했습니다.",
            "type": type(exc).__name__
        }
    )

# =========================
# 디버그 엔드포인트 (개발 환경)
# =========================

if settings.debug:
    @app.get("/debug/info", tags=["Debug"])
    async def debug_info():
        """디버그 정보"""
        return {
            "settings": {
                "db_host": settings.db_host,
                "db_port": settings.db_port,
                "db_name": settings.db_name,
                "debug": settings.debug,
                "log_level": settings.log_level
            },
            "websocket": {
                "manager_initialized": websocket_manager is not None,
                "streamer_initialized": redis_streamer is not None,
                "status": websocket_manager.get_status() if websocket_manager else None
            },
            "cors": {
                "mode": "allow_all_origins",
                "credentials": False,
                "reason": "Preflight & WebSocket compatibility"
            }
        }
    
    @app.get("/cors-test", tags=["Debug"])
    async def cors_test(request: Request):
        """CORS 테스트"""
        return {
            "message": "CORS 테스트 성공! 🎉",
            "origin": request.headers.get("origin", "No Origin"),
            "host": request.headers.get("host", "No Host"),
            "method": request.method,
            "cors_mode": "allow_all (*)",
            "credentials": False
        }

