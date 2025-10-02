# main.py - K3s 환경을 포함한 수정된 CORS 설정

from fastapi import FastAPI, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
import logging
import logging.config
import time
import json

from .config import settings, get_log_config
import os
from .database import test_db_connection
from .dependencies import verify_db_connection

# 로깅 설정
logging.config.dictConfig(get_log_config())
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info(f"{settings.app_name} v{settings.app_version} 시작 중...")
    if test_db_connection():
        logger.info("✅ 데이터베이스 연결 성공")
    else:
        logger.error("❌ 데이터베이스 연결 실패")
    
    # 분리된 WebSocket 서비스들 초기화
    try:
        # TopGainers WebSocket 초기화
        from .api.endpoints.topgainers_websocket_endpoint import initialize_topgainers_websocket_services
        await initialize_topgainers_websocket_services()
        logger.info("✅ TopGainers WebSocket 서비스 초기화 완료")
        
        # SP500 WebSocket 초기화
        from .api.endpoints.sp500_websocket_endpoint import initialize_sp500_websocket_services
        await initialize_sp500_websocket_services()
        logger.info("✅ SP500 WebSocket 서비스 초기화 완료")
        
        # Crypto WebSocket 초기화
        from .api.endpoints.crypto_websocket_endpoint import initialize_crypto_websocket_services
        await initialize_crypto_websocket_services()
        logger.info("✅ Crypto WebSocket 서비스 초기화 완료")
        
        logger.info("✅ 모든 WebSocket 서비스 초기화 완료")
    except Exception as e:
        logger.error(f"❌ WebSocket 서비스 초기화 실패: {e}")
    
    logger.info(f"🚀 서버가 http://{settings.host}:{settings.port} 에서 실행 중 입니다....")
    logger.info(f"📚 API 문서: http://{settings.host}:{settings.port}/docs")
    
    yield
    
    # Shutdown - 분리된 WebSocket 서비스들 종료
    logger.info("🛑 애플리케이션 종료 중...")
    try:
        # TopGainers WebSocket 종료
        from .api.endpoints.topgainers_websocket_endpoint import shutdown_topgainers_websocket_services
        await shutdown_topgainers_websocket_services()
        logger.info("✅ TopGainers WebSocket 서비스 종료 완료")
        
        # SP500 WebSocket 종료
        from .api.endpoints.sp500_websocket_endpoint import shutdown_sp500_websocket_services
        await shutdown_sp500_websocket_services()
        logger.info("✅ SP500 WebSocket 서비스 종료 완료")
        
        # Crypto WebSocket 종료
        from .api.endpoints.crypto_websocket_endpoint import shutdown_crypto_websocket_services
        await shutdown_crypto_websocket_services()
        logger.info("✅ Crypto WebSocket 서비스 종료 완료")
        
        logger.info("✅ 모든 WebSocket 서비스 종료 완료")
    except Exception as e:
        logger.error(f"❌ WebSocket 서비스 종료 실패: {e}")
    logger.info("✅ 정리 작업 완료")

# FastAPI 애플리케이션 생성
app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description="투자 도우미 서비스의 데이터 API",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    lifespan=lifespan,
    redirect_slashes=False
)

# 🔧 K3s 환경을 포함한 강화된 CORS 디버깅 미들웨어
@app.middleware("http")
async def cors_debug_middleware(request: Request, call_next):
    """CORS 요청 디버깅 및 처리"""
    
    origin = request.headers.get("origin")
    method = request.method
    host = request.headers.get("host", "")
    
    # 요청 정보 상세 로깅
    if origin:
        logger.info(f"🌍 CORS 요청: {method} {request.url}")
        logger.info(f"   Origin: {origin}")
        logger.info(f"   Host: {host}")
        logger.info(f"   Headers: {dict(request.headers)}")
    
    # OPTIONS 요청 (Preflight) 특별 처리 - 즉시 반환
    if method == "OPTIONS":
        logger.info(f"✈️ Preflight 요청 즉시 처리: {request.url}")
        
        response = JSONResponse(
            content={"message": "CORS preflight OK"},
            status_code=200
        )
        
        # 🔧 CORS 헤더 설정 - 디버깅 강화
        if origin:
            if origin in cors_origins:
                response.headers["Access-Control-Allow-Origin"] = origin
                logger.info(f"✅ Preflight CORS 허용: {origin}")
            else:
                logger.warning(f"❌ Preflight CORS 거부: {origin} (허용되지 않은 오리진)")
                response.headers["Access-Control-Allow-Origin"] = "null"
        else:
            # Origin 헤더가 없는 경우 (내부 네트워크)
            logger.info("⚠️ Preflight Origin 헤더 없음")
            response.headers["Access-Control-Allow-Origin"] = "*"
            
        response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS, PATCH"
        response.headers["Access-Control-Allow-Headers"] = "*"
        response.headers["Access-Control-Allow-Credentials"] = "false"
        response.headers["Access-Control-Max-Age"] = "86400"
        
        logger.info(f"✅ Preflight 응답 헤더: {dict(response.headers)}")
        return response
    
    # 일반 요청 처리
    response = await call_next(request)
    
    # 응답에 CORS 헤더 추가 - 디버깅 강화
    if origin:
        # 허용된 오리진인지 확인
        if origin in cors_origins:
            response.headers["Access-Control-Allow-Origin"] = origin
            logger.info(f"✅ CORS 허용: {origin}")
        else:
            logger.warning(f"❌ CORS 거부: {origin} (허용되지 않은 오리진)")
            response.headers["Access-Control-Allow-Origin"] = "null"
    else:
        logger.info("⚠️ Origin 헤더 없음 - 내부 요청으로 처리")
        response.headers["Access-Control-Allow-Origin"] = "*"
        
    response.headers["Access-Control-Allow-Credentials"] = "false"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS, PATCH"
    response.headers["Access-Control-Allow-Headers"] = "*"
    
    logger.info(f"✅ 응답 CORS 헤더 추가: Origin={response.headers.get('Access-Control-Allow-Origin')}")
    
    return response

# 상세 API 로깅 미들웨어 (기존과 동일)
@app.middleware("http")
async def detailed_logging_middleware(request: Request, call_next):
    """상세한 API 요청/응답 로깅 미들웨어"""
    start_time = time.time()
    
    # 요청 정보 수집
    client_ip = request.client.host if request.client else "unknown"
    user_agent = request.headers.get("user-agent", "unknown")
    method = request.method
    url = str(request.url)
    query_params = dict(request.query_params)
    
    # 요청 로깅
    logger.info(f"📥 {method} {url} - IP: {client_ip}")
    if query_params:
        logger.info(f"   Query params: {json.dumps(query_params, ensure_ascii=False)}")
    
    # 요청 처리
    try:
        response = await call_next(request)
        process_time = time.time() - start_time
        
        # 응답 로깅
        if response.status_code >= 400:
            logger.warning(f"❌ {method} {url} - {response.status_code} ({process_time:.3f}s)")
            if response.status_code == 404:
                logger.warning(f"   🔍 404 상세: 경로 '{request.url.path}'를 찾을 수 없음")
        else:
            logger.info(f"✅ {method} {url} - {response.status_code} ({process_time:.3f}s)")
            
        return response
        
    except Exception as e:
        process_time = time.time() - start_time
        logger.error(f"💥 {method} {url} - ERROR ({process_time:.3f}s): {str(e)}")
        raise

# 🔧 환경별 CORS 설정 (보안 강화)
# 환경 확인
is_production = os.getenv("ENVIRONMENT", "development").lower() == "production"
is_development = not is_production

# 기본 허용 오리진 (프로덕션용)
production_origins = [
    "https://investment-assistant.vercel.app",
    "https://investment-assistant.site", 
    "https://api.investment-assistant.site",
    "https://www.weinvesting.site",
    "https://weinvesting.site"
]

# 개발용 오리진 (개발 환경에서만)
development_origins = [
    "http://localhost:3000",
    "http://localhost:5173", 
    "http://localhost:8888",
    "http://127.0.0.1:3000",
    "http://127.0.0.1:5173",
    "http://127.0.0.1:8888",
    "http://localhost:30333",
    "http://127.0.0.1:30333",
    "http://192.168.0.27:30333",
    "http://100.108.146.70:30333"
]

# 환경에 따른 오리진 설정
cors_origins = production_origins + (development_origins if is_development else [])

logger.info(f"🔒 CORS 보안 설정 - 환경: {'프로덕션' if is_production else '개발'}")
logger.info(f"🔒 허용된 오리진 수: {len(cors_origins)}")
logger.info(f"🔒 허용된 오리진 목록: {cors_origins}")
logger.info(f"🔧 환경변수 ENVIRONMENT: {os.getenv('ENVIRONMENT', 'not_set')}")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 🔥 임시로 모든 도메인 허용
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"]
)
# 루트 엔드포인트
@app.get("/", tags=["Root"])
async def root():
    """루트 경로 - API 기본 정보 제공"""
    return {
        "message": f"Welcome to {settings.app_name}!",
        "version": settings.app_version,
        "docs": "/docs",
        "redoc": "/redoc",
        "status": "running",
        "cors_status": "enabled_for_all_environments",
        "supported_origins": [
            "Vercel 도메인 (*.vercel.app)",
            "로컬 개발 환경 (localhost, 127.0.0.1)",
            "K8s 내부 네트워크 (192.168.x.x, 10.x.x.x, 172.x.x.x)",
            "커스텀 도메인 (*.investment-assistant.site)"
        ],
        "uvicorn_reload_test": "SUCCESS 4",
        "timestamp": "2025-08-14 16:30:00"
    }

# 헬스체크 엔드포인트
@app.get("/health", tags=["Health"])
async def health_check():
    """헬스체크 - 서비스 상태 확인"""
    db_status = "connected" if test_db_connection() else "disconnected"
    
    return {
        "status": "healthy" if db_status == "connected" else "unhealthy",
        "app_name": settings.app_name,
        "version": settings.app_version,
        "database": db_status,
        "debug_mode": settings.debug,
        "cors_status": "enabled_for_all_environments"
    }

# 🔧 강화된 CORS 테스트 엔드포인트
@app.get("/cors-test", tags=["Debug"])
async def cors_test(request: Request):
    """CORS 테스트 전용 엔드포인트"""
    origin = request.headers.get("origin", "No Origin")
    host = request.headers.get("host", "No Host")
    user_agent = request.headers.get("user-agent", "No User-Agent")
    
    return {
        "message": "CORS 테스트 성공! 🎉",
        "request_info": {
            "origin": origin,
            "host": host,
            "user_agent": user_agent,
            "client_ip": request.client.host if request.client else "unknown",
            "method": request.method,
            "url": str(request.url)
        },
        "cors_info": {
            "allowed_origins": "모든 환경 허용",
            "allowed_methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"],
            "credentials": False
        },
        "timestamp": time.time()
    }

# 상세 헬스체크 엔드포인트
@app.get("/health/detailed", tags=["Health"])
async def detailed_health_check(_: None = Depends(verify_db_connection)):
    """상세 헬스체크"""
    return {
        "status": "healthy",
        "app_name": settings.app_name,
        "version": settings.app_version,
        "database": "connected",
        "services": {
            "postgresql": "connected",
            "redis": "not_implemented",
        },
        "cors": {
            "status": "enabled",
            "environments": ["vercel", "k8s", "local", "custom_domain"]
        },
        "debug_mode": settings.debug
    }

# API 라우터 등록
from .api.api_v1 import api_router
app.include_router(api_router, prefix=settings.api_v1_prefix)

# 전역 예외 처리기
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """전역 예외 처리기"""
    logger.error(f"예상하지 못한 에러 발생: {str(exc)}", exc_info=True)
    
    if settings.debug:
        return JSONResponse(
            status_code=500,
            content={
                "error": "Internal Server Error",
                "detail": str(exc),
                "type": type(exc).__name__
            }
        )
    else:
        return JSONResponse(
            status_code=500,
            content={
                "error": "Internal Server Error",
                "detail": "서버에 오류가 발생했습니다. 잠시 후 다시 시도해주세요."
            }
        )

# 개발 환경용 디버그 정보
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
            "cors": {
                "status": "enabled_for_all_environments",
                "note": "모든 Origin 허용됨 (개발 환경)"
            }
        }