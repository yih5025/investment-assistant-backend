from typing import Generator
from sqlalchemy.orm import Session
from fastapi import HTTPException, status

from .database import SessionLocal, test_db_connection
from .config import settings

def get_db() -> Generator[Session, None, None]:
    """
    데이터베이스 세션을 생성하고 반환하는 의존성 함수 ㅇ
    
    FastAPI의 Depends()와 함께 사용됩니다.
    
    동작 과정:
    1. 새로운 DB 세션 생성
    2. API 함수에 세션 전달 (yield)
    3. API 함수 완료 후 세션 자동 종료
    4. 예외 발생 시에도 안전하게 세션 정리
    
    Yields:
        Session: SQLAlchemy 데이터베이스 세션
        
    사용 예시:
        @router.get("/earnings")
        async def get_earnings(db: Session = Depends(get_db)):
            # db 세션 사용
            return service.get_data(db)
            
    Raises:
        HTTPException: 데이터베이스 연결 실패 시
    """
    db = SessionLocal()
    try:
        yield db
    except Exception:
        # 세션에서 예외 발생 시 롤백만 수행하고, 원래 예외를 그대로 전파하여
        # 실제 원인(응답 스키마 검증 실패 등)을 정확히 노출되도록 함
        db.rollback()
        raise
    finally:
        # 항상 세션 종료
        db.close()

def get_settings():
    """
    애플리케이션 설정을 반환하는 의존성 함수
    
    API 함수에서 설정값이 필요할 때 사용합니다.
    나중에 테스트 환경에서 다른 설정을 주입할 때도 유용합니다.
    
    Returns:
        Settings: 애플리케이션 설정 객체
        
    사용 예시:
        @router.get("/info")
        async def get_app_info(config: Settings = Depends(get_settings)):
            return {"app_name": config.app_name, "version": config.app_version}
    """
    return settings

async def verify_db_connection():
    """
    데이터베이스 연결 상태를 확인하는 의존성 함수
    
    중요한 API 호출 전에 DB 연결 상태를 미리 확인할 때 사용합니다.
    헬스체크 엔드포인트에서 주로 활용됩니다.
    
    Raises:
        HTTPException: 데이터베이스 연결 실패 시
        
    사용 예시:
        @router.get("/health")
        async def health_check(db_ok: None = Depends(verify_db_connection)):
            return {"status": "healthy", "database": "connected"}
    """
    if not test_db_connection():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="데이터베이스에 연결할 수 없습니다."
        )

def get_cache_key(prefix: str, *args) -> str:
    """
    캐시 키를 생성하는 유틸리티 함수
    
    Redis 캐싱에서 일관된 키 형식을 만들 때 사용합니다.
    의존성 함수는 아니지만 여러 서비스에서 공통으로 사용됩니다.
    
    Args:
        prefix: 캐시 키 접두사 (예: "earnings", "crypto")
        *args: 키를 구성할 추가 인자들
        
    Returns:
        str: 생성된 캐시 키
        
    사용 예시:
        cache_key = get_cache_key("earnings", "AAPL", "2025-08-01")
        # 결과: "investment_api:earnings:AAPL:2025-08-01"
    """
    app_prefix = settings.app_name.lower().replace(" ", "_")
    key_parts = [app_prefix, prefix] + [str(arg) for arg in args]
    return ":".join(key_parts)

# === 향후 확장을 위한 준비 (현재는 사용하지 않음) ===

def get_current_user():
    """
    현재 로그인한 사용자 정보를 반환하는 의존성 함수
    
    Django에서 사용자 인증을 처리하므로 현재는 구현하지 않습니다.
    나중에 JWT 토큰 검증이 필요한 경우 구현할 예정입니다.
    
    TODO: Django JWT 토큰 검증 로직 구현
    """
    # 현재는 구현하지 않음 (Django에서 사용자 관리)
    pass

def require_api_key():
    """
    API 키 인증을 확인하는 의존성 함수
    
    특정 API에 대해 API 키 기반 인증이 필요한 경우 사용할 예정입니다.
    
    TODO: API 키 검증 로직 구현
    """
    # 현재는 구현하지 않음 (필요시 구현)
    pass