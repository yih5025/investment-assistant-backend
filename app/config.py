import os
from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    """
    애플리케이션 전체 설정을 관리하는 클래스
    
    Pydantic의 BaseSettings를 사용하여:
    1. 환경 변수 자동 로딩
    2. 타입 검증
    3. 기본값 설정
    4. 설정 값 검증
    
    환경 변수는 다음 순서로 읽어집니다:
    1. 실제 환경 변수
    2. .env 파일
    3. 기본값
    """
    
    # === 애플리케이션 기본 설정 ===
    app_name: str = Field("Investment Assistant API", description="애플리케이션 이름")
    app_version: str = Field("1.0.0", description="애플리케이션 버전")
    debug: bool = Field(False, description="디버그 모드")
    
    # === 서버 설정 ===
    host: str = Field("0.0.0.0", description="서버 호스트")
    port: int = Field(8888, description="서버 포트")
    
    # === 데이터베이스 설정 ===
    # K3s 환경에서 PostgreSQL 서비스 주소
    db_host: str = Field(
        "postgresql.investment-assistant.svc.cluster.local", 
        description="데이터베이스 호스트"
    )
    db_port: int = Field(5432, description="데이터베이스 포트")
    db_user: str = Field("airflow", description="데이터베이스 사용자")
    db_password: str = Field("airflow123", description="데이터베이스 비밀번호")
    db_name: str = Field("investment_db", description="데이터베이스 이름")
    
    # === Redis 설정 ===
    # K3s 환경에서 Redis 서비스 주소
    redis_host: str = Field(
        "redis.investment-assistant.svc.cluster.local",
        description="Redis 호스트"
    )
    redis_port: int = Field(6379, description="Redis 포트")
    redis_db: int = Field(0, description="Redis 데이터베이스 번호")
    redis_password: Optional[str] = Field(None, description="Redis 비밀번호")
    
    # === CORS 설정 ===
    allowed_origins: list = Field(
        [
            "*",  # k3s NodePort(프론트 30333) 등 다양한 오리진 허용 (운영 시 환경변수로 제한 권장)cc
        ],
        description="CORS 허용 오리진"
    )
    
    # === API 설정 ===
    api_v1_prefix: str = Field("/api/v1", description="API v1 경로 접두사")    
    # === 로깅 설정 ===
    log_level: str = Field("WARNING", description="로그 레벨")
    
    # === 보안 설정 ===
    secret_key: str = Field(
        "asdaskdljaksldpoqkwekdljaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldjaskldj",
        description="JWT 및 암호화용 비밀 키"
    )
    
    # === 캐시 설정 ===
    cache_ttl: int = Field(60, description="캐시 TTL (초) - 실시간성 향상을 위해 60초로 단축")
    
    @property
    def database_url(self) -> str:
        """
        PostgreSQL 연결 URL을 생성합니다.
        
        SQLAlchemy에서 사용할 수 있는 형태의 연결 문자열을 반환합니다.
        형식: postgresql://user:password@host:port/database
        
        Returns:
            str: PostgreSQL 연결 URL
        """
        return f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
    
    @property
    def async_database_url(self) -> str:
        """
        비동기 PostgreSQL 연결 URL을 생성합니다.
        
        asyncpg 드라이버를 사용하는 비동기 연결용입니다.
        현재는 사용하지 않지만 향후 확장을 위해 준비해둡니다.
        
        Returns:
            str: 비동기 PostgreSQL 연결 URL
        """
        return f"postgresql+asyncpg://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
    
    @property
    def redis_url(self) -> str:
        """
        Redis 연결 URL을 생성합니다.
        
        Redis 클라이언트 라이브러리에서 사용할 수 있는 형태의 연결 문자열입니다.
        
        Returns:
            str: Redis 연결 URL
        """
        if self.redis_password:
            return f"redis://:{self.redis_password}@{self.redis_host}:{self.redis_port}/{self.redis_db}"
        else:
            return f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}"
    
    def get_db_config(self) -> dict:
        """
        데이터베이스 설정을 딕셔너리로 반환합니다.
        
        SQLAlchemy 엔진 생성 시 사용할 설정들을 반환합니다.
        
        Returns:
            dict: 데이터베이스 설정 딕셔너리
        """
        return {
            "host": self.db_host,
            "port": self.db_port,
            "user": self.db_user,
            "password": self.db_password,
            "database": self.db_name,
        }
    
    class Config:
        """
        Pydantic 설정 클래스
        
        - env_file: .env 파일에서 환경 변수 읽기
        - case_sensitive: 환경 변수 대소문자 구분하지 않음
        """
        env_file = ".env"
        case_sensitive = False
        
        # 환경 변수 예시를 API 문서에 표시
        schema_extra = {
            "example": {
                "db_host": "postgresql.investment-assistant.svc.cluster.local",
                "db_port": 5432,
                "db_user": "airflow",
                "db_name": "investment_db",
                "redis_host": "redis.investment-assistant.svc.cluster.local",
                "redis_port": 6379,
                "debug": False
            }
        }

# 전역 설정 인스턴스 생성
# 애플리케이션 전체에서 이 인스턴스를 사용합니다
settings = Settings()

def get_settings() -> Settings:
    """
    설정 인스턴스를 반환하는 팩토리 함수
    
    FastAPI의 의존성 주입에서 사용할 수 있습니다.
    나중에 테스트 환경에서 다른 설정을 주입할 때 유용합니다.
    
    Returns:
        Settings: 설정 인스턴스
    """
    return settings

# === 개발/운영 환경별 설정 ===
def is_development() -> bool:
    """개발 환경 여부를 확인합니다."""
    return settings.debug

def is_production() -> bool:
    """운영 환경 여부를 확인합니다."""
    return not settings.debug

# === 로깅 설정 ===
def get_log_config() -> dict:
    """
    로깅 설정을 반환합니다.
    
    uvicorn과 Python 표준 로깅 모듈에서 사용할 설정입니다.
    
    Returns:
        dict: 로깅 설정 딕셔너리
    """
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            },
        },
        "handlers": {
            "default": {
                "formatter": "default",
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stdout",
            },
        },
        "root": {
            "level": settings.log_level,
            "handlers": ["default"],
        },
    }