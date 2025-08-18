from typing import List, Generic, TypeVar, Optional
from pydantic import BaseModel, Field

# 제네릭 타입 변수 정의
T = TypeVar('T')

class PaginatedResponse(BaseModel):
    """
    페이지네이션이 포함된 응답을 위한 공통 스키마
    
    상속받는 클래스에서 items 필드를 재정의하여 사용합니다.
    """
    total_count: int = Field(..., description="전체 항목 수", example=150)
    page: Optional[int] = Field(None, description="현재 페이지 번호", example=1)
    page_size: Optional[int] = Field(None, description="페이지당 항목 수", example=50)
    total_pages: Optional[int] = Field(None, description="전체 페이지 수", example=3)
    has_next: Optional[bool] = Field(None, description="다음 페이지 존재 여부")
    has_previous: Optional[bool] = Field(None, description="이전 페이지 존재 여부")

class GenericPaginatedResponse(BaseModel, Generic[T]):
    """
    제네릭 타입을 사용한 페이지네이션 응답 스키마
    
    타입 안전성이 필요한 경우 사용합니다.
    """
    items: List[T] = Field(..., description="응답 데이터 목록")
    total_count: int = Field(..., description="전체 항목 수", example=150)
    page: Optional[int] = Field(None, description="현재 페이지 번호", example=1)
    page_size: Optional[int] = Field(None, description="페이지당 항목 수", example=50)
    total_pages: Optional[int] = Field(None, description="전체 페이지 수", example=3)
    has_next: Optional[bool] = Field(None, description="다음 페이지 존재 여부")
    has_previous: Optional[bool] = Field(None, description="이전 페이지 존재 여부")

class SimpleListResponse(BaseModel, Generic[T]):
    """
    간단한 목록 응답을 위한 공통 스키마
    
    페이지네이션이 필요하지 않은 단순한 목록 API에서 사용합니다.
    """
    items: List[T] = Field(..., description="응답 데이터 목록")
    total_count: int = Field(..., description="전체 항목 수")

class ErrorResponse(BaseModel):
    """
    에러 응답을 위한 공통 스키마
    """
    error: str = Field(..., description="에러 메시지")
    detail: Optional[str] = Field(None, description="상세 에러 정보")
    error_code: Optional[str] = Field(None, description="에러 코드")

class SuccessResponse(BaseModel):
    """
    성공 응답을 위한 공통 스키마
    """
    message: str = Field(..., description="성공 메시지")
    data: Optional[dict] = Field(None, description="추가 데이터")

class HealthResponse(BaseModel):
    """
    헬스체크 응답을 위한 공통 스키마
    """
    status: str = Field(..., description="서비스 상태", example="healthy")
    timestamp: str = Field(..., description="체크 시간")
    services: Optional[dict] = Field(None, description="의존 서비스 상태")

class APIResponse(BaseModel):
    """
    일반적인 API 응답을 위한 공통 스키마
    """
    success: bool = Field(..., description="성공 여부")
    message: str = Field(..., description="응답 메시지")
    data: Optional[dict] = Field(None, description="응답 데이터") 