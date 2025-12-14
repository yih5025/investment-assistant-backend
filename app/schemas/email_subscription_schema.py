# app/schemas/email_subscription_schema.py
from pydantic import BaseModel, Field, field_validator, EmailStr
from typing import Optional
from datetime import datetime
from enum import Enum
import re

class SubscriptionScope(str, Enum):
    """구독 범위 열거형"""
    SP500 = "SP500"
    NASDAQ = "NASDAQ"
    ALL = "ALL"

# =========================
# 요청 스키마
# =========================

class EmailSubscriptionRequest(BaseModel):
    """이메일 구독 요청 (Double Opt-in)"""
    email: EmailStr = Field(..., description="구독할 이메일 주소", example="user@example.com")
    scope: SubscriptionScope = Field(default=SubscriptionScope.SP500, description="구독 범위")
    agreed: bool = Field(default=True, description="개인정보 수집/이용 동의 여부")
    
    @field_validator('email')
    @classmethod
    def validate_email(cls, v):
        """이메일 형식 추가 검증"""
        if not v or len(v) > 255:
            raise ValueError('유효한 이메일 주소를 입력해주세요')
        return v.lower().strip()

class VerifyEmailRequest(BaseModel):
    """이메일 인증 요청"""
    token: str = Field(..., description="이메일 인증 토큰", example="550e8400-e29b-41d4-a716-446655440000")

class UnsubscribeRequest(BaseModel):
    """구독 취소 요청 (토큰 기반)"""
    token: str = Field(..., description="구독 취소 토큰", example="550e8400-e29b-41d4-a716-446655440000")

class UnsubscribeByEmailRequest(BaseModel):
    """구독 취소 요청 (이메일 기반)"""
    email: EmailStr = Field(..., description="구독 취소할 이메일 주소", example="user@example.com")
    scope: SubscriptionScope = Field(default=SubscriptionScope.SP500, description="구독 취소할 범위")

# =========================
# 응답 스키마
# =========================

class EmailSubscriptionResponse(BaseModel):
    """이메일 구독 응답 (Double Opt-in)"""
    success: bool = Field(..., description="성공 여부")
    message: str = Field(..., description="응답 메시지")
    email: Optional[str] = Field(None, description="구독된 이메일")
    scope: Optional[str] = Field(None, description="구독 범위")
    requires_verification: bool = Field(default=False, description="이메일 인증 필요 여부")
    
    class Config:
        from_attributes = True

class VerifyEmailResponse(BaseModel):
    """이메일 인증 응답"""
    success: bool = Field(..., description="성공 여부")
    message: str = Field(..., description="응답 메시지")
    email: Optional[str] = Field(None, description="인증된 이메일")
    
    class Config:
        from_attributes = True

class UnsubscribeResponse(BaseModel):
    """구독 취소 응답"""
    success: bool = Field(..., description="성공 여부")
    message: str = Field(..., description="응답 메시지")

class SubscriptionStatusResponse(BaseModel):
    """구독 상태 조회 응답"""
    email: str = Field(..., description="이메일 주소")
    is_subscribed: bool = Field(..., description="구독 여부")
    scope: Optional[str] = Field(None, description="구독 범위")
    subscribed_at: Optional[datetime] = Field(None, description="구독 시작 시간")
    
    class Config:
        from_attributes = True

# =========================
# 에러 응답 스키마
# =========================

class SubscriptionErrorResponse(BaseModel):
    """구독 에러 응답"""
    success: bool = Field(default=False, description="성공 여부")
    error: str = Field(..., description="에러 메시지")
    error_code: Optional[str] = Field(None, description="에러 코드")

