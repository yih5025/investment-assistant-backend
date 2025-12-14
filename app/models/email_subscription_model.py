# app/models/email_subscription_model.py
from sqlalchemy import Column, Integer, String, Boolean, DateTime, Index
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
from app.models.base import BaseModel
import logging

logger = logging.getLogger(__name__)

class EmailSubscription(BaseModel):
    """
    이메일 구독 테이블 ORM 모델
    
    실제 테이블명: email_subscriptions
    주간 실적 발표 알림 등 이메일 구독 정보를 저장합니다.
    """
    __tablename__ = "email_subscriptions"
    
    # Primary Key
    id = Column(Integer, primary_key=True, autoincrement=True,
               comment="자동 증가 Primary Key")
    
    # 구독 정보
    email = Column(String(255), nullable=False, index=True,
                  comment="구독자 이메일 주소")
    scope = Column(String(20), nullable=False, default='SP500',
                  comment="구독 범위 (SP500, NASDAQ 등)")
    
    # 구독 취소용 토큰
    unsubscribe_token = Column(UUID(as_uuid=True), nullable=True,
                              server_default=func.gen_random_uuid(),
                              comment="구독 취소용 고유 토큰")
    
    # 활성화 상태
    is_active = Column(Boolean, nullable=False, default=True,
                      comment="구독 활성화 여부")
    
    # 시스템 메타데이터
    created_at = Column(DateTime, nullable=False, server_default=func.now(),
                       comment="구독 생성 시간")
    
    # 인덱스 및 제약조건
    __table_args__ = (
        Index('idx_subs_active_scope', 'is_active', 'scope'),
        Index('idx_subs_email', 'email'),
        Index('idx_subs_token', 'unsubscribe_token'),
    )
    
    def __repr__(self):
        return f"<EmailSubscription(id={self.id}, email='{self.email}', scope='{self.scope}', is_active={self.is_active})>"
    
    def to_dict(self):
        """딕셔너리로 변환"""
        return {
            'id': self.id,
            'email': self.email,
            'scope': self.scope,
            'is_active': self.is_active,
            'created_at': self.created_at.isoformat() if self.created_at else None
        }

