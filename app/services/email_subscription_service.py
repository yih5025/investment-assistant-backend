# app/services/email_subscription_service.py
from sqlalchemy.orm import Session
from sqlalchemy import and_
from typing import Optional, Dict, Any
from datetime import datetime
import uuid
import logging

from app.models.email_subscription_model import EmailSubscription

logger = logging.getLogger(__name__)

class EmailSubscriptionService:
    """이메일 구독 관련 비즈니스 로직 서비스"""
    
    def __init__(self, db: Session):
        self.db = db
    
    def subscribe(self, email: str, scope: str = 'SP500') -> Dict[str, Any]:
        """
        이메일 구독 추가
        
        Args:
            email: 구독할 이메일 주소
            scope: 구독 범위 (SP500, NASDAQ 등)
            
        Returns:
            Dict[str, Any]: 구독 결과
        """
        try:
            email = email.lower().strip()
            
            # 기존 구독 확인
            existing = self.db.query(EmailSubscription).filter(
                and_(
                    EmailSubscription.email == email,
                    EmailSubscription.scope == scope
                )
            ).first()
            
            if existing:
                if existing.is_active:
                    return {
                        'success': False,
                        'message': '이미 구독 중인 이메일입니다.',
                        'email': email,
                        'scope': scope
                    }
                else:
                    # 비활성화된 구독 재활성화
                    existing.is_active = True
                    self.db.commit()
                    logger.info(f"✅ 구독 재활성화: {email} ({scope})")
                    return {
                        'success': True,
                        'message': '구독이 다시 활성화되었습니다.',
                        'email': email,
                        'scope': scope
                    }
            
            # 새 구독 생성
            new_subscription = EmailSubscription(
                email=email,
                scope=scope,
                is_active=True
            )
            
            self.db.add(new_subscription)
            self.db.commit()
            self.db.refresh(new_subscription)
            
            logger.info(f"✅ 새 구독 생성: {email} ({scope})")
            
            return {
                'success': True,
                'message': '구독이 완료되었습니다. 매주 일요일에 실적 발표 일정을 보내드립니다.',
                'email': email,
                'scope': scope
            }
            
        except Exception as e:
            self.db.rollback()
            logger.error(f"❌ 구독 생성 실패: {email} - {e}")
            return {
                'success': False,
                'message': f'구독 처리 중 오류가 발생했습니다: {str(e)}',
                'email': email,
                'scope': scope
            }
    
    def unsubscribe_by_token(self, token: str) -> Dict[str, Any]:
        """
        토큰을 사용하여 구독 취소
        
        Args:
            token: 구독 취소 토큰 (UUID)
            
        Returns:
            Dict[str, Any]: 구독 취소 결과
        """
        try:
            # UUID 형식 검증
            try:
                token_uuid = uuid.UUID(token)
            except ValueError:
                return {
                    'success': False,
                    'message': '유효하지 않은 토큰입니다.'
                }
            
            # 토큰으로 구독 조회
            subscription = self.db.query(EmailSubscription).filter(
                EmailSubscription.unsubscribe_token == token_uuid
            ).first()
            
            if not subscription:
                return {
                    'success': False,
                    'message': '해당 토큰에 대한 구독 정보를 찾을 수 없습니다.'
                }
            
            if not subscription.is_active:
                return {
                    'success': True,
                    'message': '이미 구독이 취소된 상태입니다.'
                }
            
            # 구독 비활성화
            subscription.is_active = False
            self.db.commit()
            
            logger.info(f"✅ 구독 취소 (토큰): {subscription.email}")
            
            return {
                'success': True,
                'message': '구독이 성공적으로 취소되었습니다.'
            }
            
        except Exception as e:
            self.db.rollback()
            logger.error(f"❌ 구독 취소 실패 (토큰): {e}")
            return {
                'success': False,
                'message': f'구독 취소 중 오류가 발생했습니다: {str(e)}'
            }
    
    def unsubscribe_by_email(self, email: str, scope: str = 'SP500') -> Dict[str, Any]:
        """
        이메일을 사용하여 구독 취소
        
        Args:
            email: 구독 취소할 이메일
            scope: 구독 범위
            
        Returns:
            Dict[str, Any]: 구독 취소 결과
        """
        try:
            email = email.lower().strip()
            
            subscription = self.db.query(EmailSubscription).filter(
                and_(
                    EmailSubscription.email == email,
                    EmailSubscription.scope == scope
                )
            ).first()
            
            if not subscription:
                return {
                    'success': False,
                    'message': '해당 이메일의 구독 정보를 찾을 수 없습니다.'
                }
            
            if not subscription.is_active:
                return {
                    'success': True,
                    'message': '이미 구독이 취소된 상태입니다.'
                }
            
            # 구독 비활성화
            subscription.is_active = False
            self.db.commit()
            
            logger.info(f"✅ 구독 취소 (이메일): {email} ({scope})")
            
            return {
                'success': True,
                'message': '구독이 성공적으로 취소되었습니다.'
            }
            
        except Exception as e:
            self.db.rollback()
            logger.error(f"❌ 구독 취소 실패 (이메일): {email} - {e}")
            return {
                'success': False,
                'message': f'구독 취소 중 오류가 발생했습니다: {str(e)}'
            }
    
    def get_subscription_status(self, email: str, scope: str = 'SP500') -> Dict[str, Any]:
        """
        이메일 구독 상태 조회
        
        Args:
            email: 조회할 이메일
            scope: 구독 범위
            
        Returns:
            Dict[str, Any]: 구독 상태 정보
        """
        try:
            email = email.lower().strip()
            
            subscription = self.db.query(EmailSubscription).filter(
                and_(
                    EmailSubscription.email == email,
                    EmailSubscription.scope == scope
                )
            ).first()
            
            if not subscription:
                return {
                    'email': email,
                    'is_subscribed': False,
                    'scope': scope,
                    'subscribed_at': None
                }
            
            return {
                'email': email,
                'is_subscribed': subscription.is_active,
                'scope': subscription.scope,
                'subscribed_at': subscription.created_at
            }
            
        except Exception as e:
            logger.error(f"❌ 구독 상태 조회 실패: {email} - {e}")
            return {
                'email': email,
                'is_subscribed': False,
                'scope': scope,
                'subscribed_at': None,
                'error': str(e)
            }
    
    def get_active_subscribers_count(self, scope: str = 'SP500') -> int:
        """
        활성 구독자 수 조회
        
        Args:
            scope: 구독 범위
            
        Returns:
            int: 활성 구독자 수
        """
        try:
            count = self.db.query(EmailSubscription).filter(
                and_(
                    EmailSubscription.is_active == True,
                    EmailSubscription.scope == scope
                )
            ).count()
            return count
        except Exception as e:
            logger.error(f"❌ 구독자 수 조회 실패: {e}")
            return 0

