# app/services/email_subscription_service.py
from sqlalchemy.orm import Session
from sqlalchemy import and_, text
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime, timedelta
import uuid
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os

from app.models.email_subscription_model import EmailSubscription

logger = logging.getLogger(__name__)

# SMTP 설정 (환경변수에서 가져옴 - Kubernetes Deployment에서 설정)
SMTP_HOST = os.getenv('SMTP_HOST', 'smtp.gmail.com')
SMTP_PORT = int(os.getenv('SMTP_PORT', '587'))
SMTP_USER = os.getenv('SMTP_USER', '')  # 환경변수 필수
SMTP_PASSWORD = os.getenv('SMTP_PASSWORD', '')  # 환경변수 필수

# 인증 토큰 유효 기간 (24시간)
VERIFICATION_TOKEN_EXPIRE_HOURS = 24

# 서비스 URL
FRONTEND_URL = os.getenv('FRONTEND_URL', 'https://investment-assistant.site')
API_URL = os.getenv('API_URL', 'https://api.investment-assistant.site/api/v1')


class EmailSubscriptionService:
    """이메일 구독 관련 비즈니스 로직 서비스 (Double Opt-in 지원)"""
    
    def __init__(self, db: Session):
        self.db = db
    
    def _get_upcoming_earnings(self) -> List[Tuple]:
        """
        오늘부터 7일 후까지의 실적 발표 일정 조회
        
        Returns:
            List[Tuple]: [(report_date, symbol, company_name, estimate, gics_sector), ...]
        """
        try:
            today = datetime.now().date()
            end_date = today + timedelta(days=7)
            
            sql = text("""
                SELECT 
                    ec.report_date,
                    ec.symbol,
                    sp.company_name,
                    ec.estimate,
                    sp.gics_sector
                FROM earnings_calendar ec
                JOIN sp500_companies sp ON ec.symbol = sp.symbol
                WHERE ec.report_date BETWEEN :start_date AND :end_date
                ORDER BY ec.report_date ASC, sp.market_cap DESC
            """)
            
            result = self.db.execute(sql, {"start_date": today, "end_date": end_date})
            return result.fetchall()
            
        except Exception as e:
            logger.error(f"❌ 실적 발표 일정 조회 실패: {e}")
            return []
    
    def _send_earnings_notification_email(self, email: str, unsubscribe_token: str) -> bool:
        """
        실적 발표 알림 이메일 발송
        
        Args:
            email: 수신자 이메일
            unsubscribe_token: 구독 취소 토큰
            
        Returns:
            bool: 발송 성공 여부
        """
        try:
            # 실적 발표 일정 조회
            earnings_data = self._get_upcoming_earnings()
            
            if not earnings_data:
                logger.info(f"📭 향후 7일간 실적 발표 일정 없음 - {email}에게 이메일 미발송")
                return False
            
            today = datetime.now().date()
            end_date = today + timedelta(days=7)
            unsubscribe_link = f"{API_URL}/email-subscription/unsubscribe?token={unsubscribe_token}"
            
            # 이메일 본문 생성
            rows_html = ""
            for row in earnings_data:
                r_date = row[0]
                symbol = row[1]
                name = row[2]
                est = row[3] if row[3] is not None else '-'
                sector = row[4] if row[4] else '-'
                
                rows_html += f"""
                    <tr>
                        <td style="padding: 12px; border-bottom: 1px solid #eee;">{r_date}</td>
                        <td style="padding: 12px; border-bottom: 1px solid #eee;"><b>{symbol}</b></td>
                        <td style="padding: 12px; border-bottom: 1px solid #eee;">{name}</td>
                        <td style="padding: 12px; border-bottom: 1px solid #eee;">{sector}</td>
                        <td style="padding: 12px; border-bottom: 1px solid #eee;">{est}</td>
                    </tr>
                """
            
            html_content = f"""
            <!DOCTYPE html>
            <html lang="ko">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
            </head>
            <body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 0; padding: 20px; background-color: #f5f5f5;">
                <div style="max-width: 600px; margin: 0 auto; background: white; border-radius: 16px; overflow: hidden; box-shadow: 0 4px 20px rgba(0,0,0,0.1);">
                    <!-- 헤더 -->
                    <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 30px; text-align: center;">
                        <h1 style="color: white; margin: 0; font-size: 22px;">📅 향후 7일간 S&P 500 실적 발표 일정</h1>
                    </div>
                    
                    <!-- 본문 -->
                    <div style="padding: 25px;">
                        <p style="color: #333; font-size: 15px; line-height: 1.6; margin-bottom: 20px;">
                            안녕하세요!<br><br>
                            <strong>{today}</strong>부터 <strong>{end_date}</strong>까지 예정된 주요 기업의 실적 발표 일정입니다.
                        </p>
                        
                        <table style="width: 100%; border-collapse: collapse; font-size: 14px;">
                            <thead>
                                <tr style="background: #f8f9fa;">
                                    <th style="padding: 12px; text-align: left; border-bottom: 2px solid #ddd;">날짜</th>
                                    <th style="padding: 12px; text-align: left; border-bottom: 2px solid #ddd;">티커</th>
                                    <th style="padding: 12px; text-align: left; border-bottom: 2px solid #ddd;">기업명</th>
                                    <th style="padding: 12px; text-align: left; border-bottom: 2px solid #ddd;">섹터</th>
                                    <th style="padding: 12px; text-align: left; border-bottom: 2px solid #ddd;">예상 EPS</th>
                                </tr>
                            </thead>
                            <tbody>
                                {rows_html}
                            </tbody>
                        </table>
                    </div>
                    
                    <!-- 푸터 -->
                    <div style="background: #f8f9fa; padding: 20px; text-align: center;">
                        <p style="color: #999; font-size: 12px; margin: 0;">
                            본 메일은 투자 정보 제공을 위해 발송되었습니다.<br>
                            더 이상 알림을 원치 않으시면 <a href="{unsubscribe_link}" style="color: #667eea;">여기</a>를 클릭하여 구독을 취소하세요.
                        </p>
                        <p style="color: #bbb; font-size: 11px; margin-top: 10px;">
                            © 2024 WE INVESTING | 주간 실적 발표 알림 서비스
                        </p>
                    </div>
                </div>
            </body>
            </html>
            """
            
            msg = MIMEMultipart('alternative')
            msg['Subject'] = f'[WE INVESTING] 향후 7일간 S&P 500 실적 발표 ({today} ~ {end_date})'
            msg['From'] = SMTP_USER
            msg['To'] = email
            
            html_part = MIMEText(html_content, 'html', 'utf-8')
            msg.attach(html_part)
            
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as server:
                server.starttls()
                server.login(SMTP_USER, SMTP_PASSWORD)
                server.sendmail(SMTP_USER, [email], msg.as_string())
            
            logger.info(f"✅ 실적 발표 알림 이메일 발송 완료: {email} ({len(earnings_data)}개 일정)")
            return True
            
        except Exception as e:
            logger.error(f"❌ 실적 발표 알림 이메일 발송 실패: {email} - {e}")
            return False
    
    def _send_verification_email(self, email: str, verification_token: str) -> bool:
        """
        인증 이메일 발송
        
        Args:
            email: 수신자 이메일
            verification_token: 인증 토큰
            
        Returns:
            bool: 발송 성공 여부
        """
        try:
            verify_link = f"{API_URL}/email-subscription/verify?token={verification_token}"
            
            html_content = f"""
            <!DOCTYPE html>
            <html lang="ko">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
            </head>
            <body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 0; padding: 20px; background-color: #f5f5f5;">
                <div style="max-width: 500px; margin: 0 auto; background: white; border-radius: 16px; overflow: hidden; box-shadow: 0 4px 20px rgba(0,0,0,0.1);">
                    <!-- 헤더 -->
                    <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 30px; text-align: center;">
                        <h1 style="color: white; margin: 0; font-size: 24px;">📧 이메일 인증</h1>
                    </div>
                    
                    <!-- 본문 -->
                    <div style="padding: 30px;">
                        <p style="color: #333; font-size: 16px; line-height: 1.6; margin-bottom: 20px;">
                            안녕하세요!<br><br>
                            <strong>WE INVESTING</strong> 주간 실적 발표 알림 구독을 신청해 주셔서 감사합니다.
                        </p>
                        
                        <p style="color: #666; font-size: 14px; line-height: 1.6; margin-bottom: 25px;">
                            아래 버튼을 클릭하여 이메일을 인증해 주세요.<br>
                            인증 완료 후부터 매주 일요일에 다음 주 S&P 500 실적 발표 일정을 받아보실 수 있습니다.
                        </p>
                        
                        <!-- 인증 버튼 -->
                        <div style="text-align: center; margin: 30px 0;">
                            <a href="{verify_link}" 
                               style="display: inline-block; padding: 14px 40px; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; text-decoration: none; border-radius: 8px; font-weight: 600; font-size: 16px;">
                                이메일 인증하기
                            </a>
                        </div>
                        
                        <p style="color: #999; font-size: 12px; line-height: 1.6; margin-top: 25px;">
                            ⏰ 이 링크는 <strong>24시간</strong> 동안 유효합니다.<br>
                            본인이 요청하지 않은 경우, 이 이메일을 무시해 주세요.
                        </p>
                        
                        <!-- 링크 복사용 -->
                        <div style="margin-top: 20px; padding: 15px; background: #f8f9fa; border-radius: 8px;">
                            <p style="color: #666; font-size: 12px; margin: 0 0 8px 0;">버튼이 작동하지 않으면 아래 링크를 복사해서 브라우저에 붙여넣으세요:</p>
                            <p style="color: #667eea; font-size: 11px; word-break: break-all; margin: 0;">{verify_link}</p>
                        </div>
                    </div>
                    
                    <!-- 푸터 -->
                    <div style="background: #f8f9fa; padding: 20px; text-align: center;">
                        <p style="color: #999; font-size: 12px; margin: 0;">
                            © 2024 WE INVESTING | 주간 실적 발표 알림 서비스
                        </p>
                    </div>
                </div>
            </body>
            </html>
            """
            
            msg = MIMEMultipart('alternative')
            msg['Subject'] = '[WE INVESTING] 이메일 인증을 완료해 주세요'
            msg['From'] = SMTP_USER
            msg['To'] = email
            
            html_part = MIMEText(html_content, 'html', 'utf-8')
            msg.attach(html_part)
            
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as server:
                server.starttls()
                server.login(SMTP_USER, SMTP_PASSWORD)
                server.sendmail(SMTP_USER, [email], msg.as_string())
            
            logger.info(f"✅ 인증 메일 발송 완료: {email}")
            return True
            
        except Exception as e:
            logger.error(f"❌ 인증 메일 발송 실패: {email} - {e}")
            return False
    
    def subscribe(self, email: str, scope: str = 'SP500', agreed: bool = True) -> Dict[str, Any]:
        """
        이메일 구독 추가 (Double Opt-in)
        
        Args:
            email: 구독할 이메일 주소
            scope: 구독 범위 (SP500, NASDAQ 등)
            agreed: 개인정보 수집/이용 동의 여부
            
        Returns:
            Dict[str, Any]: 구독 결과
        """
        try:
            email = email.lower().strip()
            
            if not agreed:
                return {
                    'success': False,
                    'message': '개인정보 수집/이용에 동의해 주세요.',
                    'email': email,
                    'scope': scope,
                    'requires_verification': False
                }
            
            # 기존 구독 확인
            existing = self.db.query(EmailSubscription).filter(
                and_(
                    EmailSubscription.email == email,
                    EmailSubscription.scope == scope
                )
            ).first()
            
            if existing:
                if existing.is_verified and existing.is_active:
                    return {
                        'success': False,
                        'message': '이미 구독 중인 이메일입니다.',
                        'email': email,
                        'scope': scope,
                        'requires_verification': False
                    }
                elif not existing.is_verified:
                    # 인증 안된 상태 - 인증 토큰 재생성 및 메일 재발송
                    existing.verification_token = uuid.uuid4()
                    existing.verification_expires_at = datetime.now() + timedelta(hours=VERIFICATION_TOKEN_EXPIRE_HOURS)
                    existing.agreed_at = datetime.now()
                    self.db.commit()
                    
                    # 인증 메일 발송
                    self._send_verification_email(email, str(existing.verification_token))
                    
                    logger.info(f"📧 인증 메일 재발송: {email} ({scope})")
                    return {
                        'success': True,
                        'message': '인증 메일을 다시 발송했습니다. 이메일을 확인해 주세요.',
                        'email': email,
                        'scope': scope,
                        'requires_verification': True
                    }
                else:
                    # 비활성화된 구독 재활성화 - 다시 인증 필요
                    existing.is_active = True
                    existing.is_verified = False
                    existing.verification_token = uuid.uuid4()
                    existing.verification_expires_at = datetime.now() + timedelta(hours=VERIFICATION_TOKEN_EXPIRE_HOURS)
                    existing.agreed_at = datetime.now()
                    self.db.commit()
                    
                    self._send_verification_email(email, str(existing.verification_token))
                    
                    logger.info(f"📧 구독 재활성화 인증 메일: {email} ({scope})")
                    return {
                        'success': True,
                        'message': '인증 메일을 발송했습니다. 이메일을 확인해 주세요.',
                        'email': email,
                        'scope': scope,
                        'requires_verification': True
                    }
            
            # 새 구독 생성 (인증 대기 상태)
            verification_token = uuid.uuid4()
            new_subscription = EmailSubscription(
                email=email,
                scope=scope,
                is_active=True,
                is_verified=False,
                verification_token=verification_token,
                verification_expires_at=datetime.now() + timedelta(hours=VERIFICATION_TOKEN_EXPIRE_HOURS),
                agreed_at=datetime.now()
            )
            
            self.db.add(new_subscription)
            self.db.commit()
            self.db.refresh(new_subscription)
            
            # 인증 메일 발송
            self._send_verification_email(email, str(verification_token))
            
            logger.info(f"✅ 새 구독 생성 (인증 대기): {email} ({scope})")
            
            return {
                'success': True,
                'message': '인증 메일을 발송했습니다. 이메일을 확인하여 인증을 완료해 주세요.',
                'email': email,
                'scope': scope,
                'requires_verification': True
            }
            
        except Exception as e:
            self.db.rollback()
            logger.error(f"❌ 구독 생성 실패: {email} - {e}")
            return {
                'success': False,
                'message': f'구독 처리 중 오류가 발생했습니다: {str(e)}',
                'email': email,
                'scope': scope,
                'requires_verification': False
            }
    
    def verify_email(self, token: str) -> Dict[str, Any]:
        """
        이메일 인증 처리
        
        Args:
            token: 인증 토큰
            
        Returns:
            Dict[str, Any]: 인증 결과
        """
        try:
            # UUID 형식 검증
            try:
                token_uuid = uuid.UUID(token)
            except ValueError:
                return {
                    'success': False,
                    'message': '유효하지 않은 인증 토큰입니다.'
                }
            
            # 토큰으로 구독 조회
            subscription = self.db.query(EmailSubscription).filter(
                EmailSubscription.verification_token == token_uuid
            ).first()
            
            if not subscription:
                return {
                    'success': False,
                    'message': '해당 인증 토큰을 찾을 수 없습니다.'
                }
            
            if subscription.is_verified:
                return {
                    'success': True,
                    'message': '이미 인증이 완료된 이메일입니다.',
                    'email': subscription.email
                }
            
            # 만료 시간 확인
            if subscription.verification_expires_at and subscription.verification_expires_at < datetime.now():
                return {
                    'success': False,
                    'message': '인증 링크가 만료되었습니다. 다시 구독 신청해 주세요.'
                }
            
            # 인증 완료 처리
            subscription.is_verified = True
            subscription.verified_at = datetime.now()
            subscription.verification_token = None  # 사용된 토큰 무효화
            subscription.verification_expires_at = None
            self.db.commit()
            
            logger.info(f"✅ 이메일 인증 완료: {subscription.email}")
            
            # 🆕 인증 완료 시 즉시 실적 발표 알림 이메일 발송
            email_sent = False
            if subscription.unsubscribe_token:
                email_sent = self._send_earnings_notification_email(
                    subscription.email, 
                    str(subscription.unsubscribe_token)
                )
            
            if email_sent:
                return {
                    'success': True,
                    'message': '이메일 인증이 완료되었습니다! 향후 7일간 실적 발표 일정을 이메일로 보내드렸습니다.',
                    'email': subscription.email
                }
            else:
                return {
                    'success': True,
                    'message': '이메일 인증이 완료되었습니다! 이제 매주 일요일에 실적 발표 일정을 받아보실 수 있습니다.',
                    'email': subscription.email
                }
            
        except Exception as e:
            self.db.rollback()
            logger.error(f"❌ 이메일 인증 실패: {e}")
            return {
                'success': False,
                'message': f'인증 처리 중 오류가 발생했습니다: {str(e)}'
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
                    'is_verified': False,
                    'scope': scope,
                    'subscribed_at': None
                }
            
            return {
                'email': email,
                'is_subscribed': subscription.is_active,
                'is_verified': subscription.is_verified,
                'scope': subscription.scope,
                'subscribed_at': subscription.created_at,
                'verified_at': subscription.verified_at
            }
            
        except Exception as e:
            logger.error(f"❌ 구독 상태 조회 실패: {email} - {e}")
            return {
                'email': email,
                'is_subscribed': False,
                'is_verified': False,
                'scope': scope,
                'subscribed_at': None,
                'error': str(e)
            }
    
    def get_active_subscribers_count(self, scope: str = 'SP500') -> int:
        """
        활성 및 인증된 구독자 수 조회
        """
        try:
            count = self.db.query(EmailSubscription).filter(
                and_(
                    EmailSubscription.is_active == True,
                    EmailSubscription.is_verified == True,
                    EmailSubscription.scope == scope
                )
            ).count()
            return count
        except Exception as e:
            logger.error(f"❌ 구독자 수 조회 실패: {e}")
            return 0
