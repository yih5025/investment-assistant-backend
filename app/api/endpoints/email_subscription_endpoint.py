# app/api/endpoints/email_subscription_endpoint.py
from fastapi import APIRouter, Depends, HTTPException, Query, Response
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session
from typing import Optional

from app.schemas.email_subscription_schema import (
    EmailSubscriptionRequest,
    EmailSubscriptionResponse,
    UnsubscribeRequest,
    UnsubscribeByEmailRequest,
    UnsubscribeResponse,
    SubscriptionStatusResponse,
    SubscriptionErrorResponse,
    SubscriptionScope,
    VerifyEmailResponse
)
from app.services.email_subscription_service import EmailSubscriptionService
from app.dependencies import get_db

# 이메일 구독 라우터 생성
router = APIRouter(
    tags=["Email Subscription"],
    responses={
        400: {"description": "잘못된 요청"},
        500: {"description": "서버 내부 오류"}
    }
)

@router.post(
    "/subscribe",
    response_model=EmailSubscriptionResponse,
    summary="이메일 구독 신청 (Double Opt-in)",
    description="주간 실적 발표 일정 이메일 알림을 구독합니다. 인증 메일 발송 후 인증을 완료해야 알림을 받을 수 있습니다."
)
async def subscribe_email(
    request: EmailSubscriptionRequest,
    db: Session = Depends(get_db)
):
    """
    **이메일 구독 신청 (Double Opt-in)**
    
    매주 일요일에 다음 주 S&P 500 실적 발표 일정을 이메일로 받아보실 수 있습니다.
    
    **요청 본문:**
    - email: 구독할 이메일 주소 (필수)
    - scope: 구독 범위 - SP500, NASDAQ, ALL (기본값: SP500)
    - agreed: 개인정보 수집/이용 동의 여부 (필수, true)
    
    **응답:**
    - success: 구독 요청 성공 여부
    - message: 결과 메시지
    - requires_verification: 이메일 인증 필요 여부 (true면 인증 메일 확인 필요)
    """
    try:
        service = EmailSubscriptionService(db)
        result = service.subscribe(request.email, request.scope.value, request.agreed)
        
        return EmailSubscriptionResponse(**result)
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"구독 처리 중 오류가 발생했습니다: {str(e)}"
        )

@router.get(
    "/verify",
    response_class=HTMLResponse,
    summary="이메일 인증 (Double Opt-in)",
    description="이메일에 포함된 인증 링크를 통해 구독을 확정합니다."
)
async def verify_email(
    token: str = Query(..., description="이메일 인증 토큰"),
    db: Session = Depends(get_db)
):
    """
    **이메일 인증 (Double Opt-in)**
    
    인증 메일에 포함된 링크를 클릭하면 이 API가 호출됩니다.
    인증이 완료되면 주간 알림을 받기 시작합니다.
    
    **쿼리 파라미터:**
    - token: 이메일 인증 토큰 (필수)
    """
    try:
        service = EmailSubscriptionService(db)
        result = service.verify_email(token)
        
        if result['success']:
            html_content = f"""
            <!DOCTYPE html>
            <html lang="ko">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>이메일 인증 완료</title>
                <style>
                    body {{
                        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                        display: flex;
                        justify-content: center;
                        align-items: center;
                        min-height: 100vh;
                        margin: 0;
                        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    }}
                    .container {{
                        background: white;
                        padding: 40px;
                        border-radius: 16px;
                        box-shadow: 0 10px 40px rgba(0,0,0,0.2);
                        text-align: center;
                        max-width: 400px;
                    }}
                    .icon {{ font-size: 64px; margin-bottom: 20px; }}
                    h1 {{ color: #333; font-size: 24px; margin-bottom: 16px; }}
                    p {{ color: #666; line-height: 1.6; }}
                    .email {{ color: #667eea; font-weight: 600; }}
                    .btn {{
                        display: inline-block;
                        margin-top: 20px;
                        padding: 12px 24px;
                        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                        color: white;
                        text-decoration: none;
                        border-radius: 8px;
                        font-weight: 500;
                    }}
                    .btn:hover {{ opacity: 0.9; }}
                </style>
            </head>
            <body>
                <div class="container">
                    <div class="icon">🎉</div>
                    <h1>이메일 인증 완료!</h1>
                    <p>
                        <span class="email">{result.get('email', '')}</span> 주소로<br>
                        매주 일요일에 S&P 500 실적 발표 일정을<br>
                        받아보실 수 있습니다.
                    </p>
                    <a href="https://investment-assistant.site" class="btn">홈으로 돌아가기</a>
                </div>
            </body>
            </html>
            """
        else:
            html_content = f"""
            <!DOCTYPE html>
            <html lang="ko">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>인증 실패</title>
                <style>
                    body {{
                        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                        display: flex;
                        justify-content: center;
                        align-items: center;
                        min-height: 100vh;
                        margin: 0;
                        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    }}
                    .container {{
                        background: white;
                        padding: 40px;
                        border-radius: 16px;
                        box-shadow: 0 10px 40px rgba(0,0,0,0.2);
                        text-align: center;
                        max-width: 400px;
                    }}
                    .icon {{ font-size: 64px; margin-bottom: 20px; }}
                    h1 {{ color: #333; font-size: 24px; margin-bottom: 16px; }}
                    p {{ color: #666; line-height: 1.6; }}
                    .btn {{
                        display: inline-block;
                        margin-top: 20px;
                        padding: 12px 24px;
                        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                        color: white;
                        text-decoration: none;
                        border-radius: 8px;
                        font-weight: 500;
                    }}
                </style>
            </head>
            <body>
                <div class="container">
                    <div class="icon">⚠️</div>
                    <h1>인증 실패</h1>
                    <p>{result['message']}</p>
                    <a href="https://investment-assistant.site" class="btn">홈으로 돌아가기</a>
                </div>
            </body>
            </html>
            """
        
        return HTMLResponse(content=html_content)
        
    except Exception as e:
        error_html = f"""
        <!DOCTYPE html>
        <html><body>
            <h1>오류 발생</h1>
            <p>인증 처리 중 오류가 발생했습니다: {str(e)}</p>
        </body></html>
        """
        return HTMLResponse(content=error_html, status_code=500)

@router.post(
    "/unsubscribe",
    response_model=UnsubscribeResponse,
    summary="이메일 구독 취소 (이메일 기반)",
    description="이메일 주소로 구독을 취소합니다."
)
async def unsubscribe_by_email(
    request: UnsubscribeByEmailRequest,
    db: Session = Depends(get_db)
):
    """
    **이메일 구독 취소 (이메일 기반)**
    
    등록된 이메일 주소로 구독을 취소합니다.
    """
    try:
        service = EmailSubscriptionService(db)
        result = service.unsubscribe_by_email(request.email, request.scope.value)
        
        return UnsubscribeResponse(**result)
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"구독 취소 중 오류가 발생했습니다: {str(e)}"
        )

@router.get(
    "/unsubscribe",
    response_class=HTMLResponse,
    summary="이메일 구독 취소 (토큰 기반)",
    description="이메일에 포함된 링크로 구독을 취소합니다."
)
async def unsubscribe_by_token(
    token: str = Query(..., description="구독 취소 토큰"),
    db: Session = Depends(get_db)
):
    """
    **이메일 구독 취소 (토큰 기반)**
    
    이메일에 포함된 구독 취소 링크를 통해 구독을 취소합니다.
    """
    try:
        service = EmailSubscriptionService(db)
        result = service.unsubscribe_by_token(token)
        
        if result['success']:
            html_content = """
            <!DOCTYPE html>
            <html lang="ko">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>구독 취소 완료</title>
                <style>
                    body {
                        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                        display: flex;
                        justify-content: center;
                        align-items: center;
                        min-height: 100vh;
                        margin: 0;
                        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    }
                    .container {
                        background: white;
                        padding: 40px;
                        border-radius: 16px;
                        box-shadow: 0 10px 40px rgba(0,0,0,0.2);
                        text-align: center;
                        max-width: 400px;
                    }
                    .icon { font-size: 64px; margin-bottom: 20px; }
                    h1 { color: #333; font-size: 24px; margin-bottom: 16px; }
                    p { color: #666; line-height: 1.6; }
                    .btn {
                        display: inline-block;
                        margin-top: 20px;
                        padding: 12px 24px;
                        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                        color: white;
                        text-decoration: none;
                        border-radius: 8px;
                        font-weight: 500;
                    }
                    .btn:hover { opacity: 0.9; }
                </style>
            </head>
            <body>
                <div class="container">
                    <div class="icon">✅</div>
                    <h1>구독이 취소되었습니다</h1>
                    <p>더 이상 주간 실적 발표 알림을 받지 않습니다.<br>
                    언제든지 다시 구독하실 수 있습니다.</p>
                    <a href="https://investment-assistant.site" class="btn">홈으로 돌아가기</a>
                </div>
            </body>
            </html>
            """
        else:
            html_content = f"""
            <!DOCTYPE html>
            <html lang="ko">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>구독 취소 오류</title>
                <style>
                    body {{
                        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                        display: flex;
                        justify-content: center;
                        align-items: center;
                        min-height: 100vh;
                        margin: 0;
                        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    }}
                    .container {{
                        background: white;
                        padding: 40px;
                        border-radius: 16px;
                        box-shadow: 0 10px 40px rgba(0,0,0,0.2);
                        text-align: center;
                        max-width: 400px;
                    }}
                    .icon {{ font-size: 64px; margin-bottom: 20px; }}
                    h1 {{ color: #333; font-size: 24px; margin-bottom: 16px; }}
                    p {{ color: #666; line-height: 1.6; }}
                    .btn {{
                        display: inline-block;
                        margin-top: 20px;
                        padding: 12px 24px;
                        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                        color: white;
                        text-decoration: none;
                        border-radius: 8px;
                        font-weight: 500;
                    }}
                </style>
            </head>
            <body>
                <div class="container">
                    <div class="icon">⚠️</div>
                    <h1>구독 취소 실패</h1>
                    <p>{result['message']}</p>
                    <a href="https://investment-assistant.site" class="btn">홈으로 돌아가기</a>
                </div>
            </body>
            </html>
            """
        
        return HTMLResponse(content=html_content)
        
    except Exception as e:
        error_html = f"""
        <!DOCTYPE html>
        <html><body>
            <h1>오류 발생</h1>
            <p>구독 취소 처리 중 오류가 발생했습니다: {str(e)}</p>
        </body></html>
        """
        return HTMLResponse(content=error_html, status_code=500)
