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
    SubscriptionScope
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
    summary="이메일 구독 신청",
    description="주간 실적 발표 일정 이메일 알림을 구독합니다."
)
async def subscribe_email(
    request: EmailSubscriptionRequest,
    db: Session = Depends(get_db)
):
    """
    **이메일 구독 신청**
    
    매주 일요일에 다음 주 S&P 500 실적 발표 일정을 이메일로 받아보실 수 있습니다.
    
    **요청 본문:**
    - email: 구독할 이메일 주소 (필수)
    - scope: 구독 범위 - SP500, NASDAQ, ALL (기본값: SP500)
    
    **응답:**
    - success: 구독 성공 여부
    - message: 결과 메시지
    - email: 구독된 이메일
    - scope: 구독 범위
    """
    try:
        service = EmailSubscriptionService(db)
        result = service.subscribe(request.email, request.scope.value)
        
        return EmailSubscriptionResponse(**result)
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"구독 처리 중 오류가 발생했습니다: {str(e)}"
        )

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
    
    **요청 본문:**
    - email: 구독 취소할 이메일 주소 (필수)
    - scope: 구독 취소할 범위 (기본값: SP500)
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
    
    **쿼리 파라미터:**
    - token: 구독 취소 토큰 (필수)
    
    **응답:**
    - HTML 페이지로 결과를 표시합니다.
    """
    try:
        service = EmailSubscriptionService(db)
        result = service.unsubscribe_by_token(token)
        
        # 성공 시 HTML 응답
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
                        background: #667eea;
                        color: white;
                        text-decoration: none;
                        border-radius: 8px;
                        font-weight: 500;
                    }
                    .btn:hover { background: #5a6fd6; }
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
                        background: #667eea;
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
