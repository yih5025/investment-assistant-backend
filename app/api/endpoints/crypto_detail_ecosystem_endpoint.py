# app/api/endpoints/crypto_detail_ecosystem_endpoint.py

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import Optional

from ...dependencies import get_db
from ...services.crypto_detail_ecosystem_service import CryptoEcosystemService
from ...schemas.crypto_detail_ecosystem_schema import CryptoEcosystemResponse
from ...schemas.common import ErrorResponse

router = APIRouter()


@router.get(
    "/ecosystem/{symbol}",
    response_model=CryptoEcosystemResponse,
    responses={
        404: {"model": ErrorResponse, "description": "Cryptocurrency not found"},
        500: {"model": ErrorResponse, "description": "Internal server error"}
    },
    summary="Crypto Ecosystem Activity Analysis",
    description="""
    암호화폐 생태계 활성도 분석 정보를 제공합니다.
    
    **포함 데이터:**
    - 개발 활성도 (GitHub 기반 자체 점수)
    - 커뮤니티 건강도 (소셜 미디어 분석)
    - 생태계 성숙도 (프로젝트 연령, 채택 수준)
    - 기술적 혁신성 (카테고리 기반 분석)
    - 리스크 요인 (종합 위험 평가)
    - 경쟁 분석 (시장 포지셔닝)
    - 투자 관점 요약
    
    **특징:**
    - null 데이터 대체 로직 적용
    - 카테고리별 가중치 기반 점수 계산
    - 상대적 순위 기반 등급 시스템
    
    **데이터 소스:** CoinGecko Coin Details (GitHub, 소셜 미디어 포함)
    """,
    tags=["Crypto Detail - Ecosystem"]
)
async def get_crypto_ecosystem_analysis(
    symbol: str,
    db: Session = Depends(get_db)
):
    """
    암호화폐 생태계 활성도 분석 데이터 조회
    
    Args:
        symbol: 암호화폐 심볼 (예: BTC, ETH, SOL)
    
    Returns:
        CryptoEcosystemResponse: 종합 생태계 활성도 분석 데이터
        
    Raises:
        HTTPException 404: 해당 암호화폐를 찾을 수 없음
        HTTPException 500: 서버 내부 오류
    """
    try:
        service = CryptoEcosystemService(db)
        analysis = await service.get_ecosystem_analysis(symbol)
        
        if not analysis:
            raise HTTPException(
                status_code=404,
                detail=f"Cryptocurrency '{symbol}' not found or insufficient data available"
            )
        
        return analysis
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to analyze ecosystem data: {str(e)}"
        )


@router.get(
    "/ecosystem/{symbol}/development",
    summary="Development Activity Only",
    description="개발 활성도 분석 데이터만 조회합니다.",
    tags=["Crypto Detail - Ecosystem"]
)
async def get_development_activity(
    symbol: str,
    db: Session = Depends(get_db)
):
    """개발 활성도 분석 단독 조회"""
    try:
        service = CryptoEcosystemService(db)
        coin_details = await service._get_coin_details(symbol)
        
        if not coin_details:
            raise HTTPException(
                status_code=404,
                detail=f"Cryptocurrency '{symbol}' not found"
            )
        
        await service._load_all_coins_data()  # 상대 비교용 데이터 로드
        development_activity = await service._analyze_development_activity(coin_details)
        
        return {
            "symbol": symbol.upper(),
            "development_activity": development_activity
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get development activity: {str(e)}"
        )


@router.get(
    "/ecosystem/{symbol}/community",
    summary="Community Health Only", 
    description="커뮤니티 건강도 분석 데이터만 조회합니다.",
    tags=["Crypto Detail - Ecosystem"]
)
async def get_community_health(
    symbol: str,
    db: Session = Depends(get_db)
):
    """커뮤니티 건강도 분석 단독 조회"""
    try:
        service = CryptoEcosystemService(db)
        coin_details = await service._get_coin_details(symbol)
        
        if not coin_details:
            raise HTTPException(
                status_code=404,
                detail=f"Cryptocurrency '{symbol}' not found"
            )
        
        community_health = await service._analyze_community_health(coin_details)
        
        return {
            "symbol": symbol.upper(),
            "community_health": community_health
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get community health: {str(e)}"
        )


@router.get(
    "/ecosystem/{symbol}/risks",
    summary="Risk Analysis Only",
    description="리스크 요인 분석 데이터만 조회합니다.",
    tags=["Crypto Detail - Ecosystem"]
)
async def get_risk_analysis(
    symbol: str,
    db: Session = Depends(get_db)
):
    """리스크 분석 단독 조회"""
    try:
        service = CryptoEcosystemService(db)
        coin_details = await service._get_coin_details(symbol)
        
        if not coin_details:
            raise HTTPException(
                status_code=404,
                detail=f"Cryptocurrency '{symbol}' not found"
            )
        
        risk_factors = await service._analyze_risk_factors(coin_details)
        
        return {
            "symbol": symbol.upper(),
            "risk_factors": risk_factors
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get risk analysis: {str(e)}"
        )