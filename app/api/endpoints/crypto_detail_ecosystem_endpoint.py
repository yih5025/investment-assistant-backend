# app/api/endpoints/crypto_detail_ecosystem_endpoint.py

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import Optional, Dict, Any

from ...dependencies import get_db
from ...services.crypto_detail_ecosystem_service import CryptoEcosystemService
from ...schemas.crypto_detail_ecosystem_schema import (
    TransparentCryptoEcosystemResponse,
    DevelopmentOnlyResponse,
    CommunityOnlyResponse,
    MarketOnlyResponse
)
from ...schemas.common import ErrorResponse

router = APIRouter()


@router.get(
    "/ecosystem/{symbol}",
    response_model=TransparentCryptoEcosystemResponse,
    responses={
        404: {"model": ErrorResponse, "description": "Cryptocurrency not found"},
        500: {"model": ErrorResponse, "description": "Internal server error"}
    },
    summary="Transparent Crypto Ecosystem Information",
    description="""
    암호화폐 생태계 정보를 투명하게 제공합니다.
    
    **포함 데이터:**
    - 기본 프로젝트 정보 (팩트 중심)
    - 개발 활동 지표 (GitHub 원시 데이터 + 해석 가이드)
    - 커뮤니티 지표 (소셜 미디어 데이터 + 크기 참조)
    - 시장 위치 (순위 정보 + 해석 기준)
    - 비교 맥락 (유사 프로젝트 정보)
    - 데이터 투명성 (가용성, 한계점, 사용법)
    
    **특징:**
    - AI 점수/등급 없음 - 원시 데이터와 해석 가이드만 제공
    - 모든 계산 과정 투명 공개
    - 데이터 한계점 솔직 공개
    - 사용자 주도 판단 지원
    
    **데이터 소스:** CoinGecko Coin Details, GitHub API
    """,
    tags=["Crypto Detail - Ecosystem V2"]
)
async def get_transparent_crypto_ecosystem_info(
    symbol: str,
    db: Session = Depends(get_db)
):
    """
    투명한 암호화폐 생태계 정보 조회
    
    Args:
        symbol: 암호화폐 심볼 (예: BTC, ETH, SOL)
    
    Returns:
        TransparentCryptoEcosystemResponse: 투명한 생태계 정보
        
    Raises:
        HTTPException 404: 해당 암호화폐를 찾을 수 없음
        HTTPException 500: 서버 내부 오류
    """
    try:
        service = CryptoEcosystemService(db)
        organized_info = await service.organize_crypto_information(symbol)
        
        if not organized_info:
            raise HTTPException(
                status_code=404,
                detail=f"Cryptocurrency '{symbol}' not found or insufficient data available"
            )
        
        # 딕셔너리를 Pydantic 모델로 변환
        response = TransparentCryptoEcosystemResponse(**organized_info)
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to organize crypto information: {str(e)}"
        )


@router.get(
    "/ecosystem/{symbol}/development",
    response_model=DevelopmentOnlyResponse,
    summary="Development Activity Information Only",
    description="개발 활동 정보만 조회합니다 (점수 없이 원시 데이터 + 가이드).",
    tags=["Crypto Detail - Ecosystem V2"]
)
async def get_development_info_only(
    symbol: str,
    db: Session = Depends(get_db)
):
    """개발 활동 정보 단독 조회"""
    try:
        service = CryptoEcosystemService(db)
        organized_info = await service.organize_crypto_information(symbol)
        
        if not organized_info:
            raise HTTPException(
                status_code=404,
                detail=f"Cryptocurrency '{symbol}' not found"
            )
        
        response_data = {
            "symbol": symbol.upper(),
            "development_activity": organized_info["development_activity"]
        }
        
        return DevelopmentOnlyResponse(**response_data)
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get development information: {str(e)}"
        )


@router.get(
    "/ecosystem/{symbol}/community",
    response_model=CommunityOnlyResponse,
    summary="Community Information Only", 
    description="커뮤니티 정보만 조회합니다 (점수 없이 원시 데이터 + 가이드).",
    tags=["Crypto Detail - Ecosystem V2"]
)
async def get_community_info_only(
    symbol: str,
    db: Session = Depends(get_db)
):
    """커뮤니티 정보 단독 조회"""
    try:
        service = CryptoEcosystemService(db)
        organized_info = await service.organize_crypto_information(symbol)
        
        if not organized_info:
            raise HTTPException(
                status_code=404,
                detail=f"Cryptocurrency '{symbol}' not found"
            )
        
        response_data = {
            "symbol": symbol.upper(),
            "community_health": organized_info["community_metrics"]  # 스키마에서는 community_health로 매핑
        }
        
        return CommunityOnlyResponse(**response_data)
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get community information: {str(e)}"
        )


@router.get(
    "/ecosystem/{symbol}/market",
    response_model=MarketOnlyResponse,
    summary="Market Position Information Only",
    description="시장 위치 정보만 조회합니다 (순위 + 해석 가이드).",
    tags=["Crypto Detail - Ecosystem V2"]
)
async def get_market_info_only(
    symbol: str,
    db: Session = Depends(get_db)
):
    """시장 위치 정보 단독 조회"""
    try:
        service = CryptoEcosystemService(db)
        organized_info = await service.organize_crypto_information(symbol)
        
        if not organized_info:
            raise HTTPException(
                status_code=404,
                detail=f"Cryptocurrency '{symbol}' not found"
            )
        
        response_data = {
            "symbol": symbol.upper(),
            "market_position": organized_info["market_position"]
        }
        
        return MarketOnlyResponse(**response_data)
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get market information: {str(e)}"
        )


@router.get(
    "/ecosystem/{symbol}/summary",
    response_model=Dict[str, Any],
    summary="User-Friendly Summary",
    description="""
    사용자 친화적 요약 정보를 제공합니다.
    
    **포함 내용:**
    - 프로젝트 개요
    - 주요 숫자들
    - 주목할 만한 특징
    - 추가 조사 영역
    - 다음 단계 제안 (초보자/분석가/투자자별)
    
    **특징:**
    - 복잡한 데이터를 이해하기 쉽게 정리
    - 판단하지 않고 객관적 기준만 제시
    - 사용자별 맞춤 가이드 제공
    """,
    tags=["Crypto Detail - Ecosystem V2"]
)
async def get_user_friendly_summary(
    symbol: str,
    db: Session = Depends(get_db)
):
    """사용자 친화적 요약 정보 조회"""
    try:
        service = CryptoEcosystemService(db)
        organized_info = await service.organize_crypto_information(symbol)
        
        if not organized_info:
            raise HTTPException(
                status_code=404,
                detail=f"Cryptocurrency '{symbol}' not found"
            )
        
        summary = await service.get_user_friendly_summary(organized_info)
        return summary
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get user-friendly summary: {str(e)}"
        )
