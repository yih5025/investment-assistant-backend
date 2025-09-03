# app/api/endpoints/crypto_detail_concept_endpoint.py

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import Optional

from ...dependencies import get_db
from ...services.crypto_detail_concept_service import CryptoConceptService
from ...schemas.crypto_detail_concept_schema import CryptoConceptResponse
from ...schemas.common import ErrorResponse

router = APIRouter()


@router.get(
    "/concept/{symbol}",
    response_model=CryptoConceptResponse,
    responses={
        404: {"model": ErrorResponse, "description": "Cryptocurrency not found"},
        500: {"model": ErrorResponse, "description": "Internal server error"}
    },
    summary="Crypto Concept Analysis",
    description="""
    암호화폐 개념 설명 정보를 제공합니다.
    
    **포함 데이터:**
    - 기본 정보 및 이미지
    - 프로젝트 탄생 배경
    - 카테고리 분류 및 설명
    - 핵심 특징 및 기술
    - 시장 위치 분석
    - 초보자를 위한 교육 콘텐츠
    - 자주 묻는 질문 (FAQ)
    
    **데이터 소스:** CoinGecko Coin Details
    """,
    tags=["Crypto Detail - Concept"]
)
async def get_crypto_concept_analysis(
    symbol: str,
    db: Session = Depends(get_db)
):
    """
    암호화폐 개념 설명 데이터 조회
    
    Args:
        symbol: 암호화폐 심볼 (예: BTC, ETH, SOL)
    
    Returns:
        CryptoConceptResponse: 종합 개념 설명 데이터
        
    Raises:
        HTTPException 404: 해당 암호화폐를 찾을 수 없음
        HTTPException 500: 서버 내부 오류
    """
    try:
        service = CryptoConceptService(db)
        analysis = await service.get_concept_analysis(symbol)
        
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
            detail=f"Failed to analyze concept data: {str(e)}"
        )


@router.get(
    "/concept/{symbol}/basic-info",
    summary="Basic Coin Information",
    description="기본 코인 정보만 간단히 조회합니다.",
    tags=["Crypto Detail - Concept"]
)
async def get_basic_coin_info(
    symbol: str,
    db: Session = Depends(get_db)
):
    """기본 코인 정보 간단 조회"""
    try:
        service = CryptoConceptService(db)
        coin_details = await service._get_coin_details(symbol)
        
        if not coin_details:
            raise HTTPException(
                status_code=404,
                detail=f"Cryptocurrency '{symbol}' not found"
            )
        
        basic_info = await service._build_basic_info(coin_details)
        market_position = await service._build_market_position(coin_details)
        
        return {
            "symbol": symbol.upper(),
            "basic_info": basic_info,
            "market_position": market_position
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get basic info: {str(e)}"
        )


@router.get(
    "/concept/{symbol}/categories",
    summary="Crypto Categories",
    description="암호화폐 카테고리 정보만 조회합니다.",
    tags=["Crypto Detail - Concept"]
)
async def get_crypto_categories(
    symbol: str,
    db: Session = Depends(get_db)
):
    """암호화폐 카테고리 정보 조회"""
    try:
        service = CryptoConceptService(db)
        coin_details = await service._get_coin_details(symbol)
        
        if not coin_details:
            raise HTTPException(
                status_code=404,
                detail=f"Cryptocurrency '{symbol}' not found"
            )
        
        category_info = await service._build_category_info(coin_details)
        
        return {
            "symbol": symbol.upper(),
            "category_info": category_info
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get category info: {str(e)}"
        )