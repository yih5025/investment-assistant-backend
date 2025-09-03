# app/api/endpoints/crypto_detail_investment_endpoint.py

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import Optional

from ...dependencies import get_db
from ...services.crypto_detail_investment_service import CryptoInvestmentService
from ...schemas.crypto_detail_investment_schema import CryptoInvestmentAnalysisResponse
from ...schemas.common import ErrorResponse

router = APIRouter()


@router.get(
    "/investment/{symbol}",
    response_model=CryptoInvestmentAnalysisResponse,
    responses={
        404: {"model": ErrorResponse, "description": "Cryptocurrency not found"},
        500: {"model": ErrorResponse, "description": "Internal server error"}
    },
    summary="Crypto Investment Analysis",
    description="""
    암호화폐 투자 분석 정보를 제공합니다.
    
    **포함 데이터:**
    - 김치 프리미엄 실시간 분석
    - 파생상품 시장 심리 분석
    - 글로벌 시장 맥락
    - 위험도 종합 평가
    - 투자 기회 분석
    - 포트폴리오 배분 가이드
    
    **데이터 소스:** CoinGecko Tickers, Derivatives, Global, Coin Details
    """,
    tags=["Crypto Detail - Investment"]
)
async def get_crypto_investment_analysis(
    symbol: str,
    db: Session = Depends(get_db)
):
    """
    암호화폐 투자 분석 데이터 조회
    
    Args:
        symbol: 암호화폐 심볼 (예: BTC, ETH, SOL)
    
    Returns:
        CryptoInvestmentAnalysisResponse: 종합 투자 분석 데이터
        
    Raises:
        HTTPException 404: 해당 암호화폐를 찾을 수 없음
        HTTPException 500: 서버 내부 오류
    """
    try:
        service = CryptoInvestmentService(db)
        analysis = await service.get_investment_analysis(symbol)
        
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
            detail=f"Failed to analyze investment data: {str(e)}"
        )


@router.get(
    "/kimchi-premium/{symbol}",
    summary="Kimchi Premium Analysis",
    description="김치 프리미엄 분석 데이터만 조회합니다.",
    tags=["Crypto Detail - Investment"]
)
async def get_kimchi_premium(
    symbol: str,
    db: Session = Depends(get_db)
):
    """김치 프리미엄 분석 단독 조회"""
    try:
        service = CryptoInvestmentService(db)
        kimchi_data = await service._analyze_kimchi_premium(symbol)
        
        if not kimchi_data.korean_price_usd:
            raise HTTPException(
                status_code=404,
                detail=f"Kimchi premium data for '{symbol}' not found"
            )
        
        return {
            "symbol": symbol.upper(),
            "kimchi_premium": kimchi_data,
            "last_updated": kimchi_data.last_updated if hasattr(kimchi_data, 'last_updated') else None
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get kimchi premium data: {str(e)}"
        )


@router.get(
    "/derivatives/{symbol}",
    summary="Derivatives Market Analysis", 
    description="파생상품 시장 분석 데이터만 조회합니다.",
    tags=["Crypto Detail - Investment"]
)
async def get_derivatives_analysis(
    symbol: str,
    db: Session = Depends(get_db)
):
    """파생상품 시장 분석 단독 조회"""
    try:
        service = CryptoInvestmentService(db)
        derivatives_data = await service._analyze_derivatives(symbol)
        
        if not derivatives_data.total_open_interest:
            raise HTTPException(
                status_code=404,
                detail=f"Derivatives data for '{symbol}' not found"
            )
        
        return {
            "symbol": symbol.upper(),
            "derivatives": derivatives_data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get derivatives data: {str(e)}"
        )