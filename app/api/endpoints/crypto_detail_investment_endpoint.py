# app/api/endpoints/crypto_detail_investment_endpoint.py

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import Optional

from ...dependencies import get_db
from ...services.crypto_detail_investment_service import CryptoInvestmentService
from ...schemas.crypto_detail_investment_schema import CryptoInvestmentAnalysisResponse, DetailedKimchiPremiumResponse
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

# app/api/endpoints/crypto_detail_investment_endpoint.py 의 수정된 엔드포인트

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
        
        # 모든 로직을 서비스에 위임
        result = await service.get_kimchi_premium_with_details(symbol)
        
        if not result:
            raise HTTPException(
                status_code=404,
                detail=f"Kimchi premium data for '{symbol}' not found"
            )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get kimchi premium data: {str(e)}"
        )


@router.get(
    "/kimchi-premium/{symbol}/detailed",
    summary="Kimchi Premium Detailed Analysis", 
    description="거래소별 상세 김치 프리미엄 분석 데이터를 조회합니다.",
    tags=["Crypto Detail - Investment"]
)
async def get_detailed_kimchi_premium(
    symbol: str,
    sort_by: str = Query("premium_desc", description="정렬 기준"),
    min_volume: float = Query(0, description="최소 거래량 필터"),
    db: Session = Depends(get_db)
):
    """거래소별 상세 김치 프리미엄 분석"""
    try:
        service = CryptoInvestmentService(db)
        
        # 모든 로직을 서비스에 위임
        result = await service.get_detailed_kimchi_premium(symbol, sort_by, min_volume)
        
        if not result:
            raise HTTPException(
                status_code=404,
                detail=f"Detailed kimchi premium data for '{symbol}' not found"
            )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get detailed kimchi premium data: {str(e)}"
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
        derivatives_data = await service.get_derivatives_analysis(symbol)
        
        if not derivatives_data or not derivatives_data.total_open_interest:
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


@router.get(
    "/kimchi-premium/{symbol}/chart",
    summary="Kimchi Premium Chart Data", 
    description="거래소별 김치 프리미엄 차트 데이터를 조회합니다. 일별 가격 추이와 프리미엄 변화를 제공합니다 (12시간마다 업데이트되는 데이터 기준).",
    tags=["Crypto Detail - Investment"]
)
async def get_kimchi_premium_chart_data(
    symbol: str,
    days: int = Query(7, description="조회할 일수 범위 (일 단위)", ge=1, le=30),  # 최대 30일
    db: Session = Depends(get_db)
):
    """김치 프리미엄 차트용 데이터 - 거래소별 일별 가격 추이"""
    try:
        service = CryptoInvestmentService(db)
        chart_data = await service.get_kimchi_premium_chart_data(symbol, days)
        
        if not chart_data:
            raise HTTPException(
                status_code=404,
                detail=f"Chart data for '{symbol}' not found"
            )
        
        return chart_data
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get chart data: {str(e)}"
        )

@router.get(
    "/investment/{symbol}/chart",
    summary="Crypto Price Chart Data",
    description="""
    암호화폐 가격 차트 데이터를 제공합니다.
    
    **시간대별 옵션:**
    - 1H: 최근 24시간 (1시간 간격)
    - 1D: 최근 30일 (1일 간격) 
    - 1W: 최근 12주 (1주 간격)
    - 1MO: 최근 12개월 (1개월 간격)
    
    **데이터 소스:** Bithumb Ticker (실시간 빗썸 거래 데이터)
    """,
    tags=["Crypto Detail - Investment"]
)
async def get_crypto_price_chart(
    symbol: str,
    timeframe: str = Query("1D", description="차트 시간대 (1H/1D/1W/1MO)", regex="^(1H|1D|1W|1MO)$"),
    db: Session = Depends(get_db)
):
    """
    암호화폐 가격 차트 데이터 조회
    
    Args:
        symbol: 암호화폐 심볼 (예: BTC, ETH, SOL)
        timeframe: 차트 시간대
            - 1H: 최근 24시간 (1시간 간격)
            - 1D: 최근 30일 (1일 간격) 
            - 1W: 최근 12주 (1주 간격)
            - 1MO: 최근 12개월 (1개월 간격)
    
    Returns:
        CryptoPriceChartResponse: 가격 차트 데이터
        
    Raises:
        HTTPException 404: 해당 암호화폐를 찾을 수 없음
        HTTPException 422: 잘못된 시간대 파라미터
        HTTPException 500: 서버 내부 오류
    """
    try:
        service = CryptoInvestmentService(db)
        chart_data = await service.get_crypto_price_chart(symbol, timeframe)
        
        if not chart_data:
            raise HTTPException(
                status_code=404,
                detail=f"Price chart data for '{symbol}' not found"
            )
        
        return chart_data
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get price chart data: {str(e)}"
        )