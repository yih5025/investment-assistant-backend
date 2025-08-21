from fastapi import APIRouter
from app.config import settings

# 도메인별 엔드포인트 라우터 imports
from .endpoints import (
    earnings_calendar_endpoint,
    earnings_calendar_news_endpoint,
    truth_social_endpoint,
    market_news_endpoint,
    financial_news_endpoint,
    company_news_endpoint,
    market_news_sentiment_endpoint,
    inflation_endpoint,
    federal_funds_rate_endpoint,
    cpi_endpoint,
    x_posts_endpoint,
    balance_sheet_endpoint,
    treasury_yield_endpoint,
    websocket_endpoint,
    sp500_endpoint,
    topgainers_endpoint
)

# API v1 메인 라우터 생성
api_router = APIRouter()

# 라우터 설정 구성 (각 도메인의 prefix, 설명, 카테고리 지정)
ROUTER_CONFIGS = [
    # 뉴스 관련 API
    {
        "router": market_news_endpoint.router,
        "prefix": "/market-news",
        "tag": "Market News",
        "category": "뉴스",
        "description": "NEWS API - 경제, 비즈니스, 기술, 공개상장, 인플레이션, 관세, 무역 전쟁, 제재, 전쟁, 정치, 선거, 정부 정책, 의회, 외교, 핵, 군사 관련 광범위한 뉴스 데이터 전달을 위한 엔드포인트"
    },
    {
        "router": financial_news_endpoint.router,
        "prefix": "/financial-news",
        "tag": "Financial News",
        "category": "뉴스",
        "description": "finnhub - crypto, forex, merger, general 카테고리별 뉴스 데이터 전달을 위한 엔드포인트"
    },
    {
        "router": company_news_endpoint.router,
        "prefix": "/company-news",
        "tag": "Company News",
        "category": "뉴스",
        "description": "finnhub - topgianers 급상승 20개, 급하락 10개, 활발한 거래량 20개 주식 종목에 맞는 뉴스 데이터 전달을 위한 엔드포인트"
    },
    {
        "router": market_news_sentiment_endpoint.router,
        "prefix": "/market-news-sentiment",
        "tag": "Market News Sentiment",
        "category": "뉴스",
        "description": "Alpha Vantage -  월: 에너지·제조(예: topics=energy_transportation, manufacturing / XOM, CVX, EOG, CAT, GE 등), 화: 기술·IPO(technology, ipo / AAPL, MSFT, NVDA, AMZN, TSLA 등), 수: 블록체인·금융(blockchain, finance / JPM, BAC, V, MA, COIN 등), 목: 실적·헬스케어(earnings, life_sciences / AAPL, MSFT, NVDA, JNJ, PFE 등), 금: 리테일·M&A(retail_wholesale, mergers_and_acquisitions / WMT, TGT, COST, DIS, NFLX 등), 토: 부동산·거시(real_estate, economy_macro / HD, LOW, CAT, GE, F, GM 등), 일: 금융시장·정책(technology, finance, earnings, ipo, blockchain, mergers_and_acquisitions, retail_wholesale, life_sciences + 주요 빅테크/금융 티커) 뉴스 감성 분석 API"
    },

    # 소셜 미디어 API
    {
        "router": truth_social_endpoint.router,
        "prefix": "/truth-social",
        "tag": "Truth Social",
        "category": "소셜미디어",
        "description": "realDonaldTrump, WhiteHouse, DonaldJTrumpJr, Truth Social 트렌딩 포스트 데이터 전달을 위한 엔드포인트"
    },
    {
        "router": x_posts_endpoint.router,
        "prefix": "/x-posts",
        "tag": "X Posts",
        "category": "소셜미디어",
        "description": "elonmusk, RayDalio, jimcramer, tim_cook, satyanadella, sundarpichai, SecYellen, VitalikButerin crypto: saylor, brian_armstrong, CoinbaseAssets, tech_ceo: jeffbezos, sundarpichai, IBM, institutional: CathieDWood, mcuban, chamath, media: CNBC, business(Bloomberg), WSJ, corporate: Tesla, nvidia x 데이터 전달을 위한 엔드포인트"
    },

    # 실적 관련 API
    {
        "router": earnings_calendar_endpoint.router,
        "prefix": "/earnings-calendar",
        "tag": "Earnings Calendar",
        "category": "실적정보",
        "description": "실적 발표 캘린더 API"
    },
    {
        "router": earnings_calendar_news_endpoint.router,
        "prefix": "/earnings-calendar-news",
        "tag": "Earnings Calendar News",
        "category": "실적정보",
        "description": "실적 캘린더 뉴스 API"
    },

    # 경제 지표 API
    {
        "router": inflation_endpoint.router,
        "prefix": "/inflation",
        "tag": "Inflation",
        "category": "경제지표",
        "description": "인플레이션 데이터 API"
    },
    {
        "router": federal_funds_rate_endpoint.router,
        "prefix": "/federal-funds-rate",
        "tag": "Federal Funds Rate",
        "category": "경제지표",
        "description": "연방기금금리 API"
    },
    {
        "router": cpi_endpoint.router,
        "prefix": "/cpi",
        "tag": "CPI",
        "category": "경제지표",
        "description": "소비자물가지수 API"
    },

    # 재무/국채 API
    {
        "router": balance_sheet_endpoint.router,
        "prefix": "/balance-sheet",
        "tag": "Balance Sheet",
        "category": "재무제표",
        "description": "재무제표 API"
    },
    {
        "router": treasury_yield_endpoint.router,
        "prefix": "/treasury-yield",
        "tag": "Treasury Yield",
        "category": "국채수익률",
        "description": "국채 수익률 API"
    },

    # WebSocket 실시간 데이터 API (이미 router 내부에 prefix="/ws" 보유)
    {
        "router": websocket_endpoint.router,
        "prefix": "",
        "tag": "WebSocket",
        "category": "실시간",
        "description": "실시간 WebSocket 데이터 API"
    },

    # 🎯 실시간 주식 데이터 API (새로 추가)
    {
        "router": topgainers_endpoint.router,
        "prefix": "/stocks/topgainers",
        "tag": "TopGainers",
        "category": "실시간주식",
        "description": "실시간 상승/하락/활발한 주식 데이터 API - WebSocket fallback 지원"
    },
    {
        "router": sp500_endpoint.router,
        "prefix": "/stocks/sp500",
        "tag": "SP500",
        "category": "실시간주식",
        "description": "실시간 S&P 500 주식 데이터 API - WebSocket fallback 지원"
    },
]

# 라우터 등록 (각 엔드포인트 모듈의 자체 태그 사용)
for config in ROUTER_CONFIGS:
    api_router.include_router(
        config["router"],
        prefix=config["prefix"],
    )


# ========== API 정보 엔드포인트 ==========

@api_router.get("/", tags=["API Info"], summary="API v1 정보")
async def api_v1_info():
    """
    API v1 기본 정보와 사용 가능한 엔드포인트 목록을 반환합니다.
    
    Returns:
        dict: API 버전, 설명, 사용 가능한 엔드포인트 목록
    """
    # 동적으로 엔드포인트 정보 생성
    available_endpoints = {}
    for config in ROUTER_CONFIGS:
        key = config["prefix"].lstrip("/") or "ws"
        available_endpoints[key] = {
            "description": config["description"],
            "prefix": f"{settings.api_v1_prefix}{config['prefix']}",
            "tag": config.get("tag"),
            "category": config.get("category"),
        }

    # 카테고리 매핑
    categories = {}
    for config in ROUTER_CONFIGS:
        categories.setdefault(config["category"], []).append(config["prefix"].lstrip("/"))

    return {
        "service": settings.app_name,
        "version": settings.app_version,
        "description": "투자 도우미 서비스의 메인 API",
        "base_url": settings.api_v1_prefix,
        "total_endpoints": len(ROUTER_CONFIGS),
        "categories": categories,
        "available_endpoints": available_endpoints,
        "documentation": {
            "swagger_ui": "/docs",
            "redoc": "/redoc",
            "openapi_json": "/openapi.json"
        }
    }


@api_router.get("/health", tags=["API Info"], summary="API 상태 확인")
async def health_check():
    """
    API 서버의 상태를 확인합니다.
    
    Returns:
        dict: API 상태 정보
    """
    return {
        "status": "healthy",
        "service": settings.app_name,
        "version": settings.app_version,
        "uptime": "operational",
        "docs": "/docs",
    }


@api_router.get("/stats", tags=["API Info"], summary="API 통계 정보")
async def api_stats():
    """
    API 사용 통계 및 구성 정보를 반환합니다.
    
    Returns:
        dict: API 구성 통계 및 기능 정보
    """
    # 카테고리별 라우터 수 계산
    category_counts = {}
    for config in ROUTER_CONFIGS:
        category_counts[config["category"]] = category_counts.get(config["category"], 0) + 1

    return {
        "api_summary": {
            "total_routers": len(ROUTER_CONFIGS),
            "categories": category_counts,
        },
        "implemented_domains": [config["prefix"].lstrip("/") for config in ROUTER_CONFIGS],
        "base_url": settings.api_v1_prefix,
        "documentation": {"swagger_ui": "/docs", "redoc": "/redoc"},
        "features": [
            "pagination",
            "filtering",
            "sorting",
            "real_time",
            "sentiment_analysis",
            "websocket_fallback",  # 🎯 새로운 기능 추가
        ],
    }


# 개발/테스트용 엔드포인트
@api_router.get("/test", tags=["Development"], summary="API 테스트")
async def api_test():
    """
    API 연결 테스트용 엔드포인트입니다.
    
    Returns:
        dict: 테스트 응답 메시지
    """
    return {
        "message": "API v1 연결 테스트 성공",
        "timestamp": "2025-01-27",
        "status": "ok"
    }