from fastapi import APIRouter
from app.config import settings

# 도메인별 엔드포인트 라우터 imports
from .endpoints import (
    # 뉴스 관련
    earnings_calendar_endpoint,
    earnings_calendar_news_endpoint,
    truth_social_endpoint,
    market_news_endpoint,
    financial_news_endpoint,
    company_news_endpoint,
    market_news_sentiment_endpoint,
    
    # 경제 지표
    inflation_endpoint,
    federal_funds_rate_endpoint,
    cpi_endpoint,
    
    # 소셜 미디어
    x_posts_endpoint,
    sns_endpoint,
    
    # 재무/국채
    balance_sheet_endpoint,
    treasury_yield_endpoint,
    
    # 실시간 주식/ETF (REST API)
    sp500_endpoint,
    etf_endpoint,
    
    # 🆕 통합 WebSocket 엔드포인트 (Push 방식)
    websocket_endpoint,
    
    # 암호화폐 투자 분석
    crypto_detail_investment_endpoint,
    crypto_detail_concept_endpoint,
    crypto_detail_ecosystem_endpoint,
    
    # SP500 실적 정보
    sp500_earnings_calendar_endpoint,
    sp500_earnings_news_endpoint,
    
    # IPO
    ipo_calendar_endpoint,
    
    # 이메일 구독
    email_subscription_endpoint,
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

    {
        "router": sns_endpoint.router,
        "prefix": "/sns",
        "tag": "SNS",
        "category": "소셜미디어",
        "description": "X, Truth Social 트렌딩 포스트 데이터 전달을 위한 엔드포인트"
    },
    {
        "router": sns_endpoint.router_analysis,
        "prefix": "/sns/analysis",  # 분석 API는 별도 prefix
        "tag": "SNS Analysis",
        "category": "소셜미디어",
        "description": "Airflow로 분석된 SNS 게시글 데이터 전달을 위한 엔드포인트"
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
    {
        "router": sp500_earnings_calendar_endpoint.router,
        "prefix": "/sp500-earnings-calendar",
        "tag": "SP500 Earnings Calendar",
        "category": "실적정보",
        "description": "S&P 500 실적 캘린더 API"
    },
    {
        "router": sp500_earnings_news_endpoint.router,
        "prefix": "/sp500-earnings-news",
        "tag": "SP500 Earnings News",
        "category": "실적정보",
        "description": "S&P 500 실적 뉴스 API"
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

    # 실시간 주식 데이터 API (REST)
    {
        "router": sp500_endpoint.router,
        "prefix": "/stocks/sp500",
        "tag": "SP500 REST API",
        "category": "실시간주식",
        "description": "S&P 500 주식 REST API - 시장 개요, 종목 상세, 검색, 차트 등"
    },
    
    # 🆕 통합 WebSocket API (Push 방식)
    {
        "router": websocket_endpoint.router,
        "prefix": "",  # 라우터 내부에 /ws/* 경로 포함
        "tag": "WebSocket Push API",
        "category": "실시간WebSocket",
        "description": "통합 실시간 WebSocket Push API - SP500(/ws/sp500), ETF(/ws/etf), Crypto(/ws/crypto)"
    },

    # 암호화폐 투자 분석 API
    {
        "router": crypto_detail_investment_endpoint.router,
        "prefix": "/crypto/details",
        "tag": "Crypto Detail - Investment",
        "category": "암호화폐분석",
        "description": "암호화폐 투자 분석 API - 김치 프리미엄, 파생상품, 위험도, 투자 기회, 포트폴리오 가이드 제공"
    },
    {
        "router": crypto_detail_concept_endpoint.router,
        "prefix": "/crypto/details",
        "tag": "Crypto Detail - Concept",
        "category": "암호화폐분석",
        "description": "암호화폐 개념 설명 API - 기본 정보, 프로젝트 탄생 배경, 카테고리 분류 및 설명, 핵심 특징 및 기술, 시장 위치 분석, 초보자를 위한 교육 콘텐츠, 자주 묻는 질문 (FAQ) 제공"
    },
    {
        "router": crypto_detail_ecosystem_endpoint.router,
        "prefix": "/crypto/details",
        "tag": "Crypto Detail - Ecosystem",
        "category": "암호화폐분석",
        "description": "암호화폐 생태계 분석 API - 개발 활성도, 커뮤니티 건강도, 생태계 성숙도, 기술적 혁신성, 리스크 요인, 경쟁 분석, 투자 관점 요약 제공"
    },

    # ETF API
    {
        "router": etf_endpoint.router,
        "prefix": "/etf",
        "tag": "ETF",
        "category": "ETF",
        "description": "ETF 실시간 데이터 API"
    },

    # IPO 캘린더 API
    {
        "router": ipo_calendar_endpoint.router,
        "prefix": "/ipo-calendar",
        "tag": "IPO Calendar",
        "category": "IPO",
        "description": "IPO 캘린더 API"
    },
    
    # 이메일 구독 API
    {
        "router": email_subscription_endpoint.router,
        "prefix": "/email-subscription",
        "tag": "Email Subscription",
        "category": "알림서비스",
        "description": "이메일 구독 API - 주간 실적 발표 알림 구독/취소"
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
        # WebSocket 엔드포인트들은 특별 처리
        if "websocket" in config["tag"].lower():
            key = config["tag"].lower().replace(" websocket", "_ws").replace(" ", "_")
        else:
            key = config["prefix"].lstrip("/") or "root"
            
        available_endpoints[key] = {
            "description": config["description"],
            "prefix": f"{settings.api_v1_prefix}{config['prefix']}",
            "tag": config.get("tag"),
            "category": config.get("category"),
        }

    # 카테고리 매핑
    categories = {}
    for config in ROUTER_CONFIGS:
        category_key = config["category"]
        endpoint_key = config["prefix"].lstrip("/") if config["prefix"] else config["tag"]
        categories.setdefault(category_key, []).append(endpoint_key)

    return {
        "service": settings.app_name,
        "version": settings.app_version,
        "description": "투자 도우미 서비스의 메인 API",
        "base_url": settings.api_v1_prefix,
        "total_endpoints": len(ROUTER_CONFIGS),
        "categories": categories,
        "available_endpoints": available_endpoints,
        "websocket_endpoints": {
            "sp500": f"{settings.api_v1_prefix}/ws/sp500",
            "etf": f"{settings.api_v1_prefix}/ws/etf",
            "crypto": f"{settings.api_v1_prefix}/ws/crypto",
            "architecture": "통합 Push 방식 WebSocket (Redis Pub/Sub + Hash)"
        },
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
        "websocket_status": "통합 Push 방식 WebSocket 지원 (SP500, ETF, Crypto)",
        "architecture": "Redis Pub/Sub + Hash 기반 실시간 데이터 스트리밍",
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
        "implemented_domains": [
            config["prefix"].lstrip("/") if config["prefix"] else config["tag"] 
            for config in ROUTER_CONFIGS
        ],
        "base_url": settings.api_v1_prefix,
        "documentation": {"swagger_ui": "/docs", "redoc": "/redoc"},
        "features": [
            "pagination",
            "filtering",
            "sorting",
            "real_time_push",
            "sentiment_analysis",
            "unified_websocket",
            "redis_pub_sub_streaming",
            "change_rate_calculation",
        ],
        "websocket_architecture": {
            "approach": "통합 Push 방식 WebSocket",
            "domains": ["sp500", "etf", "crypto"],
            "technology": "Redis Pub/Sub + Hash",
            "benefits": [
                "서버 주도 Push (클라이언트 폴링 불필요)",
                "Redis Pub/Sub 기반 실시간 이벤트",
                "Hash 구조로 효율적 데이터 저장",
                "대규모 클라이언트 확장성",
                "네트워크 트래픽 최소화"
            ]
        }
    }


# 개발/테스트용 엔드포인트
@api_router.get("/test", tags=["Development"], summary="API 테스트")
async def api_test():
    """
    API 연결 테스트용 엔드포인트입니다.
    
    Returns:
        dict: 테스트 응답 메시지
    """
    from datetime import datetime
    import pytz
    
    return {
        "message": "API v1 연결 테스트 성공",
        "timestamp": datetime.now(pytz.UTC).isoformat(),
        "status": "ok",
        "websocket_endpoints": {
            "sp500": f"{settings.api_v1_prefix}/ws/sp500",
            "etf": f"{settings.api_v1_prefix}/ws/etf",
            "crypto": f"{settings.api_v1_prefix}/ws/crypto"
        },
        "architecture": "통합 Push 방식 WebSocket (Redis Pub/Sub)",
        "test_commands": {
            "sp500": f"wscat -c ws://localhost:8000{settings.api_v1_prefix}/ws/sp500",
            "etf": f"wscat -c ws://localhost:8000{settings.api_v1_prefix}/ws/etf",
            "crypto": f"wscat -c ws://localhost:8000{settings.api_v1_prefix}/ws/crypto"
        }
    }