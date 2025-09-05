# app/services/crypto_detail_concept_service.py

from sqlalchemy.orm import Session
from sqlalchemy import func
from typing import Optional, List, Dict
from datetime import datetime
import json

from ..models.coingecko_coin_details_model import CoingeckoCoinDetails
from ..schemas.crypto_detail_concept_schema import (
    CryptoConceptResponse, BasicInfo, ConceptDescription, CategoryInfo,
    ProjectLinks, KeyFeatures, MarketPosition, EducationalContent, FAQ
)


class CryptoConceptService:
    """Crypto Detail Concept 서비스"""
    
    # 카테고리 한국어 매핑 사전
    CATEGORY_MAPPING = {
        "Smart Contract Platform": "스마트계약플랫폼",
        "Layer 1 (L1)": "레이어1블록체인", 
        "Ethereum Ecosystem": "이더리움생태계",
        "Proof of Work (PoW)": "작업증명",
        "Proof of Stake (PoS)": "지분증명",
        "DeFi": "탈중앙화금융",
        "Stablecoins": "스테이블코인",
        "USD Stablecoin": "달러연동코인",
        "Meme": "밈코인",
        "Bitcoin Ecosystem": "비트코인생태계",
        "Solana Ecosystem": "솔라나생태계",
        "Gaming": "게임",
        "Metaverse": "메타버스",
        "NFT": "대체불가토큰",
        "Exchange-based Tokens": "거래소토큰",
        "Privacy Coins": "프라이버시코인"
    }
    
    # 카테고리별 설명 템플릿
    CATEGORY_DESCRIPTIONS = {
        "스마트계약플랫폼": "프로그래밍이 가능한 계약을 자동으로 실행하는 블록체인 플랫폼",
        "레이어1블록체인": "독자적인 메인넷을 운영하는 기축 블록체인 네트워크",
        "스테이블코인": "가격 안정성을 위해 특정 자산에 연동된 암호화폐",
        "탈중앙화금융": "중앙기관 없이 블록체인상에서 이뤄지는 금융서비스",
        "밈코인": "인터넷 밈과 커뮤니티를 기반으로 만들어진 재미있는 암호화폐"
    }
    
    def __init__(self, db: Session):
        self.db = db
    
    async def get_concept_analysis(self, symbol: str) -> Optional[CryptoConceptResponse]:
        """개념 분석 데이터 종합 조회"""
        
        # 1. 기본 코인 정보 조회
        coin_details = await self._get_coin_details(symbol)
        if not coin_details:
            return None
        
        # 2. 각 분석 영역별 데이터 구성
        basic_info = await self._build_basic_info(coin_details)
        concept_description = await self._build_concept_description(coin_details)
        category_info = await self._build_category_info(coin_details)
        project_links = await self._build_project_links(coin_details)
        key_features = await self._build_key_features(coin_details, category_info)
        market_position = await self._build_market_position(coin_details)
        educational_content = await self._build_educational_content(coin_details, category_info)
        faqs = await self._build_faqs(coin_details, category_info)
        
        # 3. 데이터 완성도 평가
        data_completeness = await self._evaluate_data_completeness(coin_details)
        
        return CryptoConceptResponse(
            basic_info=basic_info,
            concept_description=concept_description,
            category_info=category_info,
            project_links=project_links,
            key_features=key_features,
            market_position=market_position,
            educational_content=educational_content,
            faqs=faqs,
            data_completeness=data_completeness
        )
    
    async def _get_coin_details(self, symbol: str) -> Optional[CoingeckoCoinDetails]:
        """코인 기본 정보 조회"""
        return self.db.query(CoingeckoCoinDetails).filter(
            func.upper(CoingeckoCoinDetails.symbol) == symbol.upper()
        ).first()
    
    async def _build_basic_info(self, coin: CoingeckoCoinDetails) -> BasicInfo:
        """기본 정보 구성"""
        return BasicInfo(
            coingecko_id=coin.coingecko_id,
            symbol=coin.symbol,
            name=coin.name,
            web_slug=coin.web_slug,
            market_cap_rank=coin.market_cap_rank,
            image_thumb=coin.image_thumb,
            image_small=coin.image_small,
            image_large=coin.image_large
        )
    
    async def _build_concept_description(self, coin: CoingeckoCoinDetails) -> ConceptDescription:
        """개념 설명 구성"""
        # 프로젝트 연령 계산
        project_age = None
        genesis_date_str = None
        
        if coin.genesis_date:
            try:
                if hasattr(coin.genesis_date, 'year'):  # Date 객체인 경우
                    genesis_year = coin.genesis_date.year
                    genesis_date_str = coin.genesis_date.strftime('%Y-%m-%d')
                else:  # string인 경우
                    genesis_year = int(str(coin.genesis_date).split('-')[0])
                    genesis_date_str = str(coin.genesis_date)
                
                current_year = datetime.now().year
                project_age = current_year - genesis_year
            except:
                project_age = None
                genesis_date_str = str(coin.genesis_date) if coin.genesis_date else None
        
        return ConceptDescription(
            description_original=coin.description_en,
            description_summary=None,  # 향후 번역/요약 기능 추가
            genesis_date=genesis_date_str,  # string으로 변환된 값
            project_age_years=project_age,
            country_origin=coin.country_origin
        )
    
    async def _build_category_info(self, coin: CoingeckoCoinDetails) -> CategoryInfo:
        """카테고리 정보 구성"""
        categories_original = []
        categories_korean = []
        
        if coin.categories:
            try:
                if isinstance(coin.categories, str):
                    categories_original = json.loads(coin.categories)
                else:
                    categories_original = coin.categories
                    
                # 한국어 매핑
                categories_korean = [
                    self.CATEGORY_MAPPING.get(cat, cat) 
                    for cat in categories_original[:5]  # 상위 5개만
                ]
            except:
                categories_original = []
        
        # 주요 카테고리 선정 (첫 번째 카테고리)
        primary_category = categories_korean[0] if categories_korean else None
        category_description = None
        
        if primary_category:
            category_description = self.CATEGORY_DESCRIPTIONS.get(primary_category)
        
        return CategoryInfo(
            categories_original=categories_original[:5],
            categories_korean=categories_korean,
            primary_category=primary_category,
            category_description=category_description
        )
    
    async def _build_project_links(self, coin: CoingeckoCoinDetails) -> ProjectLinks:
        """프로젝트 링크 정보 구성"""
        github_repos = []
        main_github_url = None
        
        if coin.github_repos:
            try:
                if isinstance(coin.github_repos, str):
                    github_data = json.loads(coin.github_repos)
                else:
                    github_data = coin.github_repos
                
                github_repos = github_data.get('github', [])
                if github_repos:
                    main_github_url = github_repos[0]  # 첫 번째를 메인으로
            except:
                github_repos = []
        
        return ProjectLinks(
            homepage_url=coin.homepage_url,
            blockchain_site=coin.blockchain_site,
            twitter_screen_name=coin.twitter_screen_name,
            facebook_username=coin.facebook_username,
            telegram_channel_identifier=coin.telegram_channel_identifier,
            subreddit_url=coin.subreddit_url,
            github_repos=github_repos,
            main_github_url=main_github_url
        )
    
    async def _build_key_features(self, coin: CoingeckoCoinDetails, category: CategoryInfo) -> KeyFeatures:
        """핵심 특징 구성"""
        # 카테고리 기반으로 특징 추론
        consensus_algorithm = None
        blockchain_type = None
        use_cases = []
        unique_features = []
        
        if category.categories_original:
            for cat in category.categories_original:
                if "Proof of Work" in cat:
                    consensus_algorithm = "작업증명 (PoW)"
                elif "Proof of Stake" in cat:
                    consensus_algorithm = "지분증명 (PoS)"
                
                if "Layer 1" in cat:
                    blockchain_type = "레이어1 메인넷"
                elif "Layer 2" in cat:
                    blockchain_type = "레이어2 확장솔루션"
                
                # 사용 사례 추론
                if "DeFi" in cat:
                    use_cases.append("탈중앙화 금융")
                if "Smart Contract" in cat:
                    use_cases.append("스마트 계약 실행")
                if "Gaming" in cat:
                    use_cases.append("게임 및 엔터테인먼트")
                if "Stablecoin" in cat:
                    use_cases.append("가치 안정화")
        
        # 코인별 고유 특징 (하드코딩으로 주요 코인들만)
        symbol_upper = coin.symbol.upper()
        if symbol_upper == "BTC":
            unique_features = [
                "세계 최초 암호화폐",
                "가장 높은 보안성",
                "디지털 금 역할",
                "2100만 개 공급 한정"
            ]
        elif symbol_upper == "ETH":
            unique_features = [
                "스마트 계약 지원",
                "가장 활발한 개발 생태계",
                "DeFi의 기반 플랫폼",
                "NFT 표준 지원"
            ]
        elif symbol_upper == "SOL":
            unique_features = [
                "초고속 트랜잭션 처리",
                "낮은 수수료",
                "고성능 DApp 지원",
                "웹3 게임 플랫폼"
            ]
        
        return KeyFeatures(
            consensus_algorithm=consensus_algorithm,
            blockchain_type=blockchain_type,
            use_cases=use_cases,
            unique_features=unique_features
        )
    
    async def _build_market_position(self, coin: CoingeckoCoinDetails) -> MarketPosition:
        """시장 위치 구성"""
        rank_description = None
        category_rank = None
        market_presence = None
        
        if coin.market_cap_rank:
            rank = coin.market_cap_rank
            if rank == 1:
                rank_description = "압도적 1위"
                market_presence = "지배적"
            elif rank <= 3:
                rank_description = "최상위권"
                market_presence = "매우 강함"
            elif rank <= 10:
                rank_description = "상위권"
                market_presence = "강함"
            elif rank <= 50:
                rank_description = "중상위권"
                market_presence = "보통"
            elif rank <= 100:
                rank_description = "중위권"
                market_presence = "보통"
            else:
                rank_description = "하위권"
                market_presence = "약함"
        
        return MarketPosition(
            market_cap_rank=coin.market_cap_rank,
            rank_description=rank_description,
            category_rank=category_rank,  # 향후 카테고리별 순위 계산
            market_presence=market_presence
        )
    
    async def _build_educational_content(self, coin: CoingeckoCoinDetails, category: CategoryInfo) -> EducationalContent:
        """교육 콘텐츠 구성"""
        symbol_upper = coin.symbol.upper()
        
        # 주요 코인별 교육 콘텐츠 (하드코딩)
        educational_templates = {
            "BTC": {
                "what_is_it": "세계 최초의 탈중앙화 디지털 화폐로, '디지털 금'이라 불립니다.",
                "how_it_works": "블록체인 기술로 중앙기관 없이 거래가 기록되고 검증됩니다.",
                "why_created": "2008년 금융위기 이후 중앙은행에 의존하지 않는 화폐 시스템을 만들기 위해",
                "real_world_usage": ["온라인 결제", "가치 저장", "송금", "투자 자산"],
                "beginner_summary": "인터넷에서 사용하는 디지털 돈",
                "difficulty_level": "보통"
            },
            "ETH": {
                "what_is_it": "스마트 계약을 지원하는 세계적인 컴퓨터 네트워크입니다.",
                "how_it_works": "전 세계 컴퓨터들이 연결되어 프로그램을 자동으로 실행합니다.",
                "why_created": "단순 화폐를 넘어 다양한 애플리케이션을 구동하기 위해",
                "real_world_usage": ["DeFi (탈중앙화 금융)", "NFT", "게임", "스마트 계약"],
                "beginner_summary": "프로그래밍이 가능한 블록체인 컴퓨터",
                "difficulty_level": "어려움"
            },
            "USDT": {
                "what_is_it": "미국 달러와 1:1로 연동되는 안정화된 암호화폐입니다.",
                "how_it_works": "실제 달러 보유량에 맞춰 발행되어 가격이 안정적으로 유지됩니다.",
                "why_created": "암호화폐의 높은 변동성을 해결하기 위해",
                "real_world_usage": ["거래 매개체", "가치 보관", "국경간 송금", "암호화폐 거래"],
                "beginner_summary": "가격이 안정적인 디지털 달러",
                "difficulty_level": "쉬움"
            }
        }
        
        template = educational_templates.get(symbol_upper, {
            "what_is_it": f"{coin.name}는 {category.primary_category or '암호화폐'}입니다.",
            "how_it_works": "블록체인 기술을 기반으로 작동합니다.",
            "why_created": "특정한 문제를 해결하기 위해 개발되었습니다.",
            "real_world_usage": [],
            "beginner_summary": f"{category.primary_category or '암호화폐'} 프로젝트",
            "difficulty_level": "보통"
        })
        
        return EducationalContent(**template)
    
    async def _build_faqs(self, coin: CoingeckoCoinDetails, category: CategoryInfo) -> List[FAQ]:
        """FAQ 구성"""
        faqs = []
        
        # 공통 FAQ
        faqs.append(FAQ(
            question=f"{coin.symbol}는 어떻게 구매하나요?",
            answer="국내 암호화폐 거래소(업비트, 빗썸 등)에서 구매할 수 있습니다.",
            category="구매"
        ))
        
        # 카테고리별 FAQ
        if category.primary_category == "스테이블코인":
            faqs.append(FAQ(
                question="스테이블코인은 왜 가격이 안정적인가요?",
                answer="실제 자산(달러, 금 등)에 연동되어 발행되기 때문에 가격 변동이 적습니다.",
                category="개념"
            ))
        
        if "스마트계약" in (category.primary_category or ""):
            faqs.append(FAQ(
                question="스마트 계약이란 무엇인가요?",
                answer="조건이 충족되면 자동으로 실행되는 디지털 계약입니다.",
                category="기술"
            ))
        
        # 코인별 특별 FAQ
        if coin.symbol.upper() == "BTC":
            faqs.append(FAQ(
                question="비트코인은 왜 2100만 개로 제한되나요?",
                answer="희소성을 만들어 인플레이션을 방지하고 가치를 보존하기 위해서입니다.",
                category="경제"
            ))
        
        return faqs[:5]  # 최대 5개로 제한
    
    async def _evaluate_data_completeness(self, coin: CoingeckoCoinDetails) -> str:
        """데이터 완성도 평가"""
        score = 0
        total = 8
        
        # 필수 데이터 체크
        if coin.description_en: score += 1
        if coin.categories: score += 1
        if coin.genesis_date: score += 1
        if coin.homepage_url: score += 1
        if coin.image_large: score += 1
        if coin.market_cap_rank: score += 1
        if coin.github_repos: score += 1
        if coin.twitter_screen_name: score += 1
        
        completion_rate = score / total
        
        if completion_rate >= 0.8:
            return "높음"
        elif completion_rate >= 0.5:
            return "보통"
        else:
            return "낮음"