# app/services/crypto_detail_ecosystem_service.py

from sqlalchemy.orm import Session
from sqlalchemy import func, and_
from typing import Optional, List, Dict, Tuple
from decimal import Decimal
from datetime import datetime
import json

from ..models.coingecko_coin_details_model import CoingeckoCoinDetails
from ..schemas.crypto_detail_ecosystem_schema import (
    CryptoEcosystemResponse, DevelopmentActivity, CommunityHealth, 
    EcosystemMaturity, TechnicalInnovation, RiskFactors,
    CompetitiveAnalysis, DataQuality
)


class CryptoEcosystemService:
    """Crypto Ecosystem Activity 서비스"""
    
    # 카테고리별 가중치
    CATEGORY_WEIGHTS = {
        "Smart Contract Platform": {
            "development": 0.35,
            "community": 0.25, 
            "maturity": 0.25,
            "innovation": 0.15
        },
        "Stablecoins": {
            "development": 0.15,
            "community": 0.20,
            "maturity": 0.45,
            "innovation": 0.20
        },
        "Layer 1 (L1)": {
            "development": 0.30,
            "community": 0.25,
            "maturity": 0.25,
            "innovation": 0.20
        },
        "Meme": {
            "development": 0.05,
            "community": 0.60,
            "maturity": 0.20,
            "innovation": 0.15
        },
        "DeFi": {
            "development": 0.40,
            "community": 0.30,
            "maturity": 0.20,
            "innovation": 0.10
        },
        "default": {
            "development": 0.30,
            "community": 0.30,
            "maturity": 0.25,
            "innovation": 0.15
        }
    }
    
    # 등급 기준점
    GRADE_THRESHOLDS = {
        "A+": 95, "A": 90, "A-": 85,
        "B+": 80, "B": 75, "B-": 70,
        "C+": 65, "C": 60, "C-": 55,
        "D+": 50, "D": 40, "F": 0
    }
    
    def __init__(self, db: Session):
        self.db = db
        # 전체 코인 데이터 캐시 (상대 비교용)
        self._all_coins_cache = None
    
    async def get_ecosystem_analysis(self, symbol: str) -> Optional[CryptoEcosystemResponse]:
        """생태계 활성도 분석 데이터 종합 조회"""
        
        # 1. 기본 코인 정보 조회
        coin_details = await self._get_coin_details(symbol)
        if not coin_details:
            return None
        
        # 2. 전체 코인 데이터 로드 (상대 비교용)
        await self._load_all_coins_data()
        
        # 3. 각 분석 영역별 데이터 구성
        development_activity = await self._analyze_development_activity(coin_details)
        community_health = await self._analyze_community_health(coin_details)
        ecosystem_maturity = await self._analyze_ecosystem_maturity(coin_details)
        technical_innovation = await self._analyze_technical_innovation(coin_details)
        risk_factors = await self._analyze_risk_factors(coin_details)
        competitive_analysis = await self._analyze_competitive_position(coin_details)
        
        # 4. 종합 점수 계산
        overall_score, overall_grade, health_status = await self._calculate_overall_score(
            coin_details, development_activity, community_health, ecosystem_maturity, technical_innovation
        )
        
        # 5. 투자 관점 요약
        investment_summary = await self._generate_investment_summary(
            coin_details, overall_score, risk_factors
        )
        
        # 6. 데이터 품질 평가
        data_quality = await self._evaluate_data_quality(coin_details)
        
        return CryptoEcosystemResponse(
            coingecko_id=coin_details.coingecko_id,
            symbol=coin_details.symbol,
            name=coin_details.name,
            market_cap_rank=coin_details.market_cap_rank,
            development_activity=development_activity,
            community_health=community_health,
            ecosystem_maturity=ecosystem_maturity,
            technical_innovation=technical_innovation,
            risk_factors=risk_factors,
            competitive_analysis=competitive_analysis,
            overall_ecosystem_score=overall_score,
            overall_grade=overall_grade,
            ecosystem_health=health_status,
            **investment_summary,
            data_quality=data_quality
        )
    
    async def _get_coin_details(self, symbol: str) -> Optional[CoingeckoCoinDetails]:
        """코인 기본 정보 조회"""
        return self.db.query(CoingeckoCoinDetails).filter(
            func.upper(CoingeckoCoinDetails.symbol) == symbol.upper()
        ).first()
    
    async def _load_all_coins_data(self):
        """전체 코인 데이터 로드 (상대 비교용)"""
        if self._all_coins_cache is None:
            self._all_coins_cache = self.db.query(CoingeckoCoinDetails).filter(
                CoingeckoCoinDetails.market_cap_rank.isnot(None),
                CoingeckoCoinDetails.market_cap_rank <= 1000  # 상위 1000개만
            ).all()
    
    async def _analyze_development_activity(self, coin: CoingeckoCoinDetails) -> DevelopmentActivity:
        """개발 활성도 분석"""
        
        # 1. 이슈 해결률 계산
        issues_resolved_rate = None
        if coin.total_issues and coin.closed_issues:
            issues_resolved_rate = (coin.closed_issues / coin.total_issues) * 100
        
        # 2. 커밋 활성도 점수 (0-25점)
        commit_score = 0
        if coin.commit_count_4_weeks is not None:
            # 주당 12.5커밋 이상이면 만점
            commit_score = min(coin.commit_count_4_weeks / 50 * 25, 25)
        else:
            # null인 경우 카테고리 평균으로 추정
            commit_score = await self._estimate_missing_metric(
                coin, 'commit_count_4_weeks', lambda x: min(x / 50 * 25, 25)
            )
        
        # 3. 인기도 점수 (0-20점)  
        popularity_score = 0
        if coin.stars:
            popularity_score = min(coin.stars / 1000 * 20, 20)
        else:
            popularity_score = await self._estimate_missing_metric(
                coin, 'stars', lambda x: min(x / 1000 * 20, 20)
            )
        
        # 4. 커뮤니티 참여 점수 (0-15점)
        engagement_score = 0
        if coin.forks:
            engagement_score = min(coin.forks / 100 * 15, 15)
        else:
            engagement_score = await self._estimate_missing_metric(
                coin, 'forks', lambda x: min(x / 100 * 15, 15)
            )
        
        # 5. 코드 품질 점수 (0-40점)
        quality_score = 0
        if issues_resolved_rate:
            quality_score = issues_resolved_rate * 40 / 100
        else:
            quality_score = await self._estimate_missing_metric(
                coin, 'closed_issues', lambda x: (x / max(coin.total_issues, 1)) * 40
            )
        
        # 6. 종합 개발 점수 계산
        development_score = int(min(commit_score + popularity_score + engagement_score + quality_score, 100))
        development_grade = self._score_to_grade(development_score)
        
        return DevelopmentActivity(
            commit_count_4_weeks=coin.commit_count_4_weeks,
            stars=coin.stars,
            forks=coin.forks,
            total_issues=coin.total_issues,
            closed_issues=coin.closed_issues,
            issues_resolved_rate=Decimal(str(round(issues_resolved_rate, 1))) if issues_resolved_rate else None,
            commit_activity_score=int(commit_score),
            popularity_score=int(popularity_score),
            community_engagement_score=int(engagement_score),
            code_quality_score=int(quality_score),
            development_score=development_score,
            development_grade=development_grade
        )
    
    async def _analyze_community_health(self, coin: CoingeckoCoinDetails) -> CommunityHealth:
        """커뮤니티 건강도 분석"""
        
        # 1. 소셜 미디어 존재감 점수 (0-30점)
        social_score = 0
        platform_count = 0
        
        if coin.telegram_channel_user_count:
            social_score += min(coin.telegram_channel_user_count / 1000 * 15, 15)
            platform_count += 1
        
        if coin.reddit_subscribers:
            social_score += min(coin.reddit_subscribers / 10000 * 10, 10)
            platform_count += 1
        
        if coin.twitter_followers:
            social_score += min(coin.twitter_followers / 100000 * 5, 5)
            platform_count += 1
        
        # 2. 참여 다양성 점수 (0-30점) - 플랫폼 다양성
        diversity_score = platform_count * 10  # 플랫폼당 10점
        
        # 3. 커뮤니티 성장 추세 (단순화)
        growth_trend = "안정적"
        if coin.telegram_channel_user_count and coin.telegram_channel_user_count > 50000:
            growth_trend = "성장 중"
        elif coin.telegram_channel_user_count and coin.telegram_channel_user_count < 1000:
            growth_trend = "초기 단계"
        
        # 4. null 값 대체 로직
        if social_score == 0:  # 모든 소셜 데이터가 없는 경우
            social_score = await self._estimate_community_score_by_rank(coin.market_cap_rank)
        
        # 5. 종합 커뮤니티 점수
        community_score = int(min(social_score + diversity_score, 100))
        community_grade = self._score_to_grade(community_score)
        
        return CommunityHealth(
            telegram_users=coin.telegram_channel_user_count,
            reddit_subscribers=coin.reddit_subscribers,
            twitter_followers=coin.twitter_followers,
            social_presence_score=int(social_score),
            engagement_diversity_score=int(diversity_score),
            community_growth_trend=growth_trend,
            community_score=community_score,
            community_grade=community_grade
        )
    
    async def _analyze_ecosystem_maturity(self, coin: CoingeckoCoinDetails) -> EcosystemMaturity:
        """생태계 성숙도 분석"""
        
        # 1. 프로젝트 연령 계산
        project_age = None
        if coin.genesis_date:
            try:
                genesis_year = int(coin.genesis_date.split('-')[0])
                project_age = datetime.now().year - genesis_year
            except:
                project_age = None
        
        # 2. 채택 수준 판단
        adoption_level = "초기"
        if coin.market_cap_rank:
            if coin.market_cap_rank <= 10:
                adoption_level = "주류"
            elif coin.market_cap_rank <= 50:
                adoption_level = "성숙"
            elif coin.market_cap_rank <= 200:
                adoption_level = "성장"
        
        # 3. 개발자 생태계 규모
        dev_ecosystem = "소규모"
        if coin.forks and coin.forks > 1000:
            dev_ecosystem = "대규모"
        elif coin.forks and coin.forks > 100:
            dev_ecosystem = "중간규모"
        
        # 4. 네트워크 효과
        network_effect = "약함"
        if coin.market_cap_rank and coin.market_cap_rank <= 20:
            network_effect = "강함"
        elif coin.market_cap_rank and coin.market_cap_rank <= 100:
            network_effect = "보통"
        
        # 5. 카테고리 내 순위 (간단 추정)
        category_rank = await self._estimate_category_rank(coin)
        
        # 6. 성숙도 점수 계산
        age_score = min(project_age * 10, 30) if project_age else 15  # 최대 30점
        rank_score = max(100 - coin.market_cap_rank, 0) if coin.market_cap_rank else 50  # 순위 기반
        maturity_score = int(min(age_score + rank_score * 0.7, 100))
        maturity_grade = self._score_to_grade(maturity_score)
        
        return EcosystemMaturity(
            project_age_years=project_age,
            market_cap_rank=coin.market_cap_rank,
            category=await self._get_primary_category(coin),
            developer_ecosystem_size=dev_ecosystem,
            adoption_level=adoption_level,
            network_effect=network_effect,
            category_rank=category_rank,
            competitive_position=adoption_level,
            market_differentiation="높음" if coin.market_cap_rank and coin.market_cap_rank <= 50 else "보통",
            maturity_score=maturity_score,
            maturity_grade=maturity_grade
        )
    
    async def _analyze_technical_innovation(self, coin: CoingeckoCoinDetails) -> TechnicalInnovation:
        """기술적 혁신성 분석"""
        
        # 카테고리에서 기술 특성 추론
        categories = await self._get_categories_list(coin)
        
        consensus_mechanism = "알 수 없음"
        blockchain_type = "알 수 없음"
        smart_contract_support = None
        
        for category in categories:
            if "Proof of Work" in category:
                consensus_mechanism = "작업증명 (PoW)"
            elif "Proof of Stake" in category:
                consensus_mechanism = "지분증명 (PoS)"
            
            if "Layer 1" in category:
                blockchain_type = "레이어1"
            elif "Layer 2" in category:
                blockchain_type = "레이어2"
            
            if "Smart Contract" in category:
                smart_contract_support = True
        
        # 혁신성 점수 (카테고리와 활동도 기반 추정)
        innovation_score = 50  # 기본값
        
        if coin.commit_count_4_weeks and coin.commit_count_4_weeks > 100:
            innovation_score += 20  # 활발한 개발
        
        if "DeFi" in categories or "Smart Contract" in categories:
            innovation_score += 15  # 혁신적 카테고리
        
        if coin.market_cap_rank and coin.market_cap_rank <= 20:
            innovation_score += 15  # 시장 인정
        
        innovation_score = min(innovation_score, 100)
        innovation_grade = self._score_to_grade(innovation_score)
        
        return TechnicalInnovation(
            consensus_mechanism=consensus_mechanism,
            blockchain_type=blockchain_type,
            smart_contract_support=smart_contract_support,
            technical_uniqueness="높음" if innovation_score > 80 else "보통",
            innovation_frequency="높음" if coin.commit_count_4_weeks and coin.commit_count_4_weeks > 50 else "보통",
            research_activity="활발" if coin.stars and coin.stars > 1000 else "보통",
            developer_adoption="높음" if coin.forks and coin.forks > 500 else "보통",
            integration_count=None,  # 향후 개발
            innovation_score=innovation_score,
            innovation_grade=innovation_grade
        )
    
    async def _analyze_risk_factors(self, coin: CoingeckoCoinDetails) -> RiskFactors:
        """리스크 요인 분석"""
        
        # 개발 리스크
        dev_risk = "보통"
        if coin.commit_count_4_weeks:
            if coin.commit_count_4_weeks < 10:
                dev_risk = "높음"
            elif coin.commit_count_4_weeks > 100:
                dev_risk = "낮음"
        
        # 커뮤니티 리스크
        community_risk = "보통"
        if coin.telegram_channel_user_count:
            if coin.telegram_channel_user_count < 1000:
                community_risk = "높음"
            elif coin.telegram_channel_user_count > 50000:
                community_risk = "낮음"
        
        # 기술 리스크
        tech_risk = "보통"
        if coin.closed_issues and coin.total_issues:
            resolution_rate = coin.closed_issues / coin.total_issues
            if resolution_rate > 0.9:
                tech_risk = "낮음"
            elif resolution_rate < 0.5:
                tech_risk = "높음"
        
        # 종합 리스크
        risk_scores = {"높음": 3, "보통": 2, "낮음": 1}
        avg_risk = (risk_scores[dev_risk] + risk_scores[community_risk] + risk_scores[tech_risk]) / 3
        
        overall_risk = "낮음" if avg_risk < 1.5 else "보통" if avg_risk < 2.5 else "높음"
        
        return RiskFactors(
            development_risk=dev_risk,
            team_centralization="알 수 없음",
            funding_sustainability="안정적" if coin.market_cap_rank and coin.market_cap_rank <= 100 else "보통",
            community_dependency=community_risk,
            governance_risk="보통",
            technical_debt=tech_risk,
            scalability_concerns="보통",
            security_track_record="양호" if coin.market_cap_rank and coin.market_cap_rank <= 50 else "보통",
            overall_risk_level=overall_risk,
            risk_mitigation_strategies=["지속적인 모니터링", "포트폴리오 분산", "정기적 재평가"]
        )
    
    async def _analyze_competitive_position(self, coin: CoingeckoCoinDetails) -> CompetitiveAnalysis:
        """경쟁 분석"""
        
        # 카테고리별 주요 경쟁자 매핑
        category = await self._get_primary_category(coin)
        competitors_map = {
            "Smart Contract Platform": ["Ethereum", "Solana", "Cardano", "Avalanche"],
            "Layer 1 (L1)": ["Bitcoin", "Ethereum", "BNB", "Solana"],
            "Stablecoins": ["USDT", "USDC", "BUSD", "DAI"],
            "DeFi": ["Uniswap", "Aave", "Compound", "SushiSwap"],
            "Meme": ["Dogecoin", "Shiba Inu", "Pepe"]
        }
        
        main_competitors = competitors_map.get(category, [])
        # 현재 코인은 경쟁자 리스트에서 제외
        main_competitors = [comp for comp in main_competitors if comp.upper() != coin.symbol.upper()]
        
        # 경쟁 우위/열세 분석
        advantages = []
        weaknesses = []
        
        if coin.market_cap_rank:
            if coin.market_cap_rank <= 10:
                advantages.append("높은 시장 인지도")
                advantages.append("강력한 네트워크 효과")
            elif coin.market_cap_rank <= 50:
                advantages.append("안정적인 시장 위치")
            else:
                weaknesses.append("제한적인 시장 인지도")
        
        if coin.commit_count_4_weeks and coin.commit_count_4_weeks > 100:
            advantages.append("활발한 개발 활동")
        elif coin.commit_count_4_weeks and coin.commit_count_4_weeks < 20:
            weaknesses.append("개발 활동 부족")
        
        if coin.telegram_channel_user_count and coin.telegram_channel_user_count > 50000:
            advantages.append("강력한 커뮤니티")
        elif coin.telegram_channel_user_count and coin.telegram_channel_user_count < 5000:
            weaknesses.append("작은 커뮤니티")
        
        # 시장 점유율 추정
        market_share = "알 수 없음"
        if coin.market_cap_rank:
            if coin.market_cap_rank <= 3:
                market_share = "지배적"
            elif coin.market_cap_rank <= 10:
                market_share = "주요"
            elif coin.market_cap_rank <= 50:
                market_share = "중간"
            else:
                market_share = "소규모"
        
        # 성장 잠재력
        growth_potential = "보통"
        if coin.commit_count_4_weeks and coin.commit_count_4_weeks > 50:
            growth_potential = "높음"
        elif not coin.commit_count_4_weeks or coin.commit_count_4_weeks < 10:
            growth_potential = "낮음"
        
        return CompetitiveAnalysis(
            main_competitors=main_competitors[:5],  # 상위 5개만
            competitive_advantages=advantages,
            competitive_weaknesses=weaknesses,
            market_share=market_share,
            growth_potential=growth_potential,
            moat_strength="강함" if coin.market_cap_rank and coin.market_cap_rank <= 10 else "보통"
        )
    
    async def _calculate_overall_score(
        self, 
        coin: CoingeckoCoinDetails,
        dev_activity: DevelopmentActivity, 
        community: CommunityHealth,
        maturity: EcosystemMaturity, 
        innovation: TechnicalInnovation
    ) -> Tuple[int, str, str]:
        """종합 점수 계산"""
        
        # 카테고리별 가중치 가져오기
        category = await self._get_primary_category(coin)
        weights = self.CATEGORY_WEIGHTS.get(category, self.CATEGORY_WEIGHTS["default"])
        
        # 가중 평균 계산
        overall_score = (
            dev_activity.development_score * weights["development"] +
            community.community_score * weights["community"] +
            maturity.maturity_score * weights["maturity"] +
            innovation.innovation_score * weights["innovation"]
        )
        
        overall_score = int(overall_score)
        overall_grade = self._score_to_grade(overall_score)
        
        # 건강도 상태
        if overall_score >= 90:
            health_status = "매우건강"
        elif overall_score >= 75:
            health_status = "건강"
        elif overall_score >= 60:
            health_status = "보통"
        elif overall_score >= 40:
            health_status = "주의"
        else:
            health_status = "위험"
        
        return overall_score, overall_grade, health_status
    
    async def _generate_investment_summary(
        self, 
        coin: CoingeckoCoinDetails, 
        overall_score: int, 
        risk_factors: RiskFactors
    ) -> Dict[str, str]:
        """투자 관점 요약 생성"""
        
        # 투자 준비도
        if overall_score >= 80 and risk_factors.overall_risk_level in ["낮음", "보통"]:
            investment_readiness = "높음"
        elif overall_score >= 60:
            investment_readiness = "보통"
        else:
            investment_readiness = "낮음"
        
        # 장기 생존 가능성
        long_term_viability = "높음"
        if coin.market_cap_rank and coin.market_cap_rank <= 50:
            long_term_viability = "높음"
        elif coin.market_cap_rank and coin.market_cap_rank <= 200:
            long_term_viability = "보통"
        else:
            long_term_viability = "불확실"
        
        # 투자 권고 요약
        if overall_score >= 85 and investment_readiness == "높음":
            recommendation = "적극 투자 검토 권장"
        elif overall_score >= 70:
            recommendation = "신중한 투자 검토 권장"
        elif overall_score >= 50:
            recommendation = "제한적 투자 고려"
        else:
            recommendation = "투자 주의 필요"
        
        return {
            "investment_readiness": investment_readiness,
            "long_term_viability": long_term_viability,
            "recommendation_summary": recommendation
        }
    
    async def _evaluate_data_quality(self, coin: CoingeckoCoinDetails) -> DataQuality:
        """데이터 품질 평가"""
        
        available_metrics = []
        missing_metrics = []
        
        # 체크할 주요 지표들
        key_metrics = {
            "commit_count_4_weeks": coin.commit_count_4_weeks,
            "stars": coin.stars,
            "forks": coin.forks,
            "total_issues": coin.total_issues,
            "closed_issues": coin.closed_issues,
            "telegram_users": coin.telegram_channel_user_count,
            "reddit_subscribers": coin.reddit_subscribers,
            "twitter_followers": coin.twitter_followers,
            "market_cap_rank": coin.market_cap_rank,
            "genesis_date": coin.genesis_date
        }
        
        for metric, value in key_metrics.items():
            if value is not None and value != 0:
                available_metrics.append(metric)
            else:
                missing_metrics.append(metric)
        
        # 데이터 완성도 계산
        completeness = len(available_metrics) / len(key_metrics) * 100
        
        # 신뢰도 점수
        reliability_score = int(completeness)
        if coin.market_cap_rank and coin.market_cap_rank <= 100:
            reliability_score = min(reliability_score + 10, 100)  # 상위 코인 보너스
        
        # 추정 방법
        estimation_methods = []
        if len(missing_metrics) > 0:
            estimation_methods.append("카테고리별 평균 추정")
            estimation_methods.append("시장 순위 기반 추정")
        
        return DataQuality(
            available_metrics=available_metrics,
            missing_metrics=missing_metrics,
            data_completeness=f"{completeness:.1f}%",
            estimation_methods=estimation_methods,
            reliability_score=reliability_score
        )
    
    # 헬퍼 메서드들
    
    def _score_to_grade(self, score: int) -> str:
        """점수를 등급으로 변환"""
        for grade, threshold in self.GRADE_THRESHOLDS.items():
            if score >= threshold:
                return grade
        return "F"
    
    async def _get_primary_category(self, coin: CoingeckoCoinDetails) -> str:
        """주요 카테고리 추출"""
        if coin.categories:
            try:
                categories = json.loads(coin.categories) if isinstance(coin.categories, str) else coin.categories
                return categories[0] if categories else "default"
            except:
                return "default"
        return "default"
    
    async def _get_categories_list(self, coin: CoingeckoCoinDetails) -> List[str]:
        """카테고리 리스트 추출"""
        if coin.categories:
            try:
                return json.loads(coin.categories) if isinstance(coin.categories, str) else coin.categories
            except:
                return []
        return []
    
    async def _estimate_missing_metric(self, coin: CoingeckoCoinDetails, metric: str, calc_func) -> float:
        """누락된 지표 추정"""
        if not self._all_coins_cache:
            return 0
        
        # 비슷한 순위의 코인들에서 평균 계산
        similar_coins = [
            c for c in self._all_coins_cache 
            if c.market_cap_rank 
            and coin.market_cap_rank
            and abs(c.market_cap_rank - coin.market_cap_rank) <= 20
            and getattr(c, metric) is not None
        ]
        
        if similar_coins:
            avg_value = sum(getattr(c, metric) for c in similar_coins) / len(similar_coins)
            return calc_func(avg_value)
        
        return 0
    
    async def _estimate_community_score_by_rank(self, rank: Optional[int]) -> float:
        """순위 기반 커뮤니티 점수 추정"""
        if not rank:
            return 20  # 기본값
        
        if rank <= 10:
            return 25
        elif rank <= 50:
            return 20
        elif rank <= 200:
            return 15
        else:
            return 10
    
    async def _estimate_category_rank(self, coin: CoingeckoCoinDetails) -> Optional[int]:
        """카테고리 내 순위 추정 (단순화)"""
        if not coin.market_cap_rank:
            return None
        
        # 단순한 추정: 전체 순위를 카테고리 크기로 나누기
        category_size_map = {
            "Smart Contract Platform": 50,
            "DeFi": 100,
            "Layer 1 (L1)": 30,
            "Stablecoins": 20,
            "Meme": 200
        }
        
        category = await self._get_primary_category(coin)
        category_size = category_size_map.get(category, 100)
        
        # 대략적인 카테고리 내 순위 추정
        estimated_rank = min(coin.market_cap_rank // (1000 // category_size) + 1, category_size)
        return estimated_rank