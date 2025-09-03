# app/schemas/crypto_detail_ecosystem_schema.py

from pydantic import BaseModel, Field
from typing import Optional, List
from decimal import Decimal
from datetime import datetime


class DevelopmentActivity(BaseModel):
    """개발 활성도"""
    # 원본 GitHub 지표
    commit_count_4_weeks: Optional[int] = None
    stars: Optional[int] = None
    forks: Optional[int] = None
    total_issues: Optional[int] = None
    closed_issues: Optional[int] = None
    
    # 계산된 지표
    issues_resolved_rate: Optional[Decimal] = Field(None, description="이슈 해결률 (%)")
    commit_activity_score: Optional[int] = Field(None, description="커밋 활성도 점수 (0-25)")
    popularity_score: Optional[int] = Field(None, description="인기도 점수 (0-20)")
    community_engagement_score: Optional[int] = Field(None, description="커뮤니티 참여 점수 (0-15)")
    code_quality_score: Optional[int] = Field(None, description="코드 품질 점수 (0-40)")
    
    # 종합 점수
    development_score: Optional[int] = Field(None, description="개발 활성도 종합 점수 (0-100)")
    development_grade: Optional[str] = Field(None, description="개발 등급 (A+/A/B+/B/C/D)")


class CommunityHealth(BaseModel):
    """커뮤니티 건강도"""
    # 소셜 미디어 지표
    telegram_users: Optional[int] = None
    reddit_subscribers: Optional[int] = None
    twitter_followers: Optional[int] = None
    
    # 계산된 지표
    social_presence_score: Optional[int] = Field(None, description="소셜 미디어 존재감 (0-30)")
    engagement_diversity_score: Optional[int] = Field(None, description="참여 다양성 점수 (0-30)")
    community_growth_trend: Optional[str] = Field(None, description="커뮤니티 성장 추세")
    
    # 종합 점수
    community_score: Optional[int] = Field(None, description="커뮤니티 건강도 점수 (0-100)")
    community_grade: Optional[str] = Field(None, description="커뮤니티 등급 (A+/A/B+/B/C/D)")


class EcosystemMaturity(BaseModel):
    """생태계 성숙도"""
    # 기본 지표
    project_age_years: Optional[int] = None
    market_cap_rank: Optional[int] = None
    category: Optional[str] = None
    
    # 생태계 지표
    developer_ecosystem_size: Optional[str] = Field(None, description="개발자 생태계 규모")
    adoption_level: Optional[str] = Field(None, description="채택 수준 (초기/성장/성숙/주류)")
    network_effect: Optional[str] = Field(None, description="네트워크 효과 강도")
    
    # 경쟁 분석
    category_rank: Optional[int] = Field(None, description="카테고리 내 순위")
    competitive_position: Optional[str] = Field(None, description="경쟁 위치")
    market_differentiation: Optional[str] = Field(None, description="시장 차별화 정도")
    
    # 성숙도 점수
    maturity_score: Optional[int] = Field(None, description="생태계 성숙도 점수 (0-100)")
    maturity_grade: Optional[str] = Field(None, description="성숙도 등급 (A+/A/B+/B/C/D)")


class TechnicalInnovation(BaseModel):
    """기술적 혁신성"""
    # 기술 특성
    consensus_mechanism: Optional[str] = None
    blockchain_type: Optional[str] = None
    smart_contract_support: Optional[bool] = None
    
    # 혁신 지표
    technical_uniqueness: Optional[str] = Field(None, description="기술적 독창성")
    innovation_frequency: Optional[str] = Field(None, description="혁신 빈도")
    research_activity: Optional[str] = Field(None, description="연구 활동 수준")
    
    # 기술 채택
    developer_adoption: Optional[str] = Field(None, description="개발자 채택도")
    integration_count: Optional[int] = Field(None, description="통합 프로젝트 수")
    
    # 혁신성 점수
    innovation_score: Optional[int] = Field(None, description="기술 혁신성 점수 (0-100)")
    innovation_grade: Optional[str] = Field(None, description="혁신성 등급 (A+/A/B+/B/C/D)")


class RiskFactors(BaseModel):
    """리스크 요인"""
    # 개발 리스크
    development_risk: Optional[str] = Field(None, description="개발 리스크 (높음/보통/낮음)")
    team_centralization: Optional[str] = Field(None, description="팀 중앙화 정도")
    funding_sustainability: Optional[str] = Field(None, description="자금 지속 가능성")
    
    # 커뮤니티 리스크
    community_dependency: Optional[str] = Field(None, description="커뮤니티 의존도")
    governance_risk: Optional[str] = Field(None, description="거버넌스 리스크")
    
    # 기술 리스크
    technical_debt: Optional[str] = Field(None, description="기술 부채 수준")
    scalability_concerns: Optional[str] = Field(None, description="확장성 우려")
    security_track_record: Optional[str] = Field(None, description="보안 이력")
    
    # 종합 리스크
    overall_risk_level: Optional[str] = Field(None, description="종합 리스크 수준")
    risk_mitigation_strategies: Optional[List[str]] = Field([], description="리스크 완화 방안")


class CompetitiveAnalysis(BaseModel):
    """경쟁 분석"""
    # 경쟁자 식별
    main_competitors: Optional[List[str]] = Field([], description="주요 경쟁자")
    competitive_advantages: Optional[List[str]] = Field([], description="경쟁 우위")
    competitive_weaknesses: Optional[List[str]] = Field([], description="경쟁 열세")
    
    # 시장 포지션
    market_share: Optional[str] = Field(None, description="시장 점유율")
    growth_potential: Optional[str] = Field(None, description="성장 잠재력")
    moat_strength: Optional[str] = Field(None, description="경제적 해자 강도")


class DataQuality(BaseModel):
    """데이터 품질 지표"""
    available_metrics: Optional[List[str]] = Field([], description="사용 가능한 지표")
    missing_metrics: Optional[List[str]] = Field([], description="누락된 지표")
    data_completeness: Optional[str] = Field(None, description="데이터 완성도 (%)")
    estimation_methods: Optional[List[str]] = Field([], description="추정 방법")
    reliability_score: Optional[int] = Field(None, description="신뢰도 점수 (0-100)")


class CryptoEcosystemResponse(BaseModel):
    """Crypto Detail Ecosystem 전체 응답"""
    # 기본 정보
    coingecko_id: str
    symbol: str
    name: str
    market_cap_rank: Optional[int] = None
    
    # 주요 분석 영역
    development_activity: DevelopmentActivity
    community_health: CommunityHealth
    ecosystem_maturity: EcosystemMaturity
    technical_innovation: TechnicalInnovation
    risk_factors: RiskFactors
    competitive_analysis: CompetitiveAnalysis
    
    # 종합 평가
    overall_ecosystem_score: Optional[int] = Field(None, description="생태계 종합 점수 (0-100)")
    overall_grade: Optional[str] = Field(None, description="종합 등급 (A+/A/B+/B/C/D)")
    ecosystem_health: Optional[str] = Field(None, description="생태계 건강도 (매우건강/건강/보통/주의/위험)")
    
    # 투자 관점 요약
    investment_readiness: Optional[str] = Field(None, description="투자 준비도")
    long_term_viability: Optional[str] = Field(None, description="장기 생존 가능성")
    recommendation_summary: Optional[str] = Field(None, description="투자 권고 요약")
    
    # 메타데이터
    data_quality: DataQuality
    last_updated: datetime = Field(default_factory=datetime.now)
    analysis_version: str = Field(default="1.0", description="분석 알고리즘 버전")
    
    class Config:
        json_encoders = {
            Decimal: lambda x: float(x) if x is not None else None,
            datetime: lambda x: x.isoformat() if x is not None else None
        }
        schema_extra = {
            "example": {
                "symbol": "BTC",
                "name": "Bitcoin",
                "development_activity": {
                    "development_score": 85,
                    "development_grade": "A"
                },
                "community_health": {
                    "community_score": 92,
                    "community_grade": "A+"
                },
                "overall_ecosystem_score": 88,
                "overall_grade": "A",
                "ecosystem_health": "매우건강"
            }
        }