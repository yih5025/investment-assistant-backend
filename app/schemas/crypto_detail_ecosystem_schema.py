# app/schemas/crypto_detail_ecosystem_schema.py

from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime


class BasicInfo(BaseModel):
    """프로젝트 기본 정보"""
    name: str
    symbol: str
    description: Optional[str] = None
    categories: Optional[List[str]] = Field(default=[], description="프로젝트 카테고리")
    genesis_date: Optional[str] = None
    project_age_days: Optional[int] = None
    official_links: Dict[str, Optional[str]] = Field(
        default={},
        description="공식 링크들 (homepage, twitter, github)"
    )
    market_cap_rank: Optional[int] = None


class GitHubMetrics(BaseModel):
    """GitHub 원시 지표"""
    commit_count_4_weeks: Optional[int] = None
    stars: Optional[int] = None
    forks: Optional[int] = None
    total_issues: Optional[int] = None
    closed_issues: Optional[int] = None
    issues_resolved_rate_percent: Optional[float] = Field(
        None, 
        description="이슈 해결률 (단순 수학 계산)"
    )


class ActivityIndicators(BaseModel):
    """활동 지표"""
    commits_per_week: Optional[float] = Field(
        None,
        description="주간 평균 커밋 수"
    )
    last_updated: str


class ComparisonBenchmarks(BaseModel):
    """비교 벤치마크"""
    weekly_commits: Dict[str, str] = Field(
        default={
            "very_high": "> 50 commits/week",
            "high": "20-50 commits/week", 
            "moderate": "5-20 commits/week",
            "low": "< 5 commits/week"
        }
    )
    examples: Dict[str, str] = Field(
        default={
            "ethereum": "~300 commits/week (대형 프로젝트)",
            "bitcoin": "~50 commits/week (성숙한 프로젝트)",
            "typical_defi": "~20 commits/week (활발한 DeFi)"
        }
    )
    community_metrics: Dict[str, Dict[str, str]] = Field(
        default={
            "stars": {
                "major_project": "> 10,000 stars",
                "popular": "1,000-10,000 stars",
                "emerging": "100-1,000 stars",
                "new": "< 100 stars"
            }
        }
    )


class InterpretationGuide(BaseModel):
    """해석 가이드"""
    commits_4w: Dict[str, str] = Field(
        default={
            "high_activity": "> 200 commits",
            "moderate_activity": "50-200 commits", 
            "low_activity": "< 50 commits",
            "note": "더 많은 커밋이 반드시 더 좋은 프로젝트를 의미하지는 않습니다."
        }
    )
    issues_resolution: Dict[str, str] = Field(
        default={
            "excellent": "> 90% 해결률",
            "good": "70-90% 해결률",
            "concerning": "< 70% 해결률",
            "note": "해결률이 높을수록 개발팀이 활발히 문제를 처리하고 있음을 의미합니다."
        }
    )


class DevelopmentActivity(BaseModel):
    """개발 활동 - 데이터와 가이드 통합"""
    github_metrics: GitHubMetrics
    activity_indicators: ActivityIndicators
    comparison_benchmarks: ComparisonBenchmarks
    interpretation_guide: InterpretationGuide


class SocialMetrics(BaseModel):
    """소셜 미디어 지표"""
    telegram_users: Optional[int] = None
    reddit_subscribers: Optional[int] = None
    twitter_followers: Optional[int] = None


class PlatformPresence(BaseModel):
    """플랫폼 존재 여부"""
    has_telegram: bool = False
    has_reddit: bool = False
    has_twitter: bool = False
    total_platforms: int = 0


class SizeReferences(BaseModel):
    """크기 참조 기준"""
    telegram: Dict[str, str] = Field(
        default={
            "small": "< 1,000 사용자",
            "medium": "1,000 - 50,000 사용자",
            "large": "> 50,000 사용자",
            "note": "텔레그램은 암호화폐 커뮤니티에서 가장 활발한 플랫폼입니다."
        }
    )
    reddit: Dict[str, str] = Field(
        default={
            "small": "< 5,000 구독자",
            "medium": "5,000 - 50,000 구독자", 
            "large": "> 50,000 구독자",
            "note": "Reddit은 기술적 토론이 많이 이루어지는 플랫폼입니다."
        }
    )


class CommunityMetrics(BaseModel):
    """커뮤니티 지표 - 데이터와 가이드 통합"""
    social_metrics: SocialMetrics
    platform_presence: PlatformPresence
    size_references: SizeReferences


class RankingInfo(BaseModel):
    """순위 정보"""
    current_rank: Optional[int] = None
    total_cryptocurrencies: Optional[int] = None
    rank_category: str = "알 수 없음"
    top_percentile: Optional[float] = Field(
        None,
        description="상위 몇 퍼센트인지"
    )


class RankingInterpretation(BaseModel):
    """순위 해석"""
    top_10: str = "글로벌 주류 암호화폐"
    top_50: str = "안정적으로 인정받는 프로젝트"
    top_200: str = "상당한 관심을 받는 프로젝트"
    top_1000: str = "틈새 시장 또는 신생 프로젝트"
    note: str = "순위는 시가총액 기준이며, 프로젝트의 기술적 우수성과는 다를 수 있습니다."


class MarketPosition(BaseModel):
    """시장 위치 - 데이터와 해석 통합"""
    ranking_info: RankingInfo
    category_context: Optional[Dict[str, Any]] = Field(default={})
    ranking_interpretation: RankingInterpretation


class ComparisonMetrics(BaseModel):
    """비교 지표"""
    how_to_compare: List[str] = Field(
        default=[
            "GitHub 활동도를 비교해보세요",
            "커뮤니티 크기를 비교해보세요", 
            "프로젝트 연령을 고려해보세요",
            "카테고리 내 특색을 파악해보세요"
        ]
    )
    what_numbers_mean: Dict[str, str] = Field(
        default={
            "github_commits": "개발 활발함의 지표 (단, 팀 크기에 따라 다름)",
            "community_size": "관심도의 지표 (단, 활성도와는 다를 수 있음)",
            "market_cap_rank": "시장 인지도 (단, 투기적 요소 포함 가능)"
        }
    )


class ComparativeContext(BaseModel):
    """비교 맥락"""
    category_peers: Optional[List[Dict[str, Any]]] = Field(
        default=[],
        description="같은 카테고리의 다른 프로젝트들"
    )
    rank_peers: Optional[List[Dict[str, Any]]] = Field(
        default=[],
        description="비슷한 순위대의 프로젝트들"
    )
    comparison_metrics: ComparisonMetrics


class DataAvailability(BaseModel):
    """데이터 가용성"""
    available: List[str] = Field(default=[], description="사용 가능한 지표들")
    missing: List[str] = Field(default=[], description="누락된 지표들")
    completeness_percent: float = Field(default=0.0, description="데이터 완성도 (%)")


class DataTransparency(BaseModel):
    """데이터 투명성"""
    data_availability: DataAvailability
    data_sources: List[str] = Field(
        default=[
            "CoinGecko API",
            "GitHub API (일부)",
            "소셜 미디어 API (일부)"
        ],
        description="데이터 출처"
    )
    limitations: List[str] = Field(
        default=[
            "모든 프로젝트가 GitHub을 공개하지 않음",
            "소셜 미디어 팔로워는 실제 활성 사용자와 다를 수 있음",
            "시가총액 순위는 시장 변동에 따라 수시로 변함",
            "개발 활동도가 높다고 반드시 좋은 프로젝트는 아님"
        ]
    )
    how_to_use: List[str] = Field(
        default=[
            "여러 지표를 종합적으로 고려하세요",
            "절대적 숫자보다는 상대적 비교에 활용하세요",
            "누락된 데이터가 있음을 감안하여 판단하세요",
            "최신 정보를 별도로 확인해보세요"
        ]
    )


class TransparentCryptoEcosystemResponse(BaseModel):
    """투명성 중심의 암호화폐 생태계 응답 - 서비스 구조 반영"""
    
    # 기본 정보
    basic_info: BasicInfo
    
    # 주요 분석 영역 (서비스 구조와 일치)
    development_activity: DevelopmentActivity
    community_metrics: CommunityMetrics
    market_position: MarketPosition
    comparative_context: ComparativeContext
    data_transparency: DataTransparency
    
    # 메타데이터
    analysis_timestamp: datetime = Field(default_factory=datetime.now)
    schema_version: str = Field(default="2.0")
    
    class Config:
        json_encoders = {
            datetime: lambda x: x.isoformat() if x is not None else None
        }
        
        schema_extra = {
            "example": {
                "basic_info": {
                    "symbol": "BTC",
                    "name": "Bitcoin",
                    "market_cap_rank": 1
                },
                "development_activity": {
                    "github_metrics": {
                        "commit_count_4_weeks": 147,
                        "stars": 70000,
                        "issues_resolved_rate_percent": 89.1
                    }
                },
                "data_transparency": {
                    "data_availability": {
                        "completeness_percent": 85.7,
                        "available": ["GitHub 커밋", "GitHub 스타", "시장 순위"],
                        "missing": ["텔레그램 사용자"]
                    }
                }
            }
        }


# 개별 섹션 응답용 스키마들
class DevelopmentOnlyResponse(BaseModel):
    """개발 활동 단독 응답"""
    symbol: str
    development_activity: DevelopmentActivity


class CommunityOnlyResponse(BaseModel):
    """커뮤니티 단독 응답"""
    symbol: str
    community_health: CommunityMetrics  # community_metrics -> community_health로 매핑


class MarketOnlyResponse(BaseModel):
    """시장 위치 단독 응답"""
    symbol: str
    market_position: MarketPosition