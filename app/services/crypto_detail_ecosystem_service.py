from sqlalchemy.orm import Session
from typing import Optional, List, Dict, Any
from datetime import datetime
import json

class CryptoEcosystemService:
    """
    AI는 판단하지 않고, 정보만 깔끔하게 정리해서 제공
    모든 판단은 사용자가 직접 수행
    """
    
    def __init__(self, db: Session):
        self.db = db
    
    async def organize_crypto_information(self, symbol: str) -> Optional[Dict[str, Any]]:
        """정보 정리 서비스 - 판단 없이 팩트만 제공"""
        
        coin = await self._get_coin_details(symbol)
        if not coin:
            return None
        
        # 1. 원시 데이터 정리
        organized_data = {
            "basic_info": await self._organize_basic_info(coin),
            "development_activity": await self._organize_development_data(coin),
            "community_metrics": await self._organize_community_data(coin),
            "market_position": await self._organize_market_data(coin),
            "comparative_context": await self._organize_comparative_context(coin),
            "data_transparency": await self._assess_data_quality(coin)
        }
        
        return organized_data
    
    async def _organize_basic_info(self, coin) -> Dict[str, Any]:
        """기본 정보 정리 - 팩트만"""
        
        return {
            "name": coin.name,
            "symbol": coin.symbol,
            "description": coin.description_en,
            "categories": await self._parse_categories(coin.categories),
            "genesis_date": coin.genesis_date,
            "project_age_days": await self._calculate_age_in_days(coin.genesis_date),
            "official_links": {
                "homepage": coin.homepage_url,
                "twitter": coin.twitter_screen_name,
                "github": await self._extract_github_url(coin)
            },
            "market_cap_rank": coin.market_cap_rank
        }
    
    async def _organize_development_data(self, coin) -> Dict[str, Any]:
        """개발 활동 데이터 정리 - 해석 없이 숫자만"""
        
        # 이슈 해결률 계산 (단순 수학)
        issues_resolved_rate = None
        if coin.total_issues and coin.closed_issues:
            issues_resolved_rate = round(coin.closed_issues / coin.total_issues * 100, 1)
        
        return {
            "github_metrics": {
                "commit_count_4_weeks": coin.commit_count_4_weeks,
                "stars": coin.stars,
                "forks": coin.forks,
                "watchers": coin.watchers,
                "total_issues": coin.total_issues,
                "closed_issues": coin.closed_issues,
                "issues_resolved_rate_percent": issues_resolved_rate
            },
            "activity_indicators": {
                "commits_per_week": coin.commit_count_4_weeks / 4 if coin.commit_count_4_weeks else None,
                "last_updated": datetime.now().isoformat()
            },
            "comparison_benchmarks": await self._get_github_benchmarks(),
            "interpretation_guide": {
                "commits_4w": {
                    "high_activity": "> 200 commits",
                    "moderate_activity": "50-200 commits", 
                    "low_activity": "< 50 commits",
                    "note": "더 많은 커밋이 반드시 더 좋은 프로젝트를 의미하지는 않습니다."
                },
                "issues_resolution": {
                    "excellent": "> 90% 해결률",
                    "good": "70-90% 해결률",
                    "concerning": "< 70% 해결률",
                    "note": "해결률이 높을수록 개발팀이 활발히 문제를 처리하고 있음을 의미합니다."
                }
            }
        }
    
    async def _organize_community_data(self, coin) -> Dict[str, Any]:
        """커뮤니티 데이터 정리 - 숫자와 맥락만 제공"""
        
        return {
            "social_metrics": {
                "telegram_users": coin.telegram_channel_user_count,
                "reddit_subscribers": coin.reddit_subscribers, 
                "twitter_followers": coin.twitter_followers
            },
            "platform_presence": {
                "has_telegram": coin.telegram_channel_user_count is not None,
                "has_reddit": coin.reddit_subscribers is not None,
                "has_twitter": coin.twitter_followers is not None,
                "total_platforms": sum([
                    1 for x in [coin.telegram_channel_user_count, coin.reddit_subscribers, coin.twitter_followers] 
                    if x is not None
                ])
            },
            "size_references": {
                "telegram": {
                    "small": "< 1,000 사용자",
                    "medium": "1,000 - 50,000 사용자",
                    "large": "> 50,000 사용자",
                    "note": "텔레그램은 암호화폐 커뮤니티에서 가장 활발한 플랫폼입니다."
                },
                "reddit": {
                    "small": "< 5,000 구독자",
                    "medium": "5,000 - 50,000 구독자", 
                    "large": "> 50,000 구독자",
                    "note": "Reddit은 기술적 토론이 많이 이루어지는 플랫폼입니다."
                }
            }
        }
    
    async def _organize_market_data(self, coin) -> Dict[str, Any]:
        """시장 데이터 정리 - 순위와 맥락 정보"""
        
        total_cryptos = await self._get_total_crypto_count()
        
        # 순위 구간 분류 (객관적 기준)
        rank_category = "알 수 없음"
        if coin.market_cap_rank:
            if coin.market_cap_rank <= 10:
                rank_category = "최상위 (Top 10)"
            elif coin.market_cap_rank <= 50:
                rank_category = "상위 (Top 50)"
            elif coin.market_cap_rank <= 200:
                rank_category = "중위 (Top 200)"
            elif coin.market_cap_rank <= 1000:
                rank_category = "하위 (Top 1000)"
            else:
                rank_category = f"기타 ({coin.market_cap_rank}위)"
        
        return {
            "ranking_info": {
                "current_rank": coin.market_cap_rank,
                "total_cryptocurrencies": total_cryptos,
                "rank_category": rank_category,
                "top_percentile": round((1 - coin.market_cap_rank / total_cryptos) * 100, 1) if coin.market_cap_rank else None
            },
            "category_context": await self._get_category_context(coin),
            "ranking_interpretation": {
                "top_10": "글로벌 주류 암호화폐",
                "top_50": "안정적으로 인정받는 프로젝트",
                "top_200": "상당한 관심을 받는 프로젝트",
                "top_1000": "틈새 시장 또는 신생 프로젝트",
                "note": "순위는 시가총액 기준이며, 프로젝트의 기술적 우수성과는 다를 수 있습니다."
            }
        }
    
    async def _organize_comparative_context(self, coin) -> Dict[str, Any]:
        """비교 맥락 제공 - 비슷한 프로젝트들과의 단순 비교"""
        
        # 같은 카테고리 프로젝트들
        similar_projects = await self._find_similar_category_projects(coin)
        
        # 비슷한 순위대 프로젝트들  
        similar_rank_projects = await self._find_similar_rank_projects(coin)
        
        return {
            "category_peers": similar_projects,
            "rank_peers": similar_rank_projects,
            "comparison_metrics": {
                "how_to_compare": [
                    "GitHub 활동도를 비교해보세요",
                    "커뮤니티 크기를 비교해보세요", 
                    "프로젝트 연령을 고려해보세요",
                    "카테고리 내 특색을 파악해보세요"
                ],
                "what_numbers_mean": {
                    "github_commits": "개발 활발함의 지표 (단, 팀 크기에 따라 다름)",
                    "community_size": "관심도의 지표 (단, 활성도와는 다를 수 있음)",
                    "market_cap_rank": "시장 인지도 (단, 투기적 요소 포함 가능)"
                }
            }
        }
    
    async def _assess_data_quality(self, coin) -> Dict[str, Any]:
        """데이터 품질 투명하게 공개"""
        
        available_fields = []
        missing_fields = []
        
        data_fields = {
            "GitHub 커밋": coin.commit_count_4_weeks,
            "GitHub 스타": coin.stars,
            "텔레그램": coin.telegram_channel_user_count,
            "Reddit": coin.reddit_subscribers,
            "Twitter": coin.twitter_followers,
            "프로젝트 설명": coin.description_en,
            "창립일": coin.genesis_date
        }
        
        for field, value in data_fields.items():
            if value is not None and str(value).strip():
                available_fields.append(field)
            else:
                missing_fields.append(field)
        
        return {
            "data_availability": {
                "available": available_fields,
                "missing": missing_fields,
                "completeness_percent": round(len(available_fields) / len(data_fields) * 100, 1)
            },
            "data_sources": [
                "CoinGecko API",
                "GitHub API (일부)",
                "소셜 미디어 API (일부)"
            ],
            "limitations": [
                "모든 프로젝트가 GitHub을 공개하지 않음",
                "소셜 미디어 팔로워는 실제 활성 사용자와 다를 수 있음",
                "시가총액 순위는 시장 변동에 따라 수시로 변함",
                "개발 활동도가 높다고 반드시 좋은 프로젝트는 아님"
            ],
            "how_to_use": [
                "여러 지표를 종합적으로 고려하세요",
                "절대적 숫자보다는 상대적 비교에 활용하세요",
                "누락된 데이터가 있음을 감안하여 판단하세요",
                "최신 정보를 별도로 확인해보세요"
            ]
        }
    
    async def _get_github_benchmarks(self) -> Dict[str, Any]:
        """GitHub 활동도 벤치마크 - 참고용 가이드라인"""
        
        return {
            "weekly_commits": {
                "very_high": "> 50 commits/week",
                "high": "20-50 commits/week", 
                "moderate": "5-20 commits/week",
                "low": "< 5 commits/week",
                "examples": {
                    "ethereum": "~300 commits/week (대형 프로젝트)",
                    "bitcoin": "~50 commits/week (성숙한 프로젝트)",
                    "typical_defi": "~20 commits/week (활발한 DeFi)"
                }
            },
            "community_metrics": {
                "stars": {
                    "major_project": "> 10,000 stars",
                    "popular": "1,000-10,000 stars",
                    "emerging": "100-1,000 stars",
                    "new": "< 100 stars"
                }
            }
        }
    
    # 사용자에게 제공할 최종 API 응답
    async def get_user_friendly_summary(self, organized_data: Dict[str, Any]) -> Dict[str, Any]:
        """사용자 친화적 요약 - 여전히 판단하지 않고 정보만 정리"""
        
        basic = organized_data["basic_info"]
        dev = organized_data["development_activity"]
        community = organized_data["community_metrics"]
        market = organized_data["market_position"]
        
        return {
            "summary": {
                "project_overview": f"{basic['name']}는 {market['ranking_info']['rank_category']} 암호화폐입니다.",
                "key_numbers": {
                    "시가총액_순위": basic["market_cap_rank"],
                    "GitHub_주간_커밋": dev["github_metrics"]["commit_count_4_weeks"],
                    "텔레그램_사용자": community["social_metrics"]["telegram_users"],
                    "프로젝트_연령": f"{basic['project_age_days']}일" if basic['project_age_days'] else "알 수 없음"
                },
                "what_stands_out": await self._identify_notable_features(organized_data),
                "areas_to_investigate": await self._suggest_investigation_areas(organized_data)
            },
            "next_steps": {
                "for_beginners": [
                    f"'{basic['name']}' 공식 홈페이지에서 프로젝트 목적 확인",
                    "백서(Whitepaper) 읽어보기",
                    "커뮤니티 참여해서 실제 활동 확인"
                ],
                "for_analysts": [
                    "GitHub 저장소에서 최근 개발 내용 확인",
                    "경쟁 프로젝트와 기술적 차별점 비교",
                    "토큰 이코노믹스 분석"
                ],
                "for_investors": [
                    "시장 순위 변화 추이 확인",
                    "실제 사용 사례 및 파트너십 조사",
                    "규제 위험 요소 검토"
                ]
            }
        }
    
    async def _identify_notable_features(self, data: Dict[str, Any]) -> List[str]:
        """주목할 만한 특징 식별 - 객관적 기준만"""
        
        notable = []
        
        # 객관적 기준으로만 판단
        dev = data["development_activity"]["github_metrics"]
        community = data["community_metrics"]["social_metrics"]
        market = data["market_position"]["ranking_info"]
        
        if market["current_rank"] and market["current_rank"] <= 10:
            notable.append("상위 10위 내 주류 암호화폐")
        
        if dev["commit_count_4_weeks"] and dev["commit_count_4_weeks"] > 200:
            notable.append("주간 200+ 커밋으로 활발한 개발 활동")
        
        if community["telegram_users"] and community["telegram_users"] > 100000:
            notable.append("10만+ 텔레그램 커뮤니티 보유")
        
        if dev["stars"] and dev["stars"] > 10000:
            notable.append("GitHub 1만+ 스타 확보")
        
        return notable if notable else ["특별히 주목할 만한 지표 없음"]
    
    async def _suggest_investigation_areas(self, data: Dict[str, Any]) -> List[str]:
        """추가 조사가 필요한 영역 제안"""
        
        suggestions = []
        
        if not data["development_activity"]["github_metrics"]["commit_count_4_weeks"]:
            suggestions.append("GitHub 개발 활동 데이터 없음 - 별도 개발 현황 확인 필요")
        
        if data["data_transparency"]["data_availability"]["completeness_percent"] < 70:
            suggestions.append("데이터 완성도 70% 미만 - 추가 정보 수집 권장")
        
        if not data["community_metrics"]["social_metrics"]["telegram_users"]:
            suggestions.append("커뮤니티 규모 불명 - 실제 사용자 활동 별도 확인")
        
        return suggestions if suggestions else ["현재 가용한 데이터로 기본 분석 가능"]