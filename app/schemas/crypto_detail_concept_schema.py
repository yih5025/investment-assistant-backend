# app/schemas/crypto_detail_concept_schema.py

from pydantic import BaseModel, Field
from typing import Optional, List, Dict
from datetime import datetime


class BasicInfo(BaseModel):
    """기본 코인 정보"""
    coingecko_id: str
    symbol: str
    name: str
    web_slug: Optional[str] = None
    market_cap_rank: Optional[int] = None
    
    # 이미지
    image_thumb: Optional[str] = None
    image_small: Optional[str] = None
    image_large: Optional[str] = None


class ConceptDescription(BaseModel):
    """개념 설명 데이터"""
    description_original: Optional[str] = Field(None, description="영어 원문 설명")
    description_summary: Optional[str] = Field(None, description="한국어 요약 (향후 추가)")
    
    # 탄생 배경
    genesis_date: Optional[str] = Field(None, description="탄생일")
    project_age_years: Optional[int] = Field(None, description="프로젝트 연령 (년)")
    country_origin: Optional[str] = Field(None, description="발원 국가")


class CategoryInfo(BaseModel):
    """카테고리 정보"""
    categories_original: Optional[List[str]] = Field([], description="영어 카테고리")
    categories_korean: Optional[List[str]] = Field([], description="한국어 카테고리 매핑")
    primary_category: Optional[str] = Field(None, description="주요 카테고리")
    category_description: Optional[str] = Field(None, description="카테고리 설명")


class ProjectLinks(BaseModel):
    """프로젝트 링크 정보"""
    homepage_url: Optional[str] = None
    blockchain_site: Optional[str] = None
    
    # 소셜 미디어
    twitter_screen_name: Optional[str] = None
    facebook_username: Optional[str] = None
    telegram_channel_identifier: Optional[str] = None
    subreddit_url: Optional[str] = None
    
    # GitHub 저장소
    github_repos: Optional[List[str]] = Field([], description="GitHub 저장소 목록")
    main_github_url: Optional[str] = Field(None, description="메인 GitHub URL")


class KeyFeatures(BaseModel):
    """핵심 특징"""
    consensus_algorithm: Optional[str] = Field(None, description="합의 알고리즘")
    blockchain_type: Optional[str] = Field(None, description="블록체인 타입")
    use_cases: Optional[List[str]] = Field([], description="주요 사용 사례")
    unique_features: Optional[List[str]] = Field([], description="고유 특징")


class MarketPosition(BaseModel):
    """시장 위치"""
    market_cap_rank: Optional[int] = None
    rank_description: Optional[str] = Field(None, description="순위 설명")
    category_rank: Optional[str] = Field(None, description="카테고리 내 순위")
    market_presence: Optional[str] = Field(None, description="시장 존재감")


class EducationalContent(BaseModel):
    """교육 콘텐츠"""
    what_is_it: Optional[str] = Field(None, description="이 코인이 무엇인가?")
    how_it_works: Optional[str] = Field(None, description="작동 원리")
    why_created: Optional[str] = Field(None, description="개발 배경")
    real_world_usage: Optional[List[str]] = Field([], description="실생활 활용 예시")
    
    # 초보자를 위한 설명
    beginner_summary: Optional[str] = Field(None, description="초보자용 한줄 요약")
    difficulty_level: Optional[str] = Field(None, description="이해 난이도 (쉬움/보통/어려움)")


class FAQ(BaseModel):
    """자주 묻는 질문"""
    question: str
    answer: str
    category: Optional[str] = Field(None, description="질문 카테고리")


class CryptoConceptResponse(BaseModel):
    """Crypto Detail Concept 전체 응답"""
    basic_info: BasicInfo
    concept_description: ConceptDescription
    category_info: CategoryInfo
    project_links: ProjectLinks
    key_features: KeyFeatures
    market_position: MarketPosition
    educational_content: EducationalContent
    faqs: Optional[List[FAQ]] = Field([], description="자주 묻는 질문들")
    
    # 메타데이터
    last_updated: datetime = Field(default_factory=datetime.now)
    data_completeness: Optional[str] = Field(None, description="데이터 완성도 (높음/보통/낮음)")
    
    class Config:
        json_encoders = {
            datetime: lambda x: x.isoformat() if x is not None else None
        }
        schema_extra = {
            "example": {
                "basic_info": {
                    "coingecko_id": "bitcoin",
                    "symbol": "BTC", 
                    "name": "Bitcoin",
                    "market_cap_rank": 1
                },
                "concept_description": {
                    "genesis_date": "2009-01-03",
                    "project_age_years": 15
                },
                "category_info": {
                    "primary_category": "디지털 화폐",
                    "categories_korean": ["화폐", "가치저장수단", "디지털골드"]
                }
            }
        }