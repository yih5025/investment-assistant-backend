# app/schemas/x_posts_schema.py

from pydantic import BaseModel, Field, HttpUrl
from datetime import datetime
from typing import Optional, List, Dict, Any, Union
from enum import Enum


class AccountCategory(str, Enum):
    """계정 카테고리 Enum"""
    CORE_INVESTORS = "core_investors"
    CRYPTO = "crypto" 
    INSTITUTIONAL = "institutional"


class CollectionSource(str, Enum):
    """수집 방식 Enum"""
    PRIMARY_TOKEN = "primary_token"
    SECONDARY_TOKEN = "secondary_token"


class SortBy(str, Enum):
    """정렬 기준 Enum"""
    CREATED_AT = "created_at"
    LIKE_COUNT = "like_count"
    RETWEET_COUNT = "retweet_count"
    REPLY_COUNT = "reply_count"
    QUOTE_COUNT = "quote_count"
    BOOKMARK_COUNT = "bookmark_count"
    IMPRESSION_COUNT = "impression_count"
    ENGAGEMENT_SCORE = "engagement_score"


class SortOrder(str, Enum):
    """정렬 순서 Enum"""
    ASC = "asc"
    DESC = "desc"


# ===== 요청 스키마들 =====

class XPostListRequest(BaseModel):
    """X Posts 목록 조회 요청"""
    limit: int = Field(default=20, ge=1, le=100, description="조회할 포스트 수")
    offset: int = Field(default=0, ge=0, description="건너뛸 포스트 수")
    category: Optional[AccountCategory] = Field(default=None, description="계정 카테고리 필터")
    sort_by: SortBy = Field(default=SortBy.CREATED_AT, description="정렬 기준")
    order: SortOrder = Field(default=SortOrder.DESC, description="정렬 순서")
    verified_only: bool = Field(default=False, description="인증된 사용자만 조회")


class XPostSearchRequest(BaseModel):
    """X Posts 검색 요청"""
    q: str = Field(..., min_length=1, max_length=100, description="검색 키워드")
    limit: int = Field(default=20, ge=1, le=100, description="조회할 포스트 수")
    category: Optional[AccountCategory] = Field(default=None, description="계정 카테고리 필터")
    sort_by: SortBy = Field(default=SortBy.CREATED_AT, description="정렬 기준")
    order: SortOrder = Field(default=SortOrder.DESC, description="정렬 순서")


class XPostRankingRequest(BaseModel):
    """X Posts 랭킹 조회 요청"""
    limit: int = Field(default=5, ge=1, le=50, description="조회할 포스트 수")
    category: Optional[AccountCategory] = Field(default=None, description="계정 카테고리 필터")
    period: str = Field(default="24h", pattern="^(1h|6h|24h|7d|30d)$", description="기간 필터")


class UserPostsRequest(BaseModel):
    """사용자별 포스트 조회 요청"""
    limit: int = Field(default=20, ge=1, le=100, description="조회할 포스트 수")
    offset: int = Field(default=0, ge=0, description="건너뛸 포스트 수")
    sort_by: SortBy = Field(default=SortBy.CREATED_AT, description="정렬 기준")
    order: SortOrder = Field(default=SortOrder.DESC, description="정렬 순서")


# ===== 응답 스키마들 =====

class AuthorInfo(BaseModel):
    """작성자 정보"""
    username: str = Field(..., description="사용자명")
    display_name: Optional[str] = Field(None, description="표시 이름")
    verified: bool = Field(False, description="인증 여부")
    followers_count: int = Field(0, description="팔로워 수")
    following_count: int = Field(0, description="팔로잉 수")
    tweet_count: int = Field(0, description="총 트윗 수")


class EngagementInfo(BaseModel):
    """인게이지먼트 정보"""
    likes: int = Field(0, description="좋아요 수")
    retweets: int = Field(0, description="리트윗 수")
    replies: int = Field(0, description="댓글 수")
    quotes: int = Field(0, description="인용 수")
    bookmarks: int = Field(0, description="북마크 수")
    impressions: int = Field(0, description="노출 수")
    engagement_score: int = Field(0, description="종합 인게이지먼트 점수")


class MetadataInfo(BaseModel):
    """메타데이터 정보"""
    hashtags: Optional[List[Dict[str, Any]]] = Field(None, description="해시태그")
    mentions: Optional[List[Dict[str, Any]]] = Field(None, description="멘션")
    urls: Optional[List[Dict[str, Any]]] = Field(None, description="URL")
    cashtags: Optional[List[Dict[str, Any]]] = Field(None, description="캐시태그")


class ClassificationInfo(BaseModel):
    """분류 정보"""
    account_category: AccountCategory = Field(..., description="계정 카테고리")
    collection_source: CollectionSource = Field(..., description="수집 방식")
    source_account: str = Field(..., description="소스 계정")


class XPostBasic(BaseModel):
    """X Posts 기본 응답 (목록용)"""
    tweet_id: str = Field(..., description="트윗 ID")
    text: str = Field(..., description="트윗 내용")
    username: str = Field(..., description="사용자명")
    display_name: Optional[str] = Field(None, description="표시 이름")
    created_at: datetime = Field(..., description="작성 시간")
    like_count: int = Field(0, description="좋아요 수")
    retweet_count: int = Field(0, description="리트윗 수")
    reply_count: int = Field(0, description="댓글 수")
    tweet_url: Optional[str] = Field(None, description="원본 트윗 URL")
    
    class Config:
        from_attributes = True


class XPostDetailed(BaseModel):
    """X Posts 상세 응답 (상세용)"""
    tweet_id: str = Field(..., description="트윗 ID")
    author_id: str = Field(..., description="작성자 ID")
    text: str = Field(..., description="트윗 내용")
    created_at: datetime = Field(..., description="작성 시간")
    lang: str = Field("en", description="언어 코드")
    author: AuthorInfo = Field(..., description="작성자 정보")
    engagement: EngagementInfo = Field(..., description="인게이지먼트 정보")
    metadata: MetadataInfo = Field(..., description="메타데이터")
    classification: ClassificationInfo = Field(..., description="분류 정보")
    tweet_url: Optional[str] = Field(None, description="원본 트윗 URL")
    
    class Config:
        from_attributes = True


class XPostListResponse(BaseModel):
    """X Posts 목록 응답"""
    items: List[XPostBasic] = Field(..., description="포스트 목록")
    total_count: int = Field(..., description="전체 포스트 수")
    limit: int = Field(..., description="요청한 제한 수")
    offset: int = Field(..., description="건너뛴 수")
    has_more: bool = Field(..., description="더 많은 데이터 존재 여부")


class XPostRankingResponse(BaseModel):
    """X Posts 랭킹 응답"""
    items: List[XPostDetailed] = Field(..., description="랭킹 포스트 목록")
    ranking_type: str = Field(..., description="랭킹 유형")
    period: str = Field(..., description="기간")
    total_count: int = Field(..., description="전체 대상 포스트 수")


class UserInfo(BaseModel):
    """사용자 정보"""
    username: str = Field(..., description="사용자명")
    display_name: Optional[str] = Field(None, description="표시 이름")
    account_category: AccountCategory = Field(..., description="계정 카테고리")
    verified: bool = Field(False, description="인증 여부")
    followers_count: int = Field(0, description="팔로워 수")
    total_posts: int = Field(0, description="총 포스트 수")
    avg_engagement: float = Field(0.0, description="평균 인게이지먼트")


class CategoryUsersResponse(BaseModel):
    """카테고리별 사용자 목록 응답"""
    category: AccountCategory = Field(..., description="카테고리")
    users: List[UserInfo] = Field(..., description="사용자 목록")
    total_count: int = Field(..., description="총 사용자 수")


class UserStatsResponse(BaseModel):
    """사용자 통계 응답"""
    username: str = Field(..., description="사용자명")
    display_name: Optional[str] = Field(None, description="표시 이름") 
    account_category: AccountCategory = Field(..., description="계정 카테고리")
    total_posts: int = Field(0, description="총 포스트 수")
    total_likes: int = Field(0, description="총 좋아요 수")
    total_retweets: int = Field(0, description="총 리트윗 수")
    total_replies: int = Field(0, description="총 댓글 수")
    avg_engagement_per_post: float = Field(0.0, description="포스트당 평균 인게이지먼트")
    most_liked_post: Optional[XPostBasic] = Field(None, description="가장 좋아요가 많은 포스트")
    followers_count: int = Field(0, description="팔로워 수")


class CategoryStatsResponse(BaseModel):
    """카테고리 통계 응답"""
    category: AccountCategory = Field(..., description="카테고리")
    total_users: int = Field(0, description="총 사용자 수")
    total_posts: int = Field(0, description="총 포스트 수")
    avg_engagement_per_post: float = Field(0.0, description="포스트당 평균 인게이지먼트")
    top_users: List[UserInfo] = Field(..., description="상위 사용자들")


class OverviewStatsResponse(BaseModel):
    """전체 통계 개요 응답"""
    total_posts: int = Field(0, description="총 포스트 수")
    total_users: int = Field(0, description="총 사용자 수")
    categories: Dict[str, int] = Field(..., description="카테고리별 포스트 수")
    recent_24h_posts: int = Field(0, description="최근 24시간 포스트 수")
    avg_engagement: float = Field(0.0, description="전체 평균 인게이지먼트")
    top_posts_today: List[XPostBasic] = Field(..., description="오늘의 인기 포스트")