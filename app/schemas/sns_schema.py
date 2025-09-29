# app/schemas/sns_schema.py

from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime

from app.schemas.common import PaginatedResponse

# ==================================================================================
# === 신규 추가/수정된 스키마: OHLCV 데이터 구조를 정의합니다. ===
# ==================================================================================

class PriceTimelineOHLCVSchema(BaseModel):
    """(신규) 1분 단위 OHLCV 시계열 데이터 스키마"""
    timestamp: datetime
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    volume: Optional[float] = None

class MarketAssetDataSchema(BaseModel):
    """(신규) 단일 자산에 대한 시장 데이터 스키마"""
    price_timeline: List[PriceTimelineOHLCVSchema]
    data_source: str
    asset_info: Dict[str, Any]

# ==================================================================================
# === 기존 스키마 ===
# ==================================================================================

class UnifiedSNSPostResponse(BaseModel):
    """(원본 데이터용) 통합 SNS 게시글 응답 스키마"""
    id: str = Field(..., description="게시글 ID")
    platform: str = Field(..., description="플랫폼 (x, truth_social_posts, truth_social_trends)")
    content: str = Field(..., description="게시글 내용")
    clean_content: Optional[str] = Field(None, description="정제된 내용")
    author: str = Field(..., description="작성자 (username)")
    display_name: Optional[str] = Field(None, description="표시명")
    created_at: datetime = Field(..., description="작성 시간")
    likes: Optional[int] = Field(None, description="좋아요 수")
    retweets: Optional[int] = Field(None, description="리트윗/리블로그 수")
    replies: Optional[int] = Field(None, description="댓글 수")
    engagement_score: int = Field(0, description="총 참여도 (표시용)")
    has_media: bool = Field(False, description="미디어 포함")
    media_thumbnail: Optional[str] = Field(None, description="썸네일 URL")
    media_type: Optional[str] = Field(None, description="미디어 타입 (image, video)")
    
    class Config:
        from_attributes = True


class SNSPostsResponse(PaginatedResponse):
    """(원본 데이터용) SNS 게시글 목록 응답 스키마"""
    items: List[UnifiedSNSPostResponse] = Field(..., description="게시글 목록")
    platform_counts: Dict[str, int] = Field(default_factory=dict, description="플랫폼별 개수")


class AuthorInfo(BaseModel):
    """작성자 정보"""
    username: str
    display_name: str
    post_count: int
    last_post_date: datetime
    verified: bool

class AvailableAuthorsResponse(BaseModel):
    """사용 가능한 작성자 목록 응답"""
    x: List[AuthorInfo]
    truth_social_posts: List[AuthorInfo]
    truth_social_trends: List[AuthorInfo]


class PostAnalysisCacheBaseSchema(BaseModel):
    """(분석 데이터용) 목록 페이지에 필요한 post_analysis_cache의 핵심 정보"""
    post_id: str
    post_source: str
    post_timestamp: datetime
    author_username: str
    affected_assets: List[dict]
    analysis_status: str

    class Config:
        from_attributes = True

class PostAnalysisCacheDetailSchema(PostAnalysisCacheBaseSchema):
    """(분석 데이터용) 상세 페이지에 필요한 모든 분석 정보 (차트 데이터 포함)"""
    price_analysis: Optional[dict] = None
    volume_analysis: Optional[dict] = None
    # --- ▼ [수정] market_data의 타입을 OHLCV 구조를 포함하는 스키마로 변경 ---
    market_data: Optional[Dict[str, MarketAssetDataSchema]] = None

class OriginalPostForAnalysisSchema(BaseModel):
    """(분석 데이터용) 분석된 게시물의 원본 내용"""
    content: Optional[str] = None

class XPostEngagementSchema(BaseModel):
    """(분석 데이터용) X(트위터) 게시물의 참여도 정보"""
    retweet_count: int = 0
    reply_count: int = 0
    like_count: int = 0
    quote_count: int = 0
    impression_count: int = 0
    account_category: Optional[str] = None

class TruthSocialMediaSchema(BaseModel):
    """Truth Social 게시물의 미디어 정보"""
    has_media: bool = False
    media_thumbnail: Optional[str] = None
    media_type: Optional[str] = None

class SNSPostAnalysisListResponse(BaseModel):
    """[분석 목록 페이지용] API 응답 스키마"""
    analysis: PostAnalysisCacheBaseSchema
    original_post: OriginalPostForAnalysisSchema
    engagement: Optional[XPostEngagementSchema] = None
    media: Optional[TruthSocialMediaSchema] = None

class SNSPostAnalysisDetailResponse(BaseModel):
    """[분석 상세 페이지용] API 응답 스키마"""
    analysis: PostAnalysisCacheDetailSchema
    original_post: OriginalPostForAnalysisSchema
    engagement: Optional[XPostEngagementSchema] = None
    media: Optional[TruthSocialMediaSchema] = None