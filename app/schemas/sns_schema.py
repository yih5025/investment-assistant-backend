# app/schemas/sns_schema.py

from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime

from app.schemas.common import PaginatedResponse


# Response Schemas (간소화)
class UnifiedSNSPostResponse(BaseModel):
    """통합 SNS 게시글 응답 스키마 - 단순화 버전"""
    id: str = Field(..., description="게시글 ID")
    platform: str = Field(..., description="플랫폼 (x, truth_social_posts, truth_social_trends)")
    content: str = Field(..., description="게시글 내용")
    clean_content: Optional[str] = Field(None, description="정제된 내용")
    author: str = Field(..., description="작성자 (username)")
    display_name: Optional[str] = Field(None, description="표시명")
    created_at: datetime = Field(..., description="작성 시간")
    
    # 참여 지표 (단순화 - 정렬 기능 제거했으므로 단순 표시용)
    likes: Optional[int] = Field(None, description="좋아요 수")
    retweets: Optional[int] = Field(None, description="리트윗/리블로그 수")
    replies: Optional[int] = Field(None, description="댓글 수")
    engagement_score: int = Field(0, description="총 참여도 (표시용)")
    
    # 기본 메타데이터
    verified: bool = Field(False, description="인증 여부")
    has_media: bool = Field(False, description="미디어 포함")
    
    # 시장 분석 관련 (향후 구현)
    has_market_impact: bool = Field(False, description="시장 영향 분석 가능")
    
    class Config:
        from_attributes = True


class SNSPostsResponse(PaginatedResponse):
    """SNS 게시글 목록 응답 스키마"""
    items: List[UnifiedSNSPostResponse] = Field(..., description="게시글 목록")
    platform_counts: Dict[str, int] = Field(default_factory=dict, description="플랫폼별 개수")


class AuthorInfo(BaseModel):
    """작성자 정보"""
    username: str = Field(..., description="사용자명")
    display_name: str = Field(..., description="표시명")
    post_count: int = Field(..., description="게시글 수 (최근 30일)")
    last_post_date: str = Field(..., description="마지막 게시 시간")
    verified: bool = Field(False, description="인증 여부")


class AvailableAuthorsResponse(BaseModel):
    """사용 가능한 작성자 목록 응답"""
    x: List[AuthorInfo] = Field(default_factory=list, description="X 작성자들")
    truth_social_posts: List[AuthorInfo] = Field(default_factory=list, description="Truth Social Posts 작성자들")
    truth_social_trends: List[AuthorInfo] = Field(default_factory=list, description="Truth Social Trends 작성자들")