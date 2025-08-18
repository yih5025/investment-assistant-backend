from datetime import datetime, date
from typing import List, Optional, Any, Dict, Union
from pydantic import BaseModel, Field
from .common import PaginatedResponse


# ==================== Truth Social Posts Schemas ====================
# 주석 추가
class TruthSocialPostBase(BaseModel):
    """Truth Social 포스트 기본 스키마"""
    username: str = Field(..., description="사용자명")
    display_name: Optional[str] = Field(None, description="표시 이름")
    content: str = Field(..., description="포스트 내용")
    clean_content: Optional[str] = Field(None, description="정제된 내용")
    language: Optional[str] = Field(None, description="언어")
    verified: bool = Field(False, description="인증 여부")


class TruthSocialPostResponse(TruthSocialPostBase):
    """Truth Social 포스트 응답 스키마"""
    id: str = Field(..., description="포스트 ID")
    account_id: str = Field(..., description="계정 ID")
    created_at: datetime = Field(..., description="작성 시간")
    
    # 상호작용 지표
    replies_count: int = Field(0, description="댓글 수")
    reblogs_count: int = Field(0, description="리블로그 수") 
    favourites_count: int = Field(0, description="좋아요 수")
    upvotes_count: int = Field(0, description="추천 수")
    downvotes_count: int = Field(0, description="비추천 수")
    
    # 미디어 정보
    has_media: bool = Field(False, description="미디어 포함 여부")
    media_count: int = Field(0, description="미디어 개수")
    media_attachments: Optional[List[Dict[str, Any]]] = Field(None, description="첨부 미디어")
    
    # 태그 및 멘션 (DB에는 문자열 또는 객체(dict) 형태로 저장될 수 있음)
    tags: List[Union[str, Dict[str, Any]]] = Field(default_factory=list, description="해시태그 목록")
    mentions: List[Union[str, Dict[str, Any]]] = Field(default_factory=list, description="멘션 목록")
    has_tags: bool = Field(False, description="태그 포함 여부")
    has_mentions: bool = Field(False, description="멘션 포함 여부")
    
    # 카드 정보
    card_url: Optional[str] = Field(None, description="카드 URL")
    card_title: Optional[str] = Field(None, description="카드 제목")
    card_description: Optional[str] = Field(None, description="카드 설명")
    
    # 메타데이터
    account_type: str = Field("individual", description="계정 유형")
    market_influence: int = Field(0, description="시장 영향도")
    visibility: str = Field("public", description="공개 범위")
    sensitive: bool = Field(False, description="민감 내용 여부")
    
    # 타임스탬프
    collected_at: datetime = Field(..., description="수집 시간")
    updated_at: datetime = Field(..., description="업데이트 시간")

    class Config:
        from_attributes = True


class TruthSocialPostsResponse(PaginatedResponse):
    """Truth Social 포스트 목록 응답"""
    items: List[TruthSocialPostResponse]


# ==================== Truth Social Tags Schemas ====================

class TruthSocialTagResponse(BaseModel):
    """Truth Social 태그 응답 스키마"""
    name: str = Field(..., description="태그 이름")
    collected_date: date = Field(..., description="수집 날짜")
    url: Optional[str] = Field(None, description="태그 URL")
    
    # 사용량 통계
    total_uses: int = Field(0, description="총 사용 횟수")
    total_accounts: int = Field(0, description="총 사용 계정 수")
    recent_statuses_count: int = Field(0, description="최근 포스트 수")
    
    # 일별 사용량 추이 (7일간)
    day_0_uses: int = Field(0, description="오늘 사용량")
    day_1_uses: int = Field(0, description="1일 전 사용량")
    day_2_uses: int = Field(0, description="2일 전 사용량")
    day_3_uses: int = Field(0, description="3일 전 사용량")
    day_4_uses: int = Field(0, description="4일 전 사용량")
    day_5_uses: int = Field(0, description="5일 전 사용량")
    day_6_uses: int = Field(0, description="6일 전 사용량")
    
    # 분석 지표
    trend_score: Optional[float] = Field(None, description="트렌드 점수")
    growth_rate: Optional[float] = Field(None, description="증가율 (%)")
    weekly_average: Optional[float] = Field(None, description="주간 평균")
    tag_category: Optional[str] = Field(None, description="태그 카테고리")
    market_relevance: int = Field(0, description="시장 관련성")
    
    # 타임스탬프
    collected_at: datetime = Field(..., description="수집 시간")
    updated_at: datetime = Field(..., description="업데이트 시간")

    class Config:
        from_attributes = True


class TruthSocialTagsResponse(PaginatedResponse):
    """Truth Social 태그 목록 응답"""
    items: List[TruthSocialTagResponse]


# ==================== Truth Social Trends Schemas ====================

class TruthSocialTrendResponse(BaseModel):
    """Truth Social 트렌드 응답 스키마"""
    id: str = Field(..., description="포스트 ID")
    created_at: datetime = Field(..., description="작성 시간")
    username: str = Field(..., description="사용자명")
    account_id: str = Field(..., description="계정 ID")
    display_name: Optional[str] = Field(None, description="표시 이름")
    content: str = Field(..., description="포스트 내용")
    clean_content: Optional[str] = Field(None, description="정제된 내용")
    language: Optional[str] = Field(None, description="언어")
    
    # 상호작용 지표
    replies_count: int = Field(0, description="댓글 수")
    reblogs_count: int = Field(0, description="리블로그 수")
    favourites_count: int = Field(0, description="좋아요 수")
    upvotes_count: int = Field(0, description="추천 수")
    downvotes_count: int = Field(0, description="비추천 수")
    
    # URL 정보
    url: Optional[str] = Field(None, description="포스트 URL")
    uri: Optional[str] = Field(None, description="포스트 URI")
    
    # 태그 및 멘션 (DB에는 문자열 또는 객체(dict) 형태로 저장될 수 있음)
    tags: List[Union[str, Dict[str, Any]]] = Field(default_factory=list, description="해시태그 목록")
    mentions: List[Union[str, Dict[str, Any]]] = Field(default_factory=list, description="멘션 목록")
    
    # 트렌드 관련
    trend_rank: Optional[int] = Field(None, description="트렌드 순위")
    trend_score: Optional[float] = Field(None, description="트렌드 점수")
    
    # 메타데이터
    visibility: str = Field("public", description="공개 범위")
    sensitive: bool = Field(False, description="민감 내용 여부")
    in_reply_to_id: Optional[str] = Field(None, description="답글 대상 ID")
    
    # 타임스탬프
    collected_at: datetime = Field(..., description="수집 시간")
    updated_at: datetime = Field(..., description="업데이트 시간")

    class Config:
        from_attributes = True


class TruthSocialTrendsResponse(PaginatedResponse):
    """Truth Social 트렌드 목록 응답"""
    items: List[TruthSocialTrendResponse]


# ==================== 공통 필터 스키마 ====================

class TruthSocialPostFilter(BaseModel):
    """Truth Social 포스트 필터"""
    username: Optional[str] = Field(None, description="사용자명 필터")
    account_type: Optional[str] = Field(None, description="계정 유형 필터")
    has_media: Optional[bool] = Field(None, description="미디어 포함 여부")
    min_market_influence: Optional[int] = Field(None, description="최소 시장 영향도")
    language: Optional[str] = Field(None, description="언어 필터")
    start_date: Optional[datetime] = Field(None, description="시작 날짜")
    end_date: Optional[datetime] = Field(None, description="종료 날짜")


class TruthSocialTagFilter(BaseModel):
    """Truth Social 태그 필터"""
    tag_category: Optional[str] = Field(None, description="태그 카테고리")
    min_total_uses: Optional[int] = Field(None, description="최소 사용 횟수")
    min_market_relevance: Optional[int] = Field(None, description="최소 시장 관련성")
    start_date: Optional[date] = Field(None, description="시작 날짜")
    end_date: Optional[date] = Field(None, description="종료 날짜")


class TruthSocialTrendFilter(BaseModel):
    """Truth Social 트렌드 필터"""
    username: Optional[str] = Field(None, description="사용자명 필터")
    min_trend_score: Optional[float] = Field(None, description="최소 트렌드 점수")
    max_rank: Optional[int] = Field(None, description="최대 순위")
    start_date: Optional[datetime] = Field(None, description="시작 날짜")
    end_date: Optional[datetime] = Field(None, description="종료 날짜")