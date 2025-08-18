from sqlalchemy import Column, Integer, String, DateTime, Date, Numeric, Boolean, JSON, Text
from sqlalchemy.dialects.postgresql import JSONB
from app.models.base import BaseModel

class TruthSocialPost(BaseModel):
    """Truth Social Posts 데이터 모델"""
    __tablename__ = "truth_social_posts"

    id = Column(Text, primary_key=True)
    created_at = Column(DateTime(timezone=True), nullable=False)
    username = Column(Text, nullable=False)
    account_id = Column(Text, nullable=False)
    display_name = Column(Text)
    verified = Column(Boolean, default=False)
    content = Column(Text, nullable=False)
    clean_content = Column(Text)
    language = Column(Text)
    
    # 상호작용 지표들
    replies_count = Column(Integer, default=0)
    reblogs_count = Column(Integer, default=0)
    favourites_count = Column(Integer, default=0)
    upvotes_count = Column(Integer, default=0)
    downvotes_count = Column(Integer, default=0)
    
    # URL 및 미디어 정보
    url = Column(Text)
    uri = Column(Text)
    has_media = Column(Boolean, default=False)
    media_count = Column(Integer, default=0)
    media_attachments = Column(JSONB)
    
    # 태그 및 멘션 (JSONB 배열)
    tags = Column(JSONB, default=lambda: [])
    mentions = Column(JSONB, default=lambda: [])
    has_tags = Column(Boolean, default=False)
    has_mentions = Column(Boolean, default=False)
    
    # 카드 정보 (링크 미리보기)
    card_url = Column(Text)
    card_title = Column(Text)
    card_description = Column(Text)
    card_image = Column(Text)
    
    # 메타데이터
    visibility = Column(Text, default='public')
    sensitive = Column(Boolean, default=False)
    spoiler_text = Column(Text)
    in_reply_to_id = Column(Text)
    quote_id = Column(Text)
    account_type = Column(Text, default='individual')
    market_influence = Column(Integer, default=0)
    
    # 타임스탬프
    collected_at = Column(DateTime, server_default='now()')
    updated_at = Column(DateTime, server_default='now()')


class TruthSocialTag(BaseModel):
    """Truth Social 해시태그 트렌드 모델"""
    __tablename__ = "truth_social_tags"
    
    # 복합 기본키
    name = Column(Text, primary_key=True)
    collected_date = Column(Date, primary_key=True)
    
    url = Column(Text)
    total_uses = Column(Integer, default=0)
    total_accounts = Column(Integer, default=0)
    recent_statuses_count = Column(Integer, default=0)
    history_data = Column(JSONB)
    
    # 일별 사용량 추이 (7일간)
    day_0_uses = Column(Integer, default=0)
    day_1_uses = Column(Integer, default=0)
    day_2_uses = Column(Integer, default=0)
    day_3_uses = Column(Integer, default=0)
    day_4_uses = Column(Integer, default=0)
    day_5_uses = Column(Integer, default=0)
    day_6_uses = Column(Integer, default=0)
    
    # 분석 지표들
    trend_score = Column(Numeric(10, 2))
    growth_rate = Column(Numeric(5, 2))
    weekly_average = Column(Numeric(10, 2))
    tag_category = Column(Text)
    market_relevance = Column(Integer, default=0)
    
    # 타임스탬프
    collected_at = Column(DateTime, server_default='now()')
    updated_at = Column(DateTime, server_default='now()')


class TruthSocialTrend(BaseModel):
    """Truth Social 트렌딩 포스트 모델"""
    __tablename__ = "truth_social_trends"
    
    id = Column(Text, primary_key=True)
    created_at = Column(DateTime(timezone=True), nullable=False)
    username = Column(Text, nullable=False)
    account_id = Column(Text, nullable=False)
    display_name = Column(Text)
    content = Column(Text, nullable=False)
    clean_content = Column(Text)
    language = Column(Text)
    
    # 상호작용 지표들
    replies_count = Column(Integer, default=0)
    reblogs_count = Column(Integer, default=0)
    favourites_count = Column(Integer, default=0)
    upvotes_count = Column(Integer, default=0)
    downvotes_count = Column(Integer, default=0)
    
    # URL 정보
    url = Column(Text)
    uri = Column(Text)
    
    # 태그 및 멘션 (JSONB 배열)
    tags = Column(JSONB, default=lambda: [])
    mentions = Column(JSONB, default=lambda: [])
    
    # 메타데이터
    visibility = Column(Text, default='public')
    sensitive = Column(Boolean, default=False)
    in_reply_to_id = Column(Text)
    
    # 트렌드 관련 필드
    trend_rank = Column(Integer)
    trend_score = Column(Numeric(10, 2))
    
    # 타임스탬프
    collected_at = Column(DateTime, server_default='now()')
    updated_at = Column(DateTime, server_default='now()')