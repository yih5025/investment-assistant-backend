# app/models/sns_model.py

from sqlalchemy import Column, String, Integer, DateTime, Text, Boolean, JSON, Numeric
from sqlalchemy.dialects.postgresql import JSONB
from app.models.base import Base


class XPost(Base):
    """X(트위터) 게시글 모델"""
    __tablename__ = "x_posts"

    tweet_id = Column(String, primary_key=True)
    author_id = Column(String, nullable=False)
    text = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False)
    lang = Column(String, default='en')
    source_account = Column(String, nullable=False)
    account_category = Column(String, default='core_investors')
    collection_source = Column(String, default='primary_token')
    
    # 참여 지표
    retweet_count = Column(Integer, default=0)
    reply_count = Column(Integer, default=0)
    like_count = Column(Integer, default=0)
    quote_count = Column(Integer, default=0)
    bookmark_count = Column(Integer, default=0)
    impression_count = Column(Integer, default=0)
    
    # 메타데이터
    hashtags = Column(JSONB)
    mentions = Column(JSONB)
    urls = Column(JSONB)
    cashtags = Column(JSONB)
    annotations = Column(JSONB)
    context_annotations = Column(JSONB)
    
    # 사용자 정보
    username = Column(String)
    display_name = Column(String)
    user_verified = Column(Boolean, default=False)
    user_followers_count = Column(Integer, default=0)
    user_following_count = Column(Integer, default=0)
    user_tweet_count = Column(Integer, default=0)
    
    # 수집 정보
    edit_history_tweet_ids = Column(JSONB)
    collected_at = Column(DateTime(timezone=True))
    updated_at = Column(DateTime(timezone=True))


class TruthSocialPost(Base):
    """Truth Social 게시글 모델"""
    __tablename__ = "truth_social_posts"

    id = Column(String, primary_key=True)
    created_at = Column(DateTime(timezone=True), nullable=False)
    username = Column(String, nullable=False)
    account_id = Column(String, nullable=False)
    display_name = Column(String)
    verified = Column(Boolean, default=False)
    
    # 내용
    content = Column(Text, nullable=False)
    clean_content = Column(Text)
    language = Column(String)
    
    # 참여 지표
    replies_count = Column(Integer, default=0)
    reblogs_count = Column(Integer, default=0)
    favourites_count = Column(Integer, default=0)
    upvotes_count = Column(Integer, default=0)
    downvotes_count = Column(Integer, default=0)
    
    # URL 정보
    url = Column(String)
    uri = Column(String)
    
    # 미디어 및 메타데이터
    has_media = Column(Boolean, default=False)
    media_count = Column(Integer, default=0)
    media_attachments = Column(JSONB)
    tags = Column(JSONB, default=[])
    mentions = Column(JSONB, default=[])
    has_tags = Column(Boolean, default=False)
    has_mentions = Column(Boolean, default=False)
    
    # 카드 정보
    card_url = Column(String)
    card_title = Column(String)
    card_description = Column(String)
    card_image = Column(String)
    
    # 게시글 속성
    visibility = Column(String, default='public')
    sensitive = Column(Boolean, default=False)
    spoiler_text = Column(String, default='')
    in_reply_to_id = Column(String)
    quote_id = Column(String)
    
    # 계정 분류
    account_type = Column(String, default='individual')
    market_influence = Column(Integer, default=0)
    
    # 수집 정보
    collected_at = Column(DateTime(timezone=True))
    updated_at = Column(DateTime(timezone=True))


class TruthSocialTrend(Base):
    """Truth Social 트렌드 게시글 모델"""
    __tablename__ = "truth_social_trends"

    id = Column(String, primary_key=True)
    created_at = Column(DateTime(timezone=True), nullable=False)
    username = Column(String, nullable=False)
    account_id = Column(String, nullable=False)
    display_name = Column(String)
    
    # 내용
    content = Column(Text, nullable=False)
    clean_content = Column(Text)
    language = Column(String)
    
    # 참여 지표
    replies_count = Column(Integer, default=0)
    reblogs_count = Column(Integer, default=0)
    favourites_count = Column(Integer, default=0)
    upvotes_count = Column(Integer, default=0)
    downvotes_count = Column(Integer, default=0)
    
    # URL 정보
    url = Column(String)
    uri = Column(String)
    
    # 메타데이터
    tags = Column(JSONB, default=[])
    mentions = Column(JSONB, default=[])
    
    # 게시글 속성
    visibility = Column(String, default='public')
    sensitive = Column(Boolean, default=False)
    in_reply_to_id = Column(String)
    
    # 트렌드 관련
    trend_rank = Column(Integer)
    trend_score = Column(Numeric(10, 2))
    
    # 수집 정보
    collected_at = Column(DateTime(timezone=True))
    updated_at = Column(DateTime(timezone=True))