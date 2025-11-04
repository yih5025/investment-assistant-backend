# app/models/coingecko_coin_details_model.py

from sqlalchemy import Column, Integer, String, DECIMAL, TIMESTAMP, BIGINT, Text, Date
from sqlalchemy.dialects.postgresql import JSONB
from .base import Base


class CoingeckoCoinDetails(Base):
    """CoinGecko 코인 상세 정보 테이블"""
    __tablename__ = "coingecko_coin_details"
    
    coingecko_id = Column(String(100), primary_key=True)
    symbol = Column(String(20), nullable=False)
    name = Column(String(200), nullable=False)
    web_slug = Column(String(200))
    
    # Tab 1: 개념 설명용 데이터
    description_en = Column(Text)
    genesis_date = Column(Date)
    country_origin = Column(String(100))
    
    # Links (Tab 1, Tab 2)
    homepage_url = Column(Text)
    blockchain_site = Column(Text)
    twitter_screen_name = Column(String(100))
    facebook_username = Column(String(100))
    telegram_channel_identifier = Column(String(100))
    subreddit_url = Column(Text)
    github_repos = Column(JSONB)
    
    # 이미지
    image_thumb = Column(String(500))
    image_small = Column(String(500))
    image_large = Column(String(500))
    
    # Categories (Tab 1, Tab 2)
    categories = Column(JSONB)
    
    # Market Data (Tab 2: 투자 분석)
    current_price_usd = Column(DECIMAL(20, 8))
    current_price_krw = Column(DECIMAL(20, 2))
    market_cap_usd = Column(BIGINT)
    market_cap_rank = Column(Integer)
    total_volume_usd = Column(BIGINT)
    
    # ATH/ATL (Tab 2: 투자 분석)
    ath_usd = Column(DECIMAL(20, 8))
    ath_change_percentage = Column(DECIMAL(20, 4))
    ath_date = Column(TIMESTAMP)
    atl_usd = Column(DECIMAL(20, 8))
    atl_change_percentage = Column(DECIMAL(20, 4))
    atl_date = Column(TIMESTAMP)
    
    # Supply Data (Tab 2: 투자 분석)
    total_supply = Column(DECIMAL(30, 2))
    circulating_supply = Column(DECIMAL(30, 2))
    max_supply = Column(DECIMAL(30, 2))
    
    # Price Changes (Tab 2: 투자 분석)
    price_change_24h_usd = Column(DECIMAL(20, 8))
    price_change_percentage_24h = Column(DECIMAL(10, 4))
    price_change_percentage_7d = Column(DECIMAL(10, 4))
    price_change_percentage_30d = Column(DECIMAL(10, 4))
    
    # Community Data (Tab 3: 프로젝트 분석)
    community_score = Column(DECIMAL(5, 2))
    twitter_followers = Column(Integer)
    reddit_subscribers = Column(Integer)
    telegram_channel_user_count = Column(Integer)
    
    # Developer Data (Tab 3: 프로젝트 분석)
    developer_score = Column(DECIMAL(5, 2))
    forks = Column(Integer)
    stars = Column(Integer)
    total_issues = Column(Integer)
    closed_issues = Column(Integer)
    commit_count_4_weeks = Column(Integer)
    
    # Public Interest & Liquidity (Tab 2, Tab 3)
    public_interest_score = Column(DECIMAL(5, 2))
    liquidity_score = Column(DECIMAL(5, 2))
    
    # Timestamps
    coingecko_last_updated = Column(TIMESTAMP)
    created_at = Column(TIMESTAMP)
    updated_at = Column(TIMESTAMP)