# app/models/coingecko_tickers_model.py

from sqlalchemy import Column, Integer, String, DECIMAL, Boolean, TIMESTAMP
from .base import Base


class CoingeckoTickers(Base):
    """CoinGecko 거래소별 Tickers 데이터 (김치프리미엄 분석용)"""
    __tablename__ = "coingecko_tickers_bithumb"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # 코인 기본 정보 (실제 DB 구조에 맞춤)
    market_code = Column(String, nullable=False)  # text 타입
    coingecko_id = Column(String, nullable=False)  # text 타입
    symbol = Column(String, nullable=False)  # text 타입
    coin_name = Column(String)  # text 타입, nullable
    
    # 거래 쌍 정보 (실제 DB 구조에 맞춤)
    base = Column(String, nullable=False)  # text 타입
    target = Column(String, nullable=False)  # text 타입
    
    # 거래소 정보 (실제 DB 구조에 맞춤)
    exchange_name = Column(String, nullable=False)  # text 타입
    exchange_id = Column(String, nullable=False)  # text 타입
    
    # 가격 및 거래량 (실제 DB 구조에 맞춤)
    last_price = Column(DECIMAL(32, 8))
    volume_24h = Column(DECIMAL(32, 8))
    
    # USD 변환 가격 (김치프리미엄 계산 핵심)
    converted_last_usd = Column(DECIMAL(32, 8))
    converted_volume_usd = Column(Integer)  # bigint 타입
    
    # 거래소 품질 지표 (실제 DB 구조에 맞춤)
    trust_score = Column(String)  # text 타입
    bid_ask_spread_percentage = Column(DECIMAL(24, 4))  # 실제 DB와 일치
    
    # 데이터 품질 지표
    is_anomaly = Column(Boolean, default=False)
    is_stale = Column(Boolean, default=False)
    
    # URL 정보
    trade_url = Column(String)  # text 타입
    
    # 코인 시장 정보
    coin_mcap_usd = Column(Integer)  # bigint 타입
    match_method = Column(String)  # text 타입
    market_cap_rank = Column(Integer)
    
    # 시간 정보
    timestamp = Column(TIMESTAMP)
    last_traded_at = Column(TIMESTAMP)
    last_fetch_at = Column(TIMESTAMP)
    created_at = Column(TIMESTAMP)
    updated_at = Column(TIMESTAMP)