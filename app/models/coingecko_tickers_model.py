# app/models/coingecko_tickers_model.py

from sqlalchemy import Column, Integer, String, DECIMAL, Boolean, TIMESTAMP
from .base import Base


class CoingeckoTickers(Base):
    """CoinGecko 거래소별 Tickers 데이터 (김치프리미엄 분석용)"""
    __tablename__ = "coingecko_tickers"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    batch_id = Column(String(50), nullable=False)
    
    # 코인 기본 정보
    coingecko_id = Column(String(100), nullable=False)
    coin_symbol = Column(String(20), nullable=False)
    coin_name = Column(String(200), nullable=False)
    market_cap_rank = Column(Integer)
    
    # 거래 쌍 정보
    base_symbol = Column(String(20), nullable=False)
    target_symbol = Column(String(20), nullable=False)
    
    # 거래소 정보
    market_name = Column(String(100), nullable=False)
    market_identifier = Column(String(50), nullable=False)
    is_korean_exchange = Column(Boolean, default=False)
    has_trading_incentive = Column(Boolean, default=False)
    
    # 가격 및 거래량
    last_price = Column(DECIMAL(25, 10))
    volume = Column(DECIMAL(25, 6))
    
    # USD 변환 가격 (김치프리미엄 계산 핵심)
    converted_last_usd = Column(DECIMAL(20, 8))
    converted_last_btc = Column(DECIMAL(15, 8))
    converted_volume_usd = Column(DECIMAL(20, 2))
    
    # 거래소 품질 지표
    trust_score = Column(String(20))
    bid_ask_spread_percentage = Column(DECIMAL(10, 6))
    
    # 유동성 지표 (Market Depth)
    cost_to_move_up_usd = Column(DECIMAL(15, 2))
    cost_to_move_down_usd = Column(DECIMAL(15, 2))
    
    # 데이터 품질 지표
    is_anomaly = Column(Boolean, default=False)
    is_stale = Column(Boolean, default=False)
    
    # URL 정보
    trade_url = Column(String(500))
    
    # 시간 정보
    timestamp = Column(TIMESTAMP)
    last_traded_at = Column(TIMESTAMP)
    last_fetch_at = Column(TIMESTAMP)
    collected_at = Column(TIMESTAMP, nullable=False)
    created_at = Column(TIMESTAMP)
    updated_at = Column(TIMESTAMP)