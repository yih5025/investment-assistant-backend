# app/models/coingecko_tickers_model.py

from sqlalchemy import Column, Integer, String, DECIMAL, Boolean, TIMESTAMP
from .base import Base


class CoingeckoTickers(Base):
    """CoinGecko 거래소별 Tickers 데이터 (김치프리미엄 분석용)"""
    __tablename__ = "coingecko_tickers_bithumb"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # 코인 기본 정보 (DAG와 일치)
    market_code = Column(String(50))
    coingecko_id = Column(String(100), nullable=False)
    symbol = Column(String(20), nullable=False)  # coin_symbol -> symbol
    coin_name = Column(String(200), nullable=False)
    
    # 거래 쌍 정보 (DAG와 일치)
    base = Column(String(20), nullable=False)  # base_symbol -> base
    target = Column(String(20), nullable=False)  # target_symbol -> target
    
    # 거래소 정보 (DAG와 일치)
    exchange_name = Column(String(100), nullable=False)  # market_name -> exchange_name
    exchange_id = Column(String(50), nullable=False)  # market_identifier -> exchange_id
    
    # 가격 및 거래량 (DAG와 일치)
    last_price = Column(DECIMAL(32, 8))  # DAG와 정밀도 일치
    volume_24h = Column(DECIMAL(32, 8))  # volume -> volume_24h (DAG와 일치)
    
    # USD 변환 가격 (김치프리미엄 계산 핵심, DAG와 일치)
    converted_last_usd = Column(DECIMAL(32, 8))  # 정밀도 증가
    converted_volume_usd = Column(Integer)  # DAG와 일치: BIGINT
    
    # 거래소 품질 지표 (DAG와 일치)
    trust_score = Column(String(20))
    bid_ask_spread_percentage = Column(DECIMAL(32, 4))  # 정밀도 증가
    
    # 데이터 품질 지표 (DAG와 일치)
    is_anomaly = Column(Boolean, default=False)
    is_stale = Column(Boolean, default=False)
    
    # URL 정보 (DAG와 일치)
    trade_url = Column(String(500))
    
    # 코인 시장 정보 (DAG와 일치)
    coin_mcap_usd = Column(Integer)  # DAG와 일치: BIGINT
    match_method = Column(String(100))
    market_cap_rank = Column(Integer)
    
    # 시간 정보 (DAG와 일치)
    timestamp = Column(TIMESTAMP)
    last_traded_at = Column(TIMESTAMP)
    last_fetch_at = Column(TIMESTAMP)
    created_at = Column(TIMESTAMP)
    updated_at = Column(TIMESTAMP)