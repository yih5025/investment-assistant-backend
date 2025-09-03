# app/models/coingecko_global_model.py

from sqlalchemy import Column, Integer, String, DECIMAL, TIMESTAMP
from sqlalchemy.dialects.postgresql import JSONB
from .base import Base


class CoingeckoGlobal(Base):
    """CoinGecko 전체 암호화폐 시장 데이터 테이블"""
    __tablename__ = "coingecko_global"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # 기본 시장 통계
    active_cryptocurrencies = Column(Integer)
    upcoming_icos = Column(Integer, default=0)
    ongoing_icos = Column(Integer, default=0)
    ended_icos = Column(Integer, default=0)
    markets = Column(Integer)  # 거래소 개수
    
    # 총 시가총액 (주요 통화별)
    total_market_cap_usd = Column(DECIMAL(25, 2))
    total_market_cap_krw = Column(DECIMAL(25, 2))
    total_market_cap_btc = Column(DECIMAL(20, 8))
    total_market_cap_eth = Column(DECIMAL(20, 8))
    
    # 총 거래량 (주요 통화별)
    total_volume_usd = Column(DECIMAL(25, 2))
    total_volume_krw = Column(DECIMAL(25, 2))
    total_volume_btc = Column(DECIMAL(20, 8))
    total_volume_eth = Column(DECIMAL(20, 8))
    
    # 시장 점유율 (도미넌스) - 주요 코인들
    btc_dominance = Column(DECIMAL(8, 4), default=0)
    eth_dominance = Column(DECIMAL(8, 4), default=0)
    bnb_dominance = Column(DECIMAL(8, 4), default=0)
    xrp_dominance = Column(DECIMAL(8, 4), default=0)
    ada_dominance = Column(DECIMAL(8, 4), default=0)
    sol_dominance = Column(DECIMAL(8, 4), default=0)
    doge_dominance = Column(DECIMAL(8, 4), default=0)
    
    # 시장 변동률
    market_cap_change_percentage_24h_usd = Column(DECIMAL(10, 6), default=0)
    
    # 원본 JSON 데이터 (전체 보존용)
    market_cap_percentage_json = Column(JSONB)
    total_market_cap_json = Column(JSONB)
    total_volume_json = Column(JSONB)
    
    # 시간 정보
    coingecko_updated_at = Column(TIMESTAMP)
    collected_at = Column(TIMESTAMP, nullable=False)
    created_at = Column(TIMESTAMP)
    updated_at = Column(TIMESTAMP)