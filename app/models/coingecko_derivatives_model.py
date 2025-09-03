# app/models/coingecko_derivatives_model.py

from sqlalchemy import Column, Integer, String, DECIMAL, TIMESTAMP
from .base import Base


class CoingeckoDerivatives(Base):
    """CoinGecko 파생상품 데이터 테이블 (펀딩비, 미체결약정 분석용)"""
    __tablename__ = "coingecko_derivatives"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    batch_id = Column(String(50), nullable=False)
    
    # 기본 정보
    market = Column(String(100), nullable=False)  # 거래소명
    symbol = Column(String(50), nullable=False)   # 거래 심볼
    index_id = Column(String(20), nullable=False)  # 기초자산 (BTC, ETH 등)
    
    # 가격 데이터
    price = Column(DECIMAL(20, 8))
    price_percentage_change_24h = Column(DECIMAL(10, 4))
    index_price = Column(DECIMAL(20, 8))  # 기초자산(현물) 가격
    
    # 파생상품 특성
    contract_type = Column(String(20))  # perpetual, futures
    basis = Column(DECIMAL(15, 10))     # 베이시스 (선물가 - 현물가 차이율)
    spread = Column(DECIMAL(10, 6))     # 스프레드
    funding_rate = Column(DECIMAL(10, 6))  # 펀딩 비율 (핵심 지표)
    
    # 시장 규모
    open_interest_usd = Column(DECIMAL(20, 2))  # 미체결약정 (USD)
    volume_24h_usd = Column(DECIMAL(20, 2))     # 24시간 거래량 (USD)
    
    # 시간 데이터
    last_traded_at = Column(TIMESTAMP)
    expired_at = Column(TIMESTAMP)  # perpetual의 경우 NULL
    collected_at = Column(TIMESTAMP, nullable=False)
    created_at = Column(TIMESTAMP)
    updated_at = Column(TIMESTAMP)