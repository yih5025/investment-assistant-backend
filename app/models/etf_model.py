# app/models/etf_model.py
import logging
import json
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from sqlalchemy import Column, Integer, String, Float, Text, DateTime, Boolean, BigInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session
from app.database import Base

logger = logging.getLogger(__name__)

class ETFBasicInfo(Base):
    """ETF 기본 정보 테이블"""
    __tablename__ = "etf_basic_info"

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String(10), unique=True, index=True, nullable=False)
    name = Column(String(255), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class ETFProfileHoldings(Base):
    """ETF 프로필 및 보유종목 정보 테이블"""
    __tablename__ = "etf_profile_holdings"

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String(10), unique=True, index=True, nullable=False)
    net_assets = Column(BigInteger)  # 순자산
    net_expense_ratio = Column(Float)  # 보수율
    portfolio_turnover = Column(Float)  # 포트폴리오 회전율
    dividend_yield = Column(Float)  # 배당수익률
    inception_date = Column(String(20))  # 설정일
    leveraged = Column(String(10))  # 레버리지 여부
    sectors = Column(Text)  # JSON 형태의 섹터 구성
    holdings = Column(Text)  # JSON 형태의 보유종목
    collected_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class ETFRealtimePrices(Base):
    """ETF 실시간 가격 데이터 테이블"""
    __tablename__ = "etf_realtime_prices"

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String(10), index=True, nullable=False)
    price = Column(Float, nullable=False)
    volume = Column(Integer)
    timestamp_ms = Column(BigInteger)  # 밀리초 타임스탬프
    trade_conditions = Column(String(50))  # 거래 조건
    created_at = Column(DateTime, default=datetime.utcnow, index=True)

    @classmethod
    def get_all_current_prices(cls, db: Session, limit: int = 100) -> List['ETFRealtimePrices']:
        """모든 ETF의 최신 가격 조회"""
        try:
            # 각 심볼별로 최신 가격 조회
            subquery = db.query(
                cls.symbol,
                db.query(cls.created_at).filter(cls.symbol == cls.symbol).order_by(cls.created_at.desc()).limit(1).as_scalar().label('max_created_at')
            ).distinct().subquery()
            
            latest_prices = db.query(cls).join(
                subquery, 
                (cls.symbol == subquery.c.symbol) & (cls.created_at == subquery.c.max_created_at)
            ).limit(limit).all()
            
            return latest_prices
            
        except Exception as e:
            logger.error(f"ETF 현재가 조회 실패: {e}")
            return []

    @classmethod
    def get_current_price(cls, db: Session, symbol: str) -> Optional['ETFRealtimePrices']:
        """특정 ETF의 최신 가격 조회"""
        try:
            return db.query(cls).filter(
                cls.symbol == symbol
            ).order_by(cls.created_at.desc()).first()
            
        except Exception as e:
            logger.error(f"{symbol} ETF 현재가 조회 실패: {e}")
            return None

    @classmethod
    def get_previous_close_price(cls, db: Session, symbol: str) -> Optional[float]:
        """특정 ETF의 전날 종가 조회"""
        try:
            # 어제 날짜 계산
            yesterday = datetime.now() - timedelta(days=1)
            yesterday_start = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
            yesterday_end = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)
            
            # 어제 마지막 거래 가격 조회
            previous_trade = db.query(cls).filter(
                cls.symbol == symbol,
                cls.created_at >= yesterday_start,
                cls.created_at <= yesterday_end
            ).order_by(cls.created_at.desc()).first()
            
            return float(previous_trade.price) if previous_trade else None
            
        except Exception as e:
            logger.error(f"{symbol} ETF 전날 종가 조회 실패: {e}")
            return None

    @classmethod
    def get_price_change_info(cls, db: Session, symbol: str) -> Dict[str, Any]:
        """ETF 가격 변동 정보 조회"""
        try:
            # 현재가
            current_trade = cls.get_current_price(db, symbol)
            if not current_trade:
                return {
                    'current_price': None,
                    'previous_close': None,
                    'change_amount': None,
                    'change_percentage': None,
                    'volume': None,
                    'last_updated': None
                }
            
            current_price = float(current_trade.price)
            
            # 전날 종가
            previous_close = cls.get_previous_close_price(db, symbol)
            
            # 변동액과 변동률 계산
            change_amount = None
            change_percentage = None
            
            if previous_close and previous_close > 0:
                change_amount = current_price - previous_close
                change_percentage = (change_amount / previous_close) * 100
            
            return {
                'current_price': current_price,
                'previous_close': previous_close,
                'change_amount': round(change_amount, 2) if change_amount else None,
                'change_percentage': round(change_percentage, 2) if change_percentage else None,
                'volume': current_trade.volume,
                'last_updated': current_trade.created_at.isoformat()
            }
            
        except Exception as e:
            logger.error(f"{symbol} ETF 가격 변동 정보 조회 실패: {e}")
            return {
                'current_price': None,
                'previous_close': None,
                'change_amount': None,
                'change_percentage': None,
                'volume': None,
                'last_updated': None
            }

    @classmethod
    def get_batch_price_changes(cls, db: Session, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
        """여러 ETF의 가격 변동 정보 일괄 조회"""
        try:
            batch_results = {}
            
            for symbol in symbols:
                change_info = cls.get_price_change_info(db, symbol)
                batch_results[symbol] = change_info
                
            return batch_results
            
        except Exception as e:
            logger.error(f"ETF 배치 가격 변동 조회 실패: {e}")
            return {}

    @classmethod
    def get_chart_data_by_timeframe(cls, db: Session, symbol: str, timeframe: str = '1D', limit: int = 200) -> List['ETFRealtimePrices']:
        """시간대별 차트 데이터 조회"""
        try:
            # 시간대별 필터링 로직
            if timeframe == '1D':
                # 1일: 최근 24시간
                start_time = datetime.now() - timedelta(hours=24)
            elif timeframe == '1W':
                # 1주일: 최근 7일
                start_time = datetime.now() - timedelta(days=7)
            elif timeframe == '1M':
                # 1개월: 최근 30일
                start_time = datetime.now() - timedelta(days=30)
            else:
                # 기본값: 1일
                start_time = datetime.now() - timedelta(hours=24)
            
            chart_data = db.query(cls).filter(
                cls.symbol == symbol,
                cls.created_at >= start_time
            ).order_by(cls.created_at.asc()).limit(limit).all()
            
            return chart_data
            
        except Exception as e:
            logger.error(f"{symbol} ETF 차트 데이터 조회 실패: {e}")
            return []

    @classmethod
    def get_market_summary(cls, db: Session) -> Dict[str, Any]:
        """ETF 시장 요약 정보"""
        try:
            # 전체 ETF 개수
            total_etfs = db.query(cls.symbol).distinct().count()
            
            # 최신 가격들 조회
            latest_prices = cls.get_all_current_prices(db, 1000)
            
            if not latest_prices:
                return {
                    'total_etfs': 0,
                    'total_trades': 0,
                    'average_price': 0,
                    'highest_price': 0,
                    'lowest_price': 0,
                    'total_volume': 0
                }
            
            prices = [float(trade.price) for trade in latest_prices if trade.price]
            volumes = [trade.volume for trade in latest_prices if trade.volume]
            
            return {
                'total_etfs': total_etfs,
                'total_trades': len(latest_prices),
                'average_price': round(sum(prices) / len(prices), 2) if prices else 0,
                'highest_price': round(max(prices), 2) if prices else 0,
                'lowest_price': round(min(prices), 2) if prices else 0,
                'total_volume': sum(volumes) if volumes else 0,
                'last_updated': datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"ETF 시장 요약 조회 실패: {e}")
            return {
                'total_etfs': 0,
                'total_trades': 0,
                'average_price': 0,
                'highest_price': 0,
                'lowest_price': 0,
                'total_volume': 0,
                'error': str(e)
            }