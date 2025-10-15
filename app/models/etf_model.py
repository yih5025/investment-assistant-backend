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

    symbol = Column(String(10), primary_key=True, index=True, nullable=False)
    name = Column(String(255), nullable=False)

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
        """
        특정 ETF의 전날 종가 조회 (주말/공휴일 고려)
        
        미국 시장의 마지막 거래일 폐장가를 정확히 조회합니다.
        주말과 공휴일을 고려하여 실제 거래가 있었던 마지막 날의 데이터를 찾습니다.
        """
        try:
            import pytz
            us_eastern = pytz.timezone('US/Eastern')
            
            # 현재 미국 동부 시간
            now_utc = datetime.now(pytz.utc)
            now_us = now_utc.astimezone(us_eastern)
            
            # 미국 공휴일 목록 (간단한 버전)
            market_holidays = {
                '2024-01-01', '2024-01-15', '2024-02-19', '2024-03-29',
                '2024-05-27', '2024-06-19', '2024-07-04', '2024-09-02',
                '2024-11-28', '2024-12-25',
                '2025-01-01', '2025-01-20', '2025-02-17', '2025-04-18',
                '2025-05-26', '2025-06-19', '2025-07-04', '2025-09-01',
                '2025-11-27', '2025-12-25'
            }
            
            # 마지막 거래일 찾기 (주말과 공휴일 제외)
            last_trading_day = now_us - timedelta(days=1)
            while (last_trading_day.weekday() >= 5 or 
                   last_trading_day.strftime('%Y-%m-%d') in market_holidays):
                last_trading_day -= timedelta(days=1)
            
            # 검색 범위: 마지막 거래일 종료 시점 전후
            search_end = last_trading_day.replace(hour=20, minute=0, second=0, microsecond=0)  # 오후 8시까지 (시간외 포함)
            search_start = search_end - timedelta(days=1)  # 24시간 전부터
            
            # 어제 마지막 거래 가격 조회
            previous_trade = db.query(cls).filter(
                cls.symbol == symbol,
                cls.created_at >= search_start.replace(tzinfo=None),
                cls.created_at <= search_end.replace(tzinfo=None)
            ).order_by(cls.created_at.desc()).first()
            
            if previous_trade:
                return float(previous_trade.price)
            
            # 데이터가 없으면 확장 검색 (5일 전까지)
            extended_search_start = search_end - timedelta(days=5)
            extended_trade = db.query(cls).filter(
                cls.symbol == symbol,
                cls.created_at >= extended_search_start.replace(tzinfo=None),
                cls.created_at <= search_end.replace(tzinfo=None)
            ).order_by(cls.created_at.desc()).first()
            
            return float(extended_trade.price) if extended_trade else None
            
        except Exception as e:
            logger.error(f"{symbol} ETF 전날 종가 조회 실패: {e}")
            return None

    @classmethod
    def get_trading_volume_24h(cls, db: Session, symbol: str) -> int:
        """
        특정 ETF의 24시간 거래량 조회
        
        Args:
            db: 데이터베이스 세션
            symbol: ETF 심볼
            
        Returns:
            int: 24시간 총 거래량
        """
        try:
            from sqlalchemy import func
            
            # 24시간 전부터 현재까지
            since_24h = datetime.now() - timedelta(hours=24)
            
            result = db.query(
                func.sum(cls.volume).label('total_volume')
            ).filter(
                cls.symbol == symbol,
                cls.created_at >= since_24h,
                cls.volume.isnot(None)
            ).scalar()
            
            return int(result) if result else 0
            
        except Exception as e:
            logger.error(f"{symbol} ETF 24시간 거래량 조회 실패: {e}")
            return 0

    @classmethod
    def get_price_change_info(cls, db: Session, symbol: str) -> Dict[str, Any]:
        """ETF 가격 변동 정보 조회 (24시간 거래량 포함)"""
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
            
            # 24시간 거래량 조회
            volume_24h = cls.get_trading_volume_24h(db, symbol)
            
            return {
                'current_price': current_price,
                'previous_close': previous_close,
                'change_amount': round(change_amount, 2) if change_amount else None,
                'change_percentage': round(change_percentage, 2) if change_percentage else None,
                'volume': volume_24h,  # 24시간 누적 거래량 사용
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
            # 해당 심볼의 최신 데이터 시점을 기준으로 조회
            latest_record = db.query(cls).filter(cls.symbol == symbol).order_by(cls.created_at.desc()).first()
            
            if not latest_record:
                logger.warning(f"ETF {symbol}의 데이터가 없습니다")
                return []
            
            latest_time = latest_record.created_at
            logger.info(f"ETF {symbol} 최신 데이터 시점: {latest_time}")
            
            # 최신 데이터 시점을 기준으로 시간대별 필터링
            if timeframe == '1D':
                # 1일: 최신 데이터 시점에서 24시간 전
                start_time = latest_time - timedelta(hours=24)
            elif timeframe == '1W':
                # 1주일: 최신 데이터 시점에서 7일 전
                start_time = latest_time - timedelta(days=7)
            elif timeframe == '1M':
                # 1개월: 최신 데이터 시점에서 30일 전
                start_time = latest_time - timedelta(days=30)
            else:
                # 기본값: 24시간 전
                start_time = latest_time - timedelta(hours=24)
            
            logger.info(f"ETF {symbol} 차트 조회 범위: {start_time} ~ {latest_time} ({timeframe})")
            
            chart_data = db.query(cls).filter(
                cls.symbol == symbol,
                cls.created_at >= start_time
            ).order_by(cls.created_at.asc()).limit(limit).all()
            
            logger.info(f"ETF {symbol} 차트 데이터 조회 결과: {len(chart_data)}개")
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