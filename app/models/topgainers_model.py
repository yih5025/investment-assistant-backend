# app/models/top_gainers_model.py
from sqlalchemy import Column, Integer, String, Numeric, BigInteger, DateTime, Boolean
from sqlalchemy.sql import func
from app.models.base import BaseModel
from sqlalchemy import Session
from typing import List, Dict, Optional
from datetime import datetime, timedelta
import pytz
import logging
from sqlalchemy import and_

logger = logging.getLogger(__name__)


class TopGainers(BaseModel):
    """
    Top Gainers 테이블 ORM 모델
    
    이 테이블은 Finnhub API에서 수집한 상승 주식, 하락 주식, 활발히 거래되는 주식 정보를 저장합니다.
    """
    __tablename__ = "top_gainers"
    __table_args__ = {'extend_existing': True}
    
    # 복합 Primary Key
    batch_id = Column(BigInteger, primary_key=True, nullable=False, 
                     comment="배치 ID (데이터 수집 단위)")
    symbol = Column(String(10), primary_key=True, nullable=False, 
                   comment="주식 심볼 (예: AAPL, TSLA)")
    category = Column(String(20), primary_key=True, nullable=False,
                     comment="카테고리 (top_gainers, top_losers, most_actively_traded)")
    
    # 데이터 필드
    last_updated = Column(DateTime, nullable=False,
                         comment="마지막 업데이트 시간")
    rank_position = Column(Integer, nullable=True,
                          comment="순위 (1~50)")
    price = Column(Numeric(10, 4), nullable=True,
                  comment="현재 가격 ($)")
    change_amount = Column(Numeric(10, 4), nullable=True,
                          comment="변동 금액 ($)")
    change_percentage = Column(String(20), nullable=True,
                              comment="변동률 (예: +5.2%)")
    volume = Column(BigInteger, nullable=True,
                   comment="거래량")
    
    # 메타데이터
    created_at = Column(DateTime, nullable=False, server_default=func.now(),
                       comment="레코드 생성 시간")
    
    def __repr__(self):
        return f"<TopGainers(batch_id={self.batch_id}, symbol='{self.symbol}', category='{self.category}', rank={self.rank_position}, price={self.price})>"
    
    def to_dict(self):
        """딕셔너리로 변환 (JSON 직렬화용)"""
        return {
            'batch_id': self.batch_id,
            'symbol': self.symbol,
            'category': self.category,
            'last_updated': self.last_updated.isoformat() if self.last_updated else None,
            'rank_position': self.rank_position,
            'price': float(self.price) if self.price else None,
            'change_amount': float(self.change_amount) if self.change_amount else None,
            'change_percentage': self.change_percentage,
            'volume': self.volume,
            'created_at': self.created_at.isoformat() if self.created_at else None
        }
    
    @classmethod
    def get_latest_batch_id(cls, db_session):
        """최신 batch_id 조회"""
        return db_session.query(cls.batch_id).order_by(cls.batch_id.desc()).first()
    @classmethod
    def get_previous_close_price(cls, db_session: Session, symbol: str) -> Optional[float]:
        """
        특정 심볼의 전일 종가 조회 (변동률 계산용)
        
        Args:
            db_session: 데이터베이스 세션
            symbol: 주식 심볼
            
        Returns:
            Optional[float]: 전일 종가 또는 None
        """
        try:
            us_eastern = pytz.timezone('US/Eastern')
            korea_tz = pytz.timezone('Asia/Seoul')
            
            # 현재 한국 시간
            now_korea = datetime.now(korea_tz)
            
            # 미국 시장 기준 "어제"의 마지막 시점 계산
            # 미국 동부 시간으로 변환하여 하루 빼기
            now_us = now_korea.astimezone(us_eastern)
            yesterday_us = now_us - timedelta(days=1)
            
            # 미국 시간 기준 어제 23:59:59를 한국 시간으로 다시 변환
            yesterday_end_us = yesterday_us.replace(hour=23, minute=59, second=59)
            yesterday_end_korea = yesterday_end_us.astimezone(korea_tz)
            
            # DB에서 해당 시점 이전 데이터 조회 (created_at은 한국 시간으로 저장됨)
            prev_trade = db_session.query(cls).filter(
                cls.symbol == symbol.upper(),
                cls.created_at <= yesterday_end_korea.replace(tzinfo=None)  # naive datetime으로 변환
            ).order_by(cls.created_at.desc()).first()
            
            return float(prev_trade.price) if prev_trade and prev_trade.price else None
            
        except Exception as e:
            logger.error(f"❌ {symbol} 전일 종가 조회 실패: {e}")
            return None

    @classmethod
    def get_batch_previous_close_prices(cls, db_session: Session, symbols: List[str]) -> Dict[str, float]:
        """
        여러 심볼의 전일 종가를 일괄 조회 (성능 최적화)
        
        Args:
            db_session: 데이터베이스 세션
            symbols: 주식 심볼 리스트
            
        Returns:
            Dict[str, float]: {symbol: previous_close_price}
        """
        try:
            # 🎯 미국 시장 기준 전일 계산
            us_eastern = pytz.timezone('US/Eastern')
            korea_tz = pytz.timezone('Asia/Seoul')
            
            # 현재 한국 시간
            now_korea = datetime.now(korea_tz)
            
            # 미국 시장 기준 "어제"의 마지막 시점 계산
            now_us = now_korea.astimezone(us_eastern)
            yesterday_us = now_us - timedelta(days=1)
            
            # 미국 시간 기준 어제 23:59:59를 한국 시간으로 다시 변환
            yesterday_end_us = yesterday_us.replace(hour=23, minute=59, second=59)
            yesterday_end_korea = yesterday_end_us.astimezone(korea_tz)
            
            # 서브쿼리: 각 심볼별 전날 최신 created_at 조회
            subquery = db_session.query(
                cls.symbol,
                func.max(cls.created_at).label('max_created_at')
            ).filter(
                cls.symbol.in_([s.upper() for s in symbols]),
                cls.created_at <= yesterday_end_korea.replace(tzinfo=None)  # naive datetime으로 변환
            ).group_by(cls.symbol).subquery()
            
            # 메인 쿼리: 전날 최신 데이터 조회
            prev_trades = db_session.query(cls).join(
                subquery,
                and_(
                    cls.symbol == subquery.c.symbol,
                    cls.created_at == subquery.c.max_created_at
                )
            ).all()
            
            # 딕셔너리로 변환
            result = {}
            for trade in prev_trades:
                if trade.price:
                    result[trade.symbol] = float(trade.price)
            
            return result
            
        except Exception as e:
            logger.error(f"❌ 일괄 전일 종가 조회 실패: {e}")
            return {}
        
    @classmethod
    def get_by_category(cls, db_session, category: str, batch_id: int = None, limit: int = 50, offset: int = 0):
        """카테고리별 데이터 조회 - offset 파라미터 추가"""
        query = db_session.query(cls).filter(cls.category == category)
        
        if batch_id:
            query = query.filter(cls.batch_id == batch_id)
        else:
            # 최신 batch_id 사용
            latest_batch = cls.get_latest_batch_id(db_session)
            if latest_batch:
                query = query.filter(cls.batch_id == latest_batch[0])
        
        return query.order_by(cls.rank_position).offset(offset).limit(limit).all()
    
    @classmethod 
    def get_symbol_data(cls, db_session, symbol: str, category: str = None):
        """특정 심볼 데이터 조회"""
        query = db_session.query(cls).filter(cls.symbol == symbol)
        
        if category:
            query = query.filter(cls.category == category)
        
        # 최신 batch_id의 데이터만
        latest_batch = cls.get_latest_batch_id(db_session)
        if latest_batch:
            query = query.filter(cls.batch_id == latest_batch[0])
        
        return query.first()
    
    @classmethod
    def get_latest_data_by_symbols(cls, db_session, category: str = None, limit: int = 50):
        """
        각 심볼의 최신 데이터 조회 (장 마감 시 사용)
        
        Args:
            db_session: 데이터베이스 세션
            category: 카테고리 필터 (top_gainers, top_losers, most_actively_traded)
            limit: 반환할 최대 개수
            
        Returns:
            List[TopGainers]: 각 심볼의 최신 데이터 리스트
        """
        from sqlalchemy import func
        
        # 서브쿼리: 각 심볼별 최신 batch_id 조회
        subquery = db_session.query(
            cls.symbol,
            func.max(cls.batch_id).label('max_batch_id')
        ).group_by(cls.symbol).subquery()
        
        # 메인 쿼리: 최신 batch_id의 데이터 조회
        query = db_session.query(cls).join(
            subquery,
            (cls.symbol == subquery.c.symbol) & 
            (cls.batch_id == subquery.c.max_batch_id)
        )
        
        # 카테고리 필터링
        if category:
            query = query.filter(cls.category == category)
        
        # 순위 또는 가격 기준 정렬
        query = query.order_by(
            cls.rank_position.asc().nulls_last(),
            cls.price.desc().nulls_last()
        )
        
        return query.limit(limit).all()