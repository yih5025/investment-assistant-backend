# app/models/finnhub_trades_model.py
from sqlalchemy import Column, Integer, String, Numeric, BigInteger, DateTime, Text, ARRAY
from sqlalchemy.sql import func
from app.models.base import BaseModel

class FinnhubTrades(BaseModel):
    """
    Finnhub 거래 데이터 테이블 ORM 모델
    
    이 테이블은 Finnhub WebSocket에서 수집한 실시간 주식 거래 데이터를 저장합니다.
    Top Gainers와 SP500 주식의 실시간 거래 정보가 포함됩니다.
    """
    __tablename__ = "finnhub_trades"
    
    # Primary Key
    id = Column(Integer, primary_key=True, autoincrement=True,
               comment="자동 증가 Primary Key")
    
    # 기본 정보
    symbol = Column(String(10), nullable=False, index=True,
                   comment="주식 심볼 (예: AAPL, TSLA)")
    price = Column(Numeric(12, 4), nullable=False,
                  comment="거래 가격 ($)")
    volume = Column(BigInteger, nullable=True,
                   comment="거래량")
    timestamp_ms = Column(BigInteger, nullable=False, index=True,
                         comment="거래 시간 (밀리초 타임스탬프)")
    
    # 거래 조건 및 메타데이터
    trade_conditions = Column(ARRAY(String), nullable=True,
                             comment="거래 조건들 (배열)")
    category = Column(String(20), nullable=True, index=True,
                     comment="카테고리 (top_gainers, most_actively_traded, top_losers)")
    
    # 데이터 소스 정보
    source = Column(String(50), nullable=False, server_default='finnhub_websocket',
                   comment="데이터 소스")
    
    # 메타데이터
    created_at = Column(DateTime, nullable=False, server_default=func.now(), index=True,
                       comment="레코드 생성 시간")
    
    def __repr__(self):
        return f"<FinnhubTrades(id={self.id}, symbol='{self.symbol}', price={self.price}, volume={self.volume}, timestamp={self.timestamp_ms})>"
    
    def to_dict(self):
        """딕셔너리로 변환 (JSON 직렬화용)"""
        return {
            'id': self.id,
            'symbol': self.symbol,
            'price': float(self.price) if self.price else None,
            'volume': self.volume,
            'timestamp_ms': self.timestamp_ms,
            'trade_conditions': self.trade_conditions,
            'category': self.category,
            'source': self.source,
            'created_at': self.created_at.isoformat() if self.created_at else None
        }
    
    def to_redis_format(self):
        """Redis 저장용 간소화된 형태로 변환"""
        return {
            'symbol': self.symbol,
            'price': float(self.price) if self.price else None,
            'volume': self.volume,
            'category': self.category,
            'timestamp': self.timestamp_ms,
            'source': 'finnhub'
        }
    
    @classmethod
    def get_latest_by_symbol(cls, db_session, symbol: str, limit: int = 10):
        """특정 심볼의 최신 거래 데이터 조회"""
        return db_session.query(cls).filter(
            cls.symbol == symbol
        ).order_by(cls.timestamp_ms.desc()).limit(limit).all()
    
    @classmethod
    def get_by_category(cls, db_session, category: str, limit: int = 50):
        """카테고리별 최신 거래 데이터 조회"""
        return db_session.query(cls).filter(
            cls.category == category
        ).order_by(cls.timestamp_ms.desc()).limit(limit).all()
    
    @classmethod
    def get_latest_prices(cls, db_session, symbols: list = None, category: str = None):
        """심볼들의 최신 가격 조회 (WebSocket용)"""
        from sqlalchemy import distinct, and_
        from sqlalchemy.orm import aliased
        
        # 서브쿼리: 각 심볼별 최신 timestamp 조회
        latest_timestamps = db_session.query(
            cls.symbol,
            func.max(cls.timestamp_ms).label('max_timestamp')
        )
        
        if symbols:
            latest_timestamps = latest_timestamps.filter(cls.symbol.in_(symbols))
        if category:
            latest_timestamps = latest_timestamps.filter(cls.category == category)
            
        latest_timestamps = latest_timestamps.group_by(cls.symbol).subquery()
        
        # 메인 쿼리: 최신 timestamp의 실제 데이터 조회
        main_query = db_session.query(cls).join(
            latest_timestamps,
            and_(
                cls.symbol == latest_timestamps.c.symbol,
                cls.timestamp_ms == latest_timestamps.c.max_timestamp
            )
        )
        
        return main_query.all()
    
    @classmethod
    def get_recent_activity(cls, db_session, minutes: int = 60, category: str = None):
        """최근 N분간의 거래 활동 조회"""
        import time
        cutoff_timestamp = (time.time() - minutes * 60) * 1000  # 밀리초로 변환
        
        query = db_session.query(cls).filter(cls.timestamp_ms >= cutoff_timestamp)
        
        if category:
            query = query.filter(cls.category == category)
            
        return query.order_by(cls.timestamp_ms.desc()).all()