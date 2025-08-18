# app/models/top_gainers_model.py
from sqlalchemy import Column, Integer, String, Numeric, BigInteger, DateTime, Boolean
from sqlalchemy.sql import func
from app.models.base import BaseModel

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
    def get_by_category(cls, db_session, category: str, batch_id: int = None, limit: int = 50):
        """카테고리별 데이터 조회"""
        query = db_session.query(cls).filter(cls.category == category)
        
        if batch_id:
            query = query.filter(cls.batch_id == batch_id)
        else:
            # 최신 batch_id 사용
            latest_batch = cls.get_latest_batch_id(db_session)
            if latest_batch:
                query = query.filter(cls.batch_id == latest_batch[0])
        
        return query.order_by(cls.rank_position).limit(limit).all()
    
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