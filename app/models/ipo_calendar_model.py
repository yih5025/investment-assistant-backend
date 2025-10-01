# app/models/ipo_calendar_model.py
from sqlalchemy import Column, String, Date, Numeric, DateTime, Integer, UniqueConstraint
from app.models.base import Base
from datetime import datetime

class IPOCalendar(Base):
    """IPO 캘린더 테이블 모델"""
    
    __tablename__ = "ipo_calendar"
    
    # 기본 키 (자동 증가)
    id = Column(Integer, primary_key=True, autoincrement=True, comment="고유 ID")
    
    # 기본 정보
    symbol = Column(String(10), nullable=False, index=True, comment="기업 심볼")
    company_name = Column(String, nullable=False, comment="기업명")
    ipo_date = Column(Date, nullable=False, index=True, comment="IPO 예정일")
    
    # 복합 유니크 제약 조건 (symbol + ipo_date 조합은 고유해야 함)
    __table_args__ = (
        UniqueConstraint('symbol', 'ipo_date', name='uq_symbol_ipo_date'),
    )
    
    # 가격 정보
    price_range_low = Column(Numeric(10, 2), nullable=True, comment="공모가 하한선")
    price_range_high = Column(Numeric(10, 2), nullable=True, comment="공모가 상한선")
    currency = Column(String(3), default="USD", comment="통화")
    
    # 거래소 정보
    exchange = Column(String(20), nullable=True, comment="상장 거래소")
    
    # 메타데이터
    fetched_at = Column(DateTime, default=datetime.utcnow, comment="데이터 수집 시간")
    created_at = Column(DateTime, default=datetime.utcnow, comment="생성 시간")
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, comment="수정 시간")
    
    def __repr__(self):
        return f"<IPOCalendar(symbol={self.symbol}, company={self.company_name}, date={self.ipo_date})>"