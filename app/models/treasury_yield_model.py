from sqlalchemy import Column, String, Date, Numeric, DateTime
from sqlalchemy.sql import func
from app.models.base import BaseModel


class TreasuryYield(BaseModel):
    """
    국채 수익률 모델
    
    테이블 구조:
    - Primary Key: (date, maturity, interval_type) 복합키
    - date: 기준일자
    - maturity: 만기 (2year, 10year, 30year)
    - interval_type: 주기 (monthly)
    - yield_rate: 수익률 (%)
    """
    __tablename__ = "treasury_yield"

    # 복합 Primary Key
    date = Column(Date, primary_key=True, nullable=False, comment="기준일자")
    maturity = Column(String(10), primary_key=True, nullable=False, comment="만기")
    interval_type = Column(String(10), primary_key=True, nullable=False, comment="주기")
    
    # 데이터 필드
    yield_rate = Column(Numeric(6, 4), comment="수익률 (%)")
    
    # 시스템 필드
    created_at = Column(DateTime(timezone=True), server_default=func.now(), comment="생성일시")
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), comment="수정일시")

    def __repr__(self):
        return f"<TreasuryYield(date={self.date}, maturity={self.maturity}, yield_rate={self.yield_rate})>"

    class Config:
        """Pydantic 설정"""
        from_attributes = True