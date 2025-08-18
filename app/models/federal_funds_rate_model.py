from sqlalchemy import Column, Date, Numeric, Text
from .base import BaseModel

class FederalFundsRate(BaseModel):
    """
    연방기금금리 (Federal Funds Rate) 모델
    - 미국 연준(Fed)에서 설정하는 기준금리
    - 월별 데이터로 제공
    - 경제정책 및 투자 분석의 핵심 지표
    """
    __tablename__ = "federal_funds_rate"
    
    # 기본키: 날짜 (월별 데이터, 매월 1일)
    date = Column(Date, primary_key=True, nullable=False)
    
    # 금리 (예: 4.330 = 4.33%)
    rate = Column(Numeric(6,3), nullable=True)
    
    # 데이터 간격 유형 (기본값: 'monthly')
    interval_type = Column(Text, default='monthly')
    
    # 단위 (기본값: 'percent')
    unit = Column(Text, default='percent')
    
    # 데이터 이름/설명
    name = Column(Text, default='Effective Federal Funds Rate')