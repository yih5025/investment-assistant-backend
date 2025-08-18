from sqlalchemy import Column, Date, Numeric, Text
from .base import BaseModel

class CPI(BaseModel):
    """
    소비자물가지수 (Consumer Price Index) 모델
    - 미국 도시 소비자들의 상품/서비스 가격 변화 지수
    - 월별 데이터로 제공
    - 인플레이션 측정의 핵심 지표
    - 기준: 1982-1984년 = 100
    """
    __tablename__ = "cpi"
    
    # 기본키: 날짜 (월별 데이터, 매월 1일)
    date = Column(Date, primary_key=True, nullable=False)
    
    # CPI 지수 값 (예: 322.561)
    cpi_value = Column(Numeric(8,3), nullable=True)
    
    # 데이터 간격 유형 (기본값: 'monthly')
    interval_type = Column(Text, default='monthly')
    
    # 단위 (기본값: 'index 1982-1984=100')
    unit = Column(Text, default='index 1982-1984=100')
    
    # 데이터 이름/설명
    name = Column(Text, default='Consumer Price Index for all Urban Consumers')