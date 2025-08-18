from sqlalchemy import Column, Date, Numeric, Text
from .base import BaseModel

class Inflation(BaseModel):
    """
    인플레이션 데이터 모델
    - 연간 미국 소비자 물가 인플레이션율 데이터
    - 1년에 1번씩 업데이트되는 연간 데이터
    """
    __tablename__ = "inflation"
    
    # 기본키: 날짜 (연도별로 1월 1일)
    date = Column(Date, primary_key=True, nullable=False)
    
    # 인플레이션율 (예: 2.950 = 2.95%)
    inflation_rate = Column(Numeric, nullable=True)
    
    # 데이터 간격 유형 (기본값: 'annual')
    interval_type = Column(Text, default='annual')
    
    # 단위 (기본값: 'percent')
    unit = Column(Text, default='percent')
    
    # 데이터 이름/설명
    name = Column(Text, default='Inflation - US Consumer Prices')