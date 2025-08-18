from sqlalchemy import Column, Text, DateTime, func
from app.models.base import BaseModel

class CompanyNews(BaseModel):
    """
    기업 뉴스 테이블 모델
    
    이 모델은 PostgreSQL의 company_news 테이블과 매핑됩니다.
    - 복합 기본키: (symbol, url)
    - 특정 기업(symbol)에 대한 뉴스 데이터를 저장
    """
    __tablename__ = "company_news"
    
    # 복합 기본키: 심볼과 URL 조합
    symbol = Column(Text, primary_key=True, nullable=False, 
                   comment="주식 심볼 (예: AAPL, TSLA)")
    url = Column(Text, primary_key=True, nullable=False,
                comment="뉴스 기사 URL (중복 방지용)")
    
    # 뉴스 기본 정보
    source = Column(Text, comment="뉴스 소스 (예: Reuters, Bloomberg)")
    title = Column(Text, comment="뉴스 제목")
    description = Column(Text, comment="뉴스 요약/설명")
    content = Column(Text, comment="뉴스 전체 내용")
    
    # 시간 정보
    published_at = Column(DateTime, nullable=False, 
                         comment="뉴스 발행 시간")
    fetched_at = Column(DateTime, default=func.now(), 
                       comment="데이터 수집 시간")
    
    def __repr__(self):
        return f"<CompanyNews(symbol='{self.symbol}', title='{self.title[:50]}...')>"

