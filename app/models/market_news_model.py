from sqlalchemy import Column, String, Text, DateTime, text
from sqlalchemy.sql import func
from app.models.base import BaseModel   


class MarketNews(BaseModel):
    """
    시장 뉴스 테이블 모델
    
    주요 특징:
    - source, url 복합 기본키
    - published_at: 뉴스 발행 시간 (중요한 정렬 기준)
    - fetched_at: 데이터 수집 시간 (자동 생성)
    - content: 뉴스 본문 (긴 텍스트)
    """
    __tablename__ = "market_news"

    # 복합 기본키 (source + url)
    source = Column(String, primary_key=True, nullable=False, comment="뉴스 소스 (예: BBC News)")
    url = Column(String, primary_key=True, nullable=False, comment="뉴스 URL (고유 식별자)")
    
    # 뉴스 메타데이터
    author = Column(String, nullable=True, comment="기사 작성자")
    title = Column(String, nullable=False, comment="뉴스 제목")
    description = Column(Text, nullable=True, comment="뉴스 요약/설명")
    content = Column(Text, nullable=True, comment="뉴스 본문 내용")
    
    # 시간 정보
    published_at = Column(DateTime, nullable=False, comment="뉴스 발행 시간")
    fetched_at = Column(
        DateTime, 
        nullable=True, 
        server_default=func.now(),  # 데이터베이스에서 자동 생성
        comment="데이터 수집 시간"
    )

    def __repr__(self):
        """객체 표현을 위한 메서드"""
        return f"<MarketNews(source='{self.source}', title='{self.title[:50]}...')>"

    @property
    def short_description(self):
        """설명문의 짧은 버전 (100자 제한)"""
        if self.description:
            return self.description[:100] + "..." if len(self.description) > 100 else self.description
        return None

    @property
    def content_preview(self):
        """본문의 미리보기 (200자 제한)"""
        if self.content:
            return self.content[:200] + "..." if len(self.content) > 200 else self.content
        return None