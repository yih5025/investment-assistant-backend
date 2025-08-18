from sqlalchemy import Column, String, Text, DateTime, BigInteger
from sqlalchemy.sql import func
from app.models.base import BaseModel


class FinancialNews(BaseModel):
    """
    Finnhub 금융 뉴스 테이블 모델 (market_news_finnhub 테이블 매핑)
    
    주요 특징:
    - category, news_id 복합 기본키
    - 4개 카테고리: crypto, forex, merger, general
    - Finnhub API에서 수집한 전문 금융 뉴스
    - news_id는 Finnhub의 고유 ID (BigInteger)
    """
    __tablename__ = "market_news_finnhub"

    # 복합 기본키 (category + news_id)
    category = Column(String, primary_key=True, nullable=False, comment="뉴스 카테고리 (crypto/forex/merger/general)")
    news_id = Column(BigInteger, primary_key=True, nullable=False, comment="Finnhub 뉴스 고유 ID")
    
    # 뉴스 시간 정보 (DB 컬럼은 'datetime', Python에서는 'published_at')  
    published_at = Column("datetime", DateTime, nullable=False, comment="뉴스 발행 시간 (Finnhub 제공)")
    
    # 뉴스 콘텐츠
    headline = Column(Text, nullable=True, comment="뉴스 헤드라인")
    image = Column(Text, nullable=True, comment="뉴스 이미지 URL")
    related = Column(Text, nullable=True, comment="관련 종목 코드 (쉼표 구분)")
    source = Column(String, nullable=True, comment="뉴스 원본 소스")
    summary = Column(Text, nullable=True, comment="뉴스 요약")
    url = Column(Text, nullable=True, comment="원본 뉴스 URL")
    
    # 데이터 수집 시간
    fetched_at = Column(
        DateTime, 
        nullable=True, 
        server_default=func.now(),
        comment="데이터 수집 시간"
    )

    def __repr__(self):
        """객체 표현을 위한 메서드"""
        return f"<FinancialNews(category='{self.category}', news_id={self.news_id}, headline='{self.headline[:50] if self.headline else 'N/A'}...')>"

    @property
    def short_headline(self):
        """헤드라인의 짧은 버전 (100자 제한)"""
        if self.headline:
            return self.headline[:100] + "..." if len(self.headline) > 100 else self.headline
        return None

    @property
    def short_summary(self):
        """요약의 짧은 버전 (200자 제한)"""
        if self.summary:
            return self.summary[:200] + "..." if len(self.summary) > 200 else self.summary
        return None

    @property
    def related_symbols(self):
        """관련 종목을 리스트로 반환"""
        if self.related:
            # 쉼표로 구분된 종목 코드를 리스트로 변환
            return [symbol.strip() for symbol in self.related.split(',') if symbol.strip()]
        return []

    @property
    def has_image(self):
        """이미지 존재 여부"""
        return bool(self.image and self.image.strip())

    @property
    def category_display_name(self):
        """카테고리 표시명"""
        category_names = {
            'crypto': '암호화폐',
            'forex': '외환',
            'merger': '인수합병',
            'general': '일반 금융'
        }
        return category_names.get(self.category, self.category)

    @classmethod
    def get_valid_categories(cls):
        """유효한 카테고리 목록 반환"""
        return ['crypto', 'forex', 'merger', 'general']