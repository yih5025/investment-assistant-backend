from sqlalchemy import Column, String, Text, DateTime, Date
from sqlalchemy.sql import func
from app.models.base import BaseModel


class EarningsCalendarNews(BaseModel):
    """
    실적 캘린더 연계 뉴스 테이블 모델 (earnings_news_finnhub 테이블 매핑)
    
    용도:
    - 실적 캘린더 UI와 연동
    - 특정 기업의 특정 실적 발표에 대한 뉴스 제공
    - earnings_calendar의 (symbol, report_date)와 연결
    """
    __tablename__ = "earnings_news_finnhub"

    # 3중 복합 기본키
    symbol = Column(String, primary_key=True, nullable=False, comment="기업 심볼")
    report_date = Column(Date, primary_key=True, nullable=False, comment="실적 발표 예정일")
    url = Column(Text, primary_key=True, nullable=False, comment="뉴스 URL")
    
    # 뉴스 정보
    category = Column(Text, nullable=True, comment="뉴스 카테고리")
    article_id = Column(Text, nullable=True, comment="Finnhub 기사 ID")
    headline = Column(Text, nullable=True, comment="뉴스 헤드라인")
    image = Column(Text, nullable=True, comment="뉴스 이미지 URL")
    related = Column(Text, nullable=True, comment="관련 종목 코드")
    source = Column(String, nullable=True, comment="뉴스 소스")
    summary = Column(Text, nullable=True, comment="뉴스 요약")
    published_at = Column(DateTime, nullable=False, comment="뉴스 발행 시간")
    fetched_at = Column(DateTime, nullable=True, server_default=func.now(), comment="데이터 수집 시간")

    def __repr__(self):
        return f"<EarningsCalendarNews(symbol='{self.symbol}', report_date='{self.report_date}', headline='{self.headline[:30] if self.headline else 'N/A'}...')>"

    @property
    def short_headline(self):
        """헤드라인 짧은 버전 (100자 제한)"""
        if self.headline:
            return self.headline[:100] + "..." if len(self.headline) > 100 else self.headline
        return None

    @property
    def short_summary(self):
        """요약 짧은 버전 (200자 제한)"""
        if self.summary:
            return self.summary[:200] + "..." if len(self.summary) > 200 else self.summary
        return None

    @property
    def related_symbols(self):
        """관련 종목을 리스트로 반환"""
        if self.related:
            return [symbol.strip() for symbol in self.related.split(',') if symbol.strip()]
        return []

    @property
    def has_image(self):
        """이미지 존재 여부"""
        return bool(self.image and self.image.strip())