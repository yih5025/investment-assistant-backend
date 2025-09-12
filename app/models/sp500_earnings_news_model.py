# app/models/sp500_earnings_news_model.py
from sqlalchemy import Column, Integer, String, DateTime, Text, ForeignKey
from sqlalchemy.orm import relationship
from app.models.base import BaseModel

class SP500EarningsNews(BaseModel):
    """
    S&P 500 실적 관련 뉴스 테이블 모델
    
    sp500_earnings_news 테이블과 매핑됩니다.
    각 실적 발표 일정과 관련된 뉴스 기사들을 저장합니다.
    """
    
    __tablename__ = "sp500_earnings_news"
    
    # 기본키
    id = Column(Integer, primary_key=True, autoincrement=True, comment="고유 ID")
    
    # 외래키: sp500_earnings_calendar.id 참조
    calendar_id = Column(
        Integer, 
        ForeignKey("sp500_earnings_calendar.id", ondelete="CASCADE"),
        nullable=False,
        comment="실적 캘린더 ID (외래키)"
    )
    
    # 뉴스 메타데이터
    source_table = Column(String, nullable=True, comment="소스 테이블명 (earnings_news_finnhub, market_news)")
    title = Column(Text, nullable=True, comment="뉴스 제목")
    url = Column(Text, nullable=True, comment="뉴스 URL")
    summary = Column(Text, nullable=True, comment="뉴스 요약")
    content = Column(Text, nullable=True, comment="뉴스 본문 (대부분 null)")
    source = Column(String, nullable=True, comment="뉴스 소스 (Yahoo, CNN, Reuters 등)")
    
    # 뉴스 시간 정보
    published_at = Column(DateTime, nullable=True, comment="뉴스 게시 시간")
    
    # 뉴스 분류 정보
    news_section = Column(String, nullable=True, comment="뉴스 섹션 (forecast, reaction)")
    days_from_earnings = Column(Integer, nullable=True, comment="실적 발표일로부터 며칠 차이")
    
    # 메타데이터
    fetched_at = Column(DateTime, nullable=True, comment="뉴스 수집 시간")
    created_at = Column(DateTime, nullable=True, comment="생성 시간")
    
    # 관계 설정: N:1 (여러 뉴스가 하나의 캘린더 이벤트에 속함)
    earnings_calendar = relationship(
        "SP500EarningsCalendar",
        back_populates="related_news",
        lazy="select"
    )
    
    def __repr__(self):
        """디버깅용 문자열 표현"""
        return f"<SP500EarningsNews(id={self.id}, calendar_id={self.calendar_id}, title='{self.title[:50] if self.title else 'None'}...')>"
    
    def to_dict(self):
        """JSON 직렬화용 딕셔너리 변환"""
        return {
            'id': self.id,
            'calendar_id': self.calendar_id,
            'source_table': self.source_table,
            'title': self.title,
            'url': self.url,
            'summary': self.summary,
            'content': self.content,
            'source': self.source,
            'published_at': self.published_at.isoformat() if self.published_at else None,
            'news_section': self.news_section,
            'days_from_earnings': self.days_from_earnings,
            'fetched_at': self.fetched_at.isoformat() if self.fetched_at else None,
            'created_at': self.created_at.isoformat() if self.created_at else None
        }
    
    @property
    def is_forecast_news(self):
        """실적 발표 전 예측 뉴스인지 확인"""
        return self.news_section == "forecast"
    
    @property
    def is_reaction_news(self):
        """실적 발표 후 반응 뉴스인지 확인"""
        return self.news_section == "reaction"
    
    @property
    def has_content(self):
        """본문 내용이 있는지 확인"""
        return self.content is not None and len(self.content.strip()) > 0
    
    @property
    def short_title(self):
        """제목 축약 버전 (50자)"""
        if not self.title:
            return "제목 없음"
        return self.title[:50] + "..." if len(self.title) > 50 else self.title
    
    @classmethod
    def get_by_calendar_id(cls, db_session, calendar_id):
        """특정 캘린더 ID의 뉴스 목록 조회"""
        return db_session.query(cls).filter(
            cls.calendar_id == calendar_id
        ).order_by(cls.published_at.desc()).all()
    
    @classmethod
    def get_forecast_news(cls, db_session, calendar_id):
        """특정 캘린더 ID의 예측 뉴스만 조회"""
        return db_session.query(cls).filter(
            cls.calendar_id == calendar_id,
            cls.news_section == "forecast"
        ).order_by(cls.published_at.desc()).all()
    
    @classmethod
    def get_reaction_news(cls, db_session, calendar_id):
        """특정 캘린더 ID의 반응 뉴스만 조회"""
        return db_session.query(cls).filter(
            cls.calendar_id == calendar_id,
            cls.news_section == "reaction"
        ).order_by(cls.published_at.desc()).all()
    
    @classmethod
    def get_recent_news(cls, db_session, limit=10):
        """최신 뉴스 조회"""
        return db_session.query(cls).order_by(
            cls.published_at.desc()
        ).limit(limit).all()