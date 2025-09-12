# app/models/sp500_earnings_calendar_model.py
from sqlalchemy import Column, Integer, String, Date, Numeric, DateTime, Text
from sqlalchemy.orm import relationship
from app.models.base import BaseModel

class SP500EarningsCalendar(BaseModel):
    """
    S&P 500 실적 발표 캘린더 테이블 모델
    
    sp500_earnings_calendar 테이블과 매핑됩니다.
    각 S&P 500 기업의 분기별 실적 발표 일정 정보를 저장합니다.
    """
    
    __tablename__ = "sp500_earnings_calendar"
    
    # 기본키
    id = Column(Integer, primary_key=True, autoincrement=True, comment="고유 ID")
    
    # 주식 및 기업 정보
    symbol = Column(String, nullable=False, comment="주식 심볼 (예: AAPL, TSLA)")
    company_name = Column(String, nullable=True, comment="회사명")
    
    # 실적 발표 일정 정보
    report_date = Column(Date, nullable=False, comment="실적 발표 예정일")
    fiscal_date_ending = Column(Date, nullable=True, comment="회계 연도 종료일")
    
    # 예상 수익 정보
    estimate = Column(Numeric, nullable=True, comment="예상 EPS (주당순이익)")
    currency = Column(String, nullable=True, comment="통화 (주로 USD)")
    
    # GICS 분류 (Global Industry Classification Standard)
    gics_sector = Column(String, nullable=True, comment="GICS 섹터")
    gics_sub_industry = Column(String, nullable=True, comment="GICS 세부 산업")
    headquarters = Column(String, nullable=True, comment="본사 위치")
    
    # 이벤트 정보
    event_type = Column(String, nullable=True, comment="이벤트 타입 (earnings_report)")
    event_title = Column(String, nullable=True, comment="이벤트 제목")
    event_description = Column(Text, nullable=True, comment="이벤트 설명")
    
    # 뉴스 카운트 정보
    total_news_count = Column(Integer, nullable=True, comment="총 뉴스 개수")
    forecast_news_count = Column(Integer, nullable=True, comment="예측 뉴스 개수")
    reaction_news_count = Column(Integer, nullable=True, comment="반응 뉴스 개수")
    
    # 메타데이터
    created_at = Column(DateTime, nullable=True, comment="생성 시간")
    updated_at = Column(DateTime, nullable=True, comment="수정 시간")
    
    # 관계 설정: 1:N (하나의 캘린더 이벤트에 여러 뉴스)
    related_news = relationship(
        "SP500EarningsNews",
        back_populates="earnings_calendar",
        lazy="select",
        cascade="all, delete-orphan"
    )
    
    def __repr__(self):
        """디버깅용 문자열 표현"""
        return f"<SP500EarningsCalendar(id={self.id}, symbol='{self.symbol}', report_date='{self.report_date}')>"
    
    def to_dict(self):
        """JSON 직렬화용 딕셔너리 변환"""
        return {
            'id': self.id,
            'symbol': self.symbol,
            'company_name': self.company_name,
            'report_date': self.report_date.isoformat() if self.report_date else None,
            'fiscal_date_ending': self.fiscal_date_ending.isoformat() if self.fiscal_date_ending else None,
            'estimate': float(self.estimate) if self.estimate else None,
            'currency': self.currency,
            'gics_sector': self.gics_sector,
            'gics_sub_industry': self.gics_sub_industry,
            'headquarters': self.headquarters,
            'event_type': self.event_type,
            'event_title': self.event_title,
            'event_description': self.event_description,
            'total_news_count': self.total_news_count,
            'forecast_news_count': self.forecast_news_count,
            'reaction_news_count': self.reaction_news_count,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }
    
    @property
    def has_estimate(self):
        """예상 수익이 있는지 확인"""
        return self.estimate is not None
    
    @property
    def is_future_date(self):
        """미래 일정인지 확인"""
        from datetime import date
        return self.report_date and self.report_date >= date.today()
    
    @property
    def has_news(self):
        """관련 뉴스가 있는지 확인"""
        return self.total_news_count and self.total_news_count > 0
    
    @classmethod
    def get_by_symbol(cls, db_session, symbol):
        """특정 심볼의 실적 일정 조회"""
        return db_session.query(cls).filter(cls.symbol == symbol.upper()).all()
    
    @classmethod
    def get_by_date_range(cls, db_session, start_date, end_date):
        """날짜 범위로 실적 일정 조회"""
        return db_session.query(cls).filter(
            cls.report_date >= start_date,
            cls.report_date <= end_date
        ).order_by(cls.report_date).all()
    
    @classmethod
    def get_weekly_events(cls, db_session):
        """이번 주 실적 일정 조회"""
        from datetime import date, timedelta
        today = date.today()
        # 이번 주 시작 (월요일)
        week_start = today - timedelta(days=today.weekday())
        # 이번 주 끝 (일요일)
        week_end = week_start + timedelta(days=6)
        
        return cls.get_by_date_range(db_session, week_start, week_end)