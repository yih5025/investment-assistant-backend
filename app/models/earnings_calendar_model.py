from sqlalchemy import Column, String, Date, Numeric, DateTime, Text
from app.models.base import BaseModel

class EarningsCalendar(BaseModel):
    """
    실적 발표 일정 테이블 모델
    
    earnings_calendar 테이블과 매핑됩니다.
    각 기업의 분기별 실적 발표 일정 정보를 저장합니다.
    """
    
    __tablename__ = "earnings_calendar"
    
    # 복합 기본키 (symbol + report_date)
    symbol = Column(String, primary_key=True, nullable=False, comment="주식 심볼 (예: AAPL, TSLA)")
    report_date = Column(Date, primary_key=True, nullable=False, comment="실적 발표 예정일")
    
    # 기업 정보
    company_name = Column(String, nullable=True, comment="회사명 (대부분 null)")
    
    # 실적 관련 정보
    fiscal_date_ending = Column(Date, nullable=True, comment="회계 연도 종료일")
    estimate = Column(Numeric, nullable=True, comment="예상 EPS (주당순이익)")
    currency = Column(String, nullable=True, comment="통화 (주로 USD)")
    event_description = Column(Text, nullable=True, comment="이벤트 설명 (대부분 null)")
    
    # 메타데이터
    fetched_at = Column(DateTime, nullable=True, comment="데이터 수집 시간")
    
    def __repr__(self):
        """디버깅용 문자열 표현"""
        return f"<EarningsCalendar(symbol='{self.symbol}', report_date='{self.report_date}')>"
    
    def to_dict(self):
        """JSON 직렬화용 딕셔너리 변환"""
        return {
            'symbol': self.symbol,
            'company_name': self.company_name,
            'report_date': self.report_date.isoformat() if self.report_date else None,
            'fiscal_date_ending': self.fiscal_date_ending.isoformat() if self.fiscal_date_ending else None,
            'estimate': float(self.estimate) if self.estimate else None,
            'currency': self.currency,
            'event_description': self.event_description,
            'fetched_at': self.fetched_at.isoformat() if self.fetched_at else None
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