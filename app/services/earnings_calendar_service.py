from sqlalchemy.orm import Session
from sqlalchemy import and_, asc, desc, func
from typing import List, Optional, Tuple
from datetime import date, datetime, timedelta

from app.models.earnings_calendar_model import EarningsCalendar
from app.schemas.earnings_calendar_schema import EarningsCalendarQueryParams

class EarningsCalendarService:
    """실적 발표 캘린더 관련 비즈니스 로직 서비스"""
    
    def __init__(self, db: Session):
        self.db = db
    
    def get_earnings_calendar(self, params: EarningsCalendarQueryParams) -> Tuple[List[EarningsCalendar], int]:
        """실적 발표 일정을 조회합니다."""
        query = self.db.query(EarningsCalendar)
        
        # 날짜 범위 필터링
        if params.start_date and params.end_date:
            query = query.filter(
                and_(
                    EarningsCalendar.report_date >= params.start_date,
                    EarningsCalendar.report_date <= params.end_date
                )
            )
        elif params.start_date:
            query = query.filter(EarningsCalendar.report_date >= params.start_date)
        elif params.end_date:
            query = query.filter(EarningsCalendar.report_date <= params.end_date)
        
        # 심볼 필터링
        if params.symbol:
            query = query.filter(EarningsCalendar.symbol.ilike(f"%{params.symbol}%"))
        
        # 전체 개수 계산
        total_count = query.count()
        
        # 정렬 및 페이징
        query = query.order_by(asc(EarningsCalendar.report_date))
        query = query.offset(params.offset).limit(params.limit)
        
        results = query.all()
        return results, total_count
    
    def get_earnings_by_date_range(self, start_date: date, end_date: date) -> List[EarningsCalendar]:
        """특정 날짜 범위의 실적 발표 일정을 조회합니다."""
        return self.db.query(EarningsCalendar).filter(
            and_(
                EarningsCalendar.report_date >= start_date,
                EarningsCalendar.report_date <= end_date
            )
        ).order_by(asc(EarningsCalendar.report_date)).all()
    
    def get_earnings_by_symbol(self, symbol: str, limit: int = 10) -> List[EarningsCalendar]:
        """특정 심볼의 실적 발표 일정을 조회합니다."""
        return self.db.query(EarningsCalendar).filter(
            EarningsCalendar.symbol == symbol.upper()
        ).order_by(asc(EarningsCalendar.report_date)).limit(limit).all()
    
    def get_upcoming_earnings(self, days: int = 7) -> List[EarningsCalendar]:
        """앞으로 N일 내의 실적 발표 일정을 조회합니다."""
        today = date.today()
        future_date = today + timedelta(days=days)
        
        return self.db.query(EarningsCalendar).filter(
            and_(
                EarningsCalendar.report_date >= today,
                EarningsCalendar.report_date <= future_date
            )
        ).order_by(asc(EarningsCalendar.report_date)).all()
    
    def get_today_earnings(self) -> List[EarningsCalendar]:
        """오늘의 실적 발표 일정을 조회합니다."""
        today = date.today()
        return self.db.query(EarningsCalendar).filter(
            EarningsCalendar.report_date == today
        ).order_by(asc(EarningsCalendar.symbol)).all()
    
    def get_total_earnings_count(self) -> int:
        """전체 실적 일정 개수를 반환합니다."""
        return self.db.query(EarningsCalendar).count()
    
    def get_companies_with_estimates_count(self) -> int:
        """예상 수익이 있는 기업 수를 반환합니다."""
        return self.db.query(EarningsCalendar).filter(
            EarningsCalendar.estimate.isnot(None)
        ).count()
    
    def get_latest_update_time(self) -> Optional[datetime]:
        """최근 업데이트 시간을 반환합니다."""
        result = self.db.query(func.max(EarningsCalendar.fetched_at)).scalar()
        return result
    
    def get_count_by_currency(self, currency: str) -> int:
        """특정 통화의 실적 일정 개수를 반환합니다."""
        return self.db.query(EarningsCalendar).filter(
            EarningsCalendar.currency == currency
        ).count()
    
    def get_count_by_currency_not(self, currency: str) -> int:
        """특정 통화가 아닌 실적 일정 개수를 반환합니다."""
        return self.db.query(EarningsCalendar).filter(
            EarningsCalendar.currency != currency
        ).count()