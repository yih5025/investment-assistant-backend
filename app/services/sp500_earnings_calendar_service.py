# app/services/sp500_earnings_calendar_service.py
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, asc, desc, func
from typing import List, Optional, Tuple, Dict, Any
from datetime import date, datetime, timedelta

from app.models.sp500_earnings_calendar_model import SP500EarningsCalendar
from app.schemas.sp500_earnings_calendar_schema import SP500EarningsCalendarQueryParams

class SP500EarningsCalendarService:
    """S&P 500 실적 발표 캘린더 관련 비즈니스 로직 서비스"""
    
    def __init__(self, db: Session):
        self.db = db
    
    def get_all_calendar_events(self, params: SP500EarningsCalendarQueryParams = None) -> Tuple[List[SP500EarningsCalendar], int]:
        """
        모든 실적 발표 일정을 조회 (프론트엔드 캘린더용)
        날짜 제한 없이 전체 데이터를 반환하되, 필터링 옵션 제공
        """
        query = self.db.query(SP500EarningsCalendar)
        
        if params:
            # 날짜 범위 필터링 (옵션)
            if params.start_date and params.end_date:
                query = query.filter(
                    and_(
                        SP500EarningsCalendar.report_date >= params.start_date,
                        SP500EarningsCalendar.report_date <= params.end_date
                    )
                )
            elif params.start_date:
                query = query.filter(SP500EarningsCalendar.report_date >= params.start_date)
            elif params.end_date:
                query = query.filter(SP500EarningsCalendar.report_date <= params.end_date)
            
            # 심볼 필터링
            if params.symbol:
                query = query.filter(SP500EarningsCalendar.symbol.ilike(f"%{params.symbol.upper()}%"))
            
            # 섹터 필터링
            if params.sector:
                query = query.filter(SP500EarningsCalendar.gics_sector.ilike(f"%{params.sector}%"))
            
            # 예상 수익 존재 여부 필터링
            if params.has_estimate is not None:
                if params.has_estimate:
                    query = query.filter(SP500EarningsCalendar.estimate.isnot(None))
                else:
                    query = query.filter(SP500EarningsCalendar.estimate.is_(None))
        
        # 전체 개수 계산
        total_count = query.count()
        
        # 정렬 및 페이징
        query = query.order_by(asc(SP500EarningsCalendar.report_date), asc(SP500EarningsCalendar.symbol))
        
        if params and params.limit and params.offset is not None:
            query = query.offset(params.offset).limit(params.limit)
        
        results = query.all()
        
        # 계산된 속성들 설정
        for result in results:
            result.has_estimate = result.estimate is not None
            result.is_future_date = result.report_date >= date.today() if result.report_date else False
            result.has_news = result.total_news_count and result.total_news_count > 0
        
        return results, total_count
    
    def get_weekly_events(self) -> Tuple[List[SP500EarningsCalendar], date, date]:
        """
        이번 주 실적 발표 일정 조회 (캘린더 하단 위젯용)
        """
        today = date.today()
        # 이번 주 시작 (월요일)
        week_start = today - timedelta(days=today.weekday())
        # 이번 주 끝 (일요일)
        week_end = week_start + timedelta(days=6)
        
        query = self.db.query(SP500EarningsCalendar).filter(
            and_(
                SP500EarningsCalendar.report_date >= week_start,
                SP500EarningsCalendar.report_date <= week_end
            )
        ).order_by(asc(SP500EarningsCalendar.report_date), asc(SP500EarningsCalendar.symbol))
        
        results = query.all()
        
        # 계산된 속성들 설정
        for result in results:
            result.has_estimate = result.estimate is not None
            result.is_future_date = result.report_date >= date.today() if result.report_date else False
            result.has_news = result.total_news_count and result.total_news_count > 0
        
        return results, week_start, week_end
    
    def get_earnings_by_symbol(self, symbol: str, limit: int = 10) -> List[SP500EarningsCalendar]:
        """
        특정 심볼의 실적 발표 일정을 조회 (옵션 기능)
        """
        query = self.db.query(SP500EarningsCalendar).filter(
            SP500EarningsCalendar.symbol == symbol.upper()
        ).order_by(desc(SP500EarningsCalendar.report_date))
        
        if limit:
            query = query.limit(limit)
        
        results = query.all()
        
        # 계산된 속성들 설정
        for result in results:
            result.has_estimate = result.estimate is not None
            result.is_future_date = result.report_date >= date.today() if result.report_date else False
            result.has_news = result.total_news_count and result.total_news_count > 0
        
        return results
    
    def get_calendar_event_by_id(self, calendar_id: int) -> Optional[SP500EarningsCalendar]:
        """
        특정 ID의 실적 캘린더 이벤트 조회
        """
        result = self.db.query(SP500EarningsCalendar).filter(
            SP500EarningsCalendar.id == calendar_id
        ).first()
        
        if result:
            result.has_estimate = result.estimate is not None
            result.is_future_date = result.report_date >= date.today() if result.report_date else False
            result.has_news = result.total_news_count and result.total_news_count > 0
        
        return result
    
    def get_upcoming_events(self, days: int = 30) -> List[SP500EarningsCalendar]:
        """
        향후 N일 내의 실적 발표 일정 조회
        """
        today = date.today()
        future_date = today + timedelta(days=days)
        
        query = self.db.query(SP500EarningsCalendar).filter(
            and_(
                SP500EarningsCalendar.report_date >= today,
                SP500EarningsCalendar.report_date <= future_date
            )
        ).order_by(asc(SP500EarningsCalendar.report_date), asc(SP500EarningsCalendar.symbol))
        
        results = query.all()
        
        # 계산된 속성들 설정
        for result in results:
            result.has_estimate = result.estimate is not None
            result.is_future_date = result.report_date >= date.today() if result.report_date else False
            result.has_news = result.total_news_count and result.total_news_count > 0
        
        return results
    