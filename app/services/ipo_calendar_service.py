# app/services/ipo_calendar_service.py
from sqlalchemy.orm import Session
from sqlalchemy import and_, func, extract
from typing import List, Optional, Tuple
from datetime import date, datetime, timedelta
from collections import defaultdict

from app.models.ipo_calendar_model import IPOCalendar

class IPOCalendarService:
    """IPO 캘린더 비즈니스 로직 서비스"""
    
    def __init__(self, db: Session):
        self.db = db
    
    def get_all_ipos(
        self, 
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        exchange: Optional[str] = None,
        limit: int = 100
    ) -> Tuple[List[IPOCalendar], int]:
        """
        전체 IPO 일정 조회 (캘린더용)
        """
        query = self.db.query(IPOCalendar)
        
        # 날짜 범위 필터링
        if start_date:
            query = query.filter(IPOCalendar.ipo_date >= start_date)
        if end_date:
            query = query.filter(IPOCalendar.ipo_date <= end_date)
        
        # 거래소 필터링
        if exchange:
            query = query.filter(IPOCalendar.exchange.ilike(f"%{exchange}%"))
        
        # 전체 개수
        total_count = query.count()
        
        # 정렬 및 제한
        items = query.order_by(IPOCalendar.ipo_date.asc()).limit(limit).all()
        
        return items, total_count
    
    def get_monthly_ipos(self, year: int = None, month: int = None) -> List[IPOCalendar]:
        """
        이번 달 IPO 일정 조회
        """
        if year is None or month is None:
            today = date.today()
            year = today.year
            month = today.month
        
        return self.db.query(IPOCalendar).filter(
            and_(
                extract('year', IPOCalendar.ipo_date) == year,
                extract('month', IPOCalendar.ipo_date) == month
            )
        ).order_by(IPOCalendar.ipo_date.asc()).all()
    
    def get_statistics(self) -> dict:
        """
        IPO 통계 정보 조회
        """
        today = date.today()
        
        # 전체 IPO 개수 (과거 포함 전체)
        total_ipos = self.db.query(func.count(IPOCalendar.id)).scalar()
        
        # 이번 달 IPO (과거 포함 이번 달 전체)
        this_month_count = self.db.query(func.count(IPOCalendar.id)).filter(
            and_(
                extract('year', IPOCalendar.ipo_date) == today.year,
                extract('month', IPOCalendar.ipo_date) == today.month
            )
        ).scalar()
        
        # 다음 달 IPO
        next_month = today.replace(day=1) + timedelta(days=32)
        next_month = next_month.replace(day=1)
        next_month_count = self.db.query(func.count(IPOCalendar.id)).filter(
            and_(
                extract('year', IPOCalendar.ipo_date) == next_month.year,
                extract('month', IPOCalendar.ipo_date) == next_month.month
            )
        ).scalar()
        
        # 향후 7일 내 IPO
        future_7days = today + timedelta(days=7)
        upcoming_7days_count = self.db.query(func.count(IPOCalendar.id)).filter(
            and_(
                IPOCalendar.ipo_date >= today,
                IPOCalendar.ipo_date <= future_7days
            )
        ).scalar()
        
        # 거래소별 개수
        exchange_stats = self.db.query(
            IPOCalendar.exchange,
            func.count(IPOCalendar.symbol)
        ).filter(
            IPOCalendar.ipo_date >= today
        ).group_by(IPOCalendar.exchange).all()
        
        by_exchange = {exchange: count for exchange, count in exchange_stats if exchange}
        
        # 평균 공모가 범위
        avg_price = self.db.query(
            func.avg(IPOCalendar.price_range_low).label('avg_low'),
            func.avg(IPOCalendar.price_range_high).label('avg_high')
        ).filter(
            and_(
                IPOCalendar.price_range_low.isnot(None),
                IPOCalendar.price_range_high.isnot(None),
                IPOCalendar.ipo_date >= today
            )
        ).first()
        
        avg_price_range = {
            "low": round(float(avg_price.avg_low), 2) if avg_price.avg_low else 0.0,
            "high": round(float(avg_price.avg_high), 2) if avg_price.avg_high else 0.0
        }
        
        return {
            "total_ipos": total_ipos or 0,
            "this_month": this_month_count or 0,
            "next_month": next_month_count or 0,
            "by_exchange": by_exchange,
            "avg_price_range": avg_price_range,
            "upcoming_7days": upcoming_7days_count or 0
        }