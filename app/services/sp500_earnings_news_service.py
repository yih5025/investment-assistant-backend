# app/services/sp500_earnings_news_service.py
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, asc, desc, func
from typing import List, Optional, Tuple, Dict, Any
from datetime import datetime

from app.models.sp500_earnings_news_model import SP500EarningsNews
from app.models.sp500_earnings_calendar_model import SP500EarningsCalendar
from app.schemas.sp500_earnings_news_schema import SP500EarningsNewsQueryParams

class SP500EarningsNewsService:
    """S&P 500 실적 관련 뉴스 비즈니스 로직 서비스"""
    
    def __init__(self, db: Session):
        self.db = db
    
    def get_news_by_calendar_id(self, calendar_id: int, params: SP500EarningsNewsQueryParams = None) -> Tuple[List[SP500EarningsNews], int]:
        """
        특정 캘린더 ID의 뉴스 목록 조회 (메인 기능)
        """
        query = self.db.query(SP500EarningsNews).filter(
            SP500EarningsNews.calendar_id == calendar_id
        )
        
        if params:
            # 뉴스 섹션 필터링 (forecast, reaction)
            if params.news_section:
                query = query.filter(SP500EarningsNews.news_section == params.news_section)
            
            # 뉴스 소스 필터링
            if params.source:
                query = query.filter(SP500EarningsNews.source.ilike(f"%{params.source}%"))
            
            # 게시일 범위 필터링
            if params.start_date and params.end_date:
                query = query.filter(
                    and_(
                        SP500EarningsNews.published_at >= params.start_date,
                        SP500EarningsNews.published_at <= params.end_date
                    )
                )
            elif params.start_date:
                query = query.filter(SP500EarningsNews.published_at >= params.start_date)
            elif params.end_date:
                query = query.filter(SP500EarningsNews.published_at <= params.end_date)
            
            # 본문 내용 존재 여부 필터링
            if params.has_content is not None:
                if params.has_content:
                    query = query.filter(SP500EarningsNews.content.isnot(None))
                    query = query.filter(SP500EarningsNews.content != '')
                else:
                    query = query.filter(
                        or_(
                            SP500EarningsNews.content.is_(None),
                            SP500EarningsNews.content == ''
                        )
                    )
        
        # 전체 개수 계산
        total_count = query.count()
        
        # 정렬 (최신순)
        query = query.order_by(desc(SP500EarningsNews.published_at))
        
        # 페이징
        if params and params.limit and params.offset is not None:
            query = query.offset(params.offset).limit(params.limit)
        
        results = query.all()
        
        # 계산된 속성들은 @property로 정의되어 있어서 별도 설정 불필요
        
        return results, total_count
    
    def get_news_with_calendar_info(self, calendar_id: int) -> Dict[str, Any]:
        """
        캘린더 정보와 함께 뉴스 목록을 반환 (UI 표시용)
        """
        # 캘린더 정보 조회
        calendar_info = self.db.query(SP500EarningsCalendar).filter(
            SP500EarningsCalendar.id == calendar_id
        ).first()
        
        if not calendar_info:
            return None
        
        # 예측 뉴스 조회
        forecast_news = self.db.query(SP500EarningsNews).filter(
            and_(
                SP500EarningsNews.calendar_id == calendar_id,
                SP500EarningsNews.news_section == "forecast"
            )
        ).order_by(desc(SP500EarningsNews.published_at)).all()
        
        # 반응 뉴스 조회
        reaction_news = self.db.query(SP500EarningsNews).filter(
            and_(
                SP500EarningsNews.calendar_id == calendar_id,
                SP500EarningsNews.news_section == "reaction"
            )
        ).order_by(desc(SP500EarningsNews.published_at)).all()
        
        # 계산된 속성들은 @property로 정의되어 있어서 별도 설정 불필요
        all_news = forecast_news + reaction_news
        
        return {
            "calendar_info": {
                "id": calendar_info.id,
                "symbol": calendar_info.symbol,
                "company_name": calendar_info.company_name,
                "report_date": calendar_info.report_date,
                "estimate": calendar_info.estimate,
                "event_title": calendar_info.event_title,
                "gics_sector": calendar_info.gics_sector
            },
            "forecast_news": forecast_news,
            "reaction_news": reaction_news,
            "total_news_count": len(all_news),
            "forecast_news_count": len(forecast_news),
            "reaction_news_count": len(reaction_news)
        }
    
    def get_forecast_news(self, calendar_id: int, limit: int = 20) -> List[SP500EarningsNews]:
        """
        특정 캘린더 ID의 예측 뉴스만 조회
        """
        query = self.db.query(SP500EarningsNews).filter(
            and_(
                SP500EarningsNews.calendar_id == calendar_id,
                SP500EarningsNews.news_section == "forecast"
            )
        ).order_by(desc(SP500EarningsNews.published_at))
        
        if limit:
            query = query.limit(limit)
        
        results = query.all()
        
        # 계산된 속성들은 @property로 정의되어 있어서 별도 설정 불필요
        
        return results
    
    def get_reaction_news(self, calendar_id: int, limit: int = 20) -> List[SP500EarningsNews]:
        """
        특정 캘린더 ID의 반응 뉴스만 조회
        """
        query = self.db.query(SP500EarningsNews).filter(
            and_(
                SP500EarningsNews.calendar_id == calendar_id,
                SP500EarningsNews.news_section == "reaction"
            )
        ).order_by(desc(SP500EarningsNews.published_at))
        
        if limit:
            query = query.limit(limit)
        
        results = query.all()
        
        # 계산된 속성들은 @property로 정의되어 있어서 별도 설정 불필요
        
        return results
    
    def get_weekly_news(self) -> Dict[str, Any]:
        """
        이번 주 실적 이벤트별 뉴스 목록 조회
        earnings_calendar의 weekly 데이터와 연계하여 뉴스까지 함께 제공
        """
        from datetime import date, timedelta
        
        # 이번 주 범위 계산
        today = date.today()
        week_start = today - timedelta(days=today.weekday())  # 월요일
        week_end = week_start + timedelta(days=6)  # 일요일
        
        # 이번 주 실적 캘린더 조회
        weekly_calendar_events = self.db.query(SP500EarningsCalendar).filter(
            and_(
                SP500EarningsCalendar.report_date >= week_start,
                SP500EarningsCalendar.report_date <= week_end
            )
        ).order_by(SP500EarningsCalendar.report_date).all()
        
        earnings_with_news = []
        total_news_count = 0
        total_forecast_count = 0
        total_reaction_count = 0
        
        for calendar_event in weekly_calendar_events:
            # 각 실적 이벤트의 뉴스 조회
            forecast_news = self.db.query(SP500EarningsNews).filter(
                and_(
                    SP500EarningsNews.calendar_id == calendar_event.id,
                    SP500EarningsNews.news_section == "forecast"
                )
            ).order_by(desc(SP500EarningsNews.published_at)).all()
            
            reaction_news = self.db.query(SP500EarningsNews).filter(
                and_(
                    SP500EarningsNews.calendar_id == calendar_event.id,
                    SP500EarningsNews.news_section == "reaction"
                )
            ).order_by(desc(SP500EarningsNews.published_at)).all()
            
            # 뉴스가 있는 이벤트만 포함하거나, 모든 이벤트를 포함할지 결정
            all_news = forecast_news + reaction_news
            
            # 실적 이벤트 정보와 뉴스를 함께 구성
            earnings_data = {
                "calendar_info": {
                    "id": calendar_event.id,
                    "symbol": calendar_event.symbol,
                    "company_name": calendar_event.company_name,
                    "report_date": calendar_event.report_date.isoformat(),
                    "estimate": str(calendar_event.estimate) if calendar_event.estimate else None,
                    "event_title": calendar_event.event_title,
                    "gics_sector": calendar_event.gics_sector
                },
                "forecast_news": [
                    {
                        **news.to_dict(),
                        'is_forecast_news': news.is_forecast_news,
                        'is_reaction_news': news.is_reaction_news,
                        'has_content': news.has_content,
                        'short_title': news.short_title
                    } for news in forecast_news
                ],
                "reaction_news": [
                    {
                        **news.to_dict(),
                        'is_forecast_news': news.is_forecast_news,
                        'is_reaction_news': news.is_reaction_news,
                        'has_content': news.has_content,
                        'short_title': news.short_title
                    } for news in reaction_news
                ],
                "news_count": len(all_news),
                "forecast_news_count": len(forecast_news),
                "reaction_news_count": len(reaction_news)
            }
            
            earnings_with_news.append(earnings_data)
            
            # 전체 통계 업데이트
            total_news_count += len(all_news)
            total_forecast_count += len(forecast_news)
            total_reaction_count += len(reaction_news)
        
        return {
            "week_start": week_start.isoformat(),
            "week_end": week_end.isoformat(),
            "earnings_with_news": earnings_with_news,
            "total_earnings_count": len(weekly_calendar_events),
            "total_news_count": total_news_count,
            "forecast_news_count": total_forecast_count,
            "reaction_news_count": total_reaction_count
        }
    
    def _get_short_title(self, title: str) -> str:
        """제목 축약 (100자)"""
        if not title:
            return "제목 없음"
        return title[:100] + "..." if len(title) > 100 else title