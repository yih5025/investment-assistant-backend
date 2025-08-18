from datetime import datetime, date, timedelta
from typing import List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_, text

# 기존 모델들 import (earnings_calendar_news 모델 대신 실제 테이블들 사용)
from app.models.earnings_calendar_model import EarningsCalendar
from app.models.company_news_model import CompanyNews

# 기존 스키마들 재활용
from app.schemas.earnings_calendar_news_schema import (
    EarningsCalendarNewsResponse,
    EarningsCalendarNewsListItem,
    EarningsCalendarNewsListResponse,
    EarningsCalendarNewsSearchResponse
)


class EarningsCalendarNewsService:
    """
    실적 캘린더 연계 뉴스 서비스 (JOIN 방식 구현)
    
    기존 인터페이스를 유지하면서 earnings_calendar + company_news 테이블을
    실시간으로 조인하여 데이터를 제공
    """

    def __init__(self, db: Session):
        self.db = db
        
        # 실적 관련 키워드 정의
        self.earnings_keywords = [
            'earnings', 'quarterly', 'results', 'eps', 'revenue',
            'profit', 'guidance', 'forecast', 'outlook', 'beats',
            'misses', 'estimates', 'consensus'
        ]

    def get_news_by_symbol_and_date(
        self,
        symbol: str,
        report_date: date,
        skip: int = 0,
        limit: int = 20
    ) -> EarningsCalendarNewsListResponse:
        """
        특정 기업의 특정 실적 발표에 대한 뉴스 조회 (JOIN 방식)
        
        Args:
            symbol: 기업 심볼 (예: AAPL)
            report_date: 실적 발표일
            skip: 페이징 offset
            limit: 페이징 limit
            
        Returns:
            EarningsCalendarNewsListResponse: 해당 실적 관련 뉴스 목록
        """
        # 1. 해당 실적 일정이 존재하는지 확인
        earnings_schedule = self.db.query(EarningsCalendar).filter(
            and_(
                EarningsCalendar.symbol == symbol,
                EarningsCalendar.report_date == report_date
            )
        ).first()
        
        if not earnings_schedule:
            # 실적 일정이 없어도 뉴스는 검색 (유연한 처리)
            pass
        
        # 2. 관련 뉴스 검색 (실적 발표일 기준 ±10일)
        start_date = report_date - timedelta(days=10)
        end_date = report_date + timedelta(days=3)  # 발표 후 3일까지
        
        # 키워드 조건 생성
        keyword_conditions = [
            CompanyNews.title.ilike(f'%{keyword}%') 
            for keyword in self.earnings_keywords
        ]
        
        # 뉴스 검색 쿼리
        query = self.db.query(CompanyNews).filter(
            and_(
                CompanyNews.symbol == symbol,
                CompanyNews.published_at >= start_date,
                CompanyNews.published_at <= end_date,
                or_(*keyword_conditions)
            )
        ).order_by(CompanyNews.published_at.desc())
        
        # 전체 개수 및 페이징
        total = query.count()
        news_items = query.offset(skip).limit(limit).all()
        
        # 3. 응답 데이터 생성 (기존 스키마 구조 유지)
        response_items = []
        for news in news_items:
            item = EarningsCalendarNewsListItem(
                symbol=news.symbol,
                report_date=report_date,  # 요청받은 실적 발표일
                url=news.url,
                headline=news.title,  # company_news.title -> headline
                source=news.source,
                published_at=news.published_at,
                short_headline=news.title[:100] if news.title else None,
                has_image=bool(news.description and 'image' in news.description.lower()),
                fetched_at=news.fetched_at
            )
            response_items.append(item)
        
        # 페이징 정보
        page = (skip // limit) + 1
        has_next = (skip + limit) < total
        
        return EarningsCalendarNewsListResponse(
            total=total,
            items=response_items,
            symbol=symbol,
            report_date=report_date,
            page=page,
            limit=limit,
            has_next=has_next
        )

    def get_news_by_symbol(
        self,
        symbol: str,
        skip: int = 0,
        limit: int = 20,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> EarningsCalendarNewsListResponse:
        """
        특정 기업의 모든 실적 관련 뉴스 조회 (JOIN 방식)
        """
        # 1. 해당 기업의 실적 일정들 조회
        earnings_query = self.db.query(EarningsCalendar).filter(
            EarningsCalendar.symbol == symbol
        )
        
        if start_date:
            earnings_query = earnings_query.filter(EarningsCalendar.report_date >= start_date)
        if end_date:
            earnings_query = earnings_query.filter(EarningsCalendar.report_date <= end_date)
            
        earnings_dates = [e.report_date for e in earnings_query.all()]
        
        # 2. 각 실적 발표일 기준으로 뉴스 검색 범위 생성
        date_ranges = []
        for report_date in earnings_dates:
            date_ranges.extend([
                report_date - timedelta(days=10),
                report_date + timedelta(days=3)
            ])
        
        if not date_ranges:
            # 실적 일정이 없으면 빈 결과 반환
            return EarningsCalendarNewsListResponse(
                total=0, items=[], symbol=symbol, report_date=None,
                page=1, limit=limit, has_next=False
            )
        
        # 3. 전체 날짜 범위에서 실적 관련 뉴스 검색
        overall_start = min(date_ranges)
        overall_end = max(date_ranges)
        
        keyword_conditions = [
            CompanyNews.title.ilike(f'%{keyword}%') 
            for keyword in self.earnings_keywords
        ]
        
        query = self.db.query(CompanyNews).filter(
            and_(
                CompanyNews.symbol == symbol,
                CompanyNews.published_at >= overall_start,
                CompanyNews.published_at <= overall_end,
                or_(*keyword_conditions)
            )
        ).order_by(CompanyNews.published_at.desc())
        
        # 페이징
        total = query.count()
        news_items = query.offset(skip).limit(limit).all()
        
        # 4. 각 뉴스에 가장 가까운 실적 발표일 매칭
        response_items = []
        for news in news_items:
            # 뉴스 발행일과 가장 가까운 실적 발표일 찾기
            closest_report_date = min(
                earnings_dates,
                key=lambda d: abs((news.published_at.date() - d).days)
            )
            
            item = EarningsCalendarNewsListItem(
                symbol=news.symbol,
                report_date=closest_report_date,
                url=news.url,
                headline=news.title,
                source=news.source,
                published_at=news.published_at,
                short_headline=news.title[:100] if news.title else None,
                has_image=bool(news.description and 'image' in news.description.lower()),
                fetched_at=news.fetched_at
            )
            response_items.append(item)
        
        page = (skip // limit) + 1
        has_next = (skip + limit) < total
        
        return EarningsCalendarNewsListResponse(
            total=total,
            items=response_items,
            symbol=symbol,
            report_date=None,
            page=page,
            limit=limit,
            has_next=has_next
        )

    def get_earnings_calendar_news_list(
        self,
        skip: int = 0,
        limit: int = 20,
        symbols: Optional[List[str]] = None,
        start_report_date: Optional[date] = None,
        end_report_date: Optional[date] = None,
        start_published_date: Optional[datetime] = None,
        end_published_date: Optional[datetime] = None,
        sources: Optional[List[str]] = None
    ) -> EarningsCalendarNewsListResponse:
        """
        전체 실적 캘린더 뉴스 목록 조회 (JOIN 방식)
        """
        # 1. 실적 캘린더에서 조건에 맞는 일정들 조회
        earnings_query = self.db.query(EarningsCalendar)
        
        if symbols:
            earnings_query = earnings_query.filter(EarningsCalendar.symbol.in_(symbols))
        if start_report_date:
            earnings_query = earnings_query.filter(EarningsCalendar.report_date >= start_report_date)
        if end_report_date:
            earnings_query = earnings_query.filter(EarningsCalendar.report_date <= end_report_date)
        
        earnings_schedules = earnings_query.all()
        
        if not earnings_schedules:
            return EarningsCalendarNewsListResponse(
                total=0, items=[], symbol=None, report_date=None,
                page=1, limit=limit, has_next=False
            )
        
        # 2. 실적 일정별로 관련 뉴스 수집
        all_news = []
        
        for schedule in earnings_schedules:
            # 각 실적 발표일 기준 ±7일 뉴스 검색
            start_date = schedule.report_date - timedelta(days=7)
            end_date = schedule.report_date + timedelta(days=3)
            
            keyword_conditions = [
                CompanyNews.title.ilike(f'%{keyword}%') 
                for keyword in self.earnings_keywords
            ]
            
            news_query = self.db.query(CompanyNews).filter(
                and_(
                    CompanyNews.symbol == schedule.symbol,
                    CompanyNews.published_at >= start_date,
                    CompanyNews.published_at <= end_date,
                    or_(*keyword_conditions)
                )
            )
            
            # 추가 필터링
            if start_published_date:
                news_query = news_query.filter(CompanyNews.published_at >= start_published_date)
            if end_published_date:
                news_query = news_query.filter(CompanyNews.published_at <= end_published_date)
            if sources:
                news_query = news_query.filter(CompanyNews.source.in_(sources))
            
            # 뉴스에 실적 발표일 정보 추가
            for news in news_query.all():
                news.matched_report_date = schedule.report_date
                all_news.append(news)
        
        # 3. 중복 제거 및 정렬
        unique_news = {}
        for news in all_news:
            key = f"{news.symbol}_{news.url}"
            if key not in unique_news:
                unique_news[key] = news
        
        sorted_news = sorted(
            unique_news.values(),
            key=lambda x: x.published_at,
            reverse=True
        )
        
        # 4. 페이징 및 응답 생성
        total = len(sorted_news)
        paginated_news = sorted_news[skip:skip + limit]
        
        response_items = []
        for news in paginated_news:
            item = EarningsCalendarNewsListItem(
                symbol=news.symbol,
                report_date=news.matched_report_date,
                url=news.url,
                headline=news.title,
                source=news.source,
                published_at=news.published_at,
                short_headline=news.title[:100] if news.title else None,
                has_image=bool(news.description and 'image' in news.description.lower()),
                fetched_at=news.fetched_at
            )
            response_items.append(item)
        
        page = (skip // limit) + 1
        has_next = (skip + limit) < total
        
        return EarningsCalendarNewsListResponse(
            total=total,
            items=response_items,
            symbol=None,
            report_date=None,
            page=page,
            limit=limit,
            has_next=has_next
        )

    def get_news_detail(
        self,
        symbol: str,
        report_date: date,
        url: str
    ) -> Optional[EarningsCalendarNewsResponse]:
        """
        특정 뉴스 상세 조회 (JOIN 방식)
        """
        # company_news에서 해당 뉴스 조회
        news = self.db.query(CompanyNews).filter(
            and_(
                CompanyNews.symbol == symbol,
                CompanyNews.url == url
            )
        ).first()
        
        if not news:
            return None
        
        # 해당 실적 일정 확인
        earnings_schedule = self.db.query(EarningsCalendar).filter(
            and_(
                EarningsCalendar.symbol == symbol,
                EarningsCalendar.report_date == report_date
            )
        ).first()
        
        return EarningsCalendarNewsResponse(
            symbol=news.symbol,
            report_date=report_date,
            url=news.url,
            headline=news.title,
            source=news.source,
            published_at=news.published_at,
            category="earnings",  # 실적 관련으로 고정
            article_id=None,  # company_news에는 없음
            image=None,  # 필요시 description에서 추출
            related=None,
            summary=news.description,  # description을 summary로 사용
            fetched_at=news.fetched_at,
            short_headline=news.title[:100] if news.title else None,
            short_summary=news.description[:200] if news.description else None,
            related_symbols=[symbol],  # 현재 심볼만
            has_image=bool(news.description and 'image' in news.description.lower())
        )

    def search_earnings_calendar_news(
        self,
        query_text: str,
        skip: int = 0,
        limit: int = 20,
        symbols: Optional[List[str]] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> EarningsCalendarNewsSearchResponse:
        """
        실적 관련 뉴스 검색 (JOIN 방식)
        """
        # 1. 실적 키워드 + 검색어 조건
        keyword_conditions = [
            CompanyNews.title.ilike(f'%{keyword}%') 
            for keyword in self.earnings_keywords
        ]
        
        search_conditions = [
            CompanyNews.title.ilike(f'%{query_text}%'),
            CompanyNews.description.ilike(f'%{query_text}%'),
            CompanyNews.source.ilike(f'%{query_text}%')
        ]
        
        # 2. 기본 검색 쿼리 (실적 관련 뉴스 중에서 검색)
        search_query = self.db.query(CompanyNews).filter(
            and_(
                or_(*keyword_conditions),  # 실적 관련 키워드 포함
                or_(*search_conditions)    # 검색어 포함
            )
        ).order_by(CompanyNews.published_at.desc())
        
        # 3. 추가 필터링
        if symbols:
            search_query = search_query.filter(CompanyNews.symbol.in_(symbols))
            
        # 날짜 필터링 (실적 발표일이 아닌 뉴스 발행일 기준)
        if start_date or end_date:
            # 실적 발표일 범위에 해당하는 뉴스들 찾기
            earnings_query = self.db.query(EarningsCalendar)
            if start_date:
                earnings_query = earnings_query.filter(EarningsCalendar.report_date >= start_date)
            if end_date:
                earnings_query = earnings_query.filter(EarningsCalendar.report_date <= end_date)
            
            valid_symbols = [e.symbol for e in earnings_query.all()]
            if valid_symbols:
                search_query = search_query.filter(CompanyNews.symbol.in_(valid_symbols))
        
        # 4. 페이징
        total = search_query.count()
        items = search_query.offset(skip).limit(limit).all()
        
        # 5. 각 뉴스에 관련 실적 발표일 매칭
        news_items = []
        for news in items:
            # 해당 뉴스와 관련된 실적 발표일 찾기
            related_earnings = self.db.query(EarningsCalendar).filter(
                EarningsCalendar.symbol == news.symbol
            ).order_by(
                func.abs(
                    func.extract('epoch', EarningsCalendar.report_date - news.published_at.date())
                )
            ).first()
            
            matched_report_date = related_earnings.report_date if related_earnings else None
            
            news_items.append(EarningsCalendarNewsResponse(
                symbol=news.symbol,
                report_date=matched_report_date,
                url=news.url,
                headline=news.title,
                source=news.source,
                published_at=news.published_at,
                category="earnings",
                article_id=None,
                image=None,
                related=None,
                summary=news.description,
                fetched_at=news.fetched_at,
                short_headline=news.title[:100] if news.title else None,
                short_summary=news.description[:200] if news.description else None,
                related_symbols=[news.symbol],
                has_image=bool(news.description and 'image' in news.description.lower())
            ))
        
        page = (skip // limit) + 1
        has_next = (skip + limit) < total
        
        return EarningsCalendarNewsSearchResponse(
            total=total,
            items=news_items,
            search_query=query_text,
            page=page,
            limit=limit,
            has_next=has_next
        )