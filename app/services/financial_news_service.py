from datetime import datetime, timedelta
from typing import List, Optional, Tuple, Dict
from sqlalchemy.orm import Session
from sqlalchemy import func, text, and_, or_

from app.models.financial_news_model import FinancialNews
from app.schemas.financial_news_schema import (
    FinancialNewsResponse,
    FinancialNewsListItem,
    FinancialNewsListResponse,
    FinancialNewsSearchResponse,
    CategoryStatsResponse,
    CategoriesStatsResponse,
    CategoryType
)


class FinancialNewsService:
    """
    Finnhub 금융 뉴스 비즈니스 로직 서비스
    
    주요 기능:
    1. 카테고리별 뉴스 조회 (crypto, forex, merger, general)
    2. 관련 종목 기반 필터링
    3. 날짜 범위 필터링
    4. 전문 검색 (헤드라인, 요약)
    5. 카테고리별 통계
    6. 최신 뉴스 조회
    """

    def __init__(self, db: Session):
        self.db = db

    def get_financial_news_list(
        self,
        skip: int = 0,
        limit: int = 20,
        categories: Optional[List[CategoryType]] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        symbols: Optional[List[str]] = None,
        sources: Optional[List[str]] = None
    ) -> FinancialNewsListResponse:
        """
        금융 뉴스 목록 조회 (페이징 + 다중 필터링)
        
        Args:
            skip: 건너뛸 항목 수
            limit: 한 페이지당 항목 수
            categories: 포함할 카테고리 목록
            start_date: 시작 날짜 필터
            end_date: 종료 날짜 필터
            symbols: 관련 종목 필터 (BTCUSD, AAPL 등)
            sources: 뉴스 소스 필터
            
        Returns:
            FinancialNewsListResponse: 페이징된 뉴스 목록
        """
        # 기본 쿼리 (최신순 정렬)
        query = self.db.query(FinancialNews).order_by(FinancialNews.published_at.desc())
        
        # 카테고리 필터링
        if categories:
            query = query.filter(FinancialNews.category.in_(categories))
            
        # 날짜 필터링
        if start_date:
            query = query.filter(FinancialNews.published_at >= start_date)
        if end_date:
            query = query.filter(FinancialNews.published_at <= end_date)
            
        # 관련 종목 필터링 (부분 문자열 검색)
        if symbols:
            symbol_conditions = []
            for symbol in symbols:
                # related 필드에서 해당 종목이 포함된 뉴스 검색
                symbol_conditions.append(FinancialNews.related.ilike(f'%{symbol}%'))
            query = query.filter(or_(*symbol_conditions))
            
        # 소스 필터링
        if sources:
            query = query.filter(FinancialNews.source.in_(sources))
        
        # 전체 개수 조회 (페이징 정보용)
        total = query.count()
        
        # 페이징 적용
        items_query = query.offset(skip).limit(limit)
        items = items_query.all()
        
        # 응답 데이터 생성
        news_items = []
        for item in items:
            news_item = FinancialNewsListItem(
                category=item.category,
                news_id=item.news_id,
                published_at=item.published_at,
                headline=item.headline,
                source=item.source,
                short_headline=item.short_headline,
                has_image=item.has_image,
                related_symbols=item.related_symbols,
                category_display_name=item.category_display_name,
                fetched_at=item.fetched_at
            )
            news_items.append(news_item)
        
        # 페이징 정보 계산
        page = (skip // limit) + 1
        has_next = (skip + limit) < total
        
        # 현재 조회 카테고리 (단일 카테고리인 경우만 표시)
        current_category = categories[0] if categories and len(categories) == 1 else None
        
        return FinancialNewsListResponse(
            total=total,
            items=news_items,
            category=current_category,
            page=page,
            limit=limit,
            has_next=has_next
        )

    def get_news_by_id(self, category: CategoryType, news_id: int) -> Optional[FinancialNewsResponse]:
        """
        특정 뉴스 상세 조회 (복합 키 사용)
        
        Args:
            category: 뉴스 카테고리
            news_id: Finnhub 뉴스 ID
            
        Returns:
            FinancialNewsResponse: 뉴스 상세 정보
        """
        news = self.db.query(FinancialNews).filter(
            and_(
                FinancialNews.category == category,
                FinancialNews.news_id == news_id
            )
        ).first()
        
        if not news:
            return None
            
        return FinancialNewsResponse(
            category=news.category,
            news_id=news.news_id,
            published_at=news.published_at,
            headline=news.headline,
            source=news.source,
            image=news.image,
            related=news.related,
            summary=news.summary,
            url=news.url,
            fetched_at=news.fetched_at,
            short_headline=news.short_headline,
            short_summary=news.short_summary,
            related_symbols=news.related_symbols,
            has_image=news.has_image,
            category_display_name=news.category_display_name
        )

    def search_financial_news(
        self,
        query_text: str,
        skip: int = 0,
        limit: int = 20,
        categories: Optional[List[CategoryType]] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> FinancialNewsSearchResponse:
        """
        금융 뉴스 전문 검색
        
        검색 대상:
        - headline (헤드라인)
        - summary (요약)
        
        Args:
            query_text: 검색어
            skip: 페이징 offset
            limit: 페이징 limit
            categories: 검색 대상 카테고리
            start_date: 날짜 필터 시작
            end_date: 날짜 필터 종료
            
        Returns:
            FinancialNewsSearchResponse: 검색 결과
        """
        # 기본 검색 쿼리
        search_query = self.db.query(FinancialNews).filter(
            or_(
                # 헤드라인에서 검색 (대소문자 무시)
                FinancialNews.headline.ilike(f'%{query_text}%'),
                # 요약에서 검색
                FinancialNews.summary.ilike(f'%{query_text}%'),
                # 소스에서 검색
                FinancialNews.source.ilike(f'%{query_text}%')
            )
        ).order_by(FinancialNews.published_at.desc())
        
        # 카테고리 필터링
        if categories:
            search_query = search_query.filter(FinancialNews.category.in_(categories))
            
        # 날짜 필터링
        if start_date:
            search_query = search_query.filter(FinancialNews.published_at >= start_date)
        if end_date:
            search_query = search_query.filter(FinancialNews.published_at <= end_date)
        
        # 전체 개수 및 페이징
        total = search_query.count()
        items = search_query.offset(skip).limit(limit).all()
        
        # 응답 생성
        news_items = []
        for item in items:
            news_items.append(FinancialNewsResponse(
                category=item.category,
                news_id=item.news_id,
                published_at=item.published_at,
                headline=item.headline,
                source=item.source,
                image=item.image,
                related=item.related,
                summary=item.summary,
                url=item.url,
                fetched_at=item.fetched_at,
                short_headline=item.short_headline,
                short_summary=item.short_summary,
                related_symbols=item.related_symbols,
                has_image=item.has_image,
                category_display_name=item.category_display_name
            ))
        
        page = (skip // limit) + 1
        has_next = (skip + limit) < total
        
        return FinancialNewsSearchResponse(
            total=total,
            items=news_items,
            search_query=query_text,
            categories=categories,
            page=page,
            limit=limit,
            has_next=has_next
        )

    def get_category_news(
        self,
        category: CategoryType,
        skip: int = 0,
        limit: int = 20,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> FinancialNewsListResponse:
        """
        특정 카테고리 뉴스 조회
        
        Args:
            category: 조회할 카테고리
            skip: 페이징 offset
            limit: 페이징 limit
            start_date: 날짜 필터 시작
            end_date: 날짜 필터 종료
            
        Returns:
            FinancialNewsListResponse: 해당 카테고리 뉴스 목록
        """
        return self.get_financial_news_list(
            skip=skip,
            limit=limit,
            categories=[category],
            start_date=start_date,
            end_date=end_date
        )

    def get_recent_financial_news(
        self, 
        hours: int = 24, 
        limit: int = 10,
        categories: Optional[List[CategoryType]] = None
    ) -> List[FinancialNewsResponse]:
        """
        최근 금융 뉴스 조회
        
        Args:
            hours: 몇 시간 이내의 뉴스 (기본 24시간)
            limit: 최대 개수
            categories: 대상 카테고리 (None이면 전체)
            
        Returns:
            List[FinancialNewsResponse]: 최근 뉴스 목록
        """
        cutoff_time = datetime.utcnow() - timedelta(hours=hours)
        
        query = self.db.query(FinancialNews).filter(
            FinancialNews.published_at >= cutoff_time
        )
        
        if categories:
            query = query.filter(FinancialNews.category.in_(categories))
            
        items = query.order_by(
            FinancialNews.published_at.desc()
        ).limit(limit).all()
        
        return [
            FinancialNewsResponse(
                category=item.category,
                news_id=item.news_id,
                published_at=item.published_at,
                headline=item.headline,
                source=item.source,
                image=item.image,
                related=item.related,
                summary=item.summary,
                url=item.url,
                fetched_at=item.fetched_at,
                short_headline=item.short_headline,
                short_summary=item.short_summary,
                related_symbols=item.related_symbols,
                has_image=item.has_image,
                category_display_name=item.category_display_name
            )
            for item in items
        ]

    def get_categories_statistics(self) -> CategoriesStatsResponse:
        """
        카테고리별 통계 조회
        
        Returns:
            CategoriesStatsResponse: 전체 카테고리 통계
        """
        # 카테고리별 뉴스 개수 및 최신 뉴스 날짜 조회
        stats_query = self.db.query(
            FinancialNews.category,
            func.count(FinancialNews.news_id).label('count'),
            func.max(FinancialNews.published_at).label('latest_date')
        ).group_by(FinancialNews.category).all()
        
        # 카테고리 표시명 매핑
        category_display_names = {
            'crypto': '암호화폐',
            'forex': '외환',
            'merger': '인수합병',
            'general': '일반 금융'
        }
        
        # 통계 데이터 생성
        categories_stats = []
        total_news = 0
        
        for stat in stats_query:
            category_stat = CategoryStatsResponse(
                category=stat.category,
                category_display_name=category_display_names.get(stat.category, stat.category),
                count=stat.count,
                latest_news_date=stat.latest_date
            )
            categories_stats.append(category_stat)
            total_news += stat.count
        
        # 카테고리별 정렬 (뉴스 개수 기준 내림차순)
        categories_stats.sort(key=lambda x: x.count, reverse=True)
        
        return CategoriesStatsResponse(
            categories=categories_stats,
            total_news=total_news,
            available_categories=FinancialNews.get_valid_categories()
        )

    def get_news_by_symbol(
        self,
        symbol: str,
        skip: int = 0,
        limit: int = 20,
        categories: Optional[List[CategoryType]] = None
    ) -> FinancialNewsListResponse:
        """
        특정 종목 관련 뉴스 조회
        
        Args:
            symbol: 검색할 종목 코드 (예: BTCUSD, AAPL)
            skip: 페이징 offset
            limit: 페이징 limit
            categories: 검색 대상 카테고리
            
        Returns:
            FinancialNewsListResponse: 해당 종목 관련 뉴스 목록
        """
        return self.get_financial_news_list(
            skip=skip,
            limit=limit,
            categories=categories,
            symbols=[symbol]
        )

    def get_trending_symbols(self, days: int = 7, limit: int = 10) -> List[Tuple[str, int]]:
        """
        최근 N일간 가장 많이 언급된 종목 조회
        
        Args:
            days: 조회할 일수
            limit: 반환할 종목 수
            
        Returns:
            List[Tuple[str, int]]: (종목코드, 언급횟수) 튜플 리스트
        """
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        
        # related 필드에서 종목 추출 (복잡한 로직이므로 Python에서 처리)
        news_items = self.db.query(FinancialNews.related).filter(
            and_(
                FinancialNews.published_at >= cutoff_date,
                FinancialNews.related.isnot(None),
                FinancialNews.related != ''
            )
        ).all()
        
        # 종목별 언급 횟수 집계
        symbol_counts = {}
        for item in news_items:
            if item.related:
                symbols = [s.strip().upper() for s in item.related.split(',') if s.strip()]
                for symbol in symbols:
                    if len(symbol) >= 2:  # 유효한 종목 코드만
                        symbol_counts[symbol] = symbol_counts.get(symbol, 0) + 1
        
        # 언급 횟수 기준 내림차순 정렬
        sorted_symbols = sorted(symbol_counts.items(), key=lambda x: x[1], reverse=True)
        
        return sorted_symbols[:limit]

    def get_sources_statistics(self, categories: Optional[List[CategoryType]] = None) -> List[Tuple[str, int]]:
        """
        뉴스 소스별 통계 조회
        
        Args:
            categories: 대상 카테고리 (None이면 전체)
            
        Returns:
            List[Tuple[str, int]]: (소스명, 뉴스 개수) 튜플 리스트
        """
        query = self.db.query(
            FinancialNews.source,
            func.count(FinancialNews.news_id).label('count')
        ).filter(
            FinancialNews.source.isnot(None)
        )
        
        if categories:
            query = query.filter(FinancialNews.category.in_(categories))
            
        results = query.group_by(
            FinancialNews.source
        ).order_by(
            func.count(FinancialNews.news_id).desc()
        ).all()
        
        return [(result.source, result.count) for result in results]

    def get_daily_news_count_by_category(
        self, 
        days: int = 30,
        category: Optional[CategoryType] = None
    ) -> List[Tuple[str, str, int]]:
        """
        일별 카테고리별 뉴스 발행 통계
        
        Args:
            days: 조회할 일수
            category: 특정 카테고리 (None이면 전체 카테고리)
            
        Returns:
            List[Tuple[str, str, int]]: (날짜, 카테고리, 뉴스개수) 튜플 리스트
        """
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        
        query = self.db.query(
            func.date(FinancialNews.published_at).label('news_date'),
            FinancialNews.category,
            func.count(FinancialNews.news_id).label('count')
        ).filter(
            FinancialNews.published_at >= cutoff_date
        )
        
        if category:
            query = query.filter(FinancialNews.category == category)
            
        results = query.group_by(
            func.date(FinancialNews.published_at),
            FinancialNews.category
        ).order_by(
            func.date(FinancialNews.published_at).desc(),
            FinancialNews.category
        ).all()
        
        return [(str(result.news_date), result.category, result.count) for result in results]