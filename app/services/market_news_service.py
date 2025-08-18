from datetime import datetime, timedelta
from typing import List, Optional, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import func, text, and_, or_
from sqlalchemy.dialects.postgresql import TSVECTOR

from app.models.market_news_model import MarketNews
from app.schemas.market_news_schema import (
    MarketNewsResponse, 
    MarketNewsListItem, 
    MarketNewsListResponse,
    MarketNewsSearchResponse
)


class MarketNewsService:
    """
    시장 뉴스 비즈니스 로직 서비스
    
    주요 기능:
    1. 기본 CRUD 작업
    2. 페이징 처리
    3. 필터링 (날짜, 소스, 키워드)
    4. 검색 (PostgreSQL 전문 검색)
    5. 통계 및 분석
    """

    def __init__(self, db: Session):
        self.db = db

    def get_news_list(
        self,
        skip: int = 0,
        limit: int = 20,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        sources: Optional[List[str]] = None,
        exclude_sources: Optional[List[str]] = None
    ) -> MarketNewsListResponse:
        """
        뉴스 목록 조회 (페이징 + 필터링)
        
        Args:
            skip: 건너뛸 항목 수 (페이징용)
            limit: 한 페이지당 항목 수
            start_date: 시작 날짜 필터
            end_date: 종료 날짜 필터
            sources: 포함할 소스 목록
            exclude_sources: 제외할 소스 목록
            
        Returns:
            MarketNewsListResponse: 페이징된 뉴스 목록
        """
        # 기본 쿼리 (최신순 정렬)
        query = self.db.query(MarketNews).order_by(MarketNews.published_at.desc())
        
        # 날짜 필터링
        if start_date:
            query = query.filter(MarketNews.published_at >= start_date)
        if end_date:
            query = query.filter(MarketNews.published_at <= end_date)
            
        # 소스 필터링
        if sources:
            query = query.filter(MarketNews.source.in_(sources))
        if exclude_sources:
            query = query.filter(~MarketNews.source.in_(exclude_sources))
        
        # 전체 개수 조회 (페이징 정보용)
        total = query.count()
        
        # 페이징 적용
        items_query = query.offset(skip).limit(limit)
        items = items_query.all()
        
        # 응답 데이터 생성
        news_items = []
        for item in items:
            # 모델의 property 메서드 활용
            news_item = MarketNewsListItem(
                source=item.source,
                url=item.url,
                author=item.author,
                title=item.title,
                description=item.description,
                published_at=item.published_at,
                fetched_at=item.fetched_at,
                short_description=item.short_description  # 모델의 property 사용
            )
            news_items.append(news_item)
        
        # 페이징 정보 계산
        page = (skip // limit) + 1
        has_next = (skip + limit) < total
        
        return MarketNewsListResponse(
            total=total,
            items=news_items,
            page=page,
            limit=limit,
            has_next=has_next
        )

    def get_news_by_url(self, source: str, url: str) -> Optional[MarketNewsResponse]:
        """
        특정 뉴스 상세 조회 (복합 키 사용)
        
        Args:
            source: 뉴스 소스
            url: 뉴스 URL
            
        Returns:
            MarketNewsResponse: 뉴스 상세 정보 (content 포함)
        """
        news = self.db.query(MarketNews).filter(
            and_(
                MarketNews.source == source,
                MarketNews.url == url
            )
        ).first()
        
        if not news:
            return None
            
        return MarketNewsResponse(
            source=news.source,
            url=news.url,
            author=news.author,
            title=news.title,
            description=news.description,
            content=news.content,
            published_at=news.published_at,
            fetched_at=news.fetched_at,
            short_description=news.short_description,
            content_preview=news.content_preview
        )

    def search_news(
        self,
        query_text: str,
        skip: int = 0,
        limit: int = 20,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> MarketNewsSearchResponse:
        """
        뉴스 전문 검색 (PostgreSQL Full-Text Search 활용)
        
        검색 대상:
        - title (제목)
        - description (설명)
        - content (본문)
        
        Args:
            query_text: 검색어
            skip: 페이징 offset
            limit: 페이징 limit
            start_date: 날짜 필터 시작
            end_date: 날짜 필터 종료
            
        Returns:
            MarketNewsSearchResponse: 검색 결과
        """
        # PostgreSQL Full-Text Search 쿼리
        # to_tsvector: 텍스트를 검색 가능한 형태로 변환
        # plainto_tsquery: 일반 텍스트를 검색 쿼리로 변환
        search_query = self.db.query(MarketNews).filter(
            or_(
                # 제목에서 검색 (인덱스 활용)
                func.to_tsvector('english', MarketNews.title).match(
                    func.plainto_tsquery('english', query_text)
                ),
                # 설명에서 검색
                func.to_tsvector('english', MarketNews.description).match(
                    func.plainto_tsquery('english', query_text)
                ),
                # 본문에서 검색
                func.to_tsvector('english', MarketNews.content).match(
                    func.plainto_tsquery('english', query_text)
                )
            )
        ).order_by(MarketNews.published_at.desc())
        
        # 날짜 필터링
        if start_date:
            search_query = search_query.filter(MarketNews.published_at >= start_date)
        if end_date:
            search_query = search_query.filter(MarketNews.published_at <= end_date)
        
        # 전체 개수 및 페이징
        total = search_query.count()
        items = search_query.offset(skip).limit(limit).all()
        
        # 응답 생성
        news_items = []
        for item in items:
            news_items.append(MarketNewsResponse(
                source=item.source,
                url=item.url,
                author=item.author,
                title=item.title,
                description=item.description,
                content=item.content,
                published_at=item.published_at,
                fetched_at=item.fetched_at,
                short_description=item.short_description,
                content_preview=item.content_preview
            ))
        
        page = (skip // limit) + 1
        has_next = (skip + limit) < total
        
        return MarketNewsSearchResponse(
            total=total,
            items=news_items,
            search_query=query_text,
            page=page,
            limit=limit,
            has_next=has_next
        )

    def get_recent_news(self, hours: int = 24, limit: int = 10) -> List[MarketNewsResponse]:
        """
        최근 뉴스 조회
        
        Args:
            hours: 몇 시간 이내의 뉴스 (기본 24시간)
            limit: 최대 개수
            
        Returns:
            List[MarketNewsResponse]: 최근 뉴스 목록
        """
        cutoff_time = datetime.utcnow() - timedelta(hours=hours)
        
        items = self.db.query(MarketNews).filter(
            MarketNews.published_at >= cutoff_time
        ).order_by(
            MarketNews.published_at.desc()
        ).limit(limit).all()
        
        return [
            MarketNewsResponse(
                source=item.source,
                url=item.url,
                author=item.author,
                title=item.title,
                description=item.description,
                content=item.content,
                published_at=item.published_at,
                fetched_at=item.fetched_at,
                short_description=item.short_description,
                content_preview=item.content_preview
            )
            for item in items
        ]

    def get_news_sources(self) -> List[Tuple[str, int]]:
        """
        뉴스 소스별 통계 조회
        
        Returns:
            List[Tuple[str, int]]: (소스명, 뉴스 개수) 튜플 리스트
        """
        results = self.db.query(
            MarketNews.source,
            func.count(MarketNews.source).label('count')
        ).group_by(
            MarketNews.source
        ).order_by(
            func.count(MarketNews.source).desc()
        ).all()
        
        return [(result.source, result.count) for result in results]

    def get_daily_news_count(self, days: int = 30) -> List[Tuple[str, int]]:
        """
        일별 뉴스 발행 통계
        
        Args:
            days: 조회할 일수
            
        Returns:
            List[Tuple[str, int]]: (날짜, 뉴스 개수) 튜플 리스트
        """
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        
        results = self.db.query(
            func.date(MarketNews.published_at).label('news_date'),
            func.count(MarketNews.source).label('count')
        ).filter(
            MarketNews.published_at >= cutoff_date
        ).group_by(
            func.date(MarketNews.published_at)
        ).order_by(
            func.date(MarketNews.published_at).desc()
        ).all()
        
        return [(str(result.news_date), result.count) for result in results]