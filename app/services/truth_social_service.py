# app/services/truth_social_service.py

from datetime import datetime, date, timedelta
from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import desc, asc, and_, or_, func, text
from app.models.truth_social_model import TruthSocialPost, TruthSocialTag, TruthSocialTrend
from app.schemas.truth_social_schema import (
    TruthSocialPostFilter, TruthSocialTagFilter, TruthSocialTrendFilter
)


class TruthSocialPostService:
    """Truth Social 포스트 서비스"""
    
    def __init__(self, db: Session):
        self.db = db

    def get_posts(
        self, 
        skip: int = 0, 
        limit: int = 100,
        filters: Optional[TruthSocialPostFilter] = None
    ) -> List[TruthSocialPost]:
        """포스트 목록 조회 (필터링 + 페이징)"""
        query = self.db.query(TruthSocialPost)
        
        # 필터 적용
        if filters:
            if filters.username:
                query = query.filter(TruthSocialPost.username == filters.username)
            if filters.account_type:
                query = query.filter(TruthSocialPost.account_type == filters.account_type)
            if filters.has_media is not None:
                query = query.filter(TruthSocialPost.has_media == filters.has_media)
            if filters.min_market_influence is not None:
                query = query.filter(TruthSocialPost.market_influence >= filters.min_market_influence)
            if filters.language:
                query = query.filter(TruthSocialPost.language == filters.language)
            if filters.start_date:
                query = query.filter(TruthSocialPost.created_at >= filters.start_date)
            if filters.end_date:
                query = query.filter(TruthSocialPost.created_at <= filters.end_date)
        
        return query.order_by(desc(TruthSocialPost.created_at)).offset(skip).limit(limit).all()

    def get_posts_count(self, filters: Optional[TruthSocialPostFilter] = None) -> int:
        """필터링된 포스트 총 개수"""
        query = self.db.query(func.count(TruthSocialPost.id))
        
        # 동일한 필터 적용
        if filters:
            if filters.username:
                query = query.filter(TruthSocialPost.username == filters.username)
            if filters.account_type:
                query = query.filter(TruthSocialPost.account_type == filters.account_type)
            if filters.has_media is not None:
                query = query.filter(TruthSocialPost.has_media == filters.has_media)
            if filters.min_market_influence is not None:
                query = query.filter(TruthSocialPost.market_influence >= filters.min_market_influence)
            if filters.language:
                query = query.filter(TruthSocialPost.language == filters.language)
            if filters.start_date:
                query = query.filter(TruthSocialPost.created_at >= filters.start_date)
            if filters.end_date:
                query = query.filter(TruthSocialPost.created_at <= filters.end_date)
        
        return query.scalar()

    def get_trump_posts(self, skip: int = 0, limit: int = 50) -> List[TruthSocialPost]:
        """트럼프 포스트만 조회 (인덱스 활용)"""
        return self.db.query(TruthSocialPost)\
            .filter(TruthSocialPost.username == 'realDonaldTrump')\
            .order_by(desc(TruthSocialPost.created_at))\
            .offset(skip)\
            .limit(limit)\
            .all()

    def get_government_posts(self, skip: int = 0, limit: int = 50) -> List[TruthSocialPost]:
        """정부 계정 포스트만 조회 (인덱스 활용)"""
        return self.db.query(TruthSocialPost)\
            .filter(TruthSocialPost.account_type == 'government')\
            .order_by(desc(TruthSocialPost.created_at))\
            .offset(skip)\
            .limit(limit)\
            .all()

    def get_high_influence_posts(self, skip: int = 0, limit: int = 50) -> List[TruthSocialPost]:
        """높은 시장 영향도 포스트 조회 (인덱스 활용)"""
        return self.db.query(TruthSocialPost)\
            .filter(TruthSocialPost.market_influence > 0)\
            .order_by(desc(TruthSocialPost.market_influence))\
            .offset(skip)\
            .limit(limit)\
            .all()

    def get_posts_with_media(self, skip: int = 0, limit: int = 50) -> List[TruthSocialPost]:
        """미디어가 포함된 포스트 조회 (인덱스 활용)"""
        return self.db.query(TruthSocialPost)\
            .filter(TruthSocialPost.has_media == True)\
            .order_by(desc(TruthSocialPost.created_at))\
            .offset(skip)\
            .limit(limit)\
            .all()

    def search_posts_by_content(self, search_term: str, skip: int = 0, limit: int = 50) -> List[TruthSocialPost]:
        """포스트 내용 전문 검색 (GIN 인덱스 활용)"""
        return self.db.query(TruthSocialPost)\
            .filter(func.to_tsvector('english', TruthSocialPost.clean_content).match(search_term))\
            .order_by(desc(TruthSocialPost.created_at))\
            .offset(skip)\
            .limit(limit)\
            .all()

    def get_post_by_id(self, post_id: str) -> Optional[TruthSocialPost]:
        """ID로 특정 포스트 조회"""
        return self.db.query(TruthSocialPost).filter(TruthSocialPost.id == post_id).first()


class TruthSocialTagService:
    """Truth Social 태그 서비스"""
    
    def __init__(self, db: Session):
        self.db = db

    def get_tags(
        self, 
        skip: int = 0, 
        limit: int = 100,
        filters: Optional[TruthSocialTagFilter] = None
    ) -> List[TruthSocialTag]:
        """태그 목록 조회 (필터링 + 페이징)"""
        query = self.db.query(TruthSocialTag)
        
        # 필터 적용
        if filters:
            if filters.tag_category:
                query = query.filter(TruthSocialTag.tag_category == filters.tag_category)
            if filters.min_total_uses is not None:
                query = query.filter(TruthSocialTag.total_uses >= filters.min_total_uses)
            if filters.min_market_relevance is not None:
                query = query.filter(TruthSocialTag.market_relevance >= filters.min_market_relevance)
            if filters.start_date:
                query = query.filter(TruthSocialTag.collected_date >= filters.start_date)
            if filters.end_date:
                query = query.filter(TruthSocialTag.collected_date <= filters.end_date)
        
        return query.order_by(desc(TruthSocialTag.collected_date), desc(TruthSocialTag.total_uses)).offset(skip).limit(limit).all()

    def get_tags_count(self, filters: Optional[TruthSocialTagFilter] = None) -> int:
        """필터링된 태그 총 개수"""
        query = self.db.query(func.count()).select_from(TruthSocialTag)
        
        # 동일한 필터 적용
        if filters:
            if filters.tag_category:
                query = query.filter(TruthSocialTag.tag_category == filters.tag_category)
            if filters.min_total_uses is not None:
                query = query.filter(TruthSocialTag.total_uses >= filters.min_total_uses)
            if filters.min_market_relevance is not None:
                query = query.filter(TruthSocialTag.market_relevance >= filters.min_market_relevance)
            if filters.start_date:
                query = query.filter(TruthSocialTag.collected_date >= filters.start_date)
            if filters.end_date:
                query = query.filter(TruthSocialTag.collected_date <= filters.end_date)
        
        return query.scalar()

    def get_trending_tags(self, skip: int = 0, limit: int = 50) -> List[TruthSocialTag]:
        """트렌드 점수 높은 태그 조회 (인덱스 활용)"""
        return self.db.query(TruthSocialTag)\
            .filter(TruthSocialTag.trend_score.isnot(None))\
            .order_by(desc(TruthSocialTag.trend_score))\
            .offset(skip)\
            .limit(limit)\
            .all()

    def get_growing_tags(self, skip: int = 0, limit: int = 50) -> List[TruthSocialTag]:
        """증가율 높은 태그 조회 (인덱스 활용)"""
        return self.db.query(TruthSocialTag)\
            .filter(TruthSocialTag.growth_rate.isnot(None))\
            .order_by(desc(TruthSocialTag.growth_rate))\
            .offset(skip)\
            .limit(limit)\
            .all()

    def get_market_relevant_tags(self, skip: int = 0, limit: int = 50) -> List[TruthSocialTag]:
        """시장 관련성 높은 태그 조회 (인덱스 활용)"""
        return self.db.query(TruthSocialTag)\
            .filter(TruthSocialTag.market_relevance > 0)\
            .order_by(desc(TruthSocialTag.market_relevance))\
            .offset(skip)\
            .limit(limit)\
            .all()

    def get_tag_by_name_and_date(self, name: str, collected_date: date) -> Optional[TruthSocialTag]:
        """이름과 날짜로 특정 태그 조회"""
        return self.db.query(TruthSocialTag)\
            .filter(and_(TruthSocialTag.name == name, TruthSocialTag.collected_date == collected_date))\
            .first()

    def get_latest_tags_by_name(self, name: str, limit: int = 10) -> List[TruthSocialTag]:
        """특정 태그의 최근 추이 조회"""
        return self.db.query(TruthSocialTag)\
            .filter(TruthSocialTag.name == name)\
            .order_by(desc(TruthSocialTag.collected_date))\
            .limit(limit)\
            .all()


class TruthSocialTrendService:
    """Truth Social 트렌드 서비스"""
    
    def __init__(self, db: Session):
        self.db = db

    def get_trends(
        self, 
        skip: int = 0, 
        limit: int = 100,
        filters: Optional[TruthSocialTrendFilter] = None
    ) -> List[TruthSocialTrend]:
        """트렌드 목록 조회 (필터링 + 페이징)"""
        query = self.db.query(TruthSocialTrend)
        
        # 필터 적용
        if filters:
            if filters.username:
                query = query.filter(TruthSocialTrend.username == filters.username)
            if filters.min_trend_score is not None:
                query = query.filter(TruthSocialTrend.trend_score >= filters.min_trend_score)
            if filters.max_rank is not None:
                query = query.filter(TruthSocialTrend.trend_rank <= filters.max_rank)
            if filters.start_date:
                query = query.filter(TruthSocialTrend.created_at >= filters.start_date)
            if filters.end_date:
                query = query.filter(TruthSocialTrend.created_at <= filters.end_date)
        
        return query.order_by(asc(TruthSocialTrend.trend_rank)).offset(skip).limit(limit).all()

    def get_trends_count(self, filters: Optional[TruthSocialTrendFilter] = None) -> int:
        """필터링된 트렌드 총 개수"""
        query = self.db.query(func.count(TruthSocialTrend.id))
        
        # 동일한 필터 적용
        if filters:
            if filters.username:
                query = query.filter(TruthSocialTrend.username == filters.username)
            if filters.min_trend_score is not None:
                query = query.filter(TruthSocialTrend.trend_score >= filters.min_trend_score)
            if filters.max_rank is not None:
                query = query.filter(TruthSocialTrend.trend_rank <= filters.max_rank)
            if filters.start_date:
                query = query.filter(TruthSocialTrend.created_at >= filters.start_date)
            if filters.end_date:
                query = query.filter(TruthSocialTrend.created_at <= filters.end_date)
        
        return query.scalar()

    def get_top_trends(self, skip: int = 0, limit: int = 50) -> List[TruthSocialTrend]:
        """상위 트렌드 조회 (순위 기준, 인덱스 활용)"""
        return self.db.query(TruthSocialTrend)\
            .filter(TruthSocialTrend.trend_rank.isnot(None))\
            .order_by(asc(TruthSocialTrend.trend_rank))\
            .offset(skip)\
            .limit(limit)\
            .all()

    def get_high_score_trends(self, skip: int = 0, limit: int = 50) -> List[TruthSocialTrend]:
        """높은 트렌드 점수 조회 (인덱스 활용)"""
        return self.db.query(TruthSocialTrend)\
            .filter(TruthSocialTrend.trend_score.isnot(None))\
            .order_by(desc(TruthSocialTrend.trend_score))\
            .offset(skip)\
            .limit(limit)\
            .all()

    def get_recent_trends(self, skip: int = 0, limit: int = 50) -> List[TruthSocialTrend]:
        """최근 트렌드 조회"""
        return self.db.query(TruthSocialTrend)\
            .order_by(desc(TruthSocialTrend.created_at))\
            .offset(skip)\
            .limit(limit)\
            .all()

    def search_trends_by_content(self, search_term: str, skip: int = 0, limit: int = 50) -> List[TruthSocialTrend]:
        """트렌드 내용 전문 검색 (GIN 인덱스 활용)"""
        return self.db.query(TruthSocialTrend)\
            .filter(func.to_tsvector('english', TruthSocialTrend.clean_content).match(search_term))\
            .order_by(desc(TruthSocialTrend.trend_score))\
            .offset(skip)\
            .limit(limit)\
            .all()

    def get_trend_by_id(self, trend_id: str) -> Optional[TruthSocialTrend]:
        """ID로 특정 트렌드 조회"""
        return self.db.query(TruthSocialTrend).filter(TruthSocialTrend.id == trend_id).first()

    def get_trends_by_username(self, username: str, skip: int = 0, limit: int = 50) -> List[TruthSocialTrend]:
        """특정 사용자의 트렌드 조회"""
        return self.db.query(TruthSocialTrend)\
            .filter(TruthSocialTrend.username == username)\
            .order_by(desc(TruthSocialTrend.trend_score))\
            .offset(skip)\
            .limit(limit)\
            .all()


# ==================== 통합 분석 서비스 ====================

class TruthSocialAnalyticsService:
    """Truth Social 통합 분석 서비스"""
    
    def __init__(self, db: Session):
        self.db = db

    def get_user_statistics(self, username: str) -> Dict[str, Any]:
        """특정 사용자의 통계 정보"""
        # 포스트 통계
        total_posts = self.db.query(func.count(TruthSocialPost.id))\
            .filter(TruthSocialPost.username == username).scalar()
        
        avg_engagement = self.db.query(
            func.avg(TruthSocialPost.favourites_count + TruthSocialPost.reblogs_count)
        ).filter(TruthSocialPost.username == username).scalar()
        
        # 트렌드 진입 횟수
        trend_count = self.db.query(func.count(TruthSocialTrend.id))\
            .filter(TruthSocialTrend.username == username).scalar()
        
        return {
            "username": username,
            "total_posts": total_posts or 0,
            "average_engagement": float(avg_engagement or 0),
            "trend_appearances": trend_count or 0,
            "engagement_rate": float(avg_engagement or 0) / max(total_posts or 1, 1)
        }

    def get_daily_post_counts(self, days: int = 7) -> List[Dict[str, Any]]:
        """일별 포스트 수 통계"""
        start_date = datetime.now() - timedelta(days=days)
        
        results = self.db.query(
            func.date(TruthSocialPost.created_at).label('date'),
            func.count(TruthSocialPost.id).label('count')
        ).filter(TruthSocialPost.created_at >= start_date)\
         .group_by(func.date(TruthSocialPost.created_at))\
         .order_by(func.date(TruthSocialPost.created_at))\
         .all()
        
        return [{"date": str(result.date), "count": result.count} for result in results]

    def get_top_hashtags(self, limit: int = 20) -> List[Dict[str, Any]]:
        """최근 인기 해시태그"""
        recent_date = date.today() - timedelta(days=7)
        
        results = self.db.query(TruthSocialTag)\
            .filter(TruthSocialTag.collected_date >= recent_date)\
            .order_by(desc(TruthSocialTag.total_uses))\
            .limit(limit)\
            .all()
        
        return [
            {
                "name": tag.name,
                "total_uses": tag.total_uses,
                "growth_rate": float(tag.growth_rate or 0),
                "trend_score": float(tag.trend_score or 0)
            }
            for tag in results
        ]

    def get_market_sentiment_summary(self) -> Dict[str, Any]:
        """시장 관련 감성 요약"""
        # 높은 시장 영향도 포스트 수
        high_influence_count = self.db.query(func.count(TruthSocialPost.id))\
            .filter(TruthSocialPost.market_influence > 0).scalar()
        
        # 시장 관련 태그 수
        market_tags_count = self.db.query(func.count())\
            .select_from(TruthSocialTag)\
            .filter(TruthSocialTag.market_relevance > 0).scalar()
        
        # 평균 시장 영향도
        avg_influence = self.db.query(func.avg(TruthSocialPost.market_influence))\
            .filter(TruthSocialPost.market_influence > 0).scalar()
        
        return {
            "high_influence_posts": high_influence_count or 0,
            "market_relevant_tags": market_tags_count or 0,
            "average_market_influence": float(avg_influence or 0),
            "last_updated": datetime.now().isoformat()
        }