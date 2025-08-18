# app/services/x_posts_service.py

from sqlalchemy.orm import Session
from sqlalchemy import func, desc, asc, and_, or_, text
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, timedelta, timezone

from app.models.x_posts_model import XPost
from app.schemas.x_posts_schema import (
    AccountCategory, SortBy, SortOrder,
    XPostListRequest, XPostSearchRequest, XPostRankingRequest, UserPostsRequest
)


class XPostsService:
    """
    X Posts 비즈니스 로직 서비스
    
    주요 기능:
    - 포스트 목록 조회 (페이징, 필터링, 정렬)
    - 랭킹 시스템 (인게이지먼트별)
    - 사용자별 포스트 분석
    - 카테고리별 통계
    - 검색 기능
    """
    
    def __init__(self, db: Session):
        self.db = db
    
    def _get_period_filter(self, period: str) -> datetime:
        """
        기간 필터를 datetime으로 변환
        
        Args:
            period: '1h', '6h', '24h', '7d', '30d' 형태의 기간
            
        Returns:
            datetime: 해당 기간의 시작 시점
        """
        now = datetime.now(timezone.utc)
        period_map = {
            '1h': now - timedelta(hours=1),
            '6h': now - timedelta(hours=6), 
            '24h': now - timedelta(hours=24),
            '7d': now - timedelta(days=7),
            '30d': now - timedelta(days=30)
        }
        return period_map.get(period, now - timedelta(hours=24))
    
    def _apply_sorting(self, query, sort_by: SortBy, order: SortOrder):
        """
        쿼리에 정렬 조건 적용
        
        Args:
            query: SQLAlchemy 쿼리 객체
            sort_by: 정렬 기준
            order: 정렬 순서
            
        Returns:
            정렬이 적용된 쿼리 객체
        """
        # 정렬 컬럼 매핑
        sort_columns = {
            SortBy.CREATED_AT: XPost.created_at,
            SortBy.LIKE_COUNT: XPost.like_count,
            SortBy.RETWEET_COUNT: XPost.retweet_count,
            SortBy.REPLY_COUNT: XPost.reply_count,
            SortBy.QUOTE_COUNT: XPost.quote_count,
            SortBy.BOOKMARK_COUNT: XPost.bookmark_count,
            SortBy.IMPRESSION_COUNT: XPost.impression_count,
        }
        
        column = sort_columns.get(sort_by, XPost.created_at)
        
        # 종합 인게이지먼트 점수는 계산 필요
        if sort_by == SortBy.ENGAGEMENT_SCORE:
            engagement_expr = (
                func.coalesce(XPost.like_count, 0) * 1 +
                func.coalesce(XPost.retweet_count, 0) * 2 +
                func.coalesce(XPost.reply_count, 0) * 3 +
                func.coalesce(XPost.quote_count, 0) * 2 +
                func.coalesce(XPost.bookmark_count, 0) * 1
            )
            column = engagement_expr
        
        if order == SortOrder.DESC:
            return query.order_by(desc(column))
        else:
            return query.order_by(asc(column))
    
    def get_posts_list(self, request: XPostListRequest) -> Tuple[List[XPost], int]:
        """
        포스트 목록 조회 (페이징, 필터링, 정렬)
        
        Args:
            request: 목록 조회 요청 파라미터
            
        Returns:
            Tuple[List[XPost], int]: (포스트 목록, 전체 개수)
        """
        query = self.db.query(XPost)
        
        # 카테고리 필터
        if request.category:
            query = query.filter(XPost.account_category == request.category.value)
        
        # 인증된 사용자만 필터
        if request.verified_only:
            query = query.filter(XPost.user_verified == True)
        
        # 전체 개수 조회 (필터 적용 후)
        total_count = query.count()
        
        # 정렬 적용
        query = self._apply_sorting(query, request.sort_by, request.order)
        
        # 페이징 적용
        posts = query.offset(request.offset).limit(request.limit).all()
        
        return posts, total_count
    
    def get_recent_posts(self, limit: int = 10, category: Optional[AccountCategory] = None) -> List[XPost]:
        """
        최신 포스트 조회
        
        Args:
            limit: 조회할 포스트 수
            category: 카테고리 필터
            
        Returns:
            List[XPost]: 최신 포스트 목록
        """
        query = self.db.query(XPost)
        
        if category:
            query = query.filter(XPost.account_category == category.value)
        
        return query.order_by(desc(XPost.created_at)).limit(limit).all()
    
    def get_posts_by_category(self, category: AccountCategory, limit: int = 20, offset: int = 0) -> Tuple[List[XPost], int]:
        """
        카테고리별 포스트 조회
        
        Args:
            category: 계정 카테고리
            limit: 조회할 포스트 수
            offset: 건너뛸 포스트 수
            
        Returns:
            Tuple[List[XPost], int]: (포스트 목록, 전체 개수)
        """
        query = self.db.query(XPost).filter(XPost.account_category == category.value)
        
        total_count = query.count()
        posts = query.order_by(desc(XPost.created_at)).offset(offset).limit(limit).all()
        
        return posts, total_count
    
    def get_user_posts(self, username: str, request: UserPostsRequest) -> Tuple[List[XPost], int]:
        """
        특정 사용자의 포스트 조회
        
        Args:
            username: 사용자명
            request: 사용자 포스트 조회 요청
            
        Returns:
            Tuple[List[XPost], int]: (포스트 목록, 전체 개수)
        """
        query = self.db.query(XPost).filter(XPost.username == username)
        
        total_count = query.count()
        
        # 정렬 적용
        query = self._apply_sorting(query, request.sort_by, request.order)
        
        posts = query.offset(request.offset).limit(request.limit).all()
        
        return posts, total_count
    
    def get_user_recent_posts(self, username: str, limit: int = 10) -> List[XPost]:
        """
        특정 사용자의 최신 포스트 조회
        
        Args:
            username: 사용자명
            limit: 조회할 포스트 수
            
        Returns:
            List[XPost]: 최신 포스트 목록
        """
        return self.db.query(XPost)\
            .filter(XPost.username == username)\
            .order_by(desc(XPost.created_at))\
            .limit(limit)\
            .all()
    
    def get_category_users(self, category: AccountCategory) -> List[Dict[str, Any]]:
        """
        카테고리별 사용자 목록 및 통계
        
        Args:
            category: 계정 카테고리
            
        Returns:
            List[Dict]: 사용자별 통계 정보
        """
        query = self.db.query(
            XPost.username,
            XPost.display_name,
            XPost.account_category,
            XPost.user_verified,
            XPost.user_followers_count,
            func.count(XPost.tweet_id).label('total_posts'),
            func.avg(
                func.coalesce(XPost.like_count, 0) + 
                func.coalesce(XPost.retweet_count, 0) + 
                func.coalesce(XPost.reply_count, 0)
            ).label('avg_engagement')
        ).filter(XPost.account_category == category.value)\
         .group_by(
             XPost.username, 
             XPost.display_name, 
             XPost.account_category,
             XPost.user_verified,
             XPost.user_followers_count
         )\
         .order_by(desc('avg_engagement'))
        
        return [
            {
                'username': row.username,
                'display_name': row.display_name,
                'account_category': row.account_category,
                'verified': row.user_verified,
                'followers_count': row.user_followers_count,
                'total_posts': row.total_posts,
                'avg_engagement': float(row.avg_engagement) if row.avg_engagement else 0.0
            }
            for row in query.all()
        ]
    
    def get_ranking_posts(self, ranking_type: str, request: XPostRankingRequest) -> List[XPost]:
        """
        인게이지먼트별 랭킹 포스트 조회
        
        Args:
            ranking_type: 랭킹 유형 ('most-liked', 'most-retweeted', etc.)
            request: 랭킹 조회 요청
            
        Returns:
            List[XPost]: 랭킹 포스트 목록
        """
        query = self.db.query(XPost)
        
        # 카테고리 필터
        if request.category:
            query = query.filter(XPost.account_category == request.category.value)
        
        # 기간 필터
        since_date = self._get_period_filter(request.period)
        query = query.filter(XPost.created_at >= since_date)
        
        # 랭킹 유형별 정렬
        if ranking_type == 'most-engaged':
            # 종합 인게이지먼트: 간단한 더하기만 사용
            engagement_expr = (
                XPost.like_count + 
                XPost.retweet_count + 
                XPost.reply_count + 
                XPost.quote_count + 
                XPost.bookmark_count
            )
            query = query.order_by(desc(engagement_expr))
        else:
            ranking_columns = {
                'most-liked': XPost.like_count,
                'most-retweeted': XPost.retweet_count,
                'most-replied': XPost.reply_count,
                'most-quoted': XPost.quote_count,
                'most-bookmarked': XPost.bookmark_count,
                'most-viewed': XPost.impression_count,
            }
            
            sort_column = ranking_columns.get(ranking_type, XPost.like_count)
            query = query.order_by(desc(sort_column))

        results = query.limit(request.limit).all()

        # 가끔 최근 기간(1h/6h/24h)에 데이터가 없을 수 있으므로 보조 기간으로 자동 완화
        if not results and request.period in {'1h', '6h', '24h'}:
            fallback_since = datetime.now(timezone.utc) - timedelta(days=7)
            fallback_query = self.db.query(XPost)
            if request.category:
                fallback_query = fallback_query.filter(XPost.account_category == request.category.value)
            fallback_query = fallback_query.filter(XPost.created_at >= fallback_since)
            if ranking_type == 'most-engaged':
                engagement_expr = (
                    XPost.like_count +
                    XPost.retweet_count +
                    XPost.reply_count +
                    XPost.quote_count +
                    XPost.bookmark_count
                )
                fallback_query = fallback_query.order_by(desc(engagement_expr))
            else:
                ranking_columns = {
                    'most-liked': XPost.like_count,
                    'most-retweeted': XPost.retweet_count,
                    'most-replied': XPost.reply_count,
                    'most-quoted': XPost.quote_count,
                    'most-bookmarked': XPost.bookmark_count,
                    'most-viewed': XPost.impression_count,
                }
                sort_column = ranking_columns.get(ranking_type, XPost.like_count)
                fallback_query = fallback_query.order_by(desc(sort_column))
            results = fallback_query.limit(request.limit).all()

        return results
    
    def search_posts(self, request: XPostSearchRequest) -> Tuple[List[XPost], int]:
        """
        포스트 텍스트 검색
        
        Args:
            request: 검색 요청 파라미터
            
        Returns:
            Tuple[List[XPost], int]: (검색 결과, 전체 개수)
        """
        # PostgreSQL의 전문 검색 사용
        search_query = self.db.query(XPost)\
            .filter(XPost.text.ilike(f"%{request.q}%"))
        
        # 카테고리 필터
        if request.category:
            search_query = search_query.filter(XPost.account_category == request.category.value)
        
        total_count = search_query.count()
        
        # 정렬 적용
        search_query = self._apply_sorting(search_query, request.sort_by, request.order)
        
        posts = search_query.limit(request.limit).all()
        
        return posts, total_count
    
    def search_mentions(self, username: str, limit: int = 20) -> List[XPost]:
        """
        특정 사용자가 멘션된 포스트 검색
        
        Args:
            username: 멘션 대상 사용자명
            limit: 조회할 포스트 수
            
        Returns:
            List[XPost]: 멘션된 포스트 목록
        """
        # JSONB에서 mentions 배열 내의 username 검색
        return self.db.query(XPost)\
            .filter(XPost.mentions.op('?')(f'[*].username ? "{username}"'))\
            .order_by(desc(XPost.created_at))\
            .limit(limit)\
            .all()
    
    def get_user_stats(self, username: str) -> Dict[str, Any]:
        """
        특정 사용자의 상세 통계
        
        Args:
            username: 사용자명
            
        Returns:
            Dict: 사용자 통계 정보
        """
        # 기본 사용자 정보
        user_info = self.db.query(XPost)\
            .filter(XPost.username == username)\
            .first()
        
        if not user_info:
            return {}
        
        # 통계 계산
        stats_query = self.db.query(
            func.count(XPost.tweet_id).label('total_posts'),
            func.sum(XPost.like_count).label('total_likes'),
            func.sum(XPost.retweet_count).label('total_retweets'),
            func.sum(XPost.reply_count).label('total_replies'),
            func.avg(
                func.coalesce(XPost.like_count, 0) + 
                func.coalesce(XPost.retweet_count, 0) + 
                func.coalesce(XPost.reply_count, 0)
            ).label('avg_engagement')
        ).filter(XPost.username == username).first()
        
        # 가장 좋아요가 많은 포스트
        most_liked_post = self.db.query(XPost)\
            .filter(XPost.username == username)\
            .order_by(desc(XPost.like_count))\
            .first()
        
        return {
            'username': user_info.username,
            'display_name': user_info.display_name,
            'account_category': user_info.account_category,
            'total_posts': stats_query.total_posts or 0,
            'total_likes': stats_query.total_likes or 0,
            'total_retweets': stats_query.total_retweets or 0,
            'total_replies': stats_query.total_replies or 0,
            'avg_engagement_per_post': float(stats_query.avg_engagement) if stats_query.avg_engagement else 0.0,
            'most_liked_post': most_liked_post,
            'followers_count': user_info.user_followers_count
        }
    
    def get_category_stats(self, category: AccountCategory) -> Dict[str, Any]:
        """
        카테고리별 통계
        
        Args:
            category: 계정 카테고리
            
        Returns:
            Dict: 카테고리 통계 정보
        """
        # 기본 통계
        basic_stats = self.db.query(
            func.count(func.distinct(XPost.username)).label('total_users'),
            func.count(XPost.tweet_id).label('total_posts'),
            func.avg(
                func.coalesce(XPost.like_count, 0) + 
                func.coalesce(XPost.retweet_count, 0) + 
                func.coalesce(XPost.reply_count, 0)
            ).label('avg_engagement')
        ).filter(XPost.account_category == category.value).first()
        
        # 상위 사용자들
        top_users = self.get_category_users(category)[:5]
        
        return {
            'category': category.value,
            'total_users': basic_stats.total_users or 0,
            'total_posts': basic_stats.total_posts or 0,
            'avg_engagement_per_post': float(basic_stats.avg_engagement) if basic_stats.avg_engagement else 0.0,
            'top_users': top_users
        }
    
    def get_overview_stats(self) -> Dict[str, Any]:
        """
        전체 통계 개요
        
        Returns:
            Dict: 전체 통계 정보
        """
        # 전체 기본 통계
        basic_stats = self.db.query(
            func.count(XPost.tweet_id).label('total_posts'),
            func.count(func.distinct(XPost.username)).label('total_users'),
            func.avg(
                func.coalesce(XPost.like_count, 0) + 
                func.coalesce(XPost.retweet_count, 0) + 
                func.coalesce(XPost.reply_count, 0)
            ).label('avg_engagement')
        ).first()
        
        # 카테고리별 포스트 수
        categories = self.db.query(
            XPost.account_category,
            func.count(XPost.tweet_id).label('post_count')
        ).group_by(XPost.account_category).all()
        
        category_dict = {cat.account_category: cat.post_count for cat in categories}
        
        # 최근 24시간 포스트 수
        recent_24h = self.db.query(func.count(XPost.tweet_id))\
            .filter(XPost.created_at >= datetime.utcnow() - timedelta(hours=24))\
            .scalar()
        
        # 오늘의 인기 포스트 (좋아요 기준)
        today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        top_posts_today = self.db.query(XPost)\
            .filter(XPost.created_at >= today_start)\
            .order_by(desc(XPost.like_count))\
            .limit(5)\
            .all()
        
        return {
            'total_posts': basic_stats.total_posts or 0,
            'total_users': basic_stats.total_users or 0,
            'categories': category_dict,
            'recent_24h_posts': recent_24h or 0,
            'avg_engagement': float(basic_stats.avg_engagement) if basic_stats.avg_engagement else 0.0,
            'top_posts_today': top_posts_today
        }