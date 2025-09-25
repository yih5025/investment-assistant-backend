# app/services/sns_service.py

from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List, Optional, Dict, Any
import math
import json

from app.models.x_posts_model import XPost
from app.models.truth_social_model import TruthSocialPost, TruthSocialTrend
from app.schemas.sns_schema import UnifiedSNSPostResponse, SNSPostsResponse


class SNSService:
    """간단한 SNS 게시글 서비스 - 최신순 + 사용자 필터링"""
    
    def __init__(self, db: Session):
        self.db = db
    
    def get_available_authors(self) -> Dict[str, List[Dict[str, Any]]]:
        """DB에서 사용 가능한 작성자 목록 조회"""
        
        # X 사용자들 (최근 30일 내 게시글이 있는 사용자만, @멘션 및 빈 내용 제외)
        x_query = """
        SELECT 
            source_account as username,
            display_name,
            COUNT(*) as post_count,
            MAX(created_at) as last_post_date,
            MAX(user_verified) as verified
        FROM x_posts 
        WHERE created_at >= NOW() - INTERVAL '30 days'
            AND text NOT LIKE '@%'
            AND text IS NOT NULL
            AND LENGTH(TRIM(text)) > 0
        GROUP BY source_account, display_name
        HAVING COUNT(*) >= 1
        ORDER BY COUNT(*) DESC, MAX(created_at) DESC
        """
        
        # Truth Social Posts 사용자들 (VIP 3계정)
        truth_posts_query = """
        SELECT 
            username,
            display_name,
            COUNT(*) as post_count,
            MAX(created_at) as last_post_date,
            MAX(verified) as verified
        FROM truth_social_posts 
        WHERE created_at >= NOW() - INTERVAL '30 days'
        GROUP BY username, display_name
        HAVING COUNT(*) >= 1
        ORDER BY COUNT(*) DESC, MAX(created_at) DESC
        """
        
        # Truth Social Trends 사용자들 (VIP 3계정 제외)
        truth_trends_query = """
        SELECT 
            username,
            display_name,
            COUNT(*) as post_count,
            MAX(created_at) as last_post_date,
            false as verified
        FROM truth_social_trends 
        WHERE created_at >= NOW() - INTERVAL '30 days'
            AND username NOT IN ('realDonaldTrump', 'WhiteHouse', 'DonaldJTrumpJr')
        GROUP BY username, display_name
        HAVING COUNT(*) >= 1
        ORDER BY COUNT(*) DESC, MAX(created_at) DESC
        """
        
        try:
            x_result = self.db.execute(text(x_query))
            x_authors = [
                {
                    "username": row.username,
                    "display_name": row.display_name or row.username,
                    "post_count": row.post_count,
                    "last_post_date": row.last_post_date.isoformat(),
                    "verified": row.verified or False
                }
                for row in x_result.fetchall()
            ]
            
            truth_posts_result = self.db.execute(text(truth_posts_query))
            truth_posts_authors = [
                {
                    "username": row.username,
                    "display_name": row.display_name or row.username,
                    "post_count": row.post_count,
                    "last_post_date": row.last_post_date.isoformat(),
                    "verified": row.verified or False
                }
                for row in truth_posts_result.fetchall()
            ]
            
            truth_trends_result = self.db.execute(text(truth_trends_query))
            truth_trends_authors = [
                {
                    "username": row.username,
                    "display_name": row.display_name or row.username,
                    "post_count": row.post_count,
                    "last_post_date": row.last_post_date.isoformat(),
                    "verified": row.verified or False
                }
                for row in truth_trends_result.fetchall()
            ]
            
            return {
                "x": x_authors,
                "truth_social_posts": truth_posts_authors,
                "truth_social_trends": truth_trends_authors
            }
            
        except Exception as e:
            print(f"작성자 목록 조회 실패: {e}")
            return {"x": [], "truth_social_posts": [], "truth_social_trends": []}
    
    def get_posts(self, platform: str = "all", author: Optional[str] = None, 
                  limit: int = 50, offset: int = 0) -> SNSPostsResponse:
        """
        SNS 게시글 조회 - 최신순만, 사용자 필터링
        """
        
        if limit > 100:
            limit = 100
        
        # 쿼리 구성
        queries = []
        count_queries = []
        
        # X Posts 쿼리
        if platform in ["all", "x"]:
            x_select = """
            SELECT 
                tweet_id as id,
                'x' as platform,
                text as content,
                source_account as author,
                display_name,
                created_at,
                like_count,
                retweet_count,
                reply_count,
                false as has_media,
                null as media_attachments,
                created_at as sort_date
            FROM x_posts
            WHERE text NOT LIKE '@%'
                AND text IS NOT NULL
                AND LENGTH(TRIM(text)) > 0
            """
            
            x_count = """
            SELECT COUNT(*) as count FROM x_posts
            WHERE text NOT LIKE '@%'
                AND text IS NOT NULL
                AND LENGTH(TRIM(text)) > 0
            """
            
            if author:
                x_select += f" AND source_account = '{author}'"
                x_count += f" AND source_account = '{author}'"
            
            queries.append(x_select)
            count_queries.append(x_count)
        
        # Truth Social Posts 쿼리
        if platform in ["all", "truth_social_posts"]:
            truth_posts_select = """
            SELECT 
                id,
                'truth_social_posts' as platform,
                clean_content as content,
                username as author,
                display_name,
                created_at,
                favourites_count as like_count,
                reblogs_count as retweet_count,
                replies_count as reply_count,
                has_media,
                media_attachments,
                created_at as sort_date
            FROM truth_social_posts
            WHERE (
                (clean_content IS NOT NULL AND LENGTH(TRIM(clean_content)) > 0)
                OR 
                (media_attachments IS NOT NULL AND media_attachments != 'null'::jsonb)
                OR
                username IN ('realDonaldTrump', 'WhiteHouse', 'DonaldJTrumpJr')
            )
            """
            
            truth_posts_count = """
            SELECT COUNT(*) as count FROM truth_social_posts
            WHERE (
                (clean_content IS NOT NULL AND LENGTH(TRIM(clean_content)) > 0)
                OR 
                (media_attachments IS NOT NULL AND media_attachments != 'null'::jsonb)
                OR
                username IN ('realDonaldTrump', 'WhiteHouse', 'DonaldJTrumpJr')
            )
            """
            
            if author:
                truth_posts_select += f" AND username = '{author}'"
                truth_posts_count += f" AND username = '{author}'"
            
            queries.append(truth_posts_select)
            count_queries.append(truth_posts_count)
        
        # Truth Social Trends 쿼리 (VIP 3계정 제외)
        if platform in ["all", "truth_social_trends"]:
            truth_trends_select = """
            SELECT 
                id,
                'truth_social_trends' as platform,
                clean_content as content,
                username as author,
                display_name,
                created_at,
                favourites_count as like_count,
                reblogs_count as retweet_count,
                replies_count as reply_count,
                false as has_media,
                null as media_attachments,
                created_at as sort_date
            FROM truth_social_trends
            WHERE clean_content IS NOT NULL
                AND LENGTH(TRIM(clean_content)) > 0
                AND username NOT IN ('realDonaldTrump', 'WhiteHouse', 'DonaldJTrumpJr')
            """
            
            truth_trends_count = """
            SELECT COUNT(*) as count FROM truth_social_trends
            WHERE clean_content IS NOT NULL
                AND LENGTH(TRIM(clean_content)) > 0
                AND username NOT IN ('realDonaldTrump', 'WhiteHouse', 'DonaldJTrumpJr')
            """
            
            if author:
                # author가 VIP 3계정 중 하나면 trends에서 결과 없음
                if author in ['realDonaldTrump', 'WhiteHouse', 'DonaldJTrumpJr']:
                    truth_trends_select += " AND 1=0"
                    truth_trends_count += " AND 1=0"
                else:
                    truth_trends_select += f" AND username = '{author}'"
                    truth_trends_count += f" AND username = '{author}'"
            
            queries.append(truth_trends_select)
            count_queries.append(truth_trends_count)
        
        if not queries:
            return SNSPostsResponse(
                items=[],
                total=0,
                page=1,
                size=limit,
                pages=0,
                platform_counts={}
            )
        
        # UNION ALL로 통합
        union_query = " UNION ALL ".join(queries)
        
        # 최종 쿼리 (최신순 정렬만)
        final_query = f"""
        WITH unified_posts AS (
            {union_query}
        )
        SELECT * FROM unified_posts 
        ORDER BY sort_date DESC
        LIMIT {limit} OFFSET {offset}
        """
        
        # 총 개수 쿼리
        union_count_query = " UNION ALL ".join(count_queries)
        total_count_query = f"""
        WITH counts AS (
            {union_count_query}
        )
        SELECT SUM(count) as total FROM counts
        """
        
        try:
            # 데이터 조회
            posts_result = self.db.execute(text(final_query))
            posts = posts_result.fetchall()
            
            # 총 개수 조회
            count_result = self.db.execute(text(total_count_query))
            total_count = count_result.fetchone().total or 0
            
            # 응답 데이터 구성
            items = []
            for post in posts:
                # 미디어 정보 처리
                thumbnail_url, media_type = self._extract_media_info(
                    getattr(post, 'media_attachments', None),
                    getattr(post, 'has_media', False)
                )
                
                # 콘텐츠 표시 처리
                display_content = self._format_content_for_display(
                    post.content, 
                    getattr(post, 'has_media', False), 
                    media_type
                )
                
                items.append(UnifiedSNSPostResponse(
                    id=post.id,
                    platform=post.platform,
                    content=display_content,
                    clean_content=display_content,
                    author=post.author,
                    display_name=post.display_name,
                    created_at=post.created_at,
                    likes=post.like_count,
                    retweets=post.retweet_count, 
                    replies=post.reply_count,
                    engagement_score=(post.like_count or 0) + (post.retweet_count or 0) + (post.reply_count or 0),
                    has_media=getattr(post, 'has_media', False),
                    media_thumbnail=thumbnail_url,
                    media_type=media_type
                ))
            
            return SNSPostsResponse(
                items=items,
                total=total_count,
                page=(offset // limit) + 1,
                size=limit,
                pages=math.ceil(total_count / limit) if total_count > 0 else 0,
                platform_counts={}
            )
            
        except Exception as e:
            raise Exception(f"SNS 게시글 조회 중 오류 발생: {str(e)}")
    
    def get_post_detail(self, post_id: str, platform: str) -> Optional[UnifiedSNSPostResponse]:
        """개별 게시글 상세 조회"""
        
        if platform == "x":
            query = """
            SELECT 
                tweet_id as id, 'x' as platform, text as content,
                source_account as author, display_name, created_at,
                like_count, retweet_count, reply_count,
                false as has_media, null as media_attachments
            FROM x_posts 
            WHERE tweet_id = :post_id
                AND text IS NOT NULL
                AND LENGTH(TRIM(text)) > 0
            """
        elif platform == "truth_social_posts":
            query = """
            SELECT 
                id, 'truth_social_posts' as platform, clean_content as content,
                username as author, display_name, created_at,
                favourites_count as like_count, reblogs_count as retweet_count, 
                replies_count as reply_count, has_media, media_attachments
            FROM truth_social_posts 
            WHERE id = :post_id
                AND (
                    (clean_content IS NOT NULL AND LENGTH(TRIM(clean_content)) > 0)
                    OR 
                    (media_attachments IS NOT NULL AND media_attachments != 'null'::jsonb)
                    OR
                    username IN ('realDonaldTrump', 'WhiteHouse', 'DonaldJTrumpJr')
                )
            """
        elif platform == "truth_social_trends":
            query = """
            SELECT 
                id, 'truth_social_trends' as platform, clean_content as content,
                username as author, display_name, created_at,
                favourites_count as like_count, reblogs_count as retweet_count,
                replies_count as reply_count, false as has_media, null as media_attachments
            FROM truth_social_trends 
            WHERE id = :post_id
                AND clean_content IS NOT NULL
                AND LENGTH(TRIM(clean_content)) > 0
                AND username NOT IN ('realDonaldTrump', 'WhiteHouse', 'DonaldJTrumpJr')
            """
        else:
            return None
        
        try:
            result = self.db.execute(text(query), {"post_id": post_id})
            row = result.fetchone()
            
            if not row:
                return None
            
            # 미디어 정보 처리
            thumbnail_url, media_type = self._extract_media_info(
                getattr(row, 'media_attachments', None),
                getattr(row, 'has_media', False)
            )
            
            # 콘텐츠 표시 처리
            display_content = self._format_content_for_display(
                row.content, 
                getattr(row, 'has_media', False), 
                media_type
            )
            
            return UnifiedSNSPostResponse(
                id=row.id,
                platform=row.platform,
                content=display_content,
                clean_content=display_content,
                author=row.author,
                display_name=row.display_name,
                created_at=row.created_at,
                likes=row.like_count,
                retweets=row.retweet_count,
                replies=row.reply_count,
                engagement_score=(row.like_count or 0) + (row.retweet_count or 0) + (row.reply_count or 0),
                has_media=getattr(row, 'has_media', False),
                media_thumbnail=thumbnail_url,
                media_type=media_type
            )
            
        except Exception as e:
            print(f"게시글 상세 조회 실패: {e}")
            return None
    
    def get_basic_stats(self) -> Dict[str, Any]:
        """기본 통계 조회"""
        
        stats_query = """
        WITH stats AS (
            SELECT 'x' as platform, 
                   COUNT(*) as total_posts,
                   COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '24 hours') as posts_24h,
                   COUNT(DISTINCT source_account) as unique_authors
            FROM x_posts
            WHERE text NOT LIKE '@%'
                AND text IS NOT NULL
                AND LENGTH(TRIM(text)) > 0
            
            UNION ALL
            
            SELECT 'truth_social_posts' as platform,
                   COUNT(*) as total_posts, 
                   COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '24 hours') as posts_24h,
                   COUNT(DISTINCT username) as unique_authors
            FROM truth_social_posts
            WHERE (
                (clean_content IS NOT NULL AND LENGTH(TRIM(clean_content)) > 0)
                OR 
                (media_attachments IS NOT NULL AND media_attachments != 'null'::jsonb)
                OR
                username IN ('realDonaldTrump', 'WhiteHouse', 'DonaldJTrumpJr')
            )
            
            UNION ALL
            
            SELECT 'truth_social_trends' as platform,
                   COUNT(*) as total_posts,
                   COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '24 hours') as posts_24h,
                   COUNT(DISTINCT username) as unique_authors
            FROM truth_social_trends
            WHERE clean_content IS NOT NULL
                AND LENGTH(TRIM(clean_content)) > 0
                AND username NOT IN ('realDonaldTrump', 'WhiteHouse', 'DonaldJTrumpJr')
        )
        SELECT * FROM stats
        """
        
        try:
            result = self.db.execute(text(stats_query))
            platform_stats = {}
            totals = {"posts": 0, "posts_24h": 0, "authors": 0}
            
            for row in result.fetchall():
                platform_stats[row.platform] = {
                    "total_posts": row.total_posts,
                    "posts_24h": row.posts_24h,
                    "unique_authors": row.unique_authors
                }
                totals["posts"] += row.total_posts
                totals["posts_24h"] += row.posts_24h
                totals["authors"] += row.unique_authors
            
            return {
                "platform_stats": platform_stats,
                "totals": totals,
                "last_updated": "실시간"
            }
            
        except Exception as e:
            print(f"통계 조회 실패: {e}")
            return {
                "platform_stats": {},
                "totals": {"posts": 0, "posts_24h": 0, "authors": 0},
                "last_updated": "오류"
            }
    
    def _extract_media_info(self, media_attachments, has_media):
        """미디어 첨부파일에서 썸네일 정보 추출"""
        if not has_media or not media_attachments:
            return None, None
        
        try:
            if isinstance(media_attachments, str):
                media_data = json.loads(media_attachments)
            else:
                media_data = media_attachments
            
            if isinstance(media_data, list) and len(media_data) > 0:
                first_media = media_data[0]
                
                # 썸네일 URL (preview_url 우선, 없으면 url)
                thumbnail_url = first_media.get('preview_url') or first_media.get('url')
                
                # 미디어 타입 (image, video 등)
                media_type = first_media.get('type', 'unknown')
                
                return thumbnail_url, media_type
                
        except (json.JSONDecodeError, KeyError, AttributeError, TypeError):
            pass
        
        return None, None
    
    def _format_content_for_display(self, content, has_media, media_type):
        """표시용 콘텐츠 포맷팅"""
        if content and len(content.strip()) > 0:
            return content
        
        # 텍스트가 없고 미디어만 있는 경우
        if has_media and media_type:
            if media_type == 'image':
                return "[이미지]"
            elif media_type == 'video':
                return "[영상]"
            else:
                return "[미디어]"
        
        return "[내용 없음]"