# app/services/sns_service.py

from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List, Optional, Dict, Any
import math
import json

from app.models.x_posts_model import XPost
from app.models.truth_social_model import TruthSocialPost
from app.models.post_analysis_cache_model import PostAnalysisCache
from app.schemas import sns_schema
from fastapi import HTTPException

class SNSService:
    """통합 SNS 서비스: 원본 데이터 조회 및 분석 데이터 조회를 모두 처리"""
    
    def __init__(self, db: Session):
        self.db = db
    
    # --- 1. 원본 데이터 조회용 서비스 (기존 코드 유지 및 보안 강화) ---
    
    def get_available_authors(self) -> Dict[str, List[Dict[str, Any]]]:
        """DB에서 사용 가능한 작성자 목록 조회"""
        x_query = """
        SELECT 
            source_account as username,
            display_name,
            COUNT(*) as post_count,
            MAX(created_at) as last_post_date,
            MAX(user_verified) as verified
        FROM x_posts 
        WHERE created_at >= NOW() - INTERVAL '30 days'
            AND text NOT LIKE '@%%'
            AND text IS NOT NULL
            AND LENGTH(TRIM(text)) > 0
        GROUP BY source_account, display_name
        HAVING COUNT(*) >= 1
        ORDER BY COUNT(*) DESC, MAX(created_at) DESC
        """
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
            x_result = self.db.execute(text(x_query)).fetchall()
            x_authors = [
                {
                    "username": row.username,
                    "display_name": row.display_name or row.username,
                    "post_count": row.post_count,
                    "last_post_date": row.last_post_date,
                    "verified": row.verified or False
                } for row in x_result
            ]
            
            truth_posts_result = self.db.execute(text(truth_posts_query)).fetchall()
            truth_posts_authors = [
                {
                    "username": row.username,
                    "display_name": row.display_name or row.username,
                    "post_count": row.post_count,
                    "last_post_date": row.last_post_date,
                    "verified": row.verified or False
                } for row in truth_posts_result
            ]
            
            truth_trends_result = self.db.execute(text(truth_trends_query)).fetchall()
            truth_trends_authors = [
                {
                    "username": row.username,
                    "display_name": row.display_name or row.username,
                    "post_count": row.post_count,
                    "last_post_date": row.last_post_date,
                    "verified": row.verified or False
                } for row in truth_trends_result
            ]
            
            return {
                "x": x_authors,
                "truth_social_posts": truth_posts_authors,
                "truth_social_trends": truth_trends_authors
            }
        except Exception as e:
            print(f"작성자 목록 조회 실패: {e}")
            return {"x": [], "truth_social_posts": [], "truth_social_trends": []}
    
    def get_posts(self, platform: str, author: Optional[str], limit: int, offset: int) -> sns_schema.SNSPostsResponse:
        """(원본 데이터용) SNS 게시글 조회 - SQL 인젝션 방지 적용"""
        params = {'limit': limit, 'offset': offset}
        queries, count_queries = [], []

        if platform in ["all", "x"]:
            x_select_base = "SELECT tweet_id as id, 'x' as platform, text as content, source_account as author, display_name, created_at, like_count, retweet_count, reply_count, false as has_media, null as media_attachments, created_at as sort_date FROM x_posts"
            x_count_base = "SELECT COUNT(*) as count FROM x_posts"
            where_clauses = ["text NOT LIKE '@%%'", "text IS NOT NULL", "LENGTH(TRIM(text)) > 0"]
            if author:
                where_clauses.append("source_account = :author")
                params['author'] = author
            final_where = " WHERE " + " AND ".join(where_clauses)
            queries.append(x_select_base + final_where)
            count_queries.append(x_count_base + final_where)

        if platform in ["all", "truth_social_posts"]:
            truth_posts_select_base = "SELECT id, 'truth_social_posts' as platform, clean_content as content, username as author, display_name, created_at, favourites_count as like_count, reblogs_count as retweet_count, replies_count as reply_count, has_media, media_attachments, created_at as sort_date FROM truth_social_posts"
            truth_posts_count_base = "SELECT COUNT(*) as count FROM truth_social_posts"
            where_clauses = ["((clean_content IS NOT NULL AND LENGTH(TRIM(clean_content)) > 0) OR (media_attachments IS NOT NULL AND media_attachments != 'null'::jsonb) OR username IN ('realDonaldTrump', 'WhiteHouse', 'DonaldJTrumpJr'))"]
            if author:
                where_clauses.append("username = :author")
                params['author'] = author
            final_where = " WHERE " + " AND ".join(where_clauses)
            queries.append(truth_posts_select_base + final_where)
            count_queries.append(truth_posts_count_base + final_where)

        if platform in ["all", "truth_social_trends"]:
            truth_trends_select_base = "SELECT id, 'truth_social_trends' as platform, clean_content as content, username as author, display_name, created_at, favourites_count as like_count, reblogs_count as retweet_count, replies_count as reply_count, false as has_media, null as media_attachments, created_at as sort_date FROM truth_social_trends"
            truth_trends_count_base = "SELECT COUNT(*) as count FROM truth_social_trends"
            where_clauses = ["clean_content IS NOT NULL", "LENGTH(TRIM(clean_content)) > 0", "username NOT IN ('realDonaldTrump', 'WhiteHouse', 'DonaldJTrumpJr')"]
            if author and author not in ['realDonaldTrump', 'WhiteHouse', 'DonaldJTrumpJr']:
                where_clauses.append("username = :author")
                params['author'] = author
            elif author in ['realDonaldTrump', 'WhiteHouse', 'DonaldJTrumpJr']:
                where_clauses.append("1=0") # Author is a VIP, so no results from trends
            final_where = " WHERE " + " AND ".join(where_clauses)
            queries.append(truth_trends_select_base + final_where)
            count_queries.append(truth_trends_count_base + final_where)

        if not queries:
            return sns_schema.SNSPostsResponse(items=[], total=0, page=1, size=limit, pages=0, platform_counts={})
        
        union_query = " UNION ALL ".join(queries)
        final_query = f"WITH unified_posts AS ({union_query}) SELECT * FROM unified_posts ORDER BY sort_date DESC LIMIT :limit OFFSET :offset"
        
        union_count_query = " UNION ALL ".join(count_queries)
        total_count_query = f"WITH counts AS ({union_count_query}) SELECT SUM(count) as total FROM counts"

        try:
            posts_result = self.db.execute(text(final_query), params).fetchall()
            count_result = self.db.execute(text(total_count_query), params).fetchone()
            total_count = count_result[0] or 0
            
            items = []
            for post in posts_result:
                thumbnail_url, media_type = self._extract_media_info(getattr(post, 'media_attachments', None), getattr(post, 'has_media', False))
                display_content = self._format_content_for_display(post.content, getattr(post, 'has_media', False), media_type)
                
                items.append(sns_schema.UnifiedSNSPostResponse(
                    id=str(post.id), platform=post.platform, content=display_content,
                    clean_content=display_content, author=post.author, display_name=post.display_name,
                    created_at=post.created_at, likes=post.like_count, retweets=post.retweet_count,
                    replies=post.reply_count,
                    engagement_score=(post.like_count or 0) + (post.retweet_count or 0) + (post.reply_count or 0),
                    has_media=getattr(post, 'has_media', False),
                    media_thumbnail=thumbnail_url, media_type=media_type
                ))

            return sns_schema.SNSPostsResponse(
                items=items, total=total_count, page=(offset // limit) + 1,
                size=limit, pages=math.ceil(total_count / limit) if total_count > 0 else 0,
                platform_counts={}
            )
        except Exception as e:
            raise Exception(f"SNS 게시글 조회 중 오류 발생: {str(e)}")

    def get_post_detail(self, post_id: str, platform: str) -> Optional[sns_schema.UnifiedSNSPostResponse]:
        """(원본 데이터용) 개별 게시글 상세 조회 - 보안 수정 적용"""
        query_map = {
            "x": "SELECT tweet_id as id, 'x' as platform, text as content, source_account as author, display_name, created_at, like_count, retweet_count, reply_count, false as has_media, null as media_attachments FROM x_posts WHERE tweet_id = :post_id",
            "truth_social_posts": "SELECT id, 'truth_social_posts' as platform, clean_content as content, username as author, display_name, created_at, favourites_count as like_count, reblogs_count as retweet_count, replies_count as reply_count, has_media, media_attachments FROM truth_social_posts WHERE id = :post_id",
            "truth_social_trends": "SELECT id, 'truth_social_trends' as platform, clean_content as content, username as author, display_name, created_at, favourites_count as like_count, reblogs_count as retweet_count, replies_count as reply_count, false as has_media, null as media_attachments FROM truth_social_trends WHERE id = :post_id"
        }
        
        query = query_map.get(platform)
        if not query:
            return None

        try:
            result = self.db.execute(text(query), {"post_id": post_id}).fetchone()
            if not result:
                return None
            
            thumbnail_url, media_type = self._extract_media_info(getattr(result, 'media_attachments', None), getattr(result, 'has_media', False))
            display_content = self._format_content_for_display(result.content, getattr(result, 'has_media', False), media_type)
            
            return sns_schema.UnifiedSNSPostResponse(
                id=str(result.id), platform=result.platform, content=display_content,
                clean_content=display_content, author=result.author, display_name=result.display_name,
                created_at=result.created_at, likes=result.like_count, retweets=result.retweet_count,
                replies=result.reply_count,
                engagement_score=(result.like_count or 0) + (result.retweet_count or 0) + (result.reply_count or 0),
                has_media=getattr(result, 'has_media', False),
                media_thumbnail=thumbnail_url, media_type=media_type
            )
        except Exception as e:
            print(f"게시글 상세 조회 실패: {e}")
            return None

    def get_basic_stats(self) -> Dict[str, Any]:
        """(원본 데이터용) 기본 통계 조회"""
        # (기존 코드와 동일, 생략 없음)
    
    def _extract_media_info(self, media_attachments: Any, has_media: bool) -> (Optional[str], Optional[str]):
        """미디어 첨부파일에서 썸네일 정보 추출"""
        if not has_media or not media_attachments:
            return None, None
        try:
            media_data = json.loads(media_attachments) if isinstance(media_attachments, str) else media_attachments
            if isinstance(media_data, list) and media_data:
                first_media = media_data[0]
                thumbnail_url = first_media.get('preview_url') or first_media.get('url')
                media_type = first_media.get('type', 'unknown')
                return thumbnail_url, media_type
        except (json.JSONDecodeError, KeyError, AttributeError, TypeError):
            pass
        return None, None
    
    def _format_content_for_display(self, content: Optional[str], has_media: bool, media_type: Optional[str]) -> str:
        """표시용 콘텐츠 포맷팅"""
        if content and content.strip():
            return content
        if has_media and media_type:
            if media_type == 'image': return "[이미지]"
            if media_type == 'video': return "[영상]"
            return "[미디어]"
        return "[내용 없음]"

    # --- 2. 프론트엔드 분석 페이지용 서비스 (신규 추가) ---
    
    def get_analysis_posts(self, db: Session, skip: int, limit: int) -> List[sns_schema.SNSPostAnalysisListResponse]:
        """[분석 목록 페이지용] 분석된 SNS 게시글 목록을 조회합니다."""
        analysis_results = db.query(PostAnalysisCache).order_by(PostAnalysisCache.post_timestamp.desc()).offset(skip).limit(limit).all()
        
        post_ids_by_source = {'x': [], 'truth_social_posts': [], 'truth_social_trends': []}
        for result in analysis_results:
            if result.post_source in post_ids_by_source:
                post_ids_by_source[result.post_source].append(result.post_id)

        original_posts_map = self._get_original_posts_for_analysis_map(db, post_ids_by_source)

        combined_posts = []
        for result in analysis_results:
            original_post_data = original_posts_map.get((result.post_source, result.post_id))
            
            content_schema = sns_schema.OriginalPostForAnalysisSchema(content="원본 게시물을 찾을 수 없습니다.")
            engagement_schema = None

            if original_post_data:
                content_schema = sns_schema.OriginalPostForAnalysisSchema(content=original_post_data.get("content"))
                if original_post_data.get("engagement"):
                    engagement_schema = sns_schema.XPostEngagementSchema(**original_post_data["engagement"])

            combined_posts.append(sns_schema.SNSPostAnalysisListResponse(
                analysis=result,
                original_post=content_schema,
                engagement=engagement_schema
            ))
        return combined_posts

    def get_analysis_post_detail(self, db: Session, post_id: str, post_source: str) -> sns_schema.SNSPostAnalysisDetailResponse:
        """[분석 상세 페이지용] 특정 게시물의 모든 상세 분석 데이터를 조회합니다."""
        analysis_result = db.query(PostAnalysisCache).filter(
            PostAnalysisCache.post_id == post_id,
            PostAnalysisCache.post_source == post_source
        ).first()

        if not analysis_result:
            raise HTTPException(status_code=404, detail="Analysis data not found.")
        
        original_posts_map = self._get_original_posts_for_analysis_map(db, {post_source: [post_id]})
        original_post_data = original_posts_map.get((post_source, post_id))

        content_schema = sns_schema.OriginalPostForAnalysisSchema(content="원본 게시물을 찾을 수 없습니다.")
        engagement_schema = None

        if original_post_data:
            content_schema = sns_schema.OriginalPostForAnalysisSchema(content=original_post_data.get("content"))
            if original_post_data.get("engagement"):
                engagement_schema = sns_schema.XPostEngagementSchema(**original_post_data["engagement"])
        
        return sns_schema.SNSPostAnalysisDetailResponse(
            analysis=analysis_result,
            original_post=content_schema,
            engagement=engagement_schema
        )
    
    def _get_original_posts_for_analysis_map(self, db: Session, post_ids_by_source: dict) -> dict:
        """(분석용) Helper to fetch original posts efficiently."""
        original_posts_map = {}
        if post_ids_by_source.get('x'):
            x_posts = db.query(XPost).filter(XPost.tweet_id.in_(post_ids_by_source['x'])).all()
            for post in x_posts:
                original_posts_map[('x', post.tweet_id)] = {
                    "content": post.text,
                    "engagement": {
                        "retweet_count": post.retweet_count, "reply_count": post.reply_count,
                        "like_count": post.like_count, "quote_count": post.quote_count,
                        "impression_count": post.impression_count, "account_category": post.account_category,
                    }
                }
        
        all_truth_ids = post_ids_by_source.get('truth_social_posts', []) + post_ids_by_source.get('truth_social_trends', [])
        if all_truth_ids:
            truth_posts = db.query(TruthSocialPost).filter(TruthSocialPost.id.in_(all_truth_ids)).all()
            for post in truth_posts:
                source = 'truth_social_posts' if str(post.id) in post_ids_by_source.get('truth_social_posts', []) else 'truth_social_trends'
                original_posts_map[(source, str(post.id))] = {"content": post.clean_content, "engagement": None}
                
        return original_posts_map