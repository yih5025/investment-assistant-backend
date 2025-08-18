# app/api/endpoints/truth_social.py

from datetime import datetime, date
from typing import Optional, List, Dict, Any, Union, Tuple
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.dependencies import get_db
from app.services.truth_social_service import (
    TruthSocialPostService, 
    TruthSocialTagService, 
    TruthSocialTrendService,
    TruthSocialAnalyticsService
)
from app.schemas.truth_social_schema import (
    TruthSocialPostsResponse, TruthSocialPostResponse,
    TruthSocialTagsResponse, TruthSocialTagResponse,
    TruthSocialTrendsResponse, TruthSocialTrendResponse,
    TruthSocialPostFilter, TruthSocialTagFilter, TruthSocialTrendFilter
)
from ...schemas.common import APIResponse

# Truth Social 라우터 생성
router = APIRouter(
    tags=["Truth Social"],
    responses={
        404: {"description": "요청한 Truth Social 데이터를 찾을 수 없습니다"},
        422: {"description": "잘못된 요청 파라미터"},
        500: {"description": "서버 내부 오류"}
    }
)


# ==================== Truth Social Posts API ====================

@router.get("/posts/", response_model=TruthSocialPostsResponse, summary="Truth Social 포스트 목록")
async def get_truth_social_posts(
    skip: int = Query(0, ge=0, description="건너뛸 항목 수"),
    limit: int = Query(100, ge=1, le=500, description="가져올 항목 수"),
    username: Optional[str] = Query(None, description="사용자명 필터"),
    account_type: Optional[str] = Query(None, description="계정 유형 필터"),
    has_media: Optional[bool] = Query(None, description="미디어 포함 여부"),
    min_market_influence: Optional[int] = Query(None, ge=0, description="최소 시장 영향도"),
    language: Optional[str] = Query(None, description="언어 필터"),
    start_date: Optional[datetime] = Query(None, description="시작 날짜"),
    end_date: Optional[datetime] = Query(None, description="종료 날짜"),
    db: Session = Depends(get_db)
):
    """
    Truth Social 포스트 목록을 조회합니다.
    
    - **skip**: 건너뛸 항목 수 (페이징)
    - **limit**: 가져올 항목 수 (최대 500개)
    - **username**: 특정 사용자의 포스트만 필터링
    - **account_type**: 계정 유형별 필터링 (individual, government 등)
    - **has_media**: 미디어 포함 여부로 필터링
    - **min_market_influence**: 최소 시장 영향도로 필터링
    - **language**: 언어별 필터링
    - **start_date**: 시작 날짜
    - **end_date**: 종료 날짜
    """
    # 서비스 호출 (Query 파라미터를 직접 전달)
    service = TruthSocialPostService(db)
    
    # 필터 객체 생성 (None 값들은 자동으로 무시됨)
    filters = TruthSocialPostFilter(
        username=username,
        account_type=account_type,
        has_media=has_media,
        min_market_influence=min_market_influence,
        language=language,
        start_date=start_date,
        end_date=end_date
    )
    
    posts = service.get_posts(skip=skip, limit=limit, filters=filters)
    total = service.get_posts_count(filters=filters)
    
    return TruthSocialPostsResponse(
        items=posts,
        total_count=total,  # total -> total_count로 변경
        skip=skip,
        limit=limit,
        has_next=skip + limit < total
    )


@router.get("/posts/trump", response_model=TruthSocialPostsResponse, summary="트럼프 포스트")
async def get_trump_posts(
    skip: int = Query(0, ge=0, description="건너뛸 항목 수"),
    limit: int = Query(50, ge=1, le=100, description="가져올 항목 수"),
    db: Session = Depends(get_db)
):
    """
    트럼프(@realDonaldTrump)의 포스트만 조회합니다.
    인덱스를 활용하여 빠른 조회가 가능합니다.
    """
    service = TruthSocialPostService(db)
    posts = service.get_trump_posts(skip=skip, limit=limit)
    
    # 트럼프 포스트 총 개수 (캐시 가능)
    trump_filter = TruthSocialPostFilter(username="realDonaldTrump")
    total = service.get_posts_count(trump_filter)
    
    return TruthSocialPostsResponse(
        items=posts,
        total_count=total,  # total -> total_count로 변경
        skip=skip,
        limit=limit,
        has_next=skip + limit < total
    )


@router.get("/posts/government", response_model=TruthSocialPostsResponse, summary="정부 계정 포스트")
async def get_government_posts(
    skip: int = Query(0, ge=0, description="건너뛸 항목 수"),
    limit: int = Query(50, ge=1, le=100, description="가져올 항목 수"),
    db: Session = Depends(get_db)
):
    """
    정부 계정의 포스트만 조회합니다.
    인덱스를 활용하여 빠른 조회가 가능합니다.
    """
    service = TruthSocialPostService(db)
    posts = service.get_government_posts(skip=skip, limit=limit)
    
    gov_filter = TruthSocialPostFilter(account_type="government")
    total = service.get_posts_count(gov_filter)
    
    return TruthSocialPostsResponse(
        items=posts,
        total_count=total,  # total -> total_count로 변경
        skip=skip,
        limit=limit,
        has_next=skip + limit < total
    )


@router.get("/posts/high-influence", response_model=TruthSocialPostsResponse, summary="높은 시장 영향도 포스트")
async def get_high_influence_posts(
    skip: int = Query(0, ge=0, description="건너뛸 항목 수"),
    limit: int = Query(50, ge=1, le=100, description="가져올 항목 수"),
    db: Session = Depends(get_db)
):
    """
    시장 영향도가 높은 포스트를 조회합니다.
    시장 영향도 순으로 정렬됩니다.
    """
    service = TruthSocialPostService(db)
    posts = service.get_high_influence_posts(skip=skip, limit=limit)
    
    influence_filter = TruthSocialPostFilter(min_market_influence=1)
    total = service.get_posts_count(influence_filter)
    
    return TruthSocialPostsResponse(
        items=posts,
        total_count=total,  # total -> total_count로 변경
        skip=skip,
        limit=limit,
        has_next=skip + limit < total
    )


@router.get("/posts/with-media", response_model=TruthSocialPostsResponse, summary="미디어 포함 포스트")
async def get_posts_with_media(
    skip: int = Query(0, ge=0, description="건너뛸 항목 수"),
    limit: int = Query(50, ge=1, le=100, description="가져올 항목 수"),
    db: Session = Depends(get_db)
):
    """
    미디어(이미지, 비디오 등)가 포함된 포스트를 조회합니다.
    """
    service = TruthSocialPostService(db)
    posts = service.get_posts_with_media(skip=skip, limit=limit)
    
    media_filter = TruthSocialPostFilter(has_media=True)
    total = service.get_posts_count(media_filter)
    
    return TruthSocialPostsResponse(
        items=posts,
        total_count=total,  # total -> total_count로 변경
        skip=skip,
        limit=limit,
        has_next=skip + limit < total
    )


@router.get("/posts/search", response_model=TruthSocialPostsResponse, summary="포스트 내용 검색")
async def search_posts(
    q: str = Query(..., min_length=2, description="검색어"),
    skip: int = Query(0, ge=0, description="건너뛸 항목 수"),
    limit: int = Query(50, ge=1, le=100, description="가져올 항목 수"),
    db: Session = Depends(get_db)
):
    """
    포스트 내용을 전문 검색합니다.
    PostgreSQL의 GIN 인덱스를 활용하여 빠른 검색이 가능합니다.
    """
    service = TruthSocialPostService(db)
    posts = service.search_posts_by_content(q, skip=skip, limit=limit)
    
    return TruthSocialPostsResponse(
        items=posts,
        total=len(posts),  # 전문 검색은 정확한 total count 계산이 복잡함
        skip=skip,
        limit=limit,
        has_next=len(posts) == limit
    )


@router.get("/posts/{post_id}", response_model=TruthSocialPostResponse, summary="특정 포스트 조회")
async def get_post_by_id(
    post_id: str,
    db: Session = Depends(get_db)
):
    """
    ID로 특정 포스트를 조회합니다.
    """
    service = TruthSocialPostService(db)
    post = service.get_post_by_id(post_id)
    
    if not post:
        raise HTTPException(status_code=404, detail="포스트를 찾을 수 없습니다")
    
    return post


# ==================== Truth Social Tags API ====================

@router.get("/tags/", response_model=TruthSocialTagsResponse, summary="Truth Social 태그 목록")
async def get_truth_social_tags(
    skip: int = Query(0, ge=0, description="건너뛸 항목 수"),
    limit: int = Query(100, ge=1, le=500, description="가져올 항목 수"),
    tag_category: Optional[str] = Query(None, description="태그 카테고리 필터"),
    min_total_uses: Optional[int] = Query(None, ge=0, description="최소 사용 횟수"),
    min_market_relevance: Optional[int] = Query(None, ge=0, description="최소 시장 관련성"),
    start_date: Optional[date] = Query(None, description="시작 날짜"),
    end_date: Optional[date] = Query(None, description="종료 날짜"),
    db: Session = Depends(get_db)
):
    """
    Truth Social 해시태그 목록을 조회합니다.
    """
    filters = TruthSocialTagFilter(
        tag_category=tag_category,
        min_total_uses=min_total_uses,
        min_market_relevance=min_market_relevance,
        start_date=start_date,
        end_date=end_date
    )
    
    service = TruthSocialTagService(db)
    tags = service.get_tags(skip=skip, limit=limit, filters=filters)
    total = service.get_tags_count(filters=filters)
    
    return TruthSocialTagsResponse(
        items=tags,
        total_count=total,  # total -> total_count로 변경
        skip=skip,
        limit=limit,
        has_next=skip + limit < total
    )


@router.get("/tags/trending", response_model=TruthSocialTagsResponse, summary="트렌딩 태그")
async def get_trending_tags(
    skip: int = Query(0, ge=0, description="건너뛸 항목 수"),
    limit: int = Query(50, ge=1, le=100, description="가져올 항목 수"),
    db: Session = Depends(get_db)
):
    """
    트렌드 점수가 높은 태그를 조회합니다.
    """
    service = TruthSocialTagService(db)
    tags = service.get_trending_tags(skip=skip, limit=limit)
    
    return TruthSocialTagsResponse(
        items=tags,
        total=len(tags),
        skip=skip,
        limit=limit,
        has_next=len(tags) == limit
    )


@router.get("/tags/growing", response_model=TruthSocialTagsResponse, summary="급성장 태그")
async def get_growing_tags(
    skip: int = Query(0, ge=0, description="건너뛸 항목 수"),
    limit: int = Query(50, ge=1, le=100, description="가져올 항목 수"),
    db: Session = Depends(get_db)
):
    """
    증가율이 높은 태그를 조회합니다.
    """
    service = TruthSocialTagService(db)
    tags = service.get_growing_tags(skip=skip, limit=limit)
    
    return TruthSocialTagsResponse(
        items=tags,
        total=len(tags),
        skip=skip,
        limit=limit,
        has_next=len(tags) == limit
    )


@router.get("/tags/market-relevant", response_model=TruthSocialTagsResponse, summary="시장 관련 태그")
async def get_market_relevant_tags(
    skip: int = Query(0, ge=0, description="건너뛸 항목 수"),
    limit: int = Query(50, ge=1, le=100, description="가져올 항목 수"),
    db: Session = Depends(get_db)
):
    """
    시장 관련성이 높은 태그를 조회합니다.
    """
    service = TruthSocialTagService(db)
    tags = service.get_market_relevant_tags(skip=skip, limit=limit)
    
    return TruthSocialTagsResponse(
        items=tags,
        total=len(tags),
        skip=skip,
        limit=limit,
        has_next=len(tags) == limit
    )


@router.get("/tags/{tag_name}/history", response_model=TruthSocialTagsResponse, summary="태그 히스토리")
async def get_tag_history(
    tag_name: str,
    limit: int = Query(10, ge=1, le=30, description="가져올 일수"),
    db: Session = Depends(get_db)
):
    """
    특정 태그의 일별 사용량 추이를 조회합니다.
    """
    service = TruthSocialTagService(db)
    tags = service.get_latest_tags_by_name(tag_name, limit=limit)
    
    return TruthSocialTagsResponse(
        items=tags,
        total=len(tags),
        skip=0,
        limit=limit,
        has_next=False
    )


# ==================== Truth Social Trends API ====================

@router.get("/trends/", response_model=TruthSocialTrendsResponse, summary="Truth Social 트렌드 목록")
async def get_truth_social_trends(
    skip: int = Query(0, ge=0, description="건너뛸 항목 수"),
    limit: int = Query(100, ge=1, le=500, description="가져올 항목 수"),
    username: Optional[str] = Query(None, description="사용자명 필터"),
    min_trend_score: Optional[float] = Query(None, ge=0, description="최소 트렌드 점수"),
    max_rank: Optional[int] = Query(None, ge=1, description="최대 순위"),
    start_date: Optional[datetime] = Query(None, description="시작 날짜"),
    end_date: Optional[datetime] = Query(None, description="종료 날짜"),
    db: Session = Depends(get_db)
):
    """
    Truth Social 트렌드 목록을 조회합니다.
    """
    filters = TruthSocialTrendFilter(
        username=username,
        min_trend_score=min_trend_score,
        max_rank=max_rank,
        start_date=start_date,
        end_date=end_date
    )
    
    service = TruthSocialTrendService(db)
    trends = service.get_trends(skip=skip, limit=limit, filters=filters)
    total = service.get_trends_count(filters=filters)
    
    return TruthSocialTrendsResponse(
        items=trends,
        total_count=total,  # total -> total_count로 변경
        skip=skip,
        limit=limit,
        has_next=skip + limit < total
    )


@router.get("/trends/top", response_model=TruthSocialTrendsResponse, summary="상위 트렌드")
async def get_top_trends(
    skip: int = Query(0, ge=0, description="건너뛸 항목 수"),
    limit: int = Query(50, ge=1, le=100, description="가져올 항목 수"),
    db: Session = Depends(get_db)
):
    """
    순위 기준 상위 트렌드를 조회합니다.
    """
    service = TruthSocialTrendService(db)
    trends = service.get_top_trends(skip=skip, limit=limit)
    
    return TruthSocialTrendsResponse(
        items=trends,
        total=len(trends),
        skip=skip,
        limit=limit,
        has_next=len(trends) == limit
    )


@router.get("/trends/high-score", response_model=TruthSocialTrendsResponse, summary="높은 점수 트렌드")
async def get_high_score_trends(
    skip: int = Query(0, ge=0, description="건너뛸 항목 수"),
    limit: int = Query(50, ge=1, le=100, description="가져올 항목 수"),
    db: Session = Depends(get_db)
):
    """
    트렌드 점수가 높은 포스트를 조회합니다.
    """
    service = TruthSocialTrendService(db)
    trends = service.get_high_score_trends(skip=skip, limit=limit)
    
    return TruthSocialTrendsResponse(
        items=trends,
        total=len(trends),
        skip=skip,
        limit=limit,
        has_next=len(trends) == limit
    )


@router.get("/trends/recent", response_model=TruthSocialTrendsResponse, summary="최근 트렌드")
async def get_recent_trends(
    skip: int = Query(0, ge=0, description="건너뛸 항목 수"),
    limit: int = Query(50, ge=1, le=100, description="가져올 항목 수"),
    db: Session = Depends(get_db)
):
    """
    최근 시간순으로 트렌드를 조회합니다.
    """
    service = TruthSocialTrendService(db)
    trends = service.get_recent_trends(skip=skip, limit=limit)
    
    return TruthSocialTrendsResponse(
        items=trends,
        total=len(trends),
        skip=skip,
        limit=limit,
        has_next=len(trends) == limit
    )


@router.get("/trends/search", response_model=TruthSocialTrendsResponse, summary="트렌드 내용 검색")
async def search_trends(
    q: str = Query(..., min_length=2, description="검색어"),
    skip: int = Query(0, ge=0, description="건너뛸 항목 수"),
    limit: int = Query(50, ge=1, le=100, description="가져올 항목 수"),
    db: Session = Depends(get_db)
):
    """
    트렌드 내용을 전문 검색합니다.
    """
    service = TruthSocialTrendService(db)
    trends = service.search_trends_by_content(q, skip=skip, limit=limit)
    
    return TruthSocialTrendsResponse(
        items=trends,
        total=len(trends),
        skip=skip,
        limit=limit,
        has_next=len(trends) == limit
    )


@router.get("/trends/{trend_id}", response_model=TruthSocialTrendResponse, summary="특정 트렌드 조회")
async def get_trend_by_id(
    trend_id: str,
    db: Session = Depends(get_db)
):
    """
    ID로 특정 트렌드를 조회합니다.
    """
    service = TruthSocialTrendService(db)
    trend = service.get_trend_by_id(trend_id)
    
    if not trend:
        raise HTTPException(status_code=404, detail="트렌드를 찾을 수 없습니다")
    
    return trend


@router.get("/trends/user/{username}", response_model=TruthSocialTrendsResponse, summary="사용자별 트렌드")
async def get_trends_by_username(
    username: str,
    skip: int = Query(0, ge=0, description="건너뛸 항목 수"),
    limit: int = Query(50, ge=1, le=100, description="가져올 항목 수"),
    db: Session = Depends(get_db)
):
    """
    특정 사용자의 트렌드 진입 포스트를 조회합니다.
    """
    service = TruthSocialTrendService(db)
    trends = service.get_trends_by_username(username, skip=skip, limit=limit)
    
    return TruthSocialTrendsResponse(
        items=trends,
        total=len(trends),
        skip=skip,
        limit=limit,
        has_next=len(trends) == limit
    )


# ==================== Truth Social Analytics API ====================

@router.get("/analytics/user/{username}", response_model=Dict[str, Any], summary="사용자 통계")
async def get_user_statistics(
    username: str,
    db: Session = Depends(get_db)
):
    """
    특정 사용자의 통계 정보를 조회합니다.
    - 총 포스트 수
    - 평균 참여도
    - 트렌드 진입 횟수
    - 참여도 비율
    """
    service = TruthSocialAnalyticsService(db)
    stats = service.get_user_statistics(username)
    
    return APIResponse(
        data=stats,
        message=f"{username} 사용자 통계 조회 완료"
    ).dict()


@router.get("/analytics/daily-posts", response_model=Dict[str, Any], summary="일별 포스트 통계")
async def get_daily_post_counts(
    days: int = Query(7, ge=1, le=30, description="조회할 일수"),
    db: Session = Depends(get_db)
):
    """
    최근 N일간 일별 포스트 수 통계를 조회합니다.
    """
    service = TruthSocialAnalyticsService(db)
    stats = service.get_daily_post_counts(days=days)
    
    return APIResponse(
        data={
            "daily_counts": stats,
            "period_days": days,
            "total_posts": sum(item["count"] for item in stats)
        },
        message=f"최근 {days}일간 일별 포스트 통계 조회 완료"
    ).dict()


@router.get("/analytics/top-hashtags", response_model=Dict[str, Any], summary="인기 해시태그")
async def get_top_hashtags(
    limit: int = Query(20, ge=5, le=50, description="조회할 태그 수"),
    db: Session = Depends(get_db)
):
    """
    최근 인기 해시태그 목록을 조회합니다.
    """
    service = TruthSocialAnalyticsService(db)
    hashtags = service.get_top_hashtags(limit=limit)
    
    return APIResponse(
        data={
            "hashtags": hashtags,
            "count": len(hashtags)
        },
        message=f"상위 {limit}개 해시태그 조회 완료"
    ).dict()


@router.get("/analytics/market-sentiment", response_model=Dict[str, Any], summary="시장 감성 요약")
async def get_market_sentiment_summary(
    db: Session = Depends(get_db)
):
    """
    시장 관련 감성 분석 요약 정보를 조회합니다.
    - 높은 시장 영향도 포스트 수
    - 시장 관련 태그 수
    - 평균 시장 영향도
    """
    service = TruthSocialAnalyticsService(db)
    summary = service.get_market_sentiment_summary()
    
    return APIResponse(
        data=summary,
        message="시장 감성 요약 조회 완료"
    ).dict()


# ==================== 헬스체크 ====================

@router.get("/health", response_model=Dict[str, Any], summary="Truth Social API 헬스체크")
async def health_check(db: Session = Depends(get_db)):
    """
    Truth Social API의 상태를 확인합니다.
    """
    try:
        # 각 테이블의 레코드 수 확인
        post_service = TruthSocialPostService(db)
        tag_service = TruthSocialTagService(db)
        trend_service = TruthSocialTrendService(db)
        
        posts_count = post_service.get_posts_count()
        tags_count = tag_service.get_tags_count()
        trends_count = trend_service.get_trends_count()
        
        return APIResponse(
            data={
                "status": "healthy",
                "database": "connected",
                "tables": {
                    "truth_social_posts": posts_count,
                    "truth_social_tags": tags_count,
                    "truth_social_trends": trends_count
                },
                "timestamp": datetime.now().isoformat()
            },
            message="Truth Social API 상태 정상"
        ).dict()
        
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"헬스체크 실패: {str(e)}"
        )