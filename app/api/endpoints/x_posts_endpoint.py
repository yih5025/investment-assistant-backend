# app/api/endpoints/x_posts_endpoint.py

from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.dependencies import get_db
from app.services.x_posts_service import XPostsService
from app.schemas.x_posts_schema import (
    # Request schemas
    XPostListRequest, XPostSearchRequest, XPostRankingRequest, UserPostsRequest,
    AccountCategory, SortBy, SortOrder,
    # Response schemas
    XPostListResponse, XPostDetailed, XPostBasic, XPostRankingResponse,
    CategoryUsersResponse, UserStatsResponse, CategoryStatsResponse, OverviewStatsResponse,
    UserInfo
)

# X Posts 라우터 생성
router = APIRouter(
    tags=["X Posts"],
    responses={
        404: {"description": "요청한 데이터를 찾을 수 없습니다"},
        422: {"description": "잘못된 요청 파라미터"},
        500: {"description": "서버 내부 오류"}
    }
)


# ===== 기본 포스트 조회 API =====

@router.get("/", response_model=XPostListResponse, summary="전체 포스트 목록 조회")
async def get_posts_list(
    limit: int = Query(20, ge=1, le=100, description="조회할 포스트 수"),
    offset: int = Query(0, ge=0, description="건너뛸 포스트 수"),
    category: Optional[AccountCategory] = Query(None, description="계정 카테고리 필터"),
    sort_by: SortBy = Query(SortBy.CREATED_AT, description="정렬 기준"),
    order: SortOrder = Query(SortOrder.DESC, description="정렬 순서"),
    verified_only: bool = Query(False, description="인증된 사용자만 조회"),
    db: Session = Depends(get_db)
):
    """
    전체 X Posts 목록을 조회합니다.
    
    **기능:**
    - 페이징 지원 (limit, offset)
    - 카테고리별 필터링 (core_investors, crypto, institutional)
    - 다양한 정렬 옵션 (작성시간, 좋아요수, 리트윗수 등)
    - 인증된 사용자만 필터링 옵션
    
    **사용 예시:**
    - `/api/v1/x-posts/?limit=10&category=crypto&sort_by=like_count`
    - `/api/v1/x-posts/?verified_only=true&order=asc`
    """
    service = XPostsService(db)
    request = XPostListRequest(
        limit=limit,
        offset=offset, 
        category=category,
        sort_by=sort_by,
        order=order,
        verified_only=verified_only
    )
    
    posts, total_count = service.get_posts_list(request)
    
    return XPostListResponse(
        items=[XPostBasic.model_validate(post) for post in posts],
        total_count=total_count,
        limit=limit,
        offset=offset,
        has_more=offset + limit < total_count
    )


@router.get("/recent/", response_model=List[XPostBasic], summary="최신 포스트 조회")
async def get_recent_posts(
    limit: int = Query(10, ge=1, le=50, description="조회할 포스트 수"),
    category: Optional[AccountCategory] = Query(None, description="계정 카테고리 필터"),
    db: Session = Depends(get_db)
):
    """
    최신 X Posts를 조회합니다.
    
    **기능:**
    - 작성시간 내림차순으로 최신 포스트 반환
    - 카테고리별 필터링 가능
    - 기본 10개, 최대 50개까지 조회 가능
    
    **사용 예시:**
    - `/api/v1/x-posts/recent/?limit=5&category=core_investors`
    """
    service = XPostsService(db)
    posts = service.get_recent_posts(limit=limit, category=category)
    
    return [XPostBasic.model_validate(post) for post in posts]


@router.get("/categories/{category}/", response_model=XPostListResponse, summary="카테고리별 포스트 조회")
async def get_posts_by_category(
    category: AccountCategory,
    limit: int = Query(20, ge=1, le=100, description="조회할 포스트 수"),
    offset: int = Query(0, ge=0, description="건너뛸 포스트 수"),
    db: Session = Depends(get_db)
):
    """
    특정 카테고리의 포스트를 조회합니다.
    
    **카테고리 종류:**
    - `core_investors`: 주요 투자자 (elonmusk 등)
    - `crypto`: 암호화폐 관련 (saylor, brian_armstrong 등)
    - `institutional`: 기관/기업 관련 (mcuban 등)
    
    **사용 예시:**
    - `/api/v1/x-posts/categories/crypto/?limit=15`
    """
    service = XPostsService(db)
    posts, total_count = service.get_posts_by_category(category, limit=limit, offset=offset)
    
    return XPostListResponse(
        items=[XPostBasic.model_validate(post) for post in posts],
        total_count=total_count,
        limit=limit,
        offset=offset,
        has_more=offset + limit < total_count
    )


# ===== 사용자별 포스트 API =====

@router.get("/users/{username}/", response_model=XPostListResponse, summary="특정 사용자 포스트 조회")
async def get_user_posts(
    username: str,
    limit: int = Query(20, ge=1, le=100, description="조회할 포스트 수"),
    offset: int = Query(0, ge=0, description="건너뛸 포스트 수"),
    sort_by: SortBy = Query(SortBy.CREATED_AT, description="정렬 기준"),
    order: SortOrder = Query(SortOrder.DESC, description="정렬 순서"),
    db: Session = Depends(get_db)
):
    """
    특정 사용자의 모든 포스트를 조회합니다.
    
    **기능:**
    - 사용자명으로 포스트 검색
    - 다양한 정렬 옵션 (최신순, 좋아요순, 리트윗순 등)
    - 페이징 지원
    
    **사용 예시:**
    - `/api/v1/x-posts/users/elonmusk/?sort_by=like_count&limit=5`
    - `/api/v1/x-posts/users/saylor/?sort_by=engagement_score`
    """
    service = XPostsService(db)
    request = UserPostsRequest(
        limit=limit,
        offset=offset,
        sort_by=sort_by,
        order=order
    )
    
    posts, total_count = service.get_user_posts(username, request)
    
    if not posts and total_count == 0:
        raise HTTPException(status_code=404, detail=f"사용자 '{username}'을 찾을 수 없습니다.")
    
    return XPostListResponse(
        items=[XPostBasic.model_validate(post) for post in posts],
        total_count=total_count,
        limit=limit,
        offset=offset,
        has_more=offset + limit < total_count
    )


@router.get("/users/{username}/recent/", response_model=List[XPostBasic], summary="특정 사용자 최신 포스트")
async def get_user_recent_posts(
    username: str,
    limit: int = Query(10, ge=1, le=50, description="조회할 포스트 수"),
    db: Session = Depends(get_db)
):
    """
    특정 사용자의 최신 포스트를 조회합니다.
    
    **기능:**
    - 해당 사용자의 가장 최근 포스트들
    - 시간순 내림차순 정렬
    
    **사용 예시:**
    - `/api/v1/x-posts/users/elonmusk/recent/?limit=5`
    """
    service = XPostsService(db)
    posts = service.get_user_recent_posts(username, limit=limit)
    
    if not posts:
        raise HTTPException(status_code=404, detail=f"사용자 '{username}'의 포스트를 찾을 수 없습니다.")
    
    return [XPostBasic.model_validate(post) for post in posts]


@router.get("/categories/{category}/users/", response_model=CategoryUsersResponse, summary="카테고리별 사용자 目록")
async def get_category_users(
    category: AccountCategory,
    db: Session = Depends(get_db)
):
    """
    특정 카테고리의 사용자 목록과 통계를 조회합니다.
    
    **제공 정보:**
    - 카테고리별 사용자 목록
    - 각 사용자의 기본 통계 (포스트 수, 평균 인게이지먼트)
    - 팔로워 수, 인증 여부 등
    
    **사용 예시:**
    - `/api/v1/x-posts/categories/crypto/users/`
    """
    service = XPostsService(db)
    users_data = service.get_category_users(category)
    
    return CategoryUsersResponse(
        category=category,
        users=[UserInfo(**user) for user in users_data],
        total_count=len(users_data)
    )


# ===== 랭킹 API =====

@router.get("/ranking/most-liked/", response_model=XPostRankingResponse, summary="좋아요 수 랭킹")
async def get_most_liked_posts(
    limit: int = Query(5, ge=1, le=50, description="조회할 포스트 수"),
    category: Optional[AccountCategory] = Query(None, description="계정 카테고리 필터"),
    period: str = Query("24h", pattern="^(1h|6h|24h|7d|30d)$", description="기간 필터"),
    db: Session = Depends(get_db)
):
    """
    좋아요 수가 가장 많은 포스트 랭킹을 조회합니다.
    
    **기간 옵션:**
    - `1h`: 최근 1시간
    - `6h`: 최근 6시간  
    - `24h`: 최근 24시간 (기본값)
    - `7d`: 최근 7일
    - `30d`: 최근 30일
    """
    service = XPostsService(db)
    request = XPostRankingRequest(limit=limit, category=category, period=period)
    
    posts = service.get_ranking_posts('most-liked', request)
    
    return XPostRankingResponse(
        items=[XPostDetailed.model_validate(post.to_dict_full()) for post in posts],
        ranking_type="most-liked",
        period=period,
        total_count=len(posts)
    )


@router.get("/ranking/most-retweeted/", response_model=XPostRankingResponse, summary="리트윗 수 랭킹")
async def get_most_retweeted_posts(
    limit: int = Query(5, ge=1, le=50, description="조회할 포스트 수"),
    category: Optional[AccountCategory] = Query(None, description="계정 카테고리 필터"),
    period: str = Query("24h", pattern="^(1h|6h|24h|7d|30d)$", description="기간 필터"),
    db: Session = Depends(get_db)
):
    """리트윗 수가 가장 많은 포스트 랭킹을 조회합니다."""
    service = XPostsService(db)
    request = XPostRankingRequest(limit=limit, category=category, period=period)
    
    posts = service.get_ranking_posts('most-retweeted', request)
    
    return XPostRankingResponse(
        items=[XPostDetailed.model_validate(post.to_dict_full()) for post in posts],
        ranking_type="most-retweeted", 
        period=period,
        total_count=len(posts)
    )


@router.get("/ranking/most-replied/", response_model=XPostRankingResponse, summary="댓글 수 랭킹")
async def get_most_replied_posts(
    limit: int = Query(5, ge=1, le=50, description="조회할 포스트 수"),
    category: Optional[AccountCategory] = Query(None, description="계정 카테고리 필터"),
    period: str = Query("24h", pattern="^(1h|6h|24h|7d|30d)$", description="기간 필터"),
    db: Session = Depends(get_db)
):
    """댓글 수가 가장 많은 포스트 랭킹을 조회합니다."""
    service = XPostsService(db)
    request = XPostRankingRequest(limit=limit, category=category, period=period)
    
    posts = service.get_ranking_posts('most-replied', request)
    
    return XPostRankingResponse(
        items=[XPostDetailed.model_validate(post.to_dict_full()) for post in posts],
        ranking_type="most-replied",
        period=period,
        total_count=len(posts)
    )


@router.get("/ranking/most-quoted/", response_model=XPostRankingResponse, summary="인용 수 랭킹")  
async def get_most_quoted_posts(
    limit: int = Query(5, ge=1, le=50, description="조회할 포스트 수"),
    category: Optional[AccountCategory] = Query(None, description="계정 카테고리 필터"),
    period: str = Query("24h", pattern="^(1h|6h|24h|7d|30d)$", description="기간 필터"),
    db: Session = Depends(get_db)
):
    """인용 수가 가장 많은 포스트 랭킹을 조회합니다."""
    service = XPostsService(db)
    request = XPostRankingRequest(limit=limit, category=category, period=period)
    
    posts = service.get_ranking_posts('most-quoted', request)
    
    return XPostRankingResponse(
        items=[XPostDetailed.model_validate(post.to_dict_full()) for post in posts],
        ranking_type="most-quoted",
        period=period,
        total_count=len(posts)
    )


@router.get("/ranking/most-bookmarked/", response_model=XPostRankingResponse, summary="북마크 수 랭킹")
async def get_most_bookmarked_posts(
    limit: int = Query(5, ge=1, le=50, description="조회할 포스트 수"),
    category: Optional[AccountCategory] = Query(None, description="계정 카테고리 필터"),
    period: str = Query("24h", pattern="^(1h|6h|24h|7d|30d)$", description="기간 필터"),
    db: Session = Depends(get_db)
):
    """북마크 수가 가장 많은 포스트 랭킹을 조회합니다."""
    service = XPostsService(db)
    request = XPostRankingRequest(limit=limit, category=category, period=period)
    
    posts = service.get_ranking_posts('most-bookmarked', request)
    
    return XPostRankingResponse(
        items=[XPostDetailed.model_validate(post.to_dict_full()) for post in posts],
        ranking_type="most-bookmarked",
        period=period,
        total_count=len(posts)
    )


@router.get("/ranking/most-viewed/", response_model=XPostRankingResponse, summary="노출 수 랭킹")
async def get_most_viewed_posts(
    limit: int = Query(5, ge=1, le=50, description="조회할 포스트 수"),
    category: Optional[AccountCategory] = Query(None, description="계정 카테고리 필터"),
    period: str = Query("24h", pattern="^(1h|6h|24h|7d|30d)$", description="기간 필터"),
    db: Session = Depends(get_db)
):
    """노출 수(impression)가 가장 많은 포스트 랭킹을 조회합니다."""
    service = XPostsService(db)
    request = XPostRankingRequest(limit=limit, category=category, period=period)
    
    posts = service.get_ranking_posts('most-viewed', request)
    
    return XPostRankingResponse(
        items=[XPostDetailed.model_validate(post.to_dict_full()) for post in posts],
        ranking_type="most-viewed",
        period=period,
        total_count=len(posts)
    )


@router.get("/ranking/most-engaged/", response_model=XPostRankingResponse, summary="종합 인게이지먼트 랭킹")
async def get_most_engaged_posts(
    limit: int = Query(5, ge=1, le=50, description="조회할 포스트 수"),
    category: Optional[AccountCategory] = Query(None, description="계정 카테고리 필터"),
    period: str = Query("24h", pattern="^(1h|6h|24h|7d|30d)$", description="기간 필터"),
    db: Session = Depends(get_db)
):
    """
    종합 인게이지먼트 점수가 가장 높은 포스트 랭킹을 조회합니다.
    
    **점수 계산 방식:**
    - 좋아요: 1점
    - 리트윗: 2점 (확산력)
    - 댓글: 3점 (상호작용)
    - 인용: 2점
    - 북마크: 1점
    """
    service = XPostsService(db)
    request = XPostRankingRequest(limit=limit, category=category, period=period)
    
    posts = service.get_ranking_posts('most-engaged', request)
    
    return XPostRankingResponse(
        items=[XPostDetailed.model_validate(post.to_dict_full()) for post in posts],
        ranking_type="most-engaged",
        period=period,
        total_count=len(posts)
    )


# ===== 검색 API =====

@router.get("/search/", response_model=XPostListResponse, summary="텍스트 검색")
async def search_posts(
    q: str = Query(..., min_length=1, max_length=100, description="검색 키워드"),
    limit: int = Query(20, ge=1, le=100, description="조회할 포스트 수"),
    category: Optional[AccountCategory] = Query(None, description="계정 카테고리 필터"),
    sort_by: SortBy = Query(SortBy.CREATED_AT, description="정렬 기준"),
    order: SortOrder = Query(SortOrder.DESC, description="정렬 순서"),
    db: Session = Depends(get_db)
):
    """
    포스트 내용에서 키워드를 검색합니다.
    
    **검색 기능:**
    - 트윗 내용(text)에서 키워드 검색
    - 대소문자 구분 없음
    - 부분 일치 검색
    - 카테고리별 필터링 가능
    
    **사용 예시:**
    - `/api/v1/x-posts/search/?q=bitcoin&category=crypto`
    - `/api/v1/x-posts/search/?q=AI&sort_by=like_count`
    """
    service = XPostsService(db)
    request = XPostSearchRequest(
        q=q,
        limit=limit,
        category=category,
        sort_by=sort_by,
        order=order
    )
    
    posts, total_count = service.search_posts(request)
    
    return XPostListResponse(
        items=[XPostBasic.model_validate(post) for post in posts],
        total_count=total_count,
        limit=limit,
        offset=0,
        has_more=len(posts) == limit
    )


@router.get("/search/mentions/{username}/", response_model=List[XPostBasic], summary="멘션 검색")
async def search_mentions(
    username: str,
    limit: int = Query(20, ge=1, le=100, description="조회할 포스트 수"),
    db: Session = Depends(get_db)
):
    """
    특정 사용자가 멘션(@사용자명)된 포스트를 검색합니다.
    
    **멘션이란:**
    - 트윗에서 "@사용자명" 형태로 다른 사용자를 언급하는 것
    - 예: "@elonmusk Tesla is great!" → elonmusk가 멘션됨
    
    **활용 사례:**
    - 특정 인물에 대한 언급 추적
    - 브랜드/기업 모니터링
    - 영향력 있는 인물들의 언급 분석
    
    **사용 예시:**
    - `/api/v1/x-posts/search/mentions/elonmusk/?limit=10`
    - `/api/v1/x-posts/search/mentions/Tesla/`
    """
    service = XPostsService(db)
    posts = service.search_mentions(username, limit=limit)
    
    if not posts:
        raise HTTPException(
            status_code=404, 
            detail=f"'{username}' 사용자에 대한 멘션을 찾을 수 없습니다."
        )
    
    return [XPostBasic.model_validate(post) for post in posts]


# ===== 통계 API =====

@router.get("/stats/users/{username}/", response_model=UserStatsResponse, summary="특정 사용자 통계")
async def get_user_stats(
    username: str,
    db: Session = Depends(get_db)
):
    """
    특정 사용자의 상세 통계를 조회합니다.
    
    **제공 통계:**
    - 총 포스트 수
    - 총 좋아요, 리트윗, 댓글 수
    - 포스트당 평균 인게이지먼트
    - 가장 인기 있었던 포스트
    - 팔로워 수
    
    **사용 예시:**
    - `/api/v1/x-posts/stats/users/elonmusk/`
    - `/api/v1/x-posts/stats/users/saylor/`
    """
    service = XPostsService(db)
    stats = service.get_user_stats(username)
    
    if not stats:
        raise HTTPException(status_code=404, detail=f"사용자 '{username}'을 찾을 수 없습니다.")
    
    return UserStatsResponse(**stats)


@router.get("/stats/categories/{category}/", response_model=CategoryStatsResponse, summary="카테고리별 통계")
async def get_category_stats(
    category: AccountCategory,
    db: Session = Depends(get_db)
):
    """
    특정 카테고리의 통계를 조회합니다.
    
    **제공 통계:**
    - 카테고리 내 총 사용자 수
    - 총 포스트 수
    - 포스트당 평균 인게이지먼트
    - 상위 사용자 목록 (인게이지먼트 기준)
    
    **사용 예시:**
    - `/api/v1/x-posts/stats/categories/crypto/`
    - `/api/v1/x-posts/stats/categories/core_investors/`
    """
    service = XPostsService(db)
    stats = service.get_category_stats(category)
    
    return CategoryStatsResponse(**stats)


@router.get("/stats/overview/", response_model=OverviewStatsResponse, summary="전체 통계 개요")
async def get_overview_stats(
    db: Session = Depends(get_db)
):
    """
    전체 X Posts 데이터의 통계 개요를 조회합니다.
    
    **제공 통계:**
    - 전체 포스트 수
    - 전체 사용자 수  
    - 카테고리별 포스트 분포
    - 최근 24시간 포스트 수
    - 전체 평균 인게이지먼트
    - 오늘의 인기 포스트
    
    **활용:**
    - 대시보드 메인 화면
    - 전체 현황 파악
    - 데이터 품질 모니터링
    """
    service = XPostsService(db)
    stats = service.get_overview_stats()
    
    return OverviewStatsResponse(**stats)