# app/api/endpoints/sns_endpoint.py

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional, Dict, Any

from app.database import get_db
from app.services.sns_service import SNSService
from app.schemas.sns_schema import SNSPostsResponse, UnifiedSNSPostResponse

router = APIRouter()


@router.get("/authors", response_model=Dict[str, List[Dict[str, Any]]])
async def get_available_authors(db: Session = Depends(get_db)):
    """
    사용 가능한 작성자 목록 조회 (최근 30일 활동 기준)
    
    **응답 구조:**
    ```json
    {
        "x": [
            {
                "username": "elonmusk",
                "display_name": "Elon Musk", 
                "post_count": 25,
                "last_post_date": "2025-09-24T10:30:00",
                "verified": true
            }
        ],
        "truth_social_posts": [...],
        "truth_social_trends": [...]
    }
    ```
    
    **활용 방법:**
    - 프론트엔드에서 사용자 선택 버튼/드롭다운 구성
    - 게시글 수 기준으로 정렬되어 있음
    - 인증 여부 표시
    """
    try:
        service = SNSService(db)
        authors = service.get_available_authors()
        return authors
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"작성자 목록 조회 중 오류 발생: {str(e)}"
        )


@router.get("/posts", response_model=SNSPostsResponse)
async def get_sns_posts(
    platform: str = Query("all", description="플랫폼 (all, x, truth_social_posts, truth_social_trends)"),
    author: Optional[str] = Query(None, description="작성자 필터 (username)"),
    limit: int = Query(50, ge=1, le=100, description="페이지 크기"),
    offset: int = Query(0, ge=0, description="오프셋"),
    db: Session = Depends(get_db)
):
    """
    SNS 게시글 목록 조회 (최신순)
    
    **기능:**
    - 최신순 정렬만 지원 (복잡한 정렬/검색 제거)
    - 플랫폼별 필터링 (X, Truth Social)
    - 작성자별 필터링
    - 50개씩 페이지네이션
    
    **사용 예시:**
    - 전체 최신 게시글: `/api/v1/sns/posts`
    - 일론 머스크만: `/api/v1/sns/posts?author=elonmusk`
    - Truth Social만: `/api/v1/sns/posts?platform=truth_social_posts`
    - 트럼프 Truth Social: `/api/v1/sns/posts?platform=truth_social_posts&author=realDonaldTrump`
    """
    
    # 플랫폼 유효성 검사
    valid_platforms = ["all", "x", "truth_social_posts", "truth_social_trends"]
    if platform not in valid_platforms:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid platform. Must be one of: {', '.join(valid_platforms)}"
        )
    
    try:
        service = SNSService(db)
        result = service.get_posts(
            platform=platform,
            author=author,
            limit=limit,
            offset=offset
        )
        return result
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"SNS 게시글 조회 중 오류 발생: {str(e)}"
        )


@router.get("/posts/{post_id}", response_model=UnifiedSNSPostResponse)
async def get_sns_post_detail(
    post_id: str,
    platform: str = Query(..., description="플랫폼 (x, truth_social_posts, truth_social_trends)"),
    db: Session = Depends(get_db)
):
    """
    개별 SNS 게시글 상세 조회
    
    **사용 예시:**
    - X 게시글: `/api/v1/sns/posts/1234567890?platform=x`
    - Truth Social: `/api/v1/sns/posts/110123456789?platform=truth_social_posts`
    """
    
    # 플랫폼 유효성 검사
    valid_platforms = ["x", "truth_social_posts", "truth_social_trends"]
    if platform not in valid_platforms:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid platform for detail view. Must be one of: {', '.join(valid_platforms)}"
        )
    
    try:
        service = SNSService(db)
        post = service.get_post_detail(post_id, platform)
        
        if not post:
            raise HTTPException(
                status_code=404,
                detail=f"게시글을 찾을 수 없습니다: {post_id} ({platform})"
            )
        
        return post
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"게시글 상세 조회 중 오류 발생: {str(e)}"
        )


@router.get("/stats")
async def get_basic_stats(db: Session = Depends(get_db)):
    """
    기본 통계 조회
    
    **응답:**
    - 플랫폼별 총 게시글 수
    - 최근 24시간 게시글 수
    - 총 작성자 수
    """
    try:
        service = SNSService(db)
        stats = service.get_basic_stats()
        return stats
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"통계 조회 중 오류 발생: {str(e)}"
        )