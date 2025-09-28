# app/api/endpoints/sns_endpoint.py

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional, Dict, Any

from app.database import get_db
from app.services.sns_service import SNSService
from app.schemas import sns_schema

# --------------------------------------------------------------------------
# 1. 프론트엔드 분석 페이지용 API 라우터 (신규)
# 설명: 이 라우터는 프론트엔드 목업 페이지(SNSPage, SNSDetailPage)에 필요한
#       '분석된 데이터'를 제공하는 역할을 합니다.
# --------------------------------------------------------------------------
router_analysis = APIRouter(
    tags=["SNS Analysis"],
    responses={
        404: {"description": "게시물을 찾을 수 없습니다"},
        500: {"description": "서버 내부 오류 발생"}
    }
)

@router_analysis.get("/posts", response_model=List[sns_schema.SNSPostAnalysisListResponse], summary="Get Analyzed SNS Posts (Feed)")
async def get_analyzed_posts(
    skip: int = Query(0, ge=0, description="페이지네이션을 위한 오프셋 (offset)"),
    limit: int = Query(20, ge=1, le=100, description="페이지당 항목 수"),
    db: Session = Depends(get_db)
):
    """
    Airflow로 분석된 SNS 게시글 목록을 페이지네이션으로 가져옵니다.
    프론트엔드의 메인 SNS 피드 페이지에서 사용됩니다.
    성능 최적화를 위해 응답 시간을 단축했습니다.
    """
    try:
        service = SNSService(db)
        return service.get_analysis_posts(db=db, skip=skip, limit=limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"분석 게시글 목록 조회 중 오류 발생: {str(e)}")

@router_analysis.get("/posts/{post_source}/{post_id}", response_model=sns_schema.SNSPostAnalysisDetailResponse, summary="Get Analyzed SNS Post Details")
async def get_analyzed_post_detail(
    post_source: str,
    post_id: str,
    db: Session = Depends(get_db)
):
    """
    특정 게시물의 모든 상세 분석 데이터를 가져옵니다.
    프론트엔드의 SNS 상세 분석 페이지에서 사용됩니다.
    성능 최적화를 위해 응답 시간을 단축했습니다.
    """
    try:
        service = SNSService(db)
        return service.get_analysis_post_detail(db=db, post_id=post_id, post_source=post_source)
    except HTTPException as e:
        raise e # 404와 같은 의도된 예외는 그대로 전달
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"분석 게시글 상세 조회 중 오류 발생: {str(e)}")


router = APIRouter() # 기존 라우터는 prefix 없이 사용

@router.get("/authors", response_model=sns_schema.AvailableAuthorsResponse)
async def get_available_authors(db: Session = Depends(get_db)):
    """사용 가능한 작성자 목록 조회 (최근 30일 활동 기준)"""
    try:
        service = SNSService(db)
        authors = service.get_available_authors()
        # Pydantic 모델로 변환하여 응답
        return sns_schema.AvailableAuthorsResponse(**authors)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"작성자 목록 조회 중 오류 발생: {str(e)}")

@router.get("/posts", response_model=sns_schema.SNSPostsResponse)
async def get_sns_posts(
    platform: str = Query("all", description="플랫폼 (all, x, truth_social_posts, truth_social_trends)"),
    author: Optional[str] = Query(None, description="작성자 필터 (username)"),
    limit: int = Query(50, ge=1, le=100, description="페이지 크기"),
    offset: int = Query(0, ge=0, description="오프셋"),
    db: Session = Depends(get_db)
):
    """SNS 게시글 목록 조회 (최신순)"""
    valid_platforms = ["all", "x", "truth_social_posts", "truth_social_trends"]
    if platform not in valid_platforms:
        raise HTTPException(status_code=400, detail=f"Invalid platform.")
    try:
        service = SNSService(db)
        return service.get_posts(platform=platform, author=author, limit=limit, offset=offset)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"SNS 게시글 조회 중 오류 발생: {str(e)}")

@router.get("/posts/{post_id}", response_model=sns_schema.UnifiedSNSPostResponse)
async def get_sns_post_detail(
    post_id: str,
    platform: str = Query(..., description="플랫폼 (x, truth_social_posts, truth_social_trends)"),
    db: Session = Depends(get_db)
):
    """개별 SNS 게시글 상세 조회"""
    valid_platforms = ["x", "truth_social_posts", "truth_social_trends"]
    if platform not in valid_platforms:
        raise HTTPException(status_code=400, detail=f"Invalid platform for detail view.")
    try:
        service = SNSService(db)
        post = service.get_post_detail(post_id, platform)
        if not post:
            raise HTTPException(status_code=404, detail=f"게시글을 찾을 수 없습니다: {post_id} ({platform})")
        return post
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"게시글 상세 조회 중 오류 발생: {str(e)}")

@router.get("/stats")
async def get_basic_stats(db: Session = Depends(get_db)):
    """기본 통계 조회"""
    try:
        service = SNSService(db)
        return service.get_basic_stats()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"통계 조회 중 오류 발생: {str(e)}")