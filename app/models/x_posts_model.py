# app/models/x_posts_model.py

from sqlalchemy import Column, String, Integer, DateTime, Boolean, Text, BigInteger
from sqlalchemy.dialects.postgresql import JSONB
from app.models.base import BaseModel


class XPost(BaseModel):
    """
    X Posts 테이블 모델
    
    트위터(X) 포스트 데이터를 저장하는 모델입니다.
    주요 기능:
    - 트윗 기본 정보 (ID, 내용, 작성시간)
    - 사용자 정보 (username, display_name, 팔로워 수 등)
    - 인게이지먼트 지표 (좋아요, 리트윗, 댓글 등)
    - 메타데이터 (해시태그, 멘션, URL 등)
    - 카테고리 분류 (core_investors, crypto, institutional)
    """
    
    __tablename__ = "x_posts"
    
    # 기본 트윗 정보
    tweet_id = Column(String, primary_key=True, comment="트윗 고유 ID")
    author_id = Column(String, nullable=False, comment="작성자 ID")
    text = Column(Text, nullable=False, comment="트윗 내용")
    created_at = Column(DateTime(timezone=True), nullable=False, comment="트윗 작성 시간")
    lang = Column(String, default='en', comment="언어 코드")
    
    # 소스 및 분류 정보
    source_account = Column(String, nullable=False, comment="소스 계정명")
    account_category = Column(String, default='core_investors', comment="계정 카테고리")
    collection_source = Column(String, default='primary_token', comment="수집 방식")
    
    # 인게이지먼트 지표 (핵심!)
    retweet_count = Column(Integer, default=0, comment="리트윗 수")
    reply_count = Column(Integer, default=0, comment="댓글 수") 
    like_count = Column(Integer, default=0, comment="좋아요 수")
    quote_count = Column(Integer, default=0, comment="인용 트윗 수")
    bookmark_count = Column(Integer, default=0, comment="북마크 수")
    impression_count = Column(Integer, default=0, comment="노출 수")
    
    # 콘텐츠 메타데이터 (JSONB)
    hashtags = Column(JSONB, comment="해시태그 배열")
    mentions = Column(JSONB, comment="멘션된 사용자 정보") 
    urls = Column(JSONB, comment="포함된 URL 정보")
    cashtags = Column(JSONB, comment="주식 심볼 태그")
    annotations = Column(JSONB, comment="개체명 인식 결과")
    context_annotations = Column(JSONB, comment="컨텍스트 분류")
    
    # 사용자 프로필 정보
    username = Column(String, comment="사용자명")
    display_name = Column(String, comment="표시 이름")
    user_verified = Column(Boolean, default=False, comment="인증 여부")
    user_followers_count = Column(Integer, default=0, comment="팔로워 수")
    user_following_count = Column(Integer, default=0, comment="팔로잉 수")
    user_tweet_count = Column(Integer, default=0, comment="총 트윗 수")
    
    # 시스템 메타데이터
    edit_history_tweet_ids = Column(JSONB, comment="편집 이력 ID 배열")
    collected_at = Column(DateTime, comment="수집 시간")
    updated_at = Column(DateTime, comment="업데이트 시간")
    
    def __repr__(self):
        return f"<XPost(tweet_id='{self.tweet_id}', username='{self.username}', likes={self.like_count})>"
    
    @property
    def engagement_score(self):
        """
        종합 인게이지먼트 점수 계산
        
        계산 방식:
        - 좋아요: 1점
        - 리트윗: 2점 (확산력이 높음)
        - 댓글: 3점 (상호작용이 높음)
        - 인용: 2점
        - 북마크: 1점
        
        Returns:
            int: 종합 인게이지먼트 점수
        """
        return (
            (self.like_count or 0) * 1 +
            (self.retweet_count or 0) * 2 +
            (self.reply_count or 0) * 3 +
            (self.quote_count or 0) * 2 +
            (self.bookmark_count or 0) * 1
        )
    
    @property
    def tweet_url(self):
        """
        X(트위터) 원본 트윗 URL 생성
        
        Returns:
            str: https://x.com/username/status/tweet_id 형태의 URL
        """
        if self.username and self.tweet_id:
            return f"https://x.com/{self.username}/status/{self.tweet_id}"
        return None
    
    def to_dict_basic(self):
        """기본 정보만 포함된 딕셔너리 반환 (목록 조회용)"""
        return {
            "tweet_id": self.tweet_id,
            "text": self.text,
            "username": self.username,
            "display_name": self.display_name,
            "created_at": self.created_at,
            "like_count": self.like_count,
            "retweet_count": self.retweet_count,
            "reply_count": self.reply_count,
            "tweet_url": self.tweet_url
        }
    
    def to_dict_full(self):
        """전체 정보가 포함된 딕셔너리 반환 (상세 조회용)"""
        return {
            "tweet_id": self.tweet_id,
            "author_id": self.author_id,
            "text": self.text,
            "created_at": self.created_at,
            "lang": self.lang,
            "author": {
                "username": self.username,
                "display_name": self.display_name,
                "verified": self.user_verified,
                "followers_count": self.user_followers_count,
                "following_count": self.user_following_count,
                "tweet_count": self.user_tweet_count
            },
            "engagement": {
                "likes": self.like_count,
                "retweets": self.retweet_count,
                "replies": self.reply_count,
                "quotes": self.quote_count,
                "bookmarks": self.bookmark_count,
                "impressions": self.impression_count,
                "engagement_score": self.engagement_score
            },
            "metadata": {
                "hashtags": self.hashtags,
                "mentions": self.mentions,
                "urls": self.urls,
                "cashtags": self.cashtags
            },
            "classification": {
                "account_category": self.account_category,
                "collection_source": self.collection_source,
                "source_account": self.source_account
            },
            "tweet_url": self.tweet_url
        }