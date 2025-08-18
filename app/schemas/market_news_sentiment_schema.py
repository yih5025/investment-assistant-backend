from pydantic import BaseModel, Field
from datetime import datetime
from typing import List, Optional, Dict, Any, Union
from decimal import Decimal

# =============================================================================
# 기본 JSONB 스키마들 (ticker_sentiment, topics 파싱용)
# =============================================================================

class TickerSentiment(BaseModel):
    """개별 티커 감성 정보"""
    ticker: str = Field(..., description="주식 심볼", example="AAPL")
    relevance_score: float = Field(..., description="관련도 점수 (0.0-1.0)")
    ticker_sentiment_label: str = Field(..., description="티커별 감성 라벨")
    ticker_sentiment_score: float = Field(..., description="티커별 감성 점수")

class TopicRelevance(BaseModel):
    """주제별 관련도 정보"""
    topic: str = Field(..., description="주제명", example="Technology")
    relevance_score: float = Field(..., description="관련도 점수 (0.0-1.0)")

# =============================================================================
# 기본 뉴스 스키마
# =============================================================================

class MarketNewsSentimentBase(BaseModel):
    """시장 뉴스 감성 분석 기본 스키마"""
    batch_id: int = Field(..., description="배치 ID")
    url: str = Field(..., description="뉴스 URL")
    title: str = Field(..., description="뉴스 제목")
    time_published: datetime = Field(..., description="뉴스 발행 시간")
    authors: Optional[str] = Field(None, description="작성자")
    summary: Optional[str] = Field(None, description="뉴스 요약")
    source: Optional[str] = Field(None, description="뉴스 소스")
    
    # 감성 분석 데이터
    overall_sentiment_score: Optional[float] = Field(None, description="전체 감성 점수")
    overall_sentiment_label: Optional[str] = Field(None, description="전체 감성 라벨")
    
    # 쿼리 정보
    query_type: str = Field(..., description="쿼리 타입")
    query_params: Optional[str] = Field(None, description="쿼리 파라미터")
    created_at: Optional[datetime] = Field(None, description="생성 시간")

class MarketNewsSentimentResponse(MarketNewsSentimentBase):
    """시장 뉴스 감성 분석 응답 스키마 (JSONB 파싱 포함)"""
    
    # 감성 해석 필드들
    sentiment_interpretation: Optional[str] = Field(None, description="감성 해석")
    sentiment_emoji: Optional[str] = Field(None, description="감성 이모지")
    
    # JSONB 데이터 파싱 결과
    related_tickers: List[TickerSentiment] = Field([], description="관련 티커 목록")
    related_topics: List[TopicRelevance] = Field([], description="관련 주제 목록")
    
    class Config:
        from_attributes = True

# =============================================================================
# 랭킹 및 집계 스키마들
# =============================================================================

class TopicRanking(BaseModel):
    """주제별 감성 랭킹"""
    topic: str = Field(..., description="주제명")
    avg_sentiment_score: float = Field(..., description="평균 감성 점수")
    news_count: int = Field(..., description="뉴스 개수")
    sentiment_label: str = Field(..., description="감성 라벨")
    sentiment_emoji: str = Field(..., description="감성 이모지")
    trend: str = Field(..., description="트렌드 (상승/하락/안정)")
    related_tickers: List[str] = Field([], description="관련 티커들")

class TickerRanking(BaseModel):
    """티커별 감성 랭킹"""
    ticker: str = Field(..., description="주식 심볼")
    avg_sentiment_score: float = Field(..., description="평균 감성 점수")
    sentiment_label: str = Field(..., description="감성 라벨")
    sentiment_emoji: str = Field(..., description="감성 이모지")
    mention_count: int = Field(..., description="언급 횟수")
    avg_relevance_score: float = Field(..., description="평균 관련도 점수")
    related_topics: List[str] = Field([], description="관련 주제들")

class BatchInfo(BaseModel):
    """배치 정보"""
    latest_batch_id: int = Field(..., description="최신 배치 ID")
    collection_date: str = Field(..., description="수집 날짜")
    total_batches: int = Field(..., description="총 배치 수")

# =============================================================================
# API 응답 스키마들
# =============================================================================

class MarketSentimentListResponse(BaseModel):
    """뉴스 목록 응답"""
    total_count: int = Field(..., description="총 뉴스 개수")
    batch_info: BatchInfo = Field(..., description="배치 정보")
    news: List[MarketNewsSentimentResponse] = Field(..., description="뉴스 목록")

class TopicListResponse(BaseModel):
    """사용 가능한 주제 목록 응답"""
    total_topics: int = Field(..., description="총 주제 개수")
    topics: List[str] = Field(..., description="주제 목록")
    topic_details: List[Dict[str, Any]] = Field([], description="주제별 상세 정보")

class TickerListResponse(BaseModel):
    """언급된 티커 목록 응답"""
    total_tickers: int = Field(..., description="총 티커 개수")
    tickers: List[str] = Field(..., description="티커 목록")
    ticker_details: List[Dict[str, Any]] = Field([], description="티커별 상세 정보")

class TopicRankingResponse(BaseModel):
    """주제별 감성 랭킹 응답"""
    ranking_period: str = Field(..., description="랭킹 기간")
    total_topics: int = Field(..., description="총 주제 개수")
    hot_topics: List[TopicRanking] = Field(..., description="긍정적 주제들")
    cold_topics: List[TopicRanking] = Field(..., description="부정적 주제들")

class TickerRankingResponse(BaseModel):
    """티커별 감성 랭킹 응답"""
    ranking_period: str = Field(..., description="랭킹 기간")
    total_tickers: int = Field(..., description="총 티커 개수")
    hot_tickers: List[TickerRanking] = Field(..., description="긍정적 티커들")
    cold_tickers: List[TickerRanking] = Field(..., description="부정적 티커들")

class TopicNewsResponse(BaseModel):
    """특정 주제 뉴스 응답"""
    topic: str = Field(..., description="주제명")
    topic_sentiment_summary: Dict[str, Any] = Field(..., description="주제별 감성 요약")
    news: List[MarketNewsSentimentResponse] = Field(..., description="관련 뉴스")

class TickerNewsResponse(BaseModel):
    """특정 티커 뉴스 응답"""
    ticker: str = Field(..., description="티커 심볼")
    ticker_sentiment_summary: Dict[str, Any] = Field(..., description="티커별 감성 요약")
    news: List[MarketNewsSentimentResponse] = Field(..., description="관련 뉴스")

class CrossAnalysisResponse(BaseModel):
    """크로스 분석 응답 (주제↔티커)"""
    analysis_type: str = Field(..., description="분석 타입")
    primary_key: str = Field(..., description="기준 키 (주제 또는 티커)")
    related_items: List[Dict[str, Any]] = Field(..., description="관련 항목들")
    sentiment_summary: Dict[str, Any] = Field(..., description="감성 요약")

# =============================================================================
# 쿼리 파라미터 스키마
# =============================================================================

class SentimentQueryParams(BaseModel):
    """감성 뉴스 조회 쿼리 파라미터"""
    days: int = Field(7, ge=1, le=30, description="최근 N일 데이터 (1-30일)")
    limit: int = Field(20, ge=1, le=100, description="결과 개수 제한")
    offset: int = Field(0, ge=0, description="페이징 오프셋")
    
    # 감정 필터
    min_sentiment: Optional[float] = Field(None, ge=-1.0, le=1.0, description="최소 감성 점수")
    max_sentiment: Optional[float] = Field(None, ge=-1.0, le=1.0, description="최대 감성 점수")
    sentiment_labels: Optional[str] = Field(None, description="감성 라벨 필터 (쉼표 구분)")
    
    # 관련도 필터
    min_relevance: Optional[float] = Field(None, ge=0.0, le=1.0, description="최소 관련도 점수")
    
    # 정렬
    sort_by: str = Field("time_published", description="정렬 기준")
    order: str = Field("desc", pattern="^(asc|desc)$", description="정렬 순서")

class RankingQueryParams(BaseModel):
    """랭킹 조회 쿼리 파라미터"""
    days: int = Field(7, ge=1, le=30, description="랭킹 계산 기간")
    top_count: int = Field(10, ge=1, le=50, description="상위 항목 개수")
    bottom_count: int = Field(10, ge=1, le=50, description="하위 항목 개수")
    min_mentions: int = Field(2, ge=1, description="최소 언급 횟수")

# =============================================================================
# 감정 점수 추이 스키마 (프론트엔드 차트용)
# =============================================================================

class SentimentTrendPoint(BaseModel):
    """시간대별 감정 점수 데이터 포인트"""
    timestamp: datetime = Field(..., description="시간")
    avg_sentiment_score: float = Field(..., description="평균 감정 점수")
    news_count: int = Field(..., description="해당 시간대 뉴스 개수")
    bullish_count: int = Field(..., description="긍정적 뉴스 개수")
    bearish_count: int = Field(..., description="부정적 뉴스 개수")
    neutral_count: int = Field(..., description="중립적 뉴스 개수")

class TickerSentimentTrend(BaseModel):
    """티커별 감정 점수 추이"""
    ticker: str = Field(..., description="주식 심볼")
    trend_data: List[SentimentTrendPoint] = Field(..., description="시간대별 감정 점수")

class TopicSentimentTrend(BaseModel):
    """주제별 감정 점수 추이"""
    topic: str = Field(..., description="주제명")
    trend_data: List[SentimentTrendPoint] = Field(..., description="시간대별 감정 점수")

class SentimentTrendsResponse(BaseModel):
    """감정 점수 추이 응답 (차트용)"""
    period: str = Field(..., description="분석 기간")
    interval: str = Field(..., description="시간 간격 (hourly/daily)")
    overall_trend: List[SentimentTrendPoint] = Field(..., description="전체 감정 점수 추이")
    ticker_trends: List[TickerSentimentTrend] = Field([], description="티커별 추이 (요청 시)")
    topic_trends: List[TopicSentimentTrend] = Field([], description="주제별 추이 (요청 시)")