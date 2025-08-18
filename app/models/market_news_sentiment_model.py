from sqlalchemy import Column, BigInteger, Text, DateTime, Numeric, func
from sqlalchemy.dialects.postgresql import JSONB
from app.models.base import BaseModel

class MarketNewsSentiment(BaseModel):
    """
    시장 뉴스 감성 분석 테이블 모델
    
    이 모델은 PostgreSQL의 market_news_sentiment 테이블과 매핑됩니다.
    - 복합 기본키: (batch_id, url)
    - JSONB 필드: ticker_sentiment, topics
    - 감성 분석 데이터: overall_sentiment_score, overall_sentiment_label
    """
    __tablename__ = "market_news_sentiment"
    
    # 복합 기본키
    batch_id = Column(BigInteger, primary_key=True, nullable=False,
                     comment="배치 ID (수집 단위)")
    url = Column(Text, primary_key=True, nullable=False,
                comment="뉴스 기사 URL (중복 방지용)")
    
    # 뉴스 기본 정보
    title = Column(Text, nullable=False, comment="뉴스 제목")
    time_published = Column(DateTime, nullable=False, comment="뉴스 발행 시간")
    authors = Column(Text, comment="작성자")
    summary = Column(Text, comment="뉴스 요약")
    source = Column(Text, comment="뉴스 소스")
    
    # 감성 분석 데이터
    overall_sentiment_score = Column(Numeric(5, 4), 
                                   comment="전체 감성 점수 (-1.0 ~ +1.0)")
    overall_sentiment_label = Column(Text, 
                                   comment="전체 감성 라벨 (Bullish/Bearish/Neutral)")
    
    # JSONB 데이터 (복합 정보)
    ticker_sentiment = Column(JSONB, 
                            comment="주식별 감성 분석 데이터 (JSON 배열)")
    topics = Column(JSONB, 
                   comment="주제별 관련도 데이터 (JSON 배열)")
    
    # 쿼리 메타데이터
    query_type = Column(Text, nullable=False,
                       comment="쿼리 타입 (예: energy_general, technology_relevant)")
    query_params = Column(Text, comment="쿼리 파라미터")
    
    # 시간 정보
    created_at = Column(DateTime, default=func.now(), comment="데이터 생성 시간")
    
    def __repr__(self):
        return f"<MarketNewsSentiment(batch_id={self.batch_id}, title='{self.title[:50]}...', sentiment={self.overall_sentiment_score})>"
    
    @property
    def sentiment_interpretation(self):
        """감성 점수를 해석 가능한 문자열로 변환"""
        if not self.overall_sentiment_score:
            return "알 수 없음"
        
        score = float(self.overall_sentiment_score)
        if score >= 0.3:
            return "매우긍정적"
        elif score >= 0.1:
            return "긍정적"
        elif score >= -0.1:
            return "중립적"
        elif score >= -0.3:
            return "부정적"
        else:
            return "매우부정적"
    
    @property
    def sentiment_emoji(self):
        """감성 점수를 이모지로 변환"""
        if not self.overall_sentiment_score:
            return "❓"
        
        score = float(self.overall_sentiment_score)
        if score >= 0.3:
            return "🚀"
        elif score >= 0.1:
            return "📈"
        elif score >= -0.1:
            return "➡️"
        elif score >= -0.3:
            return "📉"
        else:
            return "🔻"