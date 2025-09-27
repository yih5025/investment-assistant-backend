# app/models/post_analysis_cache_model.py

from sqlalchemy import Column, Integer, String, DateTime, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class PostAnalysisCache(Base):
    """
    SQLAlchemy model for the post_analysis_cache table.
    
    This table stores the results of the market impact analysis performed
    by the Airflow DAGs on social media posts.
    """
    __tablename__ = 'post_analysis_cache'

    id = Column(Integer, primary_key=True, autoincrement=True)
    post_id = Column(String, nullable=False, index=True)
    post_source = Column(String, nullable=False, index=True)
    post_timestamp = Column(DateTime(timezone=True), nullable=False, index=True)
    author_username = Column(String)
    
    # Using JSONB for efficient querying of JSON data in PostgreSQL
    affected_assets = Column(JSONB)
    market_data = Column(JSONB)
    price_analysis = Column(JSONB)
    volume_analysis = Column(JSONB)
    
    analysis_status = Column(String, default='pending')
    error_message = Column(String)
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())

    def __repr__(self):
        return f"<PostAnalysisCache(id={self.id}, post_id='{self.post_id}', source='{self.post_source}')>"