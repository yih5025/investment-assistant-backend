from sqlalchemy.orm import Session
from sqlalchemy import func, and_, desc
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta

from app.models.company_news_model import CompanyNews
from app.models.top_gainers_model import TopGainers

class CompanyNewsService:
    """
    기업 뉴스 비즈니스 로직을 처리하는 서비스 클래스
    
    주요 기능:
    1. 최신 batch_id 조회
    2. 트렌딩 주식 정보 조회
    3. 심볼별 뉴스 조회
    4. 카테고리별 뉴스 분류
    """
    
    def __init__(self, db: Session):
        self.db = db
    
    # =========================================================================
    # 유틸리티 메서드
    # =========================================================================
    
    def get_latest_batch_id(self) -> Optional[int]:
        """
        가장 최신 batch_id를 조회합니다.
        
        Returns:
            int: 최신 batch_id, 데이터가 없으면 None
        """
        result = self.db.query(func.max(TopGainers.batch_id)).scalar()
        return int(result) if result else None
    
    def get_trending_stocks(self, batch_id: int) -> List[TopGainers]:
        """
        특정 batch_id의 모든 트렌딩 주식을 조회합니다.
        
        Args:
            batch_id: 조회할 배치 ID
            
        Returns:
            List[TopGainers]: 트렌딩 주식 목록
        """
        return (self.db.query(TopGainers)
                .filter(TopGainers.batch_id == batch_id)
                .order_by(TopGainers.category, TopGainers.rank_position)
                .all())
    
    def get_trending_stocks_by_category(self, batch_id: int, category: str) -> List[TopGainers]:
        """
        특정 batch_id와 카테고리의 트렌딩 주식을 조회합니다.
        
        Args:
            batch_id: 조회할 배치 ID
            category: 카테고리 (top_gainers, top_losers, most_actively_traded)
            
        Returns:
            List[TopGainers]: 해당 카테고리의 트렌딩 주식 목록
        """
        return (self.db.query(TopGainers)
                .filter(and_(
                    TopGainers.batch_id == batch_id,
                    TopGainers.category == category
                ))
                .order_by(TopGainers.rank_position)
                .all())
    
    def get_news_by_symbols(self, symbols: List[str], days: int = 3, 
                           limit: int = 20, offset: int = 0) -> List[CompanyNews]:
        """
        여러 심볼의 뉴스를 조회합니다.
        
        Args:
            symbols: 조회할 심볼 목록
            days: 조회 기간 (일)
            limit: 최대 조회 개수
            offset: 오프셋
            
        Returns:
            List[CompanyNews]: 뉴스 목록
        """
        cutoff_date = datetime.now() - timedelta(days=days)
        
        return (self.db.query(CompanyNews)
                .filter(and_(
                    CompanyNews.symbol.in_(symbols),
                    CompanyNews.published_at >= cutoff_date
                ))
                .order_by(desc(CompanyNews.published_at))
                .limit(limit)
                .offset(offset)
                .all())
    
    def get_news_by_symbol(self, symbol: str, days: int = 3, 
                          limit: int = 20, offset: int = 0) -> List[CompanyNews]:
        """
        특정 심볼의 뉴스를 조회합니다.
        
        Args:
            symbol: 조회할 심볼
            days: 조회 기간 (일)
            limit: 최대 조회 개수
            offset: 오프셋
            
        Returns:
            List[CompanyNews]: 뉴스 목록
        """
        cutoff_date = datetime.now() - timedelta(days=days)
        
        return (self.db.query(CompanyNews)
                .filter(and_(
                    CompanyNews.symbol == symbol,
                    CompanyNews.published_at >= cutoff_date
                ))
                .order_by(desc(CompanyNews.published_at))
                .limit(limit)
                .offset(offset)
                .all())
    
    def count_news_by_symbol(self, symbol: str, days: int = 3) -> int:
        """
        특정 심볼의 뉴스 개수를 조회합니다.
        
        Args:
            symbol: 조회할 심볼
            days: 조회 기간 (일)
            
        Returns:
            int: 뉴스 개수
        """
        cutoff_date = datetime.now() - timedelta(days=days)
        
        return (self.db.query(CompanyNews)
                .filter(and_(
                    CompanyNews.symbol == symbol,
                    CompanyNews.published_at >= cutoff_date
                ))
                .count())
    
    def get_category_summary(self, trending_stocks: List[TopGainers]) -> Dict[str, int]:
        """
        카테고리별 주식 개수를 집계합니다.
        
        Args:
            trending_stocks: 트렌딩 주식 목록
            
        Returns:
            Dict[str, int]: 카테고리별 개수
        """
        summary = {
            "top_gainers": 0,
            "top_losers": 0,
            "most_actively_traded": 0
        }
        
        for stock in trending_stocks:
            if stock.category in summary:
                summary[stock.category] += 1
        
        return summary
    
    # =========================================================================
    # 메인 비즈니스 로직 메서드
    # =========================================================================
    
    def get_trending_news(self, days: int = 3, limit: int = 20, 
                         offset: int = 0) -> Tuple[Optional[Dict], List[Dict]]:
        """
        트렌딩 주식의 뉴스를 조회합니다 (전체 통합 버전).
        
        Args:
            days: 뉴스 조회 기간
            limit: 각 심볼당 최대 뉴스 개수
            offset: 오프셋
            
        Returns:
            Tuple[batch_info, stocks_with_news]: 배치 정보와 주식별 뉴스
        """
        # 1. 최신 batch_id 조회
        latest_batch_id = self.get_latest_batch_id()
        if not latest_batch_id:
            return None, []
        
        # 2. 트렌딩 주식 조회
        trending_stocks = self.get_trending_stocks(latest_batch_id)
        if not trending_stocks:
            return None, []
        
        # 3. 배치 정보 구성
        batch_info = {
            "batch_id": latest_batch_id,
            "last_updated": trending_stocks[0].last_updated,
            "total_symbols": len(trending_stocks),
            "period_days": days
        }
        
        # 4. 심볼별 뉴스 조회
        stocks_with_news = []
        symbols = [stock.symbol for stock in trending_stocks]
        
        # 각 심볼별로 뉴스 조회
        for stock in trending_stocks:
            news_list = self.get_news_by_symbol(stock.symbol, days, limit)
            
            stock_data = {
                "symbol": stock.symbol,
                "category": stock.category,
                "rank_position": int(stock.rank_position) if stock.rank_position else None,
                "price": float(stock.price) if stock.price else None,
                "change_percentage": stock.change_percentage,
                "volume": int(stock.volume) if stock.volume else None,
                "news_count": len(news_list),
                "news": news_list
            }
            stocks_with_news.append(stock_data)
        
        return batch_info, stocks_with_news
    
    def get_category_news(self, category: str, days: int = 3, 
                         limit: int = 20, offset: int = 0) -> Tuple[Optional[Dict], List[Dict]]:
        """
        특정 카테고리의 트렌딩 주식 뉴스를 조회합니다.
        
        Args:
            category: 카테고리명
            days: 뉴스 조회 기간
            limit: 각 심볼당 최대 뉴스 개수
            offset: 오프셋
            
        Returns:
            Tuple[batch_info_with_category, stocks_with_news]: 배치정보와 주식별 뉴스
        """
        # 1. 최신 batch_id 조회
        latest_batch_id = self.get_latest_batch_id()
        if not latest_batch_id:
            return None, []
        
        # 2. 해당 카테고리 트렌딩 주식 조회
        trending_stocks = self.get_trending_stocks_by_category(latest_batch_id, category)
        if not trending_stocks:
            return None, []
        
        # 3. 배치 정보 구성
        batch_info = {
            "batch_id": latest_batch_id,
            "last_updated": trending_stocks[0].last_updated,
            "total_symbols": len(trending_stocks),
            "period_days": days,
            "category": category,
            "symbols_count": len(trending_stocks)
        }
        
        # 4. 심볼별 뉴스 조회
        stocks_with_news = []
        
        for stock in trending_stocks:
            news_list = self.get_news_by_symbol(stock.symbol, days, limit)
            
            stock_data = {
                "symbol": stock.symbol,
                "category": stock.category,
                "rank_position": int(stock.rank_position) if stock.rank_position else None,
                "price": float(stock.price) if stock.price else None,
                "change_percentage": stock.change_percentage,
                "volume": int(stock.volume) if stock.volume else None,
                "news_count": len(news_list),
                "news": news_list
            }
            stocks_with_news.append(stock_data)
        
        return batch_info, stocks_with_news
    
    def get_symbol_news_with_trending_info(self, symbol: str, days: int = 3, 
                                         limit: int = 20, offset: int = 0) -> Tuple[List[CompanyNews], Dict]:
        """
        특정 심볼의 뉴스와 트렌딩 정보를 조회합니다.
        
        Args:
            symbol: 조회할 심볼
            days: 뉴스 조회 기간
            limit: 최대 뉴스 개수
            offset: 오프셋
            
        Returns:
            Tuple[news_list, trending_info]: 뉴스 목록과 트렌딩 정보
        """
        # 1. 뉴스 조회
        news_list = self.get_news_by_symbol(symbol, days, limit, offset)
        
        # 2. 최신 batch에서 트렌딩 정보 확인
        latest_batch_id = self.get_latest_batch_id()
        trending_info = {"is_trending": False}
        
        if latest_batch_id:
            trending_stock = (self.db.query(TopGainers)
                            .filter(and_(
                                TopGainers.batch_id == latest_batch_id,
                                TopGainers.symbol == symbol
                            ))
                            .first())
            
            if trending_stock:
                trending_info = {
                    "is_trending": True,
                    "batch_id": latest_batch_id,
                    "category": trending_stock.category,
                    "rank_position": int(trending_stock.rank_position) if trending_stock.rank_position else None,
                    "price": float(trending_stock.price) if trending_stock.price else None,
                    "change_percentage": trending_stock.change_percentage
                }
        
        return news_list, trending_info