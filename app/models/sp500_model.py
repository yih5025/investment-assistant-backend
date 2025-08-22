# app/models/sp500_model.py
from sqlalchemy import Column, Integer, String, Numeric, BigInteger, DateTime, Text, ARRAY, Index
from sqlalchemy.sql import func
from sqlalchemy.orm import Session
from typing import List, Optional, Dict, Any, Tuple
import logging
from datetime import datetime, timedelta

from app.models.base import BaseModel

logger = logging.getLogger(__name__)

class SP500WebsocketTrades(BaseModel):
    """
    SP500 WebSocket 거래 데이터 테이블 ORM 모델
    
    실제 테이블명: sp500_websocket_trades
    Finnhub WebSocket에서 수집한 SP500 실시간 거래 데이터를 저장합니다.
    """
    __tablename__ = "sp500_websocket_trades"
    
    # Primary Key
    id = Column(Integer, primary_key=True, autoincrement=True,
               comment="자동 증가 Primary Key")
    
    # 기본 거래 정보
    symbol = Column(String(10), nullable=False, index=True,
                   comment="주식 심볼 (예: AAPL, TSLA)")
    price = Column(Numeric(10, 4), nullable=False,
                  comment="거래 가격 ($)")
    volume = Column(BigInteger, nullable=True,
                   comment="거래량")
    timestamp_ms = Column(BigInteger, nullable=False, index=True,
                         comment="거래 시간 (밀리초 타임스탬프)")
    
    # 거래 조건 및 메타데이터
    trade_conditions = Column(ARRAY(String), nullable=True,
                             comment="거래 조건들 (배열 형태)")
    
    # WebSocket Pod 정보
    pod_name = Column(String(50), nullable=True,
                     comment="WebSocket Pod 이름 (예: sp500-websocket-5)")
    source = Column(String(50), nullable=False,
                   comment="데이터 소스 (finnhub_sp500_websocket)")
    pod_index = Column(Integer, nullable=True,
                      comment="Pod 인덱스 번호")
    
    # 시스템 메타데이터
    created_at = Column(DateTime, nullable=False, server_default=func.now(), index=True,
                       comment="레코드 생성 시간")
    
    # 성능 최적화 인덱스
    __table_args__ = (
        Index('idx_sp500_symbol_created_desc', 'symbol', 'created_at'),
        Index('idx_sp500_symbol_timestamp_desc', 'symbol', 'timestamp_ms'),
        Index('idx_sp500_created_at_desc', 'created_at'),
    )
    
    def __repr__(self):
        return f"<SP500WebsocketTrades(id={self.id}, symbol='{self.symbol}', price={self.price}, volume={self.volume})>"
    
    def to_dict(self) -> Dict[str, Any]:
        """딕셔너리로 변환 (JSON 직렬화용)"""
        return {
            'id': self.id,
            'symbol': self.symbol,
            'price': float(self.price) if self.price else None,
            'volume': self.volume,
            'timestamp_ms': self.timestamp_ms,
            'trade_conditions': self.trade_conditions,
            'pod_name': self.pod_name,
            'source': self.source,
            'pod_index': self.pod_index,
            'created_at': self.created_at.isoformat() if self.created_at else None
        }
    
    def to_api_format(self) -> Dict[str, Any]:
        """API 응답용 간소화된 형태로 변환"""
        return {
            'symbol': self.symbol,
            'price': float(self.price) if self.price else None,
            'volume': self.volume,
            'timestamp': self.timestamp_ms,
            'last_updated': self.created_at.isoformat() if self.created_at else None
        }
    
    # =========================
    # 🎯 프론트엔드용 조회 메서드
    # =========================
    
    @classmethod
    def get_current_price_by_symbol(cls, db_session: Session, symbol: str) -> Optional['SP500WebsocketTrades']:
        """
        특정 심볼의 현재가 (최신 가격) 조회
        
        Args:
            db_session: 데이터베이스 세션
            symbol: 주식 심볼 (예: 'AAPL')
            
        Returns:
            Optional[SP500WebsocketTrades]: 최신 거래 데이터 또는 None
        """
        try:
            return db_session.query(cls).filter(
                cls.symbol == symbol.upper()
            ).order_by(cls.created_at.desc()).first()
        except Exception as e:
            logger.error(f"❌ {symbol} 현재가 조회 실패: {e}")
            return None
    
    @classmethod
    def get_previous_close_price(cls, db_session: Session, symbol: str) -> Optional[float]:
        """
        특정 심볼의 전일 종가 조회 (변동률 계산용)
        
        Args:
            db_session: 데이터베이스 세션
            symbol: 주식 심볼
            
        Returns:
            Optional[float]: 전일 종가 또는 None
        """
        try:
            # 하루 전 데이터 조회 (24시간 전)
            yesterday = datetime.utcnow() - timedelta(days=1)
            
            # 하루 전 가장 최신 데이터 조회
            prev_trade = db_session.query(cls).filter(
                cls.symbol == symbol.upper(),
                cls.created_at <= yesterday
            ).order_by(cls.created_at.desc()).first()
            
            return float(prev_trade.price) if prev_trade and prev_trade.price else None
            
        except Exception as e:
            logger.error(f"❌ {symbol} 전일 종가 조회 실패: {e}")
            return None
    
    @classmethod
    def get_all_current_prices(cls, db_session: Session, limit: int = 500) -> List['SP500WebsocketTrades']:
        """
        모든 심볼의 현재가 조회 (주식 리스트 페이지용)
        각 심볼당 최신 1개씩 조회
        
        Args:
            db_session: 데이터베이스 세션
            limit: 반환할 최대 심볼 개수
            
        Returns:
            List[SP500WebsocketTrades]: 각 심볼의 최신 거래 데이터
        """
        try:
            from sqlalchemy import func, and_
            
            # 서브쿼리: 각 심볼별 최신 created_at 조회
            subquery = db_session.query(
                cls.symbol,
                func.max(cls.created_at).label('max_created_at')
            ).group_by(cls.symbol).subquery()
            
            # 메인 쿼리: 최신 created_at의 실제 데이터 조회
            query = db_session.query(cls).join(
                subquery,
                and_(
                    cls.symbol == subquery.c.symbol,
                    cls.created_at == subquery.c.max_created_at
                )
            )
            
            # 심볼 알파벳 순 정렬
            return query.order_by(cls.symbol).limit(limit).all()
            
        except Exception as e:
            logger.error(f"❌ 전체 현재가 조회 실패: {e}")
            return []
    
    @classmethod
    def get_chart_data_by_timeframe(cls, db_session: Session, symbol: str, 
                                   timeframe: str = '1D', limit: int = 1000) -> List['SP500WebsocketTrades']:
        """
        특정 심볼의 차트 데이터 조회 (시간대별)
        
        Args:
            db_session: 데이터베이스 세션
            symbol: 주식 심볼
            timeframe: 시간대 ('1D', '1H', '5M', '1M', '1W', '1MO')
            limit: 반환할 최대 개수
            
        Returns:
            List[SP500WebsocketTrades]: 시간대별 차트 데이터
        """
        try:
            # 시간대별 조회 기간 설정
            timeframe_map = {
                '1M': timedelta(minutes=1),    # 1분
                '5M': timedelta(minutes=5),    # 5분  
                '1H': timedelta(hours=1),      # 1시간
                '1D': timedelta(days=1),       # 1일
                '1W': timedelta(weeks=1),      # 1주
                '1MO': timedelta(days=30)      # 1개월
            }
            
            # 조회 시작 시간 계산
            if timeframe in timeframe_map:
                # 최근 데이터만 조회 (예: 1D면 최근 1일치)
                start_time = datetime.utcnow() - timeframe_map[timeframe] * limit
            else:
                # 기본값: 최근 1일
                start_time = datetime.utcnow() - timedelta(days=1)
            
            # 시간 범위 내 데이터 조회
            query = db_session.query(cls).filter(
                cls.symbol == symbol.upper(),
                cls.created_at >= start_time
            ).order_by(cls.created_at.asc())
            
            # 시간대별 데이터 샘플링 (성능 최적화)
            if timeframe == '1D':
                # 1일 차트: 5분 간격으로 샘플링
                return cls._sample_data_by_interval(query.all(), minutes=5)
            elif timeframe == '1W':
                # 1주 차트: 1시간 간격으로 샘플링
                return cls._sample_data_by_interval(query.all(), hours=1)
            elif timeframe == '1MO':
                # 1개월 차트: 4시간 간격으로 샘플링
                return cls._sample_data_by_interval(query.all(), hours=4)
            else:
                # 분/시간 차트: 원본 데이터 반환
                return query.limit(limit).all()
                
        except Exception as e:
            logger.error(f"❌ {symbol} 차트 데이터 조회 실패 ({timeframe}): {e}")
            return []
    
    @classmethod
    def _sample_data_by_interval(cls, data: List['SP500WebsocketTrades'], 
                                minutes: int = 0, hours: int = 0) -> List['SP500WebsocketTrades']:
        """
        데이터를 지정된 시간 간격으로 샘플링
        
        Args:
            data: 원본 데이터 리스트
            minutes: 샘플링 간격 (분)
            hours: 샘플링 간격 (시간)
            
        Returns:
            List[SP500WebsocketTrades]: 샘플링된 데이터
        """
        if not data:
            return []
        
        interval = timedelta(minutes=minutes, hours=hours)
        sampled_data = []
        last_time = None
        
        for trade in data:
            if last_time is None or trade.created_at >= last_time + interval:
                sampled_data.append(trade)
                last_time = trade.created_at
        
        return sampled_data
    
    @classmethod
    def get_trading_volume_24h(cls, db_session: Session, symbol: str) -> int:
        """
        특정 심볼의 24시간 거래량 조회
        
        Args:
            db_session: 데이터베이스 세션
            symbol: 주식 심볼
            
        Returns:
            int: 24시간 총 거래량
        """
        try:
            from sqlalchemy import func
            
            # 24시간 전부터 현재까지
            since_24h = datetime.utcnow() - timedelta(hours=24)
            
            result = db_session.query(
                func.sum(cls.volume).label('total_volume')
            ).filter(
                cls.symbol == symbol.upper(),
                cls.created_at >= since_24h,
                cls.volume.isnot(None)
            ).scalar()
            
            return int(result) if result else 0
            
        except Exception as e:
            logger.error(f"❌ {symbol} 24시간 거래량 조회 실패: {e}")
            return 0
    
    @classmethod
    def get_price_change_info(cls, db_session: Session, symbol: str) -> Dict[str, Any]:
        """
        특정 심볼의 가격 변동 정보 조회 (현재가, 전일 대비 변동)
        
        Args:
            db_session: 데이터베이스 세션
            symbol: 주식 심볼
            
        Returns:
            Dict[str, Any]: 가격 변동 정보
        """
        try:
            # 현재가 조회
            current_trade = cls.get_current_price_by_symbol(db_session, symbol)
            if not current_trade:
                return {
                    'symbol': symbol,
                    'current_price': None,
                    'previous_close': None,
                    'change_amount': None,
                    'change_percentage': None,
                    'volume': None,
                    'error': 'No current data found'
                }
            
            current_price = float(current_trade.price)
            
            # 전일 종가 조회
            previous_close = cls.get_previous_close_price(db_session, symbol)
            
            # 변동 계산
            change_amount = None
            change_percentage = None
            
            if previous_close:
                change_amount = current_price - previous_close
                change_percentage = (change_amount / previous_close) * 100
            
            # 24시간 거래량 조회
            volume_24h = cls.get_trading_volume_24h(db_session, symbol)
            
            return {
                'symbol': symbol,
                'current_price': current_price,
                'previous_close': previous_close,
                'change_amount': round(change_amount, 2) if change_amount else None,
                'change_percentage': round(change_percentage, 2) if change_percentage else None,
                'volume': volume_24h,
                'last_updated': current_trade.created_at.isoformat()
            }
            
        except Exception as e:
            logger.error(f"❌ {symbol} 가격 변동 정보 조회 실패: {e}")
            return {
                'symbol': symbol,
                'current_price': None,
                'previous_close': None,
                'change_amount': None,
                'change_percentage': None,
                'volume': None,
                'error': str(e)
            }
    
    @classmethod
    def get_market_summary(cls, db_session: Session) -> Dict[str, Any]:
        """
        전체 시장 요약 정보 조회
        
        Args:
            db_session: 데이터베이스 세션
            
        Returns:
            Dict[str, Any]: 시장 요약 정보
        """
        try:
            from sqlalchemy import func
            
            # 기본 통계 조회
            stats = db_session.query(
                func.count(func.distinct(cls.symbol)).label('total_symbols'),
                func.count(cls.id).label('total_trades'),
                func.avg(cls.price).label('avg_price'),
                func.max(cls.price).label('max_price'),
                func.min(cls.price).label('min_price'),
                func.sum(cls.volume).label('total_volume')
            ).first()
            
            # 최신 업데이트 시간
            latest_update = db_session.query(
                func.max(cls.created_at)
            ).scalar()
            
            return {
                'total_symbols': stats.total_symbols or 0,
                'total_trades': stats.total_trades or 0,
                'average_price': float(stats.avg_price) if stats.avg_price else 0,
                'highest_price': float(stats.max_price) if stats.max_price else 0,
                'lowest_price': float(stats.min_price) if stats.min_price else 0,
                'total_volume': stats.total_volume or 0,
                'last_updated': latest_update.isoformat() if latest_update else None
            }
            
        except Exception as e:
            logger.error(f"❌ 시장 요약 정보 조회 실패: {e}")
            return {
                'total_symbols': 0,
                'total_trades': 0,
                'average_price': 0,
                'highest_price': 0,
                'lowest_price': 0,
                'total_volume': 0,
                'last_updated': None,
                'error': str(e)
            }