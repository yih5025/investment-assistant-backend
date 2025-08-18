# app/models/bithumb_ticker_model.py
from sqlalchemy import Column, Integer, String, Numeric, BigInteger, DateTime, Text
from sqlalchemy.sql import func
from app.models.base import BaseModel

class BithumbTicker(BaseModel):
    """
    빗썸 티커 데이터 테이블 ORM 모델
    
    이 테이블은 빗썸 거래소의 암호화폐 실시간 시세 정보를 저장합니다.
    가격, 거래량, 변동률 등의 정보가 포함됩니다.
    """
    __tablename__ = "bithumb_ticker"
    
    # Primary Key
    id = Column(Integer, primary_key=True, autoincrement=True,
               comment="자동 증가 Primary Key")
    
    # 기본 정보
    market = Column(Text, nullable=False, index=True,
                   comment="마켓 코드 (예: KRW-BTC, KRW-ETH)")
    
    # 시간 정보
    trade_date = Column(Text, nullable=True,
                       comment="거래 날짜")
    trade_time = Column(Text, nullable=True,
                       comment="거래 시간")
    trade_date_kst = Column(Text, nullable=True,
                           comment="거래 날짜 (KST)")
    trade_time_kst = Column(Text, nullable=True,
                           comment="거래 시간 (KST)")
    trade_timestamp = Column(BigInteger, nullable=True,
                            comment="거래 타임스탬프")
    
    # 가격 정보
    opening_price = Column(Numeric, nullable=True,
                          comment="시가")
    high_price = Column(Numeric, nullable=True,
                       comment="고가")
    low_price = Column(Numeric, nullable=True,
                      comment="저가")
    trade_price = Column(Numeric, nullable=True, index=True,
                        comment="현재 거래가")
    prev_closing_price = Column(Numeric, nullable=True,
                               comment="전일 종가")
    
    # 변동 정보
    change = Column(Text, nullable=True,
                   comment="변동 상태 (RISE, FALL, EVEN)")
    change_price = Column(Numeric, nullable=True,
                         comment="변동 가격 (절댓값)")
    change_rate = Column(Numeric, nullable=True,
                        comment="변동률 (절댓값)")
    signed_change_price = Column(Numeric, nullable=True,
                                comment="부호 있는 변동 가격")
    signed_change_rate = Column(Numeric, nullable=True, index=True,
                               comment="부호 있는 변동률")
    
    # 거래량 정보
    trade_volume = Column(Numeric, nullable=True,
                         comment="거래량")
    acc_trade_price = Column(Numeric, nullable=True,
                            comment="누적 거래 대금")
    acc_trade_price_24h = Column(Numeric, nullable=True,
                                comment="24시간 누적 거래 대금")
    acc_trade_volume = Column(Numeric, nullable=True,
                             comment="누적 거래량")
    acc_trade_volume_24h = Column(Numeric, nullable=True,
                                 comment="24시간 누적 거래량")
    
    # 52주 정보
    highest_52_week_price = Column(Numeric, nullable=True,
                                  comment="52주 최고가")
    highest_52_week_date = Column(Text, nullable=True,
                                 comment="52주 최고가 날짜")
    lowest_52_week_price = Column(Numeric, nullable=True,
                                 comment="52주 최저가")
    lowest_52_week_date = Column(Text, nullable=True,
                                comment="52주 최저가 날짜")
    
    # 메타데이터
    timestamp_field = Column(BigInteger, nullable=True, index=True,
                            comment="시스템 타임스탬프")
    source = Column(Text, nullable=False, server_default='bithumb',
                   comment="데이터 소스")
    
    def __repr__(self):
        return f"<BithumbTicker(id={self.id}, market='{self.market}', trade_price={self.trade_price}, change_rate={self.signed_change_rate})>"
    
    def to_dict(self):
        """딕셔너리로 변환 (JSON 직렬화용)"""
        return {
            'id': self.id,
            'market': self.market,
            'trade_date': self.trade_date,
            'trade_time': self.trade_time,
            'trade_date_kst': self.trade_date_kst,
            'trade_time_kst': self.trade_time_kst,
            'trade_timestamp': self.trade_timestamp,
            'opening_price': float(self.opening_price) if self.opening_price else None,
            'high_price': float(self.high_price) if self.high_price else None,
            'low_price': float(self.low_price) if self.low_price else None,
            'trade_price': float(self.trade_price) if self.trade_price else None,
            'prev_closing_price': float(self.prev_closing_price) if self.prev_closing_price else None,
            'change': self.change,
            'change_price': float(self.change_price) if self.change_price else None,
            'change_rate': float(self.change_rate) if self.change_rate else None,
            'signed_change_price': float(self.signed_change_price) if self.signed_change_price else None,
            'signed_change_rate': float(self.signed_change_rate) if self.signed_change_rate else None,
            'trade_volume': float(self.trade_volume) if self.trade_volume else None,
            'acc_trade_price': float(self.acc_trade_price) if self.acc_trade_price else None,
            'acc_trade_price_24h': float(self.acc_trade_price_24h) if self.acc_trade_price_24h else None,
            'acc_trade_volume': float(self.acc_trade_volume) if self.acc_trade_volume else None,
            'acc_trade_volume_24h': float(self.acc_trade_volume_24h) if self.acc_trade_volume_24h else None,
            'highest_52_week_price': float(self.highest_52_week_price) if self.highest_52_week_price else None,
            'highest_52_week_date': self.highest_52_week_date,
            'lowest_52_week_price': float(self.lowest_52_week_price) if self.lowest_52_week_price else None,
            'lowest_52_week_date': self.lowest_52_week_date,
            'timestamp_field': self.timestamp_field,
            'source': self.source
        }
    
    def to_redis_format(self):
        """Redis 저장용 간소화된 형태로 변환 (Consumer에서 사용하는 형태와 동일)"""
        import time
        return {
            'symbol': self.market,
            'price': float(self.trade_price) if self.trade_price else None,
            'change_rate': float(self.signed_change_rate) if self.signed_change_rate else None,
            'change_price': float(self.signed_change_price) if self.signed_change_price else None,
            'volume': float(self.trade_volume) if self.trade_volume else None,
            'timestamp': int(time.time()),
            'source': 'bithumb'
        }
    
    @classmethod
    def get_latest_by_market(cls, db_session, market: str, limit: int = 10):
        """특정 마켓의 최신 데이터 조회"""
        return db_session.query(cls).filter(
            cls.market == market
        ).order_by(cls.timestamp_field.desc()).limit(limit).all()
    
    @classmethod
    def get_all_latest_prices(cls, db_session, limit: int = 100):
        """모든 마켓의 최신 가격 조회 (WebSocket용)"""
        from sqlalchemy import distinct, and_
        
        # 서브쿼리: 각 마켓별 최신 timestamp 조회
        latest_timestamps = db_session.query(
            cls.market,
            func.max(cls.timestamp_field).label('max_timestamp')
        ).group_by(cls.market).subquery()
        
        # 메인 쿼리: 최신 timestamp의 실제 데이터 조회
        main_query = db_session.query(cls).join(
            latest_timestamps,
            and_(
                cls.market == latest_timestamps.c.market,
                cls.timestamp_field == latest_timestamps.c.max_timestamp
            )
        ).limit(limit)
        
        return main_query.all()
    
    @classmethod
    def get_top_gainers(cls, db_session, limit: int = 20):
        """상승률 상위 암호화폐 조회"""
        latest_data = cls.get_all_latest_prices(db_session, limit=200)
        
        # signed_change_rate가 양수인 것들만 필터링하고 정렬
        gainers = [item for item in latest_data if item.signed_change_rate and item.signed_change_rate > 0]
        gainers.sort(key=lambda x: x.signed_change_rate, reverse=True)
        
        return gainers[:limit]
    
    @classmethod
    def get_top_volume(cls, db_session, limit: int = 20):
        """거래량 상위 암호화폐 조회"""
        latest_data = cls.get_all_latest_prices(db_session, limit=200)
        
        # 거래량이 있는 것들만 필터링하고 정렬
        volume_leaders = [item for item in latest_data if item.acc_trade_volume_24h]
        volume_leaders.sort(key=lambda x: x.acc_trade_volume_24h, reverse=True)
        
        return volume_leaders[:limit]