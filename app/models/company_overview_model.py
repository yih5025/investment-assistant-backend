# app/models/company_overview_model.py
from sqlalchemy import Column, Integer, String, Numeric, BigInteger, DateTime, Text, VARCHAR
from sqlalchemy.sql import func
from sqlalchemy.orm import Session
from typing import List, Optional, Dict, Any
import logging
from datetime import datetime

from app.models.base import BaseModel

logger = logging.getLogger(__name__)

class CompanyOverview(BaseModel):
    """
    Company Overview 테이블 ORM 모델
    
    Alpha Vantage Company Overview API에서 수집한 SP500 기업 상세 정보를 저장합니다.
    배치 ID를 통해 월별 데이터 히스토리를 관리합니다.
    """
    __tablename__ = "company_overview"
    
    # 복합 Primary Key (batch_id + symbol)
    batch_id = Column(BigInteger, nullable=False, primary_key=True,
                     comment="배치 ID (월별 수집 주기)")
    symbol = Column(String(10), nullable=False, primary_key=True,
                   comment="주식 심볼 (예: AAPL, MSFT)")
    
    # 기본 회사 정보
    asset_type = Column(VARCHAR(50), nullable=True,
                       comment="자산 유형 (Common Stock 등)")
    name = Column(VARCHAR(200), nullable=False,
                 comment="회사명")
    description = Column(Text, nullable=True,
                        comment="회사 사업 설명")
    cik = Column(VARCHAR(20), nullable=True,
                comment="SEC CIK 번호")
    exchange = Column(VARCHAR(20), nullable=True,
                     comment="거래소 (NYSE, NASDAQ 등)")
    currency = Column(VARCHAR(10), nullable=True,
                     comment="통화 (USD)")
    country = Column(VARCHAR(50), nullable=True,
                    comment="국가")
    
    # 섹터 및 산업 정보
    sector = Column(VARCHAR(100), nullable=True,
                   comment="섹터 (Technology, Healthcare 등)")
    industry = Column(VARCHAR(200), nullable=True,
                     comment="산업군 (Software, Biotechnology 등)")
    address = Column(VARCHAR(300), nullable=True,
                    comment="본사 주소")
    official_site = Column(VARCHAR(200), nullable=True,
                          comment="공식 웹사이트")
    
    # 재무 연도 정보
    fiscal_year_end = Column(VARCHAR(20), nullable=True,
                            comment="회계연도 마감월")
    latest_quarter = Column(VARCHAR(20), nullable=True,
                           comment="최근 분기")
    
    # 시장 및 재무 지표
    market_capitalization = Column(BigInteger, nullable=True,
                                  comment="시가총액 (USD)")
    ebitda = Column(BigInteger, nullable=True,
                   comment="EBITDA")
    pe_ratio = Column(Numeric(10, 4), nullable=True,
                     comment="P/E 비율")
    peg_ratio = Column(Numeric(10, 4), nullable=True,
                      comment="PEG 비율")
    book_value = Column(Numeric(10, 4), nullable=True,
                       comment="주당 장부가")
    dividend_per_share = Column(Numeric(10, 4), nullable=True,
                               comment="주당 배당금")
    dividend_yield = Column(Numeric(8, 6), nullable=True,
                           comment="배당 수익률")
    eps = Column(Numeric(10, 4), nullable=True,
                comment="주당 순이익")
    revenue_per_share_ttm = Column(Numeric(10, 4), nullable=True,
                                  comment="주당 매출 (TTM)")
    profit_margin = Column(Numeric(8, 6), nullable=True,
                          comment="순이익률")
    operating_margin_ttm = Column(Numeric(8, 6), nullable=True,
                                 comment="영업이익률 (TTM)")
    return_on_assets_ttm = Column(Numeric(8, 6), nullable=True,
                                 comment="ROA (TTM)")
    return_on_equity_ttm = Column(Numeric(8, 6), nullable=True,
                                 comment="ROE (TTM)")
    revenue_ttm = Column(BigInteger, nullable=True,
                        comment="매출 (TTM)")
    gross_profit_ttm = Column(BigInteger, nullable=True,
                             comment="매출총이익 (TTM)")
    diluted_eps_ttm = Column(Numeric(10, 4), nullable=True,
                            comment="희석 주당순이익 (TTM)")
    
    # 성장률 지표
    quarterly_earnings_growth_yoy = Column(Numeric(8, 6), nullable=True,
                                          comment="분기 실적 성장률 (YoY)")
    quarterly_revenue_growth_yoy = Column(Numeric(8, 6), nullable=True,
                                         comment="분기 매출 성장률 (YoY)")
    
    # 분석가 평가
    analyst_target_price = Column(Numeric(10, 4), nullable=True,
                                 comment="분석가 목표주가")
    
    # 밸류에이션 지표
    trailing_pe = Column(Numeric(10, 4), nullable=True,
                        comment="후행 P/E")
    forward_pe = Column(Numeric(10, 4), nullable=True,
                       comment="선행 P/E")
    price_to_sales_ratio_ttm = Column(Numeric(10, 4), nullable=True,
                                     comment="PSR (TTM)")
    price_to_book_ratio = Column(Numeric(10, 4), nullable=True,
                                comment="PBR")
    ev_to_revenue = Column(Numeric(10, 4), nullable=True,
                          comment="EV/Revenue")
    ev_to_ebitda = Column(Numeric(10, 4), nullable=True,
                         comment="EV/EBITDA")
    beta = Column(Numeric(8, 6), nullable=True,
                 comment="베타 (시장 대비 변동성)")
    
    # 주가 정보
    week_52_high = Column(Numeric(10, 4), nullable=True,
                         comment="52주 최고가")
    week_52_low = Column(Numeric(10, 4), nullable=True,
                        comment="52주 최저가")
    day_50_moving_average = Column(Numeric(10, 4), nullable=True,
                                  comment="50일 이동평균")
    day_200_moving_average = Column(Numeric(10, 4), nullable=True,
                                   comment="200일 이동평균")
    
    # 주식 정보
    shares_outstanding = Column(BigInteger, nullable=True,
                               comment="발행 주식 수")
    dividend_date = Column(VARCHAR(20), nullable=True,
                          comment="배당지급일")
    ex_dividend_date = Column(VARCHAR(20), nullable=True,
                             comment="배당락일")
    
    # 메타데이터
    created_at = Column(DateTime, nullable=False, server_default=func.now(),
                       comment="레코드 생성 시간")
    updated_at = Column(DateTime, nullable=False, server_default=func.now(),
                       comment="레코드 업데이트 시간")
    
    def __repr__(self):
        return f"<CompanyOverview(batch_id={self.batch_id}, symbol='{self.symbol}', name='{self.name}')>"
    
    def to_dict(self) -> Dict[str, Any]:
        """딕셔너리로 변환 (JSON 직렬화용)"""
        return {
            'batch_id': self.batch_id,
            'symbol': self.symbol,
            'asset_type': self.asset_type,
            'name': self.name,
            'description': self.description,
            'sector': self.sector,
            'industry': self.industry,
            'address': self.address,
            'official_site': self.official_site,
            'market_capitalization': self.market_capitalization,
            'pe_ratio': float(self.pe_ratio) if self.pe_ratio else None,
            'peg_ratio': float(self.peg_ratio) if self.peg_ratio else None,
            'dividend_yield': float(self.dividend_yield) if self.dividend_yield else None,
            'beta': float(self.beta) if self.beta else None,
            'week_52_high': float(self.week_52_high) if self.week_52_high else None,
            'week_52_low': float(self.week_52_low) if self.week_52_low else None,
            'created_at': self.created_at.isoformat() if self.created_at else None
        }
    
    # =========================
    # 클래스 메서드 (조회 로직)
    # =========================
    
    @classmethod
    def get_latest_by_symbol(cls, db_session: Session, symbol: str) -> Optional['CompanyOverview']:
        """
        특정 심볼의 최신 Company Overview 데이터 조회
        
        Args:
            db_session: 데이터베이스 세션
            symbol: 주식 심볼 (예: 'AAPL')
            
        Returns:
            Optional[CompanyOverview]: 최신 데이터 또는 None
        """
        try:
            logger.info(f"DB 쿼리 실행: CompanyOverview 테이블에서 symbol='{symbol.upper()}' 조회")
            
            # 먼저 해당 심볼의 모든 레코드 개수 확인
            total_count = db_session.query(cls).filter(cls.symbol == symbol.upper()).count()
            logger.info(f"'{symbol.upper()}' 심볼의 총 레코드 수: {total_count}")
            
            if total_count > 0:
                # 배치 ID별로 확인
                batch_info = db_session.query(cls.batch_id).filter(cls.symbol == symbol.upper()).distinct().all()
                logger.info(f"'{symbol.upper()}' 심볼의 배치 ID들: {[b[0] for b in batch_info]}")
            
            result = db_session.query(cls).filter(
                cls.symbol == symbol.upper()
            ).order_by(cls.batch_id.desc()).first()
            
            if result:
                logger.info(f"✅ {symbol} Company Overview 조회 성공 (batch_id: {result.batch_id}, name: {result.name})")
            else:
                logger.warning(f"⚠️ {symbol} Company Overview 조회 결과 없음")
            
            return result
        except Exception as e:
            logger.error(f"❌ {symbol} Company Overview 조회 실패: {e}")
            import traceback
            logger.error(f"❌ 상세 에러 트레이스: {traceback.format_exc()}")
            return None
    
    @classmethod
    def get_by_batch_and_symbol(cls, db_session: Session, batch_id: int, symbol: str) -> Optional['CompanyOverview']:
        """
        특정 배치의 특정 심볼 데이터 조회
        
        Args:
            db_session: 데이터베이스 세션
            batch_id: 배치 ID
            symbol: 주식 심볼
            
        Returns:
            Optional[CompanyOverview]: 해당 데이터 또는 None
        """
        try:
            return db_session.query(cls).filter(
                cls.batch_id == batch_id,
                cls.symbol == symbol.upper()
            ).first()
        except Exception as e:
            logger.error(f"❌ Batch {batch_id}, {symbol} 조회 실패: {e}")
            return None
    
    @classmethod
    def get_latest_batch_data(cls, db_session: Session, limit: int = 500) -> List['CompanyOverview']:
        """
        최신 배치의 모든 데이터 조회
        
        Args:
            db_session: 데이터베이스 세션
            limit: 반환할 최대 개수
            
        Returns:
            List[CompanyOverview]: 최신 배치 데이터 리스트
        """
        try:
            # 최신 batch_id 조회
            latest_batch = db_session.query(
                func.max(cls.batch_id)
            ).scalar()
            
            if not latest_batch:
                logger.warning("⚠️ Company Overview 데이터가 없습니다")
                return []
            
            return db_session.query(cls).filter(
                cls.batch_id == latest_batch
            ).order_by(cls.symbol).limit(limit).all()
            
        except Exception as e:
            logger.error(f"❌ 최신 배치 데이터 조회 실패: {e}")
            return []
    
    @classmethod
    def get_sector_companies(cls, db_session: Session, sector: str, batch_id: int = None) -> List['CompanyOverview']:
        """
        특정 섹터의 회사들 조회
        
        Args:
            db_session: 데이터베이스 세션
            sector: 섹터명 (예: 'Technology')
            batch_id: 배치 ID (None이면 최신 배치)
            
        Returns:
            List[CompanyOverview]: 해당 섹터 회사들
        """
        try:
            if batch_id is None:
                # 최신 batch_id 사용
                batch_id = db_session.query(func.max(cls.batch_id)).scalar()
                
            if not batch_id:
                return []
            
            return db_session.query(cls).filter(
                cls.batch_id == batch_id,
                cls.sector.ilike(f'%{sector}%')
            ).order_by(cls.market_capitalization.desc().nulls_last()).all()
            
        except Exception as e:
            logger.error(f"❌ 섹터 {sector} 회사 조회 실패: {e}")
            return []
    
    @classmethod
    def get_sector_statistics(cls, db_session: Session, sector: str, batch_id: int = None) -> Dict[str, Any]:
        """
        특정 섹터의 통계 정보 조회 (평균 P/E, ROE 등)
        
        Args:
            db_session: 데이터베이스 세션
            sector: 섹터명
            batch_id: 배치 ID (None이면 최신 배치)
            
        Returns:
            Dict[str, Any]: 섹터 통계 정보
        """
        try:
            if batch_id is None:
                batch_id = db_session.query(func.max(cls.batch_id)).scalar()
                
            if not batch_id:
                return {}
            
            stats = db_session.query(
                func.count(cls.symbol).label('company_count'),
                func.avg(cls.pe_ratio).label('avg_pe'),
                func.avg(cls.return_on_equity_ttm).label('avg_roe'),
                func.avg(cls.profit_margin).label('avg_profit_margin'),
                func.avg(cls.dividend_yield).label('avg_dividend_yield'),
                func.sum(cls.market_capitalization).label('total_market_cap')
            ).filter(
                cls.batch_id == batch_id,
                cls.sector.ilike(f'%{sector}%')
            ).first()
            
            return {
                'sector': sector,
                'company_count': stats.company_count or 0,
                'avg_pe_ratio': float(stats.avg_pe) if stats.avg_pe else None,
                'avg_roe': float(stats.avg_roe) if stats.avg_roe else None,
                'avg_profit_margin': float(stats.avg_profit_margin) if stats.avg_profit_margin else None,
                'avg_dividend_yield': float(stats.avg_dividend_yield) if stats.avg_dividend_yield else None,
                'total_market_cap': stats.total_market_cap or 0,
                'batch_id': batch_id
            }
            
        except Exception as e:
            logger.error(f"❌ 섹터 {sector} 통계 조회 실패: {e}")
            return {'sector': sector, 'error': str(e)}
    
    @classmethod
    def symbol_exists(cls, db_session: Session, symbol: str, batch_id: int = None) -> bool:
        """
        특정 심볼의 Company Overview 데이터 존재 여부 확인
        
        Args:
            db_session: 데이터베이스 세션
            symbol: 주식 심볼
            batch_id: 배치 ID (None이면 최신 배치에서 확인)
            
        Returns:
            bool: 데이터 존재 여부
        """
        try:
            if batch_id is None:
                # 최신 배치에서 확인
                result = db_session.query(cls).filter(
                    cls.symbol == symbol.upper()
                ).order_by(cls.batch_id.desc()).first()
            else:
                # 특정 배치에서 확인
                result = db_session.query(cls).filter(
                    cls.batch_id == batch_id,
                    cls.symbol == symbol.upper()
                ).first()
            
            return result is not None
            
        except Exception as e:
            logger.error(f"❌ {symbol} 존재 여부 확인 실패: {e}")
            return False
    
    @classmethod
    def get_available_batches(cls, db_session: Session) -> List[int]:
        """
        사용 가능한 모든 배치 ID 조회
        
        Args:
            db_session: 데이터베이스 세션
            
        Returns:
            List[int]: 배치 ID 리스트 (내림차순)
        """
        try:
            result = db_session.query(
                func.distinct(cls.batch_id)
            ).order_by(cls.batch_id.desc()).all()
            
            return [row[0] for row in result]
            
        except Exception as e:
            logger.error(f"❌ 배치 ID 조회 실패: {e}")
            return []