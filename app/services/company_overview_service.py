# app/services/company_overview_service.py
import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import pytz
from sqlalchemy.orm import Session
from sqlalchemy import func

from app.database import get_db
from app.models.company_overview_model import CompanyOverview
from app.schemas.company_overview_schema import (
    CompanyOverviewData, CompanyOverviewSummary, SectorStatistics,
    db_to_company_overview_data, db_to_company_overview_summary
)

logger = logging.getLogger(__name__)

class CompanyOverviewService:
    """
    Company Overview 전용 서비스 클래스
    
    Alpha Vantage에서 수집한 기업 상세 정보를 처리하고 분석합니다.
    데이터가 없는 경우를 안전하게 처리합니다.
    """
    
    def __init__(self):
        """CompanyOverviewService 초기화"""
        
        # 성능 통계
        self.stats = {
            "api_requests": 0,
            "db_queries": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "data_not_found": 0,
            "errors": 0,
            "last_request": None
        }
        
        logger.info("✅ CompanyOverviewService 초기화 완료")
    
    # =========================
    # 🎯 개별 회사 정보 조회
    # =========================
    
    def get_company_info(self, symbol: str, batch_id: int = None) -> Dict[str, Any]:
        """
        특정 심볼의 회사 정보 조회
        
        Args:
            symbol: 주식 심볼 (예: 'AAPL')
            batch_id: 특정 배치 ID (None이면 최신)
            
        Returns:
            Dict[str, Any]: 회사 정보 또는 에러 메시지
        """
        try:
            self.stats["api_requests"] += 1
            self.stats["last_request"] = datetime.now(pytz.UTC)
            
            symbol = symbol.upper()
            db = next(get_db())
            
            # 데이터 조회
            if batch_id:
                company = CompanyOverview.get_by_batch_and_symbol(db, batch_id, symbol)
            else:
                company = CompanyOverview.get_latest_by_symbol(db, symbol)
            
            if not company:
                self.stats["data_not_found"] += 1
                logger.warning(f"⚠️ {symbol} Company Overview 데이터 없음 (batch_id: {batch_id})")
                
                return {
                    'success': False,
                    'symbol': symbol,
                    'message': f'{symbol}에 대한 회사 정보가 아직 수집되지 않았습니다. 데이터 수집은 월별로 진행됩니다.',
                    'data_available': False,
                    'suggestions': [
                        '데이터 수집이 진행 중일 수 있습니다',
                        '다른 SP500 기업을 검색해보세요',
                        '며칠 후 다시 시도해보세요'
                    ]
                }
            
            # 성공적으로 데이터 조회
            company_data = db_to_company_overview_data(company)
            
            self.stats["db_queries"] += 1
            
            logger.info(f"✅ {symbol} Company Overview 조회 성공 (batch_id: {company.batch_id})")
            
            return {
                'success': True,
                'symbol': symbol,
                'data': company_data,
                'data_available': True,
                'message': f'{symbol} 회사 정보를 성공적으로 조회했습니다',
                'batch_id': company.batch_id
            }
            
        except Exception as e:
            logger.error(f"❌ {symbol} Company Overview 조회 실패: {e}")
            self.stats["errors"] += 1
            return {
                'success': False,
                'symbol': symbol,
                'message': f'서버 오류로 인해 {symbol} 정보를 조회할 수 없습니다',
                'error': str(e),
                'data_available': False
            }
        finally:
            db.close()
    
    def get_company_basic_metrics(self, symbol: str) -> Dict[str, Any]:
        """
        회사의 핵심 지표만 간단히 조회 (SP500 서비스와 통합용)
        
        Args:
            symbol: 주식 심볼
            
        Returns:
            Dict[str, Any]: 핵심 지표 또는 None
        """
        try:
            self.stats["api_requests"] += 1
            logger.info(f"Company Overview 핵심 지표 조회 시작: {symbol}")
            
            db = next(get_db())
            company = CompanyOverview.get_latest_by_symbol(db, symbol.upper())
            
            if not company:
                self.stats["data_not_found"] += 1
                logger.warning(f"⚠️ {symbol} Company Overview 데이터 없음 - DB에서 조회 결과 없음")
                logger.info(f"DB 쿼리 실행: CompanyOverview.get_latest_by_symbol(db, '{symbol.upper()}')")
                return {
                    'data_available': False,
                    'message': f'{symbol} 회사 정보가 없습니다',
                    'debug_info': f'DB 쿼리 결과: None for symbol {symbol.upper()}'
                }
            
            # 핵심 지표만 반환
            logger.info(f"✅ {symbol} Company Overview 핵심 지표 생성 완료 (batch_id: {company.batch_id}, name: {company.name})")
            return {
                'data_available': True,
                'company_name': company.name,
                'sector': company.sector,
                'industry': company.industry,
                'market_cap': company.market_capitalization,
                'pe_ratio': float(company.pe_ratio) if company.pe_ratio else None,
                'dividend_yield': float(company.dividend_yield) if company.dividend_yield else None,
                'beta': float(company.beta) if company.beta else None,
                'roe': float(company.return_on_equity_ttm) if company.return_on_equity_ttm else None,
                'profit_margin': float(company.profit_margin) if company.profit_margin else None,
                'description': company.description,
                'website': company.official_site,
                'batch_id': company.batch_id
            }
            
        except Exception as e:
            logger.error(f"❌ {symbol} 핵심 지표 조회 실패: {e}")
            import traceback
            logger.error(f"❌ 상세 에러 트레이스: {traceback.format_exc()}")
            return {
                'data_available': False,
                'error': f'Company Overview 조회 중 오류 발생: {str(e)}',
                'debug_info': f'Exception: {type(e).__name__}'
            }
        finally:
            db.close()
    
    # =========================
    # 🎯 섹터별 분석
    # =========================
    
    # def get_sector_analysis(self, sector: str, batch_id: int = None, limit: int = 50) -> Dict[str, Any]:
    #     """
    #     특정 섹터의 회사들과 통계 분석
        
    #     Args:
    #         sector: 섹터명 (예: 'Technology')
    #         batch_id: 특정 배치 ID (None이면 최신)
    #         limit: 반환할 최대 회사 수
            
    #     Returns:
    #         Dict[str, Any]: 섹터 분석 결과
    #     """
    #     try:
    #         self.stats["api_requests"] += 1
    #         self.stats["last_request"] = datetime.now(pytz.UTC)
            
    #         db = next(get_db())
            
    #         # 섹터 통계 조회
    #         sector_stats = CompanyOverview.get_sector_statistics(db, sector, batch_id)
            
    #         if not sector_stats or sector_stats.get('company_count', 0) == 0:
    #             self.stats["data_not_found"] += 1
    #             return {
    #                 'success': False,
    #                 'sector': sector,
    #                 'message': f'{sector} 섹터에 대한 데이터가 없습니다',
    #                 'data_available': False,
    #                 'suggestions': [
    #                     '올바른 섹터명인지 확인해주세요',
    #                     '사용 가능한 섹터: Technology, Healthcare, Financials, Energy 등',
    #                     '데이터 수집이 완료될 때까지 기다려주세요'
    #                 ]
    #             }
            
    #         # 섹터 내 회사들 조회
    #         companies = CompanyOverview.get_sector_companies(db, sector, batch_id)
    #         company_summaries = [
    #             db_to_company_overview_summary(company) 
    #             for company in companies[:limit]
    #         ]
            
    #         self.stats["db_queries"] += 1
            
    #         logger.info(f"✅ {sector} 섹터 분석 완료: {len(company_summaries)}개 회사")
            
    #         return {
    #             'success': True,
    #             'sector': sector,
    #             'sector_stats': sector_stats,
    #             'companies': company_summaries,
    #             'total_companies': len(company_summaries),
    #             'data_available': True,
    #             'message': f'{sector} 섹터 분석을 완료했습니다'
    #         }
            
    #     except Exception as e:
    #         logger.error(f"❌ {sector} 섹터 분석 실패: {e}")
    #         self.stats["errors"] += 1
    #         return {
    #             'success': False,
    #             'sector': sector,
    #             'message': f'{sector} 섹터 분석 중 오류가 발생했습니다',
    #             'error': str(e),
    #             'data_available': False
    #         }
    #     finally:
    #         db.close()
    
    # def get_sector_comparison(self, symbol: str) -> Dict[str, Any]:
    #     """
    #     특정 회사와 해당 섹터 평균 비교
        
    #     Args:
    #         symbol: 주식 심볼
            
    #     Returns:
    #         Dict[str, Any]: 섹터 비교 결과
    #     """
    #     try:
    #         self.stats["api_requests"] += 1
            
    #         db = next(get_db())
    #         company = CompanyOverview.get_latest_by_symbol(db, symbol.upper())
            
    #         if not company or not company.sector:
    #             return {
    #                 'success': False,
    #                 'symbol': symbol,
    #                 'message': f'{symbol}의 섹터 정보가 없습니다',
    #                 'data_available': False
    #             }
            
    #         # 섹터 통계 조회
    #         sector_stats = CompanyOverview.get_sector_statistics(db, company.sector)
            
    #         if not sector_stats:
    #             return {
    #                 'success': False,
    #                 'symbol': symbol,
    #                 'message': f'{company.sector} 섹터 통계를 조회할 수 없습니다',
    #                 'data_available': False
    #             }
            
    #         # 비교 결과 계산
    #         comparison = self._calculate_sector_comparison(company, sector_stats)
            
    #         logger.info(f"✅ {symbol} vs {company.sector} 섹터 비교 완료")
            
    #         return {
    #             'success': True,
    #             'symbol': symbol,
    #             'company_sector': company.sector,
    #             'comparison': comparison,
    #             'sector_stats': sector_stats,
    #             'data_available': True,
    #             'message': f'{symbol}와 {company.sector} 섹터 평균을 비교했습니다'
    #         }
            
    #     except Exception as e:
    #         logger.error(f"❌ {symbol} 섹터 비교 실패: {e}")
    #         self.stats["errors"] += 1
    #         return {
    #             'success': False,
    #             'symbol': symbol,
    #             'error': str(e),
    #             'data_available': False
    #         }
    #     finally:
    #         db.close()
    
    # =========================
    # 🎯 전체 데이터 조회
    # =========================
    
    def get_all_companies(self, batch_id: int = None, limit: int = 500) -> Dict[str, Any]:
        """
        전체 회사 리스트 조회 (요약 정보)
        
        Args:
            batch_id: 특정 배치 ID (None이면 최신)
            limit: 반환할 최대 개수
            
        Returns:
            Dict[str, Any]: 전체 회사 리스트
        """
        try:
            self.stats["api_requests"] += 1
            self.stats["last_request"] = datetime.now(pytz.UTC)
            
            db = next(get_db())
            
            if batch_id:
                # 특정 배치의 데이터 조회 (추후 구현 필요)
                companies = []
                used_batch_id = batch_id
            else:
                # 최신 배치 데이터 조회
                companies = CompanyOverview.get_latest_batch_data(db, limit)
                used_batch_id = companies[0].batch_id if companies else None
            
            if not companies:
                self.stats["data_not_found"] += 1
                return {
                    'success': False,
                    'message': 'Company Overview 데이터가 없습니다. 데이터 수집이 완료될 때까지 기다려주세요.',
                    'companies': [],
                    'total_count': 0,
                    'data_available': False
                }
            
            # 요약 정보로 변환
            company_summaries = [
                db_to_company_overview_summary(company) 
                for company in companies
            ]
            
            self.stats["db_queries"] += 1
            
            logger.info(f"✅ 전체 회사 리스트 조회 완료: {len(company_summaries)}개")
            
            return {
                'success': True,
                'companies': company_summaries,
                'total_count': len(company_summaries),
                'batch_id': used_batch_id,
                'data_available': True,
                'message': f'{len(company_summaries)}개 회사 정보를 조회했습니다'
            }
            
        except Exception as e:
            logger.error(f"❌ 전체 회사 리스트 조회 실패: {e}")
            self.stats["errors"] += 1
            return {
                'success': False,
                'companies': [],
                'total_count': 0,
                'error': str(e),
                'data_available': False
            }
        finally:
            db.close()
    
    def get_available_sectors(self) -> Dict[str, Any]:
        """
        사용 가능한 섹터 리스트 조회
        
        Returns:
            Dict[str, Any]: 섹터 리스트와 각 섹터별 회사 수
        """
        try:
            self.stats["api_requests"] += 1
            
            db = next(get_db())
            
            # 최신 배치에서 섹터별 회사 수 조회
            from sqlalchemy import func
            
            latest_batch = db.query(func.max(CompanyOverview.batch_id)).scalar()
            
            if not latest_batch:
                return {
                    'success': False,
                    'message': 'Company Overview 데이터가 없습니다',
                    'sectors': [],
                    'data_available': False
                }
            
            sector_counts = db.query(
                CompanyOverview.sector,
                func.count(CompanyOverview.symbol).label('company_count')
            ).filter(
                CompanyOverview.batch_id == latest_batch,
                CompanyOverview.sector.isnot(None)
            ).group_by(
                CompanyOverview.sector
            ).order_by(
                func.count(CompanyOverview.symbol).desc()
            ).all()
            
            sectors = [
                {
                    'sector': row.sector,
                    'company_count': row.company_count
                }
                for row in sector_counts
            ]
            
            logger.info(f"✅ 사용 가능한 섹터 조회 완료: {len(sectors)}개")
            
            return {
                'success': True,
                'sectors': sectors,
                'total_sectors': len(sectors),
                'batch_id': latest_batch,
                'data_available': True
            }
            
        except Exception as e:
            logger.error(f"❌ 섹터 리스트 조회 실패: {e}")
            self.stats["errors"] += 1
            return {
                'success': False,
                'sectors': [],
                'error': str(e),
                'data_available': False
            }
        finally:
            db.close()
    
    # =========================
    # 🎯 유틸리티 메서드
    # =========================
    
    def _calculate_sector_comparison(self, company: CompanyOverview, sector_stats: Dict[str, Any]) -> Dict[str, Any]:
        """
        회사와 섹터 평균 비교 계산
        
        Args:
            company: 회사 데이터
            sector_stats: 섹터 통계
            
        Returns:
            Dict[str, Any]: 비교 결과
        """
        def safe_comparison(company_value, sector_avg, metric_name):
            """안전한 비교 계산"""
            if company_value is None or sector_avg is None:
                return {
                    'company_value': company_value,
                    'sector_average': sector_avg,
                    'difference': None,
                    'percentage_diff': None,
                    'comparison': 'N/A'
                }
            
            company_val = float(company_value)
            sector_val = float(sector_avg)
            difference = company_val - sector_val
            percentage_diff = (difference / sector_val) * 100 if sector_val != 0 else None
            
            if percentage_diff is not None:
                if percentage_diff > 10:
                    comparison = 'Much Higher'
                elif percentage_diff > 0:
                    comparison = 'Higher'
                elif percentage_diff < -10:
                    comparison = 'Much Lower'
                elif percentage_diff < 0:
                    comparison = 'Lower'
                else:
                    comparison = 'Similar'
            else:
                comparison = 'N/A'
            
            return {
                'company_value': company_val,
                'sector_average': sector_val,
                'difference': round(difference, 4),
                'percentage_diff': round(percentage_diff, 2) if percentage_diff else None,
                'comparison': comparison
            }
        
        return {
            'pe_ratio': safe_comparison(company.pe_ratio, sector_stats.get('avg_pe_ratio'), 'P/E Ratio'),
            'roe': safe_comparison(company.return_on_equity_ttm, sector_stats.get('avg_roe'), 'ROE'),
            'profit_margin': safe_comparison(company.profit_margin, sector_stats.get('avg_profit_margin'), 'Profit Margin'),
            'dividend_yield': safe_comparison(company.dividend_yield, sector_stats.get('avg_dividend_yield'), 'Dividend Yield')
        }
    
    def check_data_availability(self, symbol: str = None) -> Dict[str, Any]:
        """
        데이터 가용성 체크
        
        Args:
            symbol: 특정 심볼 체크 (None이면 전체 현황)
            
        Returns:
            Dict[str, Any]: 데이터 가용성 정보
        """
        try:
            db = next(get_db())
            
            if symbol:
                # 특정 심볼의 데이터 존재 여부
                exists = CompanyOverview.symbol_exists(db, symbol.upper())
                return {
                    'symbol': symbol.upper(),
                    'data_available': exists,
                    'message': f'{symbol} 데이터가 {"있습니다" if exists else "없습니다"}'
                }
            else:
                # 전체 데이터 현황
                available_batches = CompanyOverview.get_available_batches(db)
                
                if not available_batches:
                    return {
                        'data_available': False,
                        'message': 'Company Overview 데이터가 없습니다',
                        'batches': []
                    }
                
                # 최신 배치 정보
                latest_batch = available_batches[0]
                latest_count = db.query(func.count(CompanyOverview.symbol)).filter(
                    CompanyOverview.batch_id == latest_batch
                ).scalar()
                
                return {
                    'data_available': True,
                    'latest_batch_id': latest_batch,
                    'latest_batch_companies': latest_count,
                    'total_batches': len(available_batches),
                    'all_batches': available_batches,
                    'message': f'총 {len(available_batches)}개 배치, 최신 배치에 {latest_count}개 회사'
                }
                
        except Exception as e:
            logger.error(f"❌ 데이터 가용성 체크 실패: {e}")
            return {
                'data_available': False,
                'error': str(e)
            }
        finally:
            db.close()
    
    # =========================
    # 🎯 서비스 상태 및 통계
    # =========================
    
    def get_service_stats(self) -> Dict[str, Any]:
        """서비스 통계 정보 반환"""
        return {
            'service_name': 'CompanyOverviewService',
            'stats': self.stats,
            'uptime': datetime.now(pytz.UTC).isoformat()
        }
    
    def health_check(self) -> Dict[str, Any]:
        """서비스 헬스 체크"""
        try:
            db = next(get_db())
            
            # 데이터베이스 연결 테스트
            result = db.execute("SELECT 1").fetchone()
            db_status = "healthy" if result else "unhealthy"
            
            # 데이터 존재 여부 확인
            data_count = db.query(func.count(CompanyOverview.symbol)).scalar()
            
            return {
                'status': 'healthy' if db_status == 'healthy' and data_count > 0 else 'degraded',
                'database': db_status,
                'data_count': data_count,
                'data_available': data_count > 0,
                'last_check': datetime.now(pytz.UTC).isoformat()
            }
            
        except Exception as e:
            logger.error(f"❌ Company Overview 헬스 체크 실패: {e}")
            return {
                'status': 'unhealthy',
                'database': 'error',
                'error': str(e),
                'last_check': datetime.now(pytz.UTC).isoformat()
            }
        finally:
            db.close()