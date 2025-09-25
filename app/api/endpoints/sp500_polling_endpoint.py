# app/api/endpoints/sp500_endpoint.py
from fastapi import APIRouter, HTTPException, Depends, Query, Path
from fastapi.responses import JSONResponse
from typing import List, Optional, Dict, Any
import logging
from datetime import datetime
import pytz
from app.database import get_db
from app.services.sp500_service import SP500Service
from app.services.company_overview_service import CompanyOverviewService  # 🆕 추가
from app.services.balance_sheet_service import BalanceSheetService  # 🆕 추가
from app.schemas.sp500_schema import (
    StockListResponse, StockDetail, CategoryStockResponse,
    SearchResponse, MarketOverviewResponse,
    ServiceStats, HealthCheckResponse, ErrorResponse,
    TimeframeEnum, create_error_response
)

# 로깅 설정
logger = logging.getLogger(__name__)

# 라우터 생성
router = APIRouter()

# 서비스 인스턴스 생성 (의존성)
def get_sp500_service() -> SP500Service:
    """SP500Service 의존성 제공"""
    return SP500Service()

def get_company_overview_service() -> CompanyOverviewService:
    """CompanyOverviewService 의존성 제공"""
    return CompanyOverviewService()

def get_balance_sheet_service() -> BalanceSheetService:
    """BalanceSheetService 의존성 제공"""
    return BalanceSheetService()
# =========================
# 🎯 주식 리스트 및 개요 엔드포인트
# =========================

@router.get("/", response_model=StockListResponse, summary="전체 주식 리스트 조회")
async def get_all_stocks(
    limit: int = Query(default=500, ge=1, le=500, description="반환할 최대 주식 개수"),
    sp500_service: SP500Service = Depends(get_sp500_service)
):
    """
    전체 SP500 주식 리스트 조회
    
    **주요 기능:**
    - 모든 SP500 주식의 현재가 조회
    - 전일 대비 변동률 계산
    - 거래량 정보 포함
    - 시장 상태 정보 제공
    
    **사용 예시:**
    ```
    GET /stocks/sp500/?limit=100
    ```
    
    **응답 데이터:**
    - 주식 심볼, 회사명, 현재가
    - 변동 금액, 변동률 (전일 대비)
    - 거래량, 섹터 정보
    - 시장 상태 (개장/마감)
    """
    try:
        logger.info(f"📊 전체 주식 리스트 조회 요청 (limit: {limit})")
        
        result = sp500_service.get_stock_list(limit)
        
        if result.get('error'):
            logger.error(f"❌ 주식 리스트 조회 실패: {result['error']}")
            raise HTTPException(
                status_code=500,
                detail=create_error_response(
                    error_type="DATA_FETCH_ERROR",
                    message=f"Failed to fetch stock list: {result['error']}",
                    path="/stocks/sp500/"
                ).model_dump()
            )
        
        logger.info(f"✅ 주식 리스트 조회 성공: {result['total_count']}개")
        return StockListResponse(**result)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ 예상치 못한 오류: {e}")
        raise HTTPException(
            status_code=500,
            detail=create_error_response(
                error_type="INTERNAL_ERROR",
                message="Internal server error occurred",
                path="/stocks/sp500/"
            ).model_dump()
        )

@router.get("/market-overview", response_model=MarketOverviewResponse, summary="시장 개요 조회")
async def get_market_overview(
    sp500_service: SP500Service = Depends(get_sp500_service)
):
    """
    전체 시장 개요 및 하이라이트 조회
    
    **주요 기능:**
    - 시장 전체 요약 통계
    - 상위 상승/하락 종목 (각 5개)
    - 가장 활발한 거래 종목 (5개)
    - 시장 상태 정보
    
    **사용 예시:**
    ```
    GET /stocks/sp500/market-overview
    ```
    
    **응답 데이터:**
    - 총 심볼 수, 평균 가격, 최고/최저가
    - 상위 상승 종목 리스트
    - 상위 하락 종목 리스트
    - 활발한 거래 종목 리스트
    """
    try:
        logger.info("📊 시장 개요 조회 요청")
        
        result = sp500_service.get_market_overview()
        
        if result.get('error'):
            logger.error(f"❌ 시장 개요 조회 실패: {result['error']}")
            raise HTTPException(
                status_code=500,
                detail=create_error_response(
                    error_type="MARKET_DATA_ERROR",
                    message=f"Failed to fetch market overview: {result['error']}",
                    path="/stocks/sp500/market-overview"
                ).model_dump()
            )
        
        logger.info("✅ 시장 개요 조회 성공")
        return MarketOverviewResponse(**result)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ 예상치 못한 오류: {e}")
        raise HTTPException(
            status_code=500,
            detail=create_error_response(
                error_type="INTERNAL_ERROR",
                message="Internal server error occurred",
                path="/stocks/sp500/market-overview"
            ).model_dump()
        )

@router.get("/polling", response_model=dict, summary="SP500 실시간 폴링 데이터 (페이징 방식)")
async def get_sp500_polling_data(
    limit: int = Query(default=50, ge=1, le=500, description="페이지당 항목 수"),
    offset: int = Query(default=0, ge=0, description="시작 위치 (0부터 시작)"),
    sort_by: str = Query(default="volume", description="정렬 기준: volume, change_percent, price"),
    order: str = Query(default="desc", regex="^(asc|desc)$", description="정렬 순서"),
    sp500_service: SP500Service = Depends(get_sp500_service)
):
    """
    SP500 실시간 폴링 데이터 (페이징 방식, "더보기" 지원)
    
    **동작 방식:**
    - offset=0, limit=50: 첫 번째 페이지 (1~50번째)
    - offset=50, limit=50: 두 번째 페이지 (51~100번째)
    - offset=100, limit=50: 세 번째 페이지 (101~150번째)
    
    **정렬 옵션:**
    - `volume`: 거래량 순 (기본값)
    - `change_percent`: 변동률 순  
    - `price`: 가격 순
    
    **사용 예시:**
    ```
    GET /api/v1/stocks/sp500/polling                           # 첫 페이지 (0~49)
    GET /api/v1/stocks/sp500/polling?offset=50                 # 두 번째 페이지 (50~99)
    GET /api/v1/stocks/sp500/polling?offset=100&limit=50       # 세 번째 페이지 (100~149)
    ```
    
    **응답 데이터:**
    - 요청한 페이지의 데이터만 반환
    - 페이징 메타데이터 (total_count, has_next, current_page 등)
    - 프론트엔드에서 "더보기" 버튼 구현 가능
    """
    try:
        logger.info(f"📡 SP500 폴링 데이터 요청 (offset: {offset}, limit: {limit}, sort: {sort_by})")
        
        result = await sp500_service.get_realtime_polling_data(
            limit=limit,
            offset=offset,
            sort_by=sort_by,
            order=order
        )
        
        if result.get('error'):
            logger.error(f"❌ SP500 폴링 데이터 조회 실패: {result['error']}")
            raise HTTPException(status_code=500, detail=result['error'])
        
        logger.info(f"✅ SP500 폴링 데이터 조회 성공: {len(result.get('data', []))}개 반환 (페이지: {offset//limit + 1})")
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ 예상치 못한 오류: {e}")
        raise HTTPException(status_code=500, detail=str(e))
# =========================
# 🎯 개별 주식 상세 조회 엔드포인트 (Company Overview 통합) 🆕
# =========================

@router.get("/symbol/{symbol}", summary="개별 주식 통합 정보 조회 (Company Overview + Balance Sheet)")
async def get_stock_detail_with_integrated_data(
    symbol: str = Path(..., description="주식 심볼 (예: AAPL)", regex=r"^[A-Z]{1,5}$"),
    sp500_service: SP500Service = Depends(get_sp500_service),
    company_service: CompanyOverviewService = Depends(get_company_overview_service),
    balance_service: BalanceSheetService = Depends(get_balance_sheet_service)  # 추가
):
    """
    개별 주식 통합 정보 조회 (4가지 데이터 케이스 지원)
    
    **주요 기능:**
    - SP500 실시간 데이터 (현재가, 변동률, 거래량)
    - Company Overview 데이터 (기업 정보, 재무 지표)
    - Balance Sheet 데이터 (재무상태표, 재무비율)
    - **4가지 케이스별 적응형 응답**
    
    **4가지 데이터 케이스:**
    1. **완전 데이터**: Company Overview + Balance Sheet 모두 있음
    2. **기업 정보만**: Company Overview만 있음 (Balance Sheet 수집 중)
    3. **재무 데이터만**: Balance Sheet만 있음 (Company Overview 수집 중)
    4. **기본 데이터만**: 둘 다 없음 (실시간 주가 데이터만)
    
    **사용 예시:**
    ```
    GET /stocks/sp500/symbol/AAPL  # 애플 통합 정보
    GET /stocks/sp500/symbol/TSLA  # 테슬라 통합 정보
    ```
    
    **응답 구조:**
    - `current_price`: 실시간 주가 정보 (SP500)
    - `company_info`: 회사 상세 정보 (Company Overview)
    - `financial_data`: 재무상태표 정보 (Balance Sheet)
    - `integrated_analysis`: 통합 분석 결과
    - `data_status`: 각 데이터 소스별 가용성
    """
    try:
        symbol = symbol.upper()
        logger.info(f"통합 주식 정보 조회: {symbol}")
        
        # 1. SP500 실시간 데이터 조회 (필수)
        stock_result = sp500_service.get_stock_basic_info(symbol)
        
        if stock_result.get('error'):
            if 'No data found' in stock_result['error']:
                logger.warning(f"주식 데이터 없음: {symbol}")
                raise HTTPException(
                    status_code=404,
                    detail=create_error_response(
                        error_type="STOCK_NOT_FOUND",
                        message=f"No stock data found for symbol: {symbol}",
                        path=f"/stocks/sp500/symbol/{symbol}"
                    ).model_dump()
                )
            else:
                raise HTTPException(
                    status_code=500,
                    detail=create_error_response(
                        error_type="STOCK_DATA_ERROR",
                        message=f"Failed to fetch stock data: {stock_result['error']}",
                        path=f"/stocks/sp500/symbol/{symbol}"
                    ).model_dump()
                )
        
        # 2. Company Overview 데이터 조회 (옵션)
        logger.info(f"Company Overview 데이터 조회 시작: {symbol}")
        company_result = company_service.get_company_basic_metrics(symbol)
        has_company_data = company_result.get('data_available', False)
        logger.info(f"Company Overview 결과: data_available={has_company_data}, result_keys={list(company_result.keys())}")
        
        if not has_company_data:
            logger.warning(f"⚠️ {symbol} Company Overview 데이터 없음: {company_result.get('message', 'Unknown reason')}")
            if 'debug_info' in company_result:
                logger.info(f"디버그 정보: {company_result['debug_info']}")
        else:
            logger.info(f"✅ {symbol} Company Overview 데이터 있음: {company_result.get('company_name', 'Unknown')}")
        
        # 3. Balance Sheet 데이터 조회 (옵션) - 새로 추가
        balance_result = _get_balance_sheet_summary(balance_service, symbol)
        has_balance_data = balance_result.get('data_available', False)
        
        # 4. 데이터 케이스 결정
        data_case = _determine_data_case(has_company_data, has_balance_data)
        
        # 5. 케이스별 통합 응답 구성
        integrated_response = _build_integrated_response(
            symbol=symbol,
            data_case=data_case,
            current_price=stock_result,
            company_data=company_result,
            balance_data=balance_result
        )
        
        logger.info(f"통합 데이터 조회 성공: {symbol} (케이스: {data_case})")
        return JSONResponse(content=integrated_response)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"통합 데이터 조회 실패: {symbol} - {e}")
        raise HTTPException(
            status_code=500,
            detail=create_error_response(
                error_type="INTERNAL_ERROR",
                message="Internal server error occurred",
                path=f"/stocks/sp500/symbol/{symbol}"
            ).model_dump()
        )

# 헬퍼 함수들
def _get_balance_sheet_summary(balance_service: BalanceSheetService, symbol: str) -> Dict[str, Any]:
    """Balance Sheet 요약 데이터 조회"""
    try:
        logger.info(f"Balance Sheet 데이터 조회 시작: {symbol}")
        
        # 최신 재무상태표 조회
        latest_balance = balance_service.get_latest_by_symbol(symbol)
        
        if not latest_balance:
            logger.warning(f"⚠️ {symbol} Balance Sheet 데이터 없음 - DB에서 조회 결과 없음")
            return {
                'data_available': False,
                'message': f'{symbol} 재무제표 데이터가 아직 수집되지 않았습니다',
                'debug_info': f'DB 쿼리 결과: None for symbol {symbol}'
            }
        
        # 재무비율 계산
        financial_ratios = balance_service.calculate_financial_ratios(latest_balance)
        
        # 재무건전성 등급 계산
        health_grade = balance_service.calculate_financial_health_grade(financial_ratios)
        
        # 핵심 지표만 추출
        key_metrics = {
            'total_assets': int(latest_balance.totalassets) if latest_balance.totalassets else None,
            'total_liabilities': int(latest_balance.totalliabilities) if latest_balance.totalliabilities else None,
            'shareholders_equity': int(latest_balance.totalshareholderequity) if latest_balance.totalshareholderequity else None,
            'cash_and_equivalents': int(latest_balance.cashandcashequivalentsatcarryingvalue) if latest_balance.cashandcashequivalentsatcarryingvalue else None,
            'fiscal_date_ending': latest_balance.fiscaldateending.isoformat() if latest_balance.fiscaldateending else None
        }
        
        # 핵심 비율만 추출
        key_ratios = {}
        if 'current_ratio' in financial_ratios:
            key_ratios['current_ratio'] = {
                'value': financial_ratios['current_ratio'].value,
                'status': financial_ratios['current_ratio'].status,
                'description': financial_ratios['current_ratio'].description
            }
        if 'debt_to_asset' in financial_ratios:
            key_ratios['debt_to_asset'] = {
                'value': financial_ratios['debt_to_asset'].value,
                'status': financial_ratios['debt_to_asset'].status,
                'description': financial_ratios['debt_to_asset'].description
            }
        
        logger.info(f"✅ {symbol} Balance Sheet 요약 데이터 생성 완료")
        return {
            'data_available': True,
            'key_metrics': key_metrics,
            'key_ratios': key_ratios,
            'financial_health': {
                'grade': health_grade.grade,
                'score': health_grade.score,
                'status': health_grade.status
            },
            'latest_period': latest_balance.fiscaldateending.isoformat(),
            'message': '재무제표 데이터가 있습니다'
        }
        
    except Exception as e:
        logger.error(f"❌ Balance Sheet 요약 조회 실패: {symbol} - {e}")
        import traceback
        logger.error(f"❌ 상세 에러 트레이스: {traceback.format_exc()}")
        return {
            'data_available': False,
            'error': f'Balance Sheet 조회 중 오류 발생: {str(e)}',
            'debug_info': f'Exception: {type(e).__name__}'
        }

def _determine_data_case(has_company: bool, has_balance: bool) -> str:
    """데이터 케이스 결정"""
    if has_company and has_balance:
        return "complete_data"      # 케이스 1: 모든 데이터 있음
    elif has_company and not has_balance:
        return "company_only"       # 케이스 2: 기업 정보만
    elif not has_company and has_balance:
        return "financial_only"     # 케이스 3: 재무 데이터만
    else:
        return "basic_only"         # 케이스 4: 기본 주가 데이터만

def _build_integrated_response(
    symbol: str,
    data_case: str,
    current_price: Dict[str, Any],
    company_data: Dict[str, Any],
    balance_data: Dict[str, Any]
) -> Dict[str, Any]:
    """케이스별 통합 응답 구성"""
    
    # 공통 기본 구조
    response = {
        'symbol': symbol,
        'data_case': data_case,
        'timestamp': datetime.now(pytz.UTC).isoformat(),
        
        # 실시간 주가 데이터 (항상 포함)
        'current_price': current_price,
        
        # 데이터 가용성 상태
        'data_availability': {
            'current_price': True,  # 여기까지 왔으면 주가 데이터는 있음
            'company_overview': company_data.get('data_available', False),
            'balance_sheet': balance_data.get('data_available', False)
        }
    }
    
    # 케이스별 데이터 추가
    if data_case == "complete_data":
        # 케이스 1: 모든 데이터 있음 - 풍부한 정보 제공
        response.update({
            'company_info': {
                'available': True,
                'data': {
                    'name': company_data.get('company_name'),
                    'sector': company_data.get('sector'),
                    'industry': company_data.get('industry'),
                    'description': company_data.get('description'),
                    'website': company_data.get('website'),
                    'market_cap': company_data.get('market_cap'),
                    'pe_ratio': company_data.get('pe_ratio'),
                    'dividend_yield': company_data.get('dividend_yield'),
                    'beta': company_data.get('beta'),
                    'roe': company_data.get('roe'),
                    'profit_margin': company_data.get('profit_margin'),
                    'batch_id': company_data.get('batch_id')
                }
            },
            'financial_data': {
                'available': True,
                'data': balance_data
            },
            'integrated_analysis': {
                'available': True,
                'summary': f"{symbol}는 완전한 재무 데이터를 보유하고 있어 종합적인 투자 분석이 가능합니다.",
                'investment_perspective': _generate_investment_perspective(company_data, balance_data),
                'key_highlights': _generate_key_highlights(company_data, balance_data)
            }
        })
        
    elif data_case == "company_only":
        # 케이스 2: 기업 정보만 있음
        response.update({
            'company_info': {
                'available': True,
                'data': {
                    'name': company_data.get('company_name'),
                    'sector': company_data.get('sector'),
                    'industry': company_data.get('industry'),
                    'description': company_data.get('description'),
                    'website': company_data.get('website'),
                    'market_cap': company_data.get('market_cap'),
                    'pe_ratio': company_data.get('pe_ratio'),
                    'dividend_yield': company_data.get('dividend_yield'),
                    'beta': company_data.get('beta'),
                    'roe': company_data.get('roe'),
                    'profit_margin': company_data.get('profit_margin')
                }
            },
            'financial_data': {
                'available': False,
                'status': 'collecting',
                'message': '재무제표 데이터 수집 중입니다. 곧 업데이트 될 예정입니다.',
                'expected_completion': '데이터 수집 진행 중'
            },
            'integrated_analysis': {
                'available': False,
                'message': '재무제표 데이터 수집 완료 후 통합 분석이 제공됩니다.'
            }
        })
        
    elif data_case == "financial_only":
        # 케이스 3: 재무 데이터만 있음
        response.update({
            'company_info': {
                'available': False,
                'status': 'collecting',
                'message': '기업 상세 정보 수집 중입니다. 곧 업데이트 될 예정입니다.',
                'expected_completion': '데이터 수집 진행 중'
            },
            'financial_data': {
                'available': True,
                'data': balance_data
            },
            'integrated_analysis': {
                'available': False,
                'message': '기업 정보 수집 완료 후 통합 분석이 제공됩니다.'
            }
        })
        
    else:  # basic_only
        # 케이스 4: 기본 주가 데이터만
        response.update({
            'company_info': {
                'available': False,
                'status': 'collecting',
                'message': '기업 상세 정보 수집 중입니다.',
                'expected_completion': '데이터 수집 진행 중'
            },
            'financial_data': {
                'available': False,
                'status': 'collecting', 
                'message': '재무제표 데이터 수집 중입니다.',
                'expected_completion': '데이터 수집 진행 중'
            },
            'integrated_analysis': {
                'available': False,
                'message': '모든 데이터 수집 완료 후 통합 분석이 제공됩니다.'
            }
        })
    
    # 데이터 소스 정보 추가
    response['data_sources'] = {
        'current_price': 'sp500_websocket_trades',
        'company_overview': 'alpha_vantage_company_overview' if company_data.get('data_available') else 'not_collected',
        'balance_sheet': 'alpha_vantage_balance_sheet' if balance_data.get('data_available') else 'not_collected'
    }
    
    return response

def _generate_investment_perspective(company_data: Dict[str, Any], balance_data: Dict[str, Any]) -> str:
    """투자 관점 생성 (완전한 데이터가 있을 때만)"""
    try:
        pe_ratio = company_data.get('pe_ratio')
        health_grade = balance_data.get('financial_health', {}).get('grade', 'N/A')
        sector = company_data.get('sector', 'Unknown')
        
        if pe_ratio and pe_ratio < 15:
            pe_assessment = "저평가"
        elif pe_ratio and pe_ratio > 25:
            pe_assessment = "고평가"
        else:
            pe_assessment = "적정평가"
        
        return f"{sector} 섹터의 {pe_assessment} 종목으로, 재무건전성 {health_grade} 등급을 보이고 있습니다."
    
    except Exception:
        return "투자 관점 분석을 위해 추가 데이터를 처리하고 있습니다."

def _generate_key_highlights(company_data: Dict[str, Any], balance_data: Dict[str, Any]) -> List[str]:
    """핵심 하이라이트 생성"""
    highlights = []
    
    try:
        # Company Overview 하이라이트
        market_cap = company_data.get('market_cap')
        if market_cap:
            if market_cap > 100_000_000_000:  # 100B+
                highlights.append("대형주: 시가총액 1,000억 달러 이상")
            elif market_cap > 10_000_000_000:  # 10B+
                highlights.append("중형주: 안정적인 시가총액")
        
        dividend_yield = company_data.get('dividend_yield')
        if dividend_yield and dividend_yield > 0.03:  # 3%+
            highlights.append(f"배당주: 배당수익률 {dividend_yield*100:.1f}%")
        
        # Balance Sheet 하이라이트
        health_grade = balance_data.get('financial_health', {}).get('grade')
        if health_grade and health_grade.startswith('A'):
            highlights.append("우수한 재무건전성")
        
        # 기본 하이라이트가 없으면 기본 메시지
        if not highlights:
            highlights.append("종합적인 재무 분석 가능한 종목")
        
        return highlights[:3]  # 최대 3개까지
    
    except Exception:
        return ["데이터 분석 진행 중"]

# =========================
# 🎯 차트 데이터 전용 엔드포인트 🆕
# =========================

@router.get("/chart/{symbol}", summary="주식 차트 데이터 조회")
async def get_stock_chart_data(
    symbol: str = Path(..., description="주식 심볼 (예: AAPL)", regex=r"^[A-Z]{1,5}$"),
    timeframe: TimeframeEnum = Query(default=TimeframeEnum.ONE_DAY, description="차트 시간대"),
    sp500_service: SP500Service = Depends(get_sp500_service)
):
    """
    주식 차트 데이터 전용 조회
    
    **주요 기능:**
    - 시간대별 차트 데이터만 제공
    - 가격 및 거래량 시계열 데이터
    - 다양한 시간대 지원
    
    **지원하는 차트 시간대:**
    - `1M`: 1분 차트
    - `5M`: 5분 차트
    - `1H`: 1시간 차트
    - `1D`: 1일 차트 (기본값)
    - `1W`: 1주 차트
    - `1MO`: 1개월 차트
    
    **사용 예시:**
    ```
    GET /stocks/sp500/chart/AAPL?timeframe=1D
    GET /stocks/sp500/chart/TSLA?timeframe=1H
    GET /stocks/sp500/chart/MSFT?timeframe=1W
    ```
    
    **응답 데이터:**
    - 시간별 가격 데이터 (timestamp, price, volume)
    - 차트 렌더링에 필요한 모든 데이터 포인트
    - 시간대별 최적화된 데이터 샘플링
    """
    try:
        symbol = symbol.upper()
        logger.info(f"📈 {symbol} 차트 데이터 조회 요청 (timeframe: {timeframe})")
        
        # 차트 데이터만 조회
        chart_result = sp500_service.get_chart_data_only(symbol, timeframe.value)
        
        if chart_result.get('error'):
            if 'No data found' in chart_result['error']:
                logger.warning(f"⚠️ {symbol} 차트 데이터 없음")
                raise HTTPException(
                    status_code=404,
                    detail=create_error_response(
                        error_type="CHART_DATA_NOT_FOUND",
                        message=f"No chart data found for symbol: {symbol}",
                        code="CHART_404",
                        path=f"/stocks/sp500/chart/{symbol}"
                    ).model_dump()
                )
            else:
                logger.error(f"❌ {symbol} 차트 데이터 조회 실패: {chart_result['error']}")
                raise HTTPException(
                    status_code=500,
                    detail=create_error_response(
                        error_type="CHART_DATA_ERROR",
                        message=f"Failed to fetch chart data: {chart_result['error']}",
                        path=f"/stocks/sp500/chart/{symbol}"
                    ).model_dump()
                )
        
        logger.info(f"✅ {symbol} 차트 데이터 조회 성공 (timeframe: {timeframe}, 데이터: {len(chart_result.get('chart_data', []))}개)")
        return JSONResponse(content=chart_result)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ 예상치 못한 오류: {e}")
        raise HTTPException(
            status_code=500,
            detail=create_error_response(
                error_type="INTERNAL_ERROR",
                message="Internal server error occurred",
                path=f"/stocks/sp500/chart/{symbol}"
            ).model_dump()
        )

# =========================
# 🎯 카테고리별 주식 조회 엔드포인트
# =========================

@router.get("/gainers", response_model=CategoryStockResponse, summary="상위 상승 종목 조회")
async def get_top_gainers(
    limit: int = Query(default=20, ge=1, le=100, description="반환할 최대 종목 개수"),
    sp500_service: SP500Service = Depends(get_sp500_service)
):
    """
    상위 상승 종목 조회
    
    **주요 기능:**
    - 전일 대비 상승률이 높은 종목들 조회
    - 상승률 기준 내림차순 정렬
    - 현재가, 변동률, 거래량 정보 포함
    
    **사용 예시:**
    ```
    GET /stocks/sp500/gainers?limit=10
    ```
    """
    try:
        logger.info(f"📈 상위 상승 종목 조회 요청 (limit: {limit})")
        
        result = sp500_service.get_top_gainers(limit)
        
        if result.get('error'):
            logger.error(f"❌ 상위 상승 종목 조회 실패: {result['error']}")
            raise HTTPException(
                status_code=500,
                detail=create_error_response(
                    error_type="DATA_FETCH_ERROR",
                    message=f"Failed to fetch top gainers: {result['error']}",
                    path="/stocks/sp500/gainers"
                ).model_dump()
            )
        
        logger.info(f"✅ 상위 상승 종목 조회 성공: {result['total_count']}개")
        return CategoryStockResponse(**result)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ 예상치 못한 오류: {e}")
        raise HTTPException(
            status_code=500,
            detail=create_error_response(
                error_type="INTERNAL_ERROR",
                message="Internal server error occurred",
                path="/stocks/sp500/gainers"
            ).model_dump()
        )

@router.get("/losers", response_model=CategoryStockResponse, summary="상위 하락 종목 조회")
async def get_top_losers(
    limit: int = Query(default=20, ge=1, le=100, description="반환할 최대 종목 개수"),
    sp500_service: SP500Service = Depends(get_sp500_service)
):
    """
    상위 하락 종목 조회
    
    **주요 기능:**
    - 전일 대비 하락률이 높은 종목들 조회
    - 하락률 기준 내림차순 정렬 (가장 많이 떨어진 순)
    - 현재가, 변동률, 거래량 정보 포함
    
    **사용 예시:**
    ```
    GET /stocks/sp500/losers?limit=15
    ```
    """
    try:
        logger.info(f"📉 상위 하락 종목 조회 요청 (limit: {limit})")
        
        result = sp500_service.get_top_losers(limit)
        
        if result.get('error'):
            logger.error(f"❌ 상위 하락 종목 조회 실패: {result['error']}")
            raise HTTPException(
                status_code=500,
                detail=create_error_response(
                    error_type="DATA_FETCH_ERROR",
                    message=f"Failed to fetch top losers: {result['error']}",
                    path="/stocks/sp500/losers"
                ).model_dump()
            )
        
        logger.info(f"✅ 상위 하락 종목 조회 성공: {result['total_count']}개")
        return CategoryStockResponse(**result)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ 예상치 못한 오류: {e}")
        raise HTTPException(
            status_code=500,
            detail=create_error_response(
                error_type="INTERNAL_ERROR",
                message="Internal server error occurred",
                path="/stocks/sp500/losers"
            ).model_dump()
        )

@router.get("/most-active", response_model=CategoryStockResponse, summary="가장 활발한 거래 종목 조회")
async def get_most_active(
    limit: int = Query(default=20, ge=1, le=100, description="반환할 최대 종목 개수"),
    sp500_service: SP500Service = Depends(get_sp500_service)
):
    """
    가장 활발한 거래 종목 조회
    
    **주요 기능:**
    - 거래량이 가장 많은 종목들 조회
    - 거래량 기준 내림차순 정렬
    - 현재가, 변동률, 거래량 정보 포함
    
    **사용 예시:**
    ```
    GET /stocks/sp500/most-active?limit=25
    ```
    """
    try:
        logger.info(f"📊 활발한 거래 종목 조회 요청 (limit: {limit})")
        
        result = sp500_service.get_most_active(limit)
        
        if result.get('error'):
            logger.error(f"❌ 활발한 거래 종목 조회 실패: {result['error']}")
            raise HTTPException(
                status_code=500,
                detail=create_error_response(
                    error_type="DATA_FETCH_ERROR",
                    message=f"Failed to fetch most active stocks: {result['error']}",
                    path="/stocks/sp500/most-active"
                ).model_dump()
            )
        
        logger.info(f"✅ 활발한 거래 종목 조회 성공: {result['total_count']}개")
        return CategoryStockResponse(**result)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ 예상치 못한 오류: {e}")
        raise HTTPException(
            status_code=500,
            detail=create_error_response(
                error_type="INTERNAL_ERROR",
                message="Internal server error occurred",
                path="/stocks/sp500/most-active"
            ).model_dump()
        )

# =========================
# 🎯 검색 및 필터 엔드포인트
# =========================

@router.get("/search", response_model=SearchResponse, summary="주식 검색")
async def search_stocks(
    q: str = Query(..., description="검색어 (심볼 또는 회사명)", min_length=1, max_length=50),
    limit: int = Query(default=20, ge=1, le=100, description="반환할 최대 결과 개수"),
    sp500_service: SP500Service = Depends(get_sp500_service)
):
    """
    주식 검색 (심볼 또는 회사명 기준)
    
    **주요 기능:**
    - 주식 심볼 또는 회사명으로 검색
    - 부분 일치 검색 지원
    - 심볼 알파벳 순 정렬
    
    **사용 예시:**
    ```
    GET /stocks/sp500/search?q=apple
    GET /stocks/sp500/search?q=AAPL
    GET /stocks/sp500/search?q=tech&limit=10
    ```
    
    **검색 대상:**
    - 주식 심볼 (예: AAPL, MSFT)
    - 회사명 (예: Apple, Microsoft)
    """
    try:
        logger.info(f"🔍 주식 검색 요청: '{q}' (limit: {limit})")
        
        result = sp500_service.search_stocks(q.strip(), limit)
        
        if result.get('error'):
            logger.error(f"❌ 주식 검색 실패: {result['error']}")
            raise HTTPException(
                status_code=500,
                detail=create_error_response(
                    error_type="SEARCH_ERROR",
                    message=f"Search failed: {result['error']}",
                    path="/stocks/sp500/search"
                ).model_dump()
            )
        
        logger.info(f"✅ 주식 검색 성공: '{q}' -> {result['total_count']}개 결과")
        return SearchResponse(**result)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ 예상치 못한 오류: {e}")
        raise HTTPException(
            status_code=500,
            detail=create_error_response(
                error_type="INTERNAL_ERROR",
                message="Internal server error occurred",
                path="/stocks/sp500/search"
            ).model_dump()
        )

# =========================
# 🎯 Company Overview 전용 엔드포인트 제거 (복잡성 감소) 🆕
# =========================

# Company Overview 관련 복잡한 엔드포인트들은 제거하고
# /symbol/{symbol} 에서 간단하게 통합 제공

# =========================
# 🎯 서비스 상태 및 관리 엔드포인트
# =========================

@router.get("/health", response_model=HealthCheckResponse, summary="서비스 헬스 체크")
async def health_check(
    sp500_service: SP500Service = Depends(get_sp500_service)
):
    """
    SP500 서비스 헬스 체크
    
    **주요 기능:**
    - 데이터베이스 연결 상태 확인
    - 데이터 신선도 체크
    - 시장 상태 정보 제공
    - 서비스 전반적인 상태 진단
    
    **사용 예시:**
    ```
    GET /stocks/sp500/health
    ```
    
    **응답 상태:**
    - `healthy`: 모든 서비스가 정상
    - `degraded`: 일부 서비스에 문제
    - `unhealthy`: 서비스 사용 불가
    """
    try:
        logger.info("🏥 SP500 서비스 헬스 체크 요청")
        
        result = sp500_service.health_check()
        
        logger.info(f"✅ 헬스 체크 완료: {result['status']}")
        return HealthCheckResponse(**result)
        
    except Exception as e:
        logger.error(f"❌ 헬스 체크 실패: {e}")
        return HealthCheckResponse(
            status="unhealthy",
            database="error",
            data_freshness="unknown",
            market_status={
                'is_open': False,
                'status': 'UNKNOWN',
                'current_time_et': 'N/A',
                'current_time_utc': 'N/A',
                'timezone': 'US/Eastern'
            },
            last_check=datetime.now(pytz.UTC).isoformat(),
            error=str(e)
        )

@router.get("/stats", response_model=ServiceStats, summary="서비스 통계 조회")
async def get_service_stats(
    sp500_service: SP500Service = Depends(get_sp500_service)
):
    """
    SP500 서비스 통계 정보 조회
    
    **주요 기능:**
    - API 요청 횟수 통계
    - 데이터베이스 쿼리 통계
    - 에러 발생 현황
    - 서비스 가동 시간
    
    **사용 예시:**
    ```
    GET /stocks/sp500/stats
    ```
    """
    try:
        logger.info("📊 SP500 서비스 통계 조회 요청")
        
        result = sp500_service.get_service_stats()
        
        logger.info("✅ 서비스 통계 조회 성공")
        return ServiceStats(**result)
        
    except Exception as e:
        logger.error(f"❌ 서비스 통계 조회 실패: {e}")
        raise HTTPException(
            status_code=500,
            detail=create_error_response(
                error_type="STATS_ERROR",
                message="Failed to fetch service statistics",
                path="/stocks/sp500/stats"
            ).model_dump()
        )