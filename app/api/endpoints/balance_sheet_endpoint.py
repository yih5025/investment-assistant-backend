from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import date
from app.models.balance_sheet_model import BalanceSheet
from app.dependencies import get_db
from app.services.balance_sheet_service import BalanceSheetService
from app.schemas.balance_sheet_schema import (
    BalanceSheetResponse, BalanceSheetListResponse, FinancialAnalysis,
    BalanceSheetTrends, BalanceSheetStatistics, PeriodBalanceSheetResponse
)

# 라우터 생성 (prefix는 api_v1.py에서 설정됨)
router = APIRouter(tags=["Balance Sheet"])

@router.get("/", response_model=BalanceSheetListResponse)
async def get_balance_sheets(
    page: int = Query(1, ge=1, description="페이지 번호"),
    size: int = Query(20, ge=1, le=100, description="페이지 크기"),
    symbol: Optional[str] = Query(None, description="기업 심볼 필터"),
    start_date: Optional[date] = Query(None, description="시작 날짜"),
    end_date: Optional[date] = Query(None, description="종료 날짜"),
    db: Session = Depends(get_db)
):
    """
    재무상태표 목록 조회 (페이징)
    
    **주요 기능:**
    - 페이징 처리로 대용량 데이터 효율적 조회
    - 기업 심볼, 날짜 범위로 필터링 가능
    - 최신순 정렬
    
    **사용 예시:**
    - GET /api/v1/balance-sheet/?page=1&size=20
    - GET /api/v1/balance-sheet/?symbol=AAPL&page=1&size=10
    - GET /api/v1/balance-sheet/?start_date=2024-01-01&end_date=2024-12-31
    """
    service = BalanceSheetService(db)
    
    # 오프셋 계산
    skip = (page - 1) * size
    
    try:
        # 데이터 조회
        balance_sheets = service.get_balance_sheets(
            skip=skip, 
            limit=size,
            symbol=symbol,
            start_date=start_date,
            end_date=end_date
        )
        
        # 총 개수 조회
        total = service.get_balance_sheet_count(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date
        )
        
        # 응답 데이터 변환
        items = []
        for bs in balance_sheets:
            item = BalanceSheetResponse.from_orm(bs)
            # 계산된 필드 추가
            item.total_assets_billions = bs.total_assets_billions
            item.market_cap_category = bs.market_cap_category
            item.fiscal_date_str = bs.fiscal_date_str
            items.append(item)
        
        return BalanceSheetListResponse(
            items=items,
            total=total,
            page=page,
            size=size,
            has_next=(skip + size) < total
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"데이터 조회 중 오류 발생: {str(e)}")

@router.get("/symbol/{symbol}", response_model=List[BalanceSheetResponse])
async def get_balance_sheet_by_symbol(
    symbol: str,
    limit: int = Query(10, ge=1, le=50, description="조회할 분기 수"),
    db: Session = Depends(get_db)
):
    """
    특정 기업의 모든 재무상태표 조회
    
    **주요 기능:**
    - 특정 기업의 시계열 재무 데이터 조회
    - 최신순 정렬 (최신 분기부터)
    - 분기 수 제한 가능
    
    **사용 예시:**
    - GET /api/v1/balance-sheet/symbol/AAPL
    - GET /api/v1/balance-sheet/symbol/MSFT?limit=5
    """
    service = BalanceSheetService(db)
    
    try:
        balance_sheets = service.get_by_symbol(symbol)
        
        if not balance_sheets:
            raise HTTPException(status_code=404, detail=f"기업 '{symbol}'의 재무상태표를 찾을 수 없습니다")
        
        # 제한된 개수만 반환
        limited_sheets = balance_sheets[:limit]
        
        # 응답 데이터 변환
        items = []
        for bs in limited_sheets:
            item = BalanceSheetResponse.from_orm(bs)
            item.total_assets_billions = bs.total_assets_billions
            item.market_cap_category = bs.market_cap_category
            item.fiscal_date_str = bs.fiscal_date_str
            items.append(item)
        
        return items
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"데이터 조회 중 오류 발생: {str(e)}")

@router.get("/symbol/{symbol}/recent", response_model=BalanceSheetResponse)
async def get_latest_balance_sheet(
    symbol: str,
    db: Session = Depends(get_db)
):
    """
    특정 기업의 가장 최신 재무상태표 조회
    
    **주요 기능:**
    - 가장 최근 분기 재무상태표만 반환
    - 빠른 조회를 위한 단일 레코드 응답
    
    **사용 예시:**
    - GET /api/v1/balance-sheet/symbol/AAPL/recent
    """
    service = BalanceSheetService(db)
    
    try:
        balance_sheet = service.get_latest_by_symbol(symbol)
        
        if not balance_sheet:
            raise HTTPException(status_code=404, detail=f"기업 '{symbol}'의 재무상태표를 찾을 수 없습니다")
        
        # 응답 데이터 변환
        item = BalanceSheetResponse.from_orm(balance_sheet)
        item.total_assets_billions = balance_sheet.total_assets_billions
        item.market_cap_category = balance_sheet.market_cap_category
        item.fiscal_date_str = balance_sheet.fiscal_date_str
        
        return item
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"데이터 조회 중 오류 발생: {str(e)}")

@router.get("/analysis/{symbol}", response_model=FinancialAnalysis)
async def get_financial_analysis(
    symbol: str,
    db: Session = Depends(get_db)
):
    """
    🧠 재무제표 해석 분석 (핵심 기능!)
    
    **주요 기능:**
    - 복잡한 재무제표를 일반인이 이해하기 쉽게 해석
    - 재무비율 자동 계산 (유동성, 레버리지, 효율성)
    - A~D 등급의 재무건전성 평가
    - 강점/우려사항 자동 분석
    - 투자 추천 의견 제공
    
    **계산되는 재무비율:**
    - 유동비율 = 유동자산 / 유동부채
    - 당좌비율 = (유동자산-재고) / 유동부채  
    - 부채비율 = 총부채 / 총자산
    - 자기자본비율 = 자기자본 / 총자산
    - 기타 주요 재무안정성 지표들
    
    **사용 예시:**
    - GET /api/v1/balance-sheet/analysis/AAPL
    """
    service = BalanceSheetService(db)
    
    try:
        analysis = service.analyze_financial_health(symbol)
        
        if not analysis:
            raise HTTPException(status_code=404, detail=f"기업 '{symbol}'의 재무 분석 데이터를 찾을 수 없습니다")
        
        return analysis
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"재무 분석 중 오류 발생: {str(e)}")

@router.get("/trends/{symbol}", response_model=BalanceSheetTrends)
async def get_financial_trends(
    symbol: str,
    periods: int = Query(4, ge=2, le=12, description="분석할 분기 수"),
    db: Session = Depends(get_db)
):
    """
    기업 재무상태표 시계열 변화 분석
    
    **주요 기능:**
    - 최근 N분기간 주요 재무지표 변화 추이 분석
    - 전분기 대비 증가/감소율 계산
    - 트렌드 방향성 판단 (증가/감소/유지)
    - 전반적인 재무 트렌드 요약
    
    **분석 지표:**
    - 총자산, 총부채, 자기자본 변화
    - 유동자산, 현금성 자산 변화
    - 성장성 및 안정성 트렌드
    
    **사용 예시:**
    - GET /api/v1/balance-sheet/trends/AAPL
    - GET /api/v1/balance-sheet/trends/MSFT?periods=8
    """
    service = BalanceSheetService(db)
    
    try:
        trends = service.get_financial_trends(symbol, periods)
        
        if not trends:
            raise HTTPException(
                status_code=404, 
                detail=f"기업 '{symbol}'의 충분한 시계열 데이터를 찾을 수 없습니다 (최소 2분기 필요)"
            )
        
        return trends
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"트렌드 분석 중 오류 발생: {str(e)}")

@router.get("/ratios/{symbol}")
async def get_financial_ratios(
    symbol: str,
    db: Session = Depends(get_db)
):
    """
    재무비율 상세 계산
    
    **주요 기능:**
    - 재무비율만 별도로 상세 조회
    - 각 비율의 업계 벤치마크 비교
    - 비율별 상태 및 해석 제공
    
    **계산 비율:**
    - **유동성 비율**: 유동비율, 당좌비율, 현금비율
    - **레버리지 비율**: 부채비율, 부채자기자본비율, 자기자본비율
    - **효율성 비율**: 재고자산비율 등
    
    **사용 예시:**
    - GET /api/v1/balance-sheet/ratios/AAPL
    """
    service = BalanceSheetService(db)
    
    try:
        # 최신 재무상태표 조회
        latest_bs = service.get_latest_by_symbol(symbol)
        if not latest_bs:
            raise HTTPException(status_code=404, detail=f"기업 '{symbol}'의 재무상태표를 찾을 수 없습니다")
        
        # 재무비율 계산
        ratios = service.calculate_financial_ratios(latest_bs)
        if not ratios:
            raise HTTPException(status_code=404, detail=f"기업 '{symbol}'의 재무비율을 계산할 수 없습니다")
        
        # 응답 구조화
        response = {
            "symbol": symbol,
            "analysis_date": latest_bs.fiscaldateending,
            "ratios": ratios,
            "summary": f"{symbol}의 {latest_bs.fiscaldateending} 기준 재무비율 분석 결과입니다"
        }
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"재무비율 계산 중 오류 발생: {str(e)}")

@router.get("/statistics", response_model=BalanceSheetStatistics)
async def get_balance_sheet_statistics(
    db: Session = Depends(get_db)
):
    """
    재무상태표 전체 통계 정보
    
    **주요 기능:**
    - 전체 기업 수, 최신 보고 기간
    - 평균 총자산, 중간값 부채비율
    - 기업 규모별 분포 (대형/중형/소형)
    
    **통계 지표:**
    - 총 기업 수
    - 최신 보고 기간
    - 평균 총자산 (십억 단위)
    - 중간값 부채비율
    - 규모별 기업 분포
    
    **사용 예시:**
    - GET /api/v1/balance-sheet/statistics
    """
    service = BalanceSheetService(db)
    
    try:
        statistics = service.get_statistics()
        return statistics
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"통계 조회 중 오류 발생: {str(e)}")

@router.get("/period/{period_date}", response_model=PeriodBalanceSheetResponse)
async def get_balance_sheet_by_period(
    period_date: date,
    limit: int = Query(50, ge=1, le=200, description="조회할 기업 수"),
    db: Session = Depends(get_db)
):
    """
    특정 분기 전체 기업 재무상태표 조회
    
    **주요 기능:**
    - 동일한 회계연도 종료일을 가진 모든 기업 조회
    - 기업간 재무상태 비교 분석 가능
    - 특정 시점의 시장 전반 재무 건전성 파악
    
    **사용 예시:**
    - GET /api/v1/balance-sheet/period/2024-12-31
    - GET /api/v1/balance-sheet/period/2024-09-30?limit=100
    """
    service = BalanceSheetService(db)
    
    try:
        balance_sheets = service.get_by_period(period_date)
        
        if not balance_sheets:
            raise HTTPException(
                status_code=404, 
                detail=f"날짜 '{period_date}'에 해당하는 재무상태표를 찾을 수 없습니다"
            )
        
        # 제한된 개수만 반환
        limited_sheets = balance_sheets[:limit]
        
        # 응답 데이터 변환
        companies = []
        total_assets_sum = 0
        company_count = len(limited_sheets)
        
        for bs in limited_sheets:
            item = BalanceSheetResponse.from_orm(bs)
            item.total_assets_billions = bs.total_assets_billions
            item.market_cap_category = bs.market_cap_category
            item.fiscal_date_str = bs.fiscal_date_str
            companies.append(item)
            
            # 통계 계산용
            if bs.totalassets:
                total_assets_sum += float(bs.totalassets)
        
        # 기간 요약 통계
        summary = {
            "period": period_date.strftime('%Y-%m-%d'),
            "total_companies": len(balance_sheets),
            "returned_companies": company_count,
            "average_total_assets_billions": round(total_assets_sum / company_count / 1_000_000_000, 2) if company_count > 0 else 0,
            "description": f"{period_date} 기준 {len(balance_sheets)}개 기업의 재무상태표 데이터"
        }
        
        return PeriodBalanceSheetResponse(
            period=period_date,
            companies=companies,
            summary=summary
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"기간별 데이터 조회 중 오류 발생: {str(e)}")

# ========== 추가 유틸리티 엔드포인트 ==========

@router.get("/symbols")
async def get_available_symbols(
    db: Session = Depends(get_db)
):
    """
    재무상태표 데이터가 있는 기업 심볼 목록 조회
    
    **주요 기능:**
    - 데이터가 존재하는 모든 기업 심볼 반환
    - 프론트엔드에서 드롭다운 등에 활용 가능
    
    **사용 예시:**
    - GET /api/v1/balance-sheet/symbols
    """
    try:
        symbols = db.query(BalanceSheet.symbol)\
            .distinct()\
            .order_by(BalanceSheet.symbol)\
            .all()
        
        symbol_list = [s.symbol for s in symbols]
        
        return {
            "symbols": symbol_list,
            "count": len(symbol_list),
            "description": "재무상태표 데이터가 있는 기업 심볼 목록"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"심볼 목록 조회 중 오류 발생: {str(e)}")

@router.get("/periods")
async def get_available_periods(
    db: Session = Depends(get_db)
):
    """
    사용 가능한 회계연도 종료일 목록 조회
    
    **주요 기능:**
    - 데이터가 존재하는 모든 회계연도 종료일 반환
    - 시계열 분석 기간 선택에 활용
    
    **사용 예시:**
    - GET /api/v1/balance-sheet/periods
    """
    try:
        periods = db.query(BalanceSheet.fiscaldateending)\
            .distinct()\
            .order_by(BalanceSheet.fiscaldateending.desc())\
            .all()
        
        period_list = [p.fiscaldateending.strftime('%Y-%m-%d') for p in periods]
        
        return {
            "periods": period_list,
            "count": len(period_list),
            "latest": period_list[0] if period_list else None,
            "oldest": period_list[-1] if period_list else None,
            "description": "사용 가능한 회계연도 종료일 목록"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"기간 목록 조회 중 오류 발생: {str(e)}")