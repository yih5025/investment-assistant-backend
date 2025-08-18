from typing import List, Optional
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import desc, asc, func, extract
from ..models.cpi_model import CPI
from ..schemas.cpi_schema import (
    CPIResponse,
    CPIChartData,
    CPIChartResponse,
    CPIStatsResponse,
    CPIInflationAnalysis,
    CPIComparisonResponse,
    CPIMonthlyDetailResponse
)

class CPIService:
    """소비자물가지수 비즈니스 로직"""
    
    def __init__(self, db: Session):
        self.db = db

    def get_all_cpi_data(self, order_by: str = "desc") -> List[CPIResponse]:
        """전체 CPI 데이터 조회"""
        query = self.db.query(CPI)
        
        if order_by == "desc":
            query = query.order_by(desc(CPI.date))
        else:
            query = query.order_by(asc(CPI.date))
            
        cpi_data = query.all()
        
        result = []
        for cpi in cpi_data:
            result.append(CPIResponse(
                date=cpi.date,
                cpi_value=float(cpi.cpi_value) if cpi.cpi_value else None,
                interval_type=cpi.interval_type or "monthly",
                unit=cpi.unit or "index 1982-1984=100",
                name=cpi.name or "Consumer Price Index for all Urban Consumers"
            ))
        return result

    def get_chart_data(self) -> CPIChartResponse:
        """차트용 데이터 + 인플레이션 분석"""
        cpi_data = self.db.query(CPI).order_by(asc(CPI.date)).all()
        
        if not cpi_data:
            return CPIChartResponse(
                data=[],
                current_cpi=None,
                previous_cpi=None,
                monthly_change=None,
                annual_inflation=None,
                avg_cpi_12m=None,
                max_cpi=None,
                min_cpi=None
            )
        
        # 차트용 데이터 변환 + 인플레이션율 계산
        chart_data = []
        cpi_values = []
        
        for i, cpi in enumerate(cpi_data):
            if cpi.cpi_value is not None:
                cpi_value = float(cpi.cpi_value)
                
                # 전년 동월 대비 인플레이션율 계산
                inflation_rate = None
                if i >= 12:  # 12개월 전 데이터가 있는 경우
                    prev_year_cpi = float(cpi_data[i-12].cpi_value) if cpi_data[i-12].cpi_value else None
                    if prev_year_cpi:
                        inflation_rate = ((cpi_value - prev_year_cpi) / prev_year_cpi) * 100
                
                chart_data.append(CPIChartData(
                    year=cpi.date.year,
                    month=cpi.date.month,
                    cpi_value=cpi_value,
                    date_str=cpi.date.strftime('%Y-%m'),
                    inflation_rate=round(inflation_rate, 2) if inflation_rate else None
                ))
                cpi_values.append(cpi_value)
        
        # 현재 CPI 및 변화율 분석
        current_cpi = cpi_values[-1] if cpi_values else None
        previous_cpi = cpi_values[-2] if len(cpi_values) >= 2 else None
        
        # 전월 대비 변화율
        monthly_change = None
        if current_cpi and previous_cpi:
            monthly_change = ((current_cpi - previous_cpi) / previous_cpi) * 100
        
        # 연간 인플레이션율 (전년 동월 대비)
        annual_inflation = None
        if len(cpi_values) >= 12:
            year_ago_cpi = cpi_values[-13] if len(cpi_values) >= 13 else cpi_values[-12]
            annual_inflation = ((current_cpi - year_ago_cpi) / year_ago_cpi) * 100
        
        # 12개월 평균
        recent_12_cpi = cpi_values[-12:] if len(cpi_values) >= 12 else cpi_values
        avg_cpi_12m = sum(recent_12_cpi) / len(recent_12_cpi) if recent_12_cpi else None
        
        return CPIChartResponse(
            data=chart_data,
            current_cpi=current_cpi,
            previous_cpi=previous_cpi,
            monthly_change=round(monthly_change, 3) if monthly_change else None,
            annual_inflation=round(annual_inflation, 2) if annual_inflation else None,
            avg_cpi_12m=round(avg_cpi_12m, 2) if avg_cpi_12m else None,
            max_cpi=max(cpi_values) if cpi_values else None,
            min_cpi=min(cpi_values) if cpi_values else None
        )

    def get_recent_months(self, months: int = 12) -> List[CPIResponse]:
        """최근 N개월 데이터 조회"""
        cpi_data = (
            self.db.query(CPI)
            .order_by(desc(CPI.date))
            .limit(months)
            .all()
        )
        
        result = []
        for cpi in cpi_data:
            result.append(CPIResponse(
                date=cpi.date,
                cpi_value=float(cpi.cpi_value) if cpi.cpi_value else None,
                interval_type=cpi.interval_type or "monthly",
                unit=cpi.unit or "index 1982-1984=100",
                name=cpi.name or "Consumer Price Index for all Urban Consumers"
            ))
        return result

    def get_statistics(self) -> CPIStatsResponse:
        """CPI 통계 정보"""
        # 전체 통계
        stats = self.db.query(
            func.count(CPI.date).label('total_count'),
            func.avg(CPI.cpi_value).label('avg_cpi'),
            func.max(CPI.cpi_value).label('max_cpi'),
            func.min(CPI.cpi_value).label('min_cpi')
        ).first()
        
        # 최근 데이터 (현재 + 이전월 + 전년 동월)
        recent_cpi = (
            self.db.query(CPI)
            .order_by(desc(CPI.date))
            .limit(13)  # 현재 + 12개월
            .all()
        )
        
        if not recent_cpi or not stats:
            raise ValueError("CPI 데이터가 없습니다")
        
        latest = recent_cpi[0]
        previous = recent_cpi[1] if len(recent_cpi) > 1 else None
        year_ago = recent_cpi[12] if len(recent_cpi) > 12 else None
        
        latest_cpi = float(latest.cpi_value)
        previous_cpi = float(previous.cpi_value) if previous else None
        year_ago_cpi = float(year_ago.cpi_value) if year_ago else None
        
        # 전월 대비 변화
        monthly_change = None
        monthly_change_value = None
        if previous_cpi:
            monthly_change_value = latest_cpi - previous_cpi
            monthly_change = (monthly_change_value / previous_cpi) * 100
        
        # 연간 인플레이션
        annual_inflation = None
        if year_ago_cpi:
            annual_inflation = ((latest_cpi - year_ago_cpi) / year_ago_cpi) * 100
        
        # 트렌드 판단 (최근 3개월 기준)
        trend = "stable"
        if len(recent_cpi) >= 3:
            recent_3_values = [float(cpi.cpi_value) for cpi in recent_cpi[:3]]
            if recent_3_values[0] > recent_3_values[1] > recent_3_values[2]:
                trend = "rising"
            elif recent_3_values[0] < recent_3_values[1] < recent_3_values[2]:
                trend = "falling"
        
        # 최근 12개월 통계
        recent_12m_values = [float(cpi.cpi_value) for cpi in recent_cpi if cpi.cpi_value]
        avg_12m = sum(recent_12m_values) / len(recent_12m_values) if recent_12m_values else 0
        max_12m = max(recent_12m_values) if recent_12m_values else 0
        min_12m = min(recent_12m_values) if recent_12m_values else 0
        
        return CPIStatsResponse(
            latest_date=latest.date.strftime('%Y-%m-%d'),
            latest_cpi=latest_cpi,
            previous_month_cpi=previous_cpi,
            monthly_change=round(monthly_change, 3) if monthly_change else None,
            monthly_change_value=round(monthly_change_value, 3) if monthly_change_value else None,
            annual_inflation=round(annual_inflation, 2) if annual_inflation else None,
            avg_12_months=round(avg_12m, 2),
            max_12_months=max_12m,
            min_12_months=min_12m,
            trend=trend,
            total_months=stats.total_count
        )

    def get_inflation_analysis(self) -> CPIInflationAnalysis:
        """상세 인플레이션 분석"""
        # 최근 12개월 데이터
        recent_12m = (
            self.db.query(CPI)
            .order_by(desc(CPI.date))
            .limit(24)  # 전년 동월 비교를 위해 24개월
            .all()
        )
        
        if len(recent_12m) < 12:
            raise ValueError("인플레이션 분석을 위한 충분한 데이터가 없습니다")
        
        # 차트 데이터 생성 (최근 12개월)
        chart_data_12m = []
        chart_data_6m = []
        inflation_rates_12m = []
        inflation_rates_6m = []
        
        for i in range(12):  # 최근 12개월
            current_cpi = recent_12m[i]
            
            # 전년 동월 CPI (있는 경우만)
            prev_year_cpi = recent_12m[i + 12] if i + 12 < len(recent_12m) else None
            
            inflation_rate = None
            if prev_year_cpi and current_cpi.cpi_value and prev_year_cpi.cpi_value:
                current_value = float(current_cpi.cpi_value)
                prev_value = float(prev_year_cpi.cpi_value)
                inflation_rate = ((current_value - prev_value) / prev_value) * 100
                inflation_rates_12m.append(inflation_rate)
                
                if i < 6:  # 최근 6개월
                    inflation_rates_6m.append(inflation_rate)
            
            chart_item = CPIChartData(
                year=current_cpi.date.year,
                month=current_cpi.date.month,
                cpi_value=float(current_cpi.cpi_value) if current_cpi.cpi_value else 0,
                date_str=current_cpi.date.strftime('%Y-%m'),
                inflation_rate=round(inflation_rate, 2) if inflation_rate else None
            )
            
            chart_data_12m.append(chart_item)
            if i < 6:  # 최근 6개월
                chart_data_6m.append(chart_item)
        
        # 평균 인플레이션율
        avg_inflation_6m = sum(inflation_rates_6m) / len(inflation_rates_6m) if inflation_rates_6m else 0
        avg_inflation_12m = sum(inflation_rates_12m) / len(inflation_rates_12m) if inflation_rates_12m else 0
        
        # 인플레이션 트렌드 분석
        inflation_trend = self._analyze_inflation_trend(inflation_rates_12m)
        
        # 변동성 계산
        volatility_score = self._calculate_cpi_volatility(inflation_rates_12m)
        
        # 가격 안정성 상태
        price_stability = self._assess_price_stability(avg_inflation_12m)
        
        return CPIInflationAnalysis(
            recent_6m=list(reversed(chart_data_6m)),   # 오래된 것부터
            recent_12m=list(reversed(chart_data_12m)), # 오래된 것부터
            avg_inflation_6m=round(avg_inflation_6m, 2),
            avg_inflation_12m=round(avg_inflation_12m, 2),
            inflation_trend=inflation_trend,
            volatility_score=volatility_score,
            price_stability_status=price_stability
        )

    def get_period_comparison(self, start_date: str, end_date: str) -> CPIComparisonResponse:
        """기간별 CPI 비교"""
        from datetime import datetime
        
        start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
        end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
        
        # 시작일과 종료일의 CPI 데이터 조회
        start_cpi_data = self.db.query(CPI).filter(CPI.date == start_dt).first()
        end_cpi_data = self.db.query(CPI).filter(CPI.date == end_dt).first()
        
        if not start_cpi_data or not end_cpi_data:
            raise ValueError("지정된 날짜의 CPI 데이터를 찾을 수 없습니다")
        
        start_cpi = float(start_cpi_data.cpi_value)
        end_cpi = float(end_cpi_data.cpi_value)
        
        # 변화 계산
        total_change = end_cpi - start_cpi
        total_change_percent = (total_change / start_cpi) * 100
        
        # 월 수 계산
        months_diff = (end_dt.year - start_dt.year) * 12 + (end_dt.month - start_dt.month)
        average_monthly_change = total_change_percent / months_diff if months_diff > 0 else 0
        
        # 누적 인플레이션율 (복리 계산)
        cumulative_inflation = total_change_percent
        
        return CPIComparisonResponse(
            start_date=start_date,
            end_date=end_date,
            start_cpi=start_cpi,
            end_cpi=end_cpi,
            total_change=round(total_change, 3),
            total_change_percent=round(total_change_percent, 2),
            average_monthly_change=round(average_monthly_change, 3),
            cumulative_inflation=round(cumulative_inflation, 2)
        )

    def _analyze_inflation_trend(self, inflation_rates: List[float]) -> str:
        """인플레이션 트렌드 분석"""
        if len(inflation_rates) < 6:
            return "stable"
        
        recent_6 = inflation_rates[:6]  # 최근 6개월
        older_6 = inflation_rates[6:12] if len(inflation_rates) >= 12 else inflation_rates[6:]
        
        recent_avg = sum(recent_6) / len(recent_6)
        older_avg = sum(older_6) / len(older_6) if older_6 else recent_avg
        
        diff = recent_avg - older_avg
        
        if diff > 0.5:  # 0.5%p 이상 상승
            return "accelerating"
        elif diff < -0.5:  # 0.5%p 이상 하락
            return "decelerating"
        else:
            return "stable"

    def _calculate_cpi_volatility(self, inflation_rates: List[float]) -> float:
        """CPI 변동성 점수 계산 (0-10)"""
        if len(inflation_rates) < 2:
            return 0.0
        
        # 표준편차 계산
        avg_rate = sum(inflation_rates) / len(inflation_rates)
        variance = sum((r - avg_rate) ** 2 for r in inflation_rates) / len(inflation_rates)
        std_dev = variance ** 0.5
        
        # 0-10 스케일로 변환
        volatility_score = min(std_dev * 2, 10.0)
        return round(volatility_score, 2)

    def _assess_price_stability(self, avg_inflation: float) -> str:
        """가격 안정성 평가"""
        if avg_inflation < 1.0:
            return "stable"  # 1% 미만
        elif avg_inflation < 3.0:
            return "moderate_inflation"  # 1-3%
        else:
            return "high_inflation"  # 3% 이상
        


def get_monthly_detail(self, year: int, month: int) -> CPIMonthlyDetailResponse:
    """특정월 CPI 상세 정보"""
    from datetime import date
    target_date = date(year, month, 1)
    
    # 해당월 CPI 데이터
    current_cpi_data = self.db.query(CPI).filter(CPI.date == target_date).first()
    if not current_cpi_data or not current_cpi_data.cpi_value:
        raise ValueError(f"{year}년 {month}월 CPI 데이터를 찾을 수 없습니다")
    
    current_cpi = float(current_cpi_data.cpi_value)
    
    # 전월 CPI (이전달)
    prev_month_date = date(year, month-1, 1) if month > 1 else date(year-1, 12, 1)
    prev_month_data = self.db.query(CPI).filter(CPI.date == prev_month_date).first()
    prev_month_cpi = float(prev_month_data.cpi_value) if prev_month_data and prev_month_data.cpi_value else None
    
    # 전년 동월 CPI
    prev_year_date = date(year-1, month, 1)
    prev_year_data = self.db.query(CPI).filter(CPI.date == prev_year_date).first()
    prev_year_cpi = float(prev_year_data.cpi_value) if prev_year_data and prev_year_data.cpi_value else None
    
    # 인플레이션율 계산
    year_over_year_inflation = None
    if prev_year_cpi:
        year_over_year_inflation = ((current_cpi - prev_year_cpi) / prev_year_cpi) * 100
    
    # 전월 대비 변화
    month_over_month_change = None
    month_over_month_value = None
    if prev_month_cpi:
        month_over_month_value = current_cpi - prev_month_cpi
        month_over_month_change = (month_over_month_value / prev_month_cpi) * 100
    
    # 연중 순위 계산 (추가 분석)
    year_data = (
        self.db.query(CPI)
        .filter(extract('year', CPI.date) == year)
        .filter(CPI.cpi_value.isnot(None))
        .order_by(desc(CPI.cpi_value))
        .all()
    )
    
    ranking_in_year = None
    is_yearly_high = False
    is_yearly_low = False
    
    if year_data:
        cpi_values = [float(item.cpi_value) for item in year_data]
        max_cpi = max(cpi_values)
        min_cpi = min(cpi_values)
        
        is_yearly_high = (current_cpi == max_cpi)
        is_yearly_low = (current_cpi == min_cpi)
        
        # 순위 계산 (내림차순)
        sorted_values = sorted(cpi_values, reverse=True)
        ranking_in_year = sorted_values.index(current_cpi) + 1
    
    return CPIMonthlyDetailResponse(
        date=target_date,
        cpi_value=current_cpi,
        year_over_year_inflation=round(year_over_year_inflation, 2) if year_over_year_inflation else None,
        month_over_month_change=round(month_over_month_change, 3) if month_over_month_change else None,
        month_over_month_value=round(month_over_month_value, 3) if month_over_month_value else None,
        previous_month_cpi=prev_month_cpi,
        previous_year_cpi=prev_year_cpi,
        interval_type=current_cpi_data.interval_type or "monthly",
        unit=current_cpi_data.unit or "index 1982-1984=100",
        name=current_cpi_data.name or "Consumer Price Index for all Urban Consumers",
        ranking_in_year=ranking_in_year,
        is_yearly_high=is_yearly_high,
        is_yearly_low=is_yearly_low
    )