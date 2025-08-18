from typing import List, Optional
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import desc, asc, func
from ..models.federal_funds_rate_model import FederalFundsRate
from ..schemas.federal_funds_rate_schema import (
    FederalFundsRateResponse,
    FederalFundsRateChartData,
    FederalFundsRateChartResponse,
    FederalFundsRateStatsResponse,
    FederalFundsRateTrendResponse
)

class FederalFundsRateService:
    """연방기금금리 비즈니스 로직"""
    
    def __init__(self, db: Session):
        self.db = db

    def get_all_rates(self, order_by: str = "desc") -> List[FederalFundsRateResponse]:
        """전체 금리 데이터 조회"""
        query = self.db.query(FederalFundsRate)
        
        if order_by == "desc":
            query = query.order_by(desc(FederalFundsRate.date))
        else:
            query = query.order_by(asc(FederalFundsRate.date))
            
        rates = query.all()
        
        result = []
        for rate in rates:
            result.append(FederalFundsRateResponse(
                date=rate.date,
                rate=float(rate.rate) if rate.rate else None,
                interval_type=rate.interval_type or "monthly",
                unit=rate.unit or "percent",
                name=rate.name or "Effective Federal Funds Rate"
            ))
        return result

    def get_chart_data(self) -> FederalFundsRateChartResponse:
        """차트용 데이터 + 분석 정보"""
        rates = self.db.query(FederalFundsRate).order_by(asc(FederalFundsRate.date)).all()
        
        if not rates:
            return FederalFundsRateChartResponse(
                data=[],
                current_rate=None,
                rate_trend=None,
                change_from_prev=None,
                avg_rate_12m=None,
                max_rate=None,
                min_rate=None
            )
        
        # 차트용 데이터 변환
        chart_data = []
        rate_values = []
        
        for rate in rates:
            if rate.rate is not None:
                rate_value = float(rate.rate)
                chart_data.append(FederalFundsRateChartData(
                    year=rate.date.year,
                    month=rate.date.month,
                    rate=rate_value,
                    date_str=rate.date.strftime('%Y-%m')
                ))
                rate_values.append(rate_value)
        
        # 현재 금리 및 트렌드 분석
        current_rate = rate_values[-1] if rate_values else None
        previous_rate = rate_values[-2] if len(rate_values) >= 2 else None
        
        # 전월 대비 변화
        change_from_prev = None
        rate_trend = "stable"
        if current_rate and previous_rate:
            change_from_prev = current_rate - previous_rate
            if change_from_prev > 0.01:  # 0.01% 이상 상승
                rate_trend = "rising"
            elif change_from_prev < -0.01:  # 0.01% 이상 하락
                rate_trend = "falling"
        
        # 12개월 평균 (최근 12개 데이터)
        recent_12_rates = rate_values[-12:] if len(rate_values) >= 12 else rate_values
        avg_rate_12m = sum(recent_12_rates) / len(recent_12_rates) if recent_12_rates else None
        
        return FederalFundsRateChartResponse(
            data=chart_data,
            current_rate=current_rate,
            rate_trend=rate_trend,
            change_from_prev=change_from_prev,
            avg_rate_12m=round(avg_rate_12m, 3) if avg_rate_12m else None,
            max_rate=max(rate_values) if rate_values else None,
            min_rate=min(rate_values) if rate_values else None
        )

    def get_recent_months(self, months: int = 12) -> List[FederalFundsRateResponse]:
        """최근 N개월 데이터 조회"""
        rates = (
            self.db.query(FederalFundsRate)
            .order_by(desc(FederalFundsRate.date))
            .limit(months)
            .all()
        )
        
        result = []
        for rate in rates:
            result.append(FederalFundsRateResponse(
                date=rate.date,
                rate=float(rate.rate) if rate.rate else None,
                interval_type=rate.interval_type or "monthly",
                unit=rate.unit or "percent",
                name=rate.name or "Effective Federal Funds Rate"
            ))
        return result

    def get_statistics(self) -> FederalFundsRateStatsResponse:
        """금리 통계 정보"""
        # 전체 통계
        stats = self.db.query(
            func.count(FederalFundsRate.date).label('total_count'),
            func.avg(FederalFundsRate.rate).label('avg_rate'),
            func.max(FederalFundsRate.rate).label('max_rate'),
            func.min(FederalFundsRate.rate).label('min_rate')
        ).first()
        
        # 최근 2개월 데이터 (현재 + 이전)
        recent_rates = (
            self.db.query(FederalFundsRate)
            .order_by(desc(FederalFundsRate.date))
            .limit(2)
            .all()
        )
        
        if not recent_rates or not stats:
            raise ValueError("연방기금금리 데이터가 없습니다")
        
        latest = recent_rates[0]
        previous = recent_rates[1] if len(recent_rates) > 1 else None
        
        latest_rate = float(latest.rate)
        previous_rate = float(previous.rate) if previous else None
        rate_change = latest_rate - previous_rate if previous_rate else None
        
        # 트렌드 판단
        trend = "stable"
        if rate_change:
            if rate_change > 0.01:
                trend = "rising"
            elif rate_change < -0.01:
                trend = "falling"
        
        # 최근 12개월 통계
        recent_12m = (
            self.db.query(FederalFundsRate)
            .order_by(desc(FederalFundsRate.date))
            .limit(12)
            .all()
        )
        
        recent_12m_rates = [float(r.rate) for r in recent_12m if r.rate]
        avg_12m = sum(recent_12m_rates) / len(recent_12m_rates) if recent_12m_rates else 0
        max_12m = max(recent_12m_rates) if recent_12m_rates else 0
        min_12m = min(recent_12m_rates) if recent_12m_rates else 0
        
        return FederalFundsRateStatsResponse(
            latest_date=latest.date.strftime('%Y-%m-%d'),
            latest_rate=latest_rate,
            previous_rate=previous_rate,
            rate_change=rate_change,
            trend=trend,
            avg_12_months=round(avg_12m, 3),
            max_12_months=max_12m,
            min_12_months=min_12m,
            total_months=stats.total_count
        )

    def get_trend_analysis(self) -> FederalFundsRateTrendResponse:
        """금리 트렌드 분석"""
        # 최근 12개월 데이터
        recent_12m = (
            self.db.query(FederalFundsRate)
            .order_by(desc(FederalFundsRate.date))
            .limit(12)
            .all()
        )
        
        if len(recent_12m) < 6:
            raise ValueError("트렌드 분석을 위한 충분한 데이터가 없습니다")
        
        # 차트 데이터 생성
        chart_data_12m = []
        chart_data_6m = []
        rates = []
        
        for i, rate in enumerate(reversed(recent_12m)):  # 오래된 것부터
            if rate.rate is not None:
                rate_value = float(rate.rate)
                chart_item = FederalFundsRateChartData(
                    year=rate.date.year,
                    month=rate.date.month,
                    rate=rate_value,
                    date_str=rate.date.strftime('%Y-%m')
                )
                chart_data_12m.append(chart_item)
                rates.append(rate_value)
                
                # 최근 6개월
                if i >= 6:  # 최근 6개월만
                    chart_data_6m.append(chart_item)
        
        # 트렌드 분석
        trend_direction = self._analyze_trend_direction(rates)
        volatility_score = self._calculate_volatility(rates)
        rate_cycle_phase = self._determine_cycle_phase(rates)
        
        return FederalFundsRateTrendResponse(
            recent_6m=chart_data_6m,
            recent_12m=chart_data_12m,
            trend_direction=trend_direction,
            volatility_score=volatility_score,
            rate_cycle_phase=rate_cycle_phase
        )

    def _analyze_trend_direction(self, rates: List[float]) -> str:
        """트렌드 방향 분석"""
        if len(rates) < 3:
            return "stable"
        
        recent_3 = rates[-3:]  # 최근 3개월
        first_avg = sum(recent_3[:2]) / 2  # 첫 2개월 평균
        last_rate = recent_3[-1]  # 최신 금리
        
        change = last_rate - first_avg
        
        if change > 0.1:  # 0.1% 이상 상승
            return "increasing"
        elif change < -0.1:  # 0.1% 이상 하락
            return "decreasing"
        else:
            return "stable"

    def _calculate_volatility(self, rates: List[float]) -> float:
        """변동성 점수 계산 (0-10)"""
        if len(rates) < 2:
            return 0.0
        
        # 표준편차 계산
        avg_rate = sum(rates) / len(rates)
        variance = sum((r - avg_rate) ** 2 for r in rates) / len(rates)
        std_dev = variance ** 0.5
        
        # 0-10 스케일로 변환 (금리의 표준편차를 기준으로)
        volatility_score = min(std_dev * 2, 10.0)  # 최대 10점
        return round(volatility_score, 2)

    def _determine_cycle_phase(self, rates: List[float]) -> str:
        """금리 사이클 단계 판단"""
        if len(rates) < 6:
            return "neutral"
        
        recent_6 = rates[-6:]  # 최근 6개월
        first_3_avg = sum(recent_6[:3]) / 3
        last_3_avg = sum(recent_6[3:]) / 3
        
        change = last_3_avg - first_3_avg
        
        if change > 0.2:  # 0.2% 이상 상승 추세
            return "tightening"  # 긴축
        elif change < -0.2:  # 0.2% 이상 하락 추세
            return "easing"  # 완화
        else:
            return "neutral"  # 중립