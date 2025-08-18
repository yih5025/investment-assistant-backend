import datetime
from datetime import timedelta
from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, desc, asc, func
from decimal import Decimal
import statistics

from app.models.treasury_yield_model import TreasuryYield
from app.schemas.treasury_yield_schema import (
    TreasuryYieldListResponse,
    TreasuryYieldChartResponse,
    TreasuryYieldChartPoint,
    YieldCurveResponse,
    YieldCurvePoint,
    TreasuryYieldStatisticsResponse,
    TreasuryYieldStatistics,
    TreasuryYieldTrendsResponse,
    TreasuryYieldTrend,
    TreasuryYieldRecentResponse
)


class TreasuryYieldService:
    """국채 수익률 비즈니스 로직 서비스"""

    @staticmethod
    def get_treasury_yields(
        db: Session,
        page: int = 1,
        size: int = 50,
        maturity: Optional[str] = None,
        start_date: Optional[datetime.date] = None,
        end_date: Optional[datetime.date] = None,
        order_by: str = "date_desc"
    ) -> TreasuryYieldListResponse:
        """국채 수익률 목록 조회 (페이징, 필터링)"""
        
        # 기본 쿼리
        query = db.query(TreasuryYield)
        
        # 필터링
        filters = []
        if maturity:
            filters.append(TreasuryYield.maturity == maturity)
        if start_date:
            filters.append(TreasuryYield.date >= start_date)
        if end_date:
            filters.append(TreasuryYield.date <= end_date)
            
        if filters:
            query = query.filter(and_(*filters))
        
        # 정렬
        if order_by == "date_desc":
            query = query.order_by(desc(TreasuryYield.date), TreasuryYield.maturity)
        elif order_by == "date_asc":
            query = query.order_by(asc(TreasuryYield.date), TreasuryYield.maturity)
        elif order_by == "yield_desc":
            query = query.order_by(desc(TreasuryYield.yield_rate))
        elif order_by == "yield_asc":
            query = query.order_by(asc(TreasuryYield.yield_rate))
        
        # 전체 개수
        total = query.count()
        
        # 페이징
        offset = (page - 1) * size
        items = query.offset(offset).limit(size).all()
        
        # 페이지 계산
        pages = (total + size - 1) // size
        
        return TreasuryYieldListResponse(
            items=items,
            total=total,
            page=page,
            size=size,
            pages=pages
        )

    @staticmethod
    def get_recent_data(db: Session, days: int = 30) -> TreasuryYieldRecentResponse:
        """최근 데이터 조회"""
        
        # 최신 날짜 조회
        latest_date_result = db.query(func.max(TreasuryYield.date)).scalar()
        if not latest_date_result:
            return TreasuryYieldRecentResponse(
                latest_date=datetime.date.today(),
                data=[],
                previous_data=[],
                changes={}
            )
        
        latest_date = latest_date_result
        previous_date = latest_date - timedelta(days=days)
        
        # 최신 데이터
        latest_data = db.query(TreasuryYield).filter(
            TreasuryYield.date == latest_date
        ).order_by(TreasuryYield.maturity).all()
        
        # 이전 데이터 (비교용)
        previous_data = db.query(TreasuryYield).filter(
            TreasuryYield.date == previous_date
        ).order_by(TreasuryYield.maturity).all()
        
        # 변화량 계산
        changes = {}
        for latest in latest_data:
            for previous in previous_data:
                if latest.maturity == previous.maturity:
                    if latest.yield_rate and previous.yield_rate:
                        change = latest.yield_rate - previous.yield_rate
                        changes[latest.maturity] = {
                            "absolute": float(change),
                            "percentage": float((change / previous.yield_rate) * 100)
                        }
                    break
        
        return TreasuryYieldRecentResponse(
            latest_date=latest_date,
            data=latest_data,
            previous_data=previous_data,
            changes=changes
        )

    @staticmethod
    def get_chart_data(
        db: Session,
        maturity: str = "10year",
        period: str = "1year"
    ) -> TreasuryYieldChartResponse:
        """차트용 시계열 데이터 조회 (만기별)"""
        
        # 기간 계산
        end_date = datetime.date.today()
        if period == "1month":
            start_date = end_date - timedelta(days=30)
        elif period == "3months":
            start_date = end_date - timedelta(days=90)
        elif period == "6months":
            start_date = end_date - timedelta(days=180)
        elif period == "1year":
            start_date = end_date - timedelta(days=365)
        elif period == "2years":
            start_date = end_date - timedelta(days=730)
        else:
            start_date = end_date - timedelta(days=365)
        
        # 데이터 조회
        data = db.query(TreasuryYield).filter(
            and_(
                TreasuryYield.maturity == maturity,
                TreasuryYield.date >= start_date,
                TreasuryYield.date <= end_date
            )
        ).order_by(TreasuryYield.date).all()
        
        # 차트 포인트 생성
        chart_points = [
            TreasuryYieldChartPoint(
                date=item.date,
                yield_rate=item.yield_rate
            ) for item in data
        ]
        
        # 요약 정보 계산
        rates = [float(item.yield_rate) for item in data if item.yield_rate]
        summary = {}
        if rates:
            summary = {
                "current": rates[-1] if rates else None,
                "min": min(rates),
                "max": max(rates),
                "avg": statistics.mean(rates),
                "change": rates[-1] - rates[0] if len(rates) > 1 else 0,
                "volatility": statistics.stdev(rates) if len(rates) > 1 else 0
            }
        
        return TreasuryYieldChartResponse(
            maturity=maturity,
            period=period,
            data=chart_points,
            summary=summary
        )

    @staticmethod
    def get_yield_curve(db: Session, target_date: Optional[datetime.date] = None) -> YieldCurveResponse:
        """수익률 곡선 데이터 조회"""
        
        # 기준일 설정 (최신 데이터 날짜)
        if not target_date:
            latest_date_result = db.query(func.max(TreasuryYield.date)).scalar()
            target_date = latest_date_result or datetime.date.today()
        
        # 해당 날짜의 모든 만기 데이터 조회
        data = db.query(TreasuryYield).filter(
            TreasuryYield.date == target_date
        ).order_by(TreasuryYield.maturity).all()
        
        # 수익률 곡선 포인트 생성
        maturity_years = {"2year": 2, "10year": 10, "30year": 30}
        curve_points = []
        
        for item in data:
            if item.maturity in maturity_years:
                curve_points.append(YieldCurvePoint(
                    maturity=item.maturity,
                    years=maturity_years[item.maturity],
                    yield_rate=item.yield_rate
                ))
        
        # 곡선 유형 분석
        curve_type = "normal"
        rates_by_years = {point.years: float(point.yield_rate) for point in curve_points if point.yield_rate}
        
        if len(rates_by_years) >= 2:
            if 2 in rates_by_years and 10 in rates_by_years:
                if rates_by_years[2] > rates_by_years[10]:
                    curve_type = "inverted"
                elif abs(rates_by_years[2] - rates_by_years[10]) < 0.1:
                    curve_type = "flat"
        
        # 분석 정보
        analysis = {
            "curve_type": curve_type,
            "yield_spread_2_10": None,
            "yield_spread_10_30": None,
            "inversion_signal": curve_type == "inverted"
        }
        
        if 2 in rates_by_years and 10 in rates_by_years:
            analysis["yield_spread_2_10"] = rates_by_years[10] - rates_by_years[2]
        if 10 in rates_by_years and 30 in rates_by_years:
            analysis["yield_spread_10_30"] = rates_by_years[30] - rates_by_years[10]
        
        return YieldCurveResponse(
            date=target_date,
            curve_type=curve_type,
            data=curve_points,
            analysis=analysis
        )

    @staticmethod
    def get_statistics(db: Session) -> TreasuryYieldStatisticsResponse:
        """통계 정보 조회"""
        
        # 최신 날짜
        latest_date = db.query(func.max(TreasuryYield.date)).scalar() or datetime.date.today()
        
        # 만기별 통계 계산
        maturities = ["2year", "10year", "30year"]
        statistics_list = []
        
        for maturity in maturities:
            # 현재 수익률
            current = db.query(TreasuryYield).filter(
                and_(
                    TreasuryYield.maturity == maturity,
                    TreasuryYield.date == latest_date
                )
            ).first()
            
            # 기간별 데이터
            one_month_ago = latest_date - timedelta(days=30)
            three_months_ago = latest_date - timedelta(days=90)
            one_year_ago = latest_date - timedelta(days=365)
            
            # 평균, 최소, 최대 계산
            year_data = db.query(TreasuryYield.yield_rate).filter(
                and_(
                    TreasuryYield.maturity == maturity,
                    TreasuryYield.date >= one_year_ago
                )
            ).all()
            
            year_rates = [float(r[0]) for r in year_data if r[0]]
            
            month_avg = db.query(func.avg(TreasuryYield.yield_rate)).filter(
                and_(
                    TreasuryYield.maturity == maturity,
                    TreasuryYield.date >= one_month_ago
                )
            ).scalar()
            
            three_month_avg = db.query(func.avg(TreasuryYield.yield_rate)).filter(
                and_(
                    TreasuryYield.maturity == maturity,
                    TreasuryYield.date >= three_months_ago
                )
            ).scalar()
            
            year_avg = db.query(func.avg(TreasuryYield.yield_rate)).filter(
                and_(
                    TreasuryYield.maturity == maturity,
                    TreasuryYield.date >= one_year_ago
                )
            ).scalar()
            
            statistics_list.append(TreasuryYieldStatistics(
                maturity=maturity,
                current_rate=current.yield_rate if current else None,
                avg_1month=month_avg,
                avg_3month=three_month_avg,
                avg_1year=year_avg,
                min_1year=min(year_rates) if year_rates else None,
                max_1year=max(year_rates) if year_rates else None,
                volatility=statistics.stdev(year_rates) if len(year_rates) > 1 else None
            ))
        
        # 시장 요약
        market_summary = {
            "analysis_date": latest_date.isoformat(),
            "total_data_points": db.query(TreasuryYield).count(),
            "latest_update": latest_date.isoformat()
        }
        
        return TreasuryYieldStatisticsResponse(
            as_of_date=latest_date,
            statistics=statistics_list,
            market_summary=market_summary
        )

    @staticmethod
    def get_trends(db: Session) -> TreasuryYieldTrendsResponse:
        """트렌드 분석"""
        
        latest_date = db.query(func.max(TreasuryYield.date)).scalar() or datetime.date.today()
        maturities = ["2year", "10year", "30year"]
        trends = []
        
        for maturity in maturities:
            # 현재 수익률
            current = db.query(TreasuryYield).filter(
                and_(
                    TreasuryYield.maturity == maturity,
                    TreasuryYield.date == latest_date
                )
            ).first()
            
            if not current:
                continue
            
            # 과거 데이터
            one_month_ago = latest_date - timedelta(days=30)
            three_months_ago = latest_date - timedelta(days=90)
            one_year_ago = latest_date - timedelta(days=365)
            
            month_data = db.query(TreasuryYield).filter(
                and_(
                    TreasuryYield.maturity == maturity,
                    TreasuryYield.date == one_month_ago
                )
            ).first()
            
            three_month_data = db.query(TreasuryYield).filter(
                and_(
                    TreasuryYield.maturity == maturity,
                    TreasuryYield.date == three_months_ago
                )
            ).first()
            
            year_data = db.query(TreasuryYield).filter(
                and_(
                    TreasuryYield.maturity == maturity,
                    TreasuryYield.date == one_year_ago
                )
            ).first()
            
            # 변화량 계산
            change_1month = None
            change_3month = None
            change_1year = None
            
            if month_data and month_data.yield_rate and current.yield_rate:
                change_1month = current.yield_rate - month_data.yield_rate
            if three_month_data and three_month_data.yield_rate and current.yield_rate:
                change_3month = current.yield_rate - three_month_data.yield_rate
            if year_data and year_data.yield_rate and current.yield_rate:
                change_1year = current.yield_rate - year_data.yield_rate
            
            # 추세 방향 판단
            trend_direction = "flat"
            if change_1month:
                if change_1month > 0.1:
                    trend_direction = "rising"
                elif change_1month < -0.1:
                    trend_direction = "falling"
            
            # 추세 강도 판단
            trend_strength = "weak"
            if change_1month and abs(change_1month) > 0.5:
                trend_strength = "strong"
            elif change_1month and abs(change_1month) > 0.2:
                trend_strength = "moderate"
            
            trends.append(TreasuryYieldTrend(
                maturity=maturity,
                trend_direction=trend_direction,
                change_1month=change_1month,
                change_3month=change_3month,
                change_1year=change_1year,
                trend_strength=trend_strength
            ))
        
        # 시장 전망
        market_outlook = {
            "analysis_date": latest_date.isoformat(),
            "overall_trend": "mixed",  # 실제로는 더 복잡한 로직 필요
            "key_insights": []
        }
        
        return TreasuryYieldTrendsResponse(
            analysis_date=latest_date,
            trends=trends,
            market_outlook=market_outlook
        )

    @staticmethod
    def get_by_maturity(
        db: Session,
        maturity: str,
        page: int = 1,
        size: int = 50
    ) -> TreasuryYieldListResponse:
        """특정 만기별 데이터 조회"""
        
        return TreasuryYieldService.get_treasury_yields(
            db=db,
            page=page,
            size=size,
            maturity=maturity,
            order_by="date_desc"
        )