from typing import List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import desc, asc, func
from ..models.inflation_model import Inflation
from ..schemas.inflation_schema import (
    InflationResponse, 
    InflationChartData, 
    InflationChartResponse,
    InflationStatsResponse
)

class InflationService:
    """인플레이션 데이터 비즈니스 로직(에러 수정)"""
    
    def __init__(self, db: Session):
        self.db = db

    def get_all_inflation_data(self, order_by: str = "desc") -> List[InflationResponse]:
        """
        전체 인플레이션 데이터 조회
        Args:
            order_by: 정렬 순서 ("desc" or "asc")
        Returns:
            인플레이션 데이터 리스트
        """
        query = self.db.query(Inflation)
        
        if order_by == "desc":
            query = query.order_by(desc(Inflation.date))
        else:
            query = query.order_by(asc(Inflation.date))
            
        inflation_data = query.all()
        return [InflationResponse.model_validate(item) for item in inflation_data]

    def get_chart_data(self) -> InflationChartResponse:
        """
        프론트엔드 차트용 데이터 생성
        Returns:
            차트 데이터 + 통계 정보
        """
        # 전체 데이터 조회 (날짜 오름차순)
        inflation_data = self.db.query(Inflation).order_by(asc(Inflation.date)).all()
        
        if not inflation_data:
            return InflationChartResponse(
                data=[],
                latest_rate=None,
                avg_rate=None,
                min_rate=None,
                max_rate=None
            )
        
        # 차트용 데이터 변환 (연도와 비율만)
        chart_data = []
        rates = []
        
        for item in inflation_data:
            if item.inflation_rate is not None:
                year = item.date.year
                rate = item.inflation_rate  # 이미 float이므로 변환 불필요
                
                chart_data.append(InflationChartData(year=year, rate=rate))
                rates.append(rate)
        
        # 통계 계산
        latest_rate = rates[-1] if rates else None
        avg_rate = sum(rates) / len(rates) if rates else None
        min_rate = min(rates) if rates else None
        max_rate = max(rates) if rates else None
        
        return InflationChartResponse(
            data=chart_data,
            latest_rate=latest_rate,
            avg_rate=round(avg_rate, 3) if avg_rate else None,
            min_rate=min_rate,
            max_rate=max_rate
        )

    def get_recent_years(self, years: int = 10) -> List[InflationResponse]:
        """
        최근 N년 데이터 조회
        Args:
            years: 조회할 연수 (기본값: 10년)
        Returns:
            최근 N년 인플레이션 데이터
        """
        inflation_data = (
            self.db.query(Inflation)
            .order_by(desc(Inflation.date))
            .limit(years)
            .all()
        )
        return [InflationResponse.model_validate(item) for item in inflation_data]

    def get_year_data(self, year: int) -> Optional[InflationResponse]:
        """
        특정 연도 데이터 조회
        Args:
            year: 조회할 연도
        Returns:
            해당 연도 인플레이션 데이터 또는 None
        """
        # 해당 연도의 1월 1일 데이터 조회
        from datetime import date
        target_date = date(year, 1, 1)
        
        inflation_data = (
            self.db.query(Inflation)
            .filter(Inflation.date == target_date)
            .first()
        )
        
        return InflationResponse.model_validate(inflation_data) if inflation_data else None

    def get_statistics(self) -> InflationStatsResponse:
        """
        인플레이션 통계 정보 생성
        Returns:
            통계 정보 (최신값, 평균, 최고/최저값 등)
        """
        # 데이터베이스에서 직접 통계 계산
        stats_query = self.db.query(
            func.count(Inflation.date).label('total_count'),
            func.avg(Inflation.inflation_rate).label('avg_rate'),
            func.min(Inflation.inflation_rate).label('min_rate'),
            func.max(Inflation.inflation_rate).label('max_rate')
        ).first()
        
        # 최신 데이터 조회
        latest_data = (
            self.db.query(Inflation)
            .order_by(desc(Inflation.date))
            .first()
        )
        
        if not latest_data or not stats_query:
            raise ValueError("인플레이션 데이터가 없습니다")
        
        return InflationStatsResponse(
            latest_year=latest_data.date.year,
            latest_rate=float(latest_data.inflation_rate),
            average_rate=round(float(stats_query.avg_rate), 3),
            min_rate=float(stats_query.min_rate),
            max_rate=float(stats_query.max_rate),
            total_years=stats_query.total_count
        )

    def get_range_data(self, start_year: int, end_year: int) -> List[InflationResponse]:
        """
        연도 범위별 데이터 조회
        Args:
            start_year: 시작 연도
            end_year: 종료 연도
        Returns:
            지정 범위의 인플레이션 데이터
        """
        from datetime import date
        start_date = date(start_year, 1, 1)
        end_date = date(end_year, 12, 31)
        
        inflation_data = (
            self.db.query(Inflation)
            .filter(Inflation.date >= start_date)
            .filter(Inflation.date <= end_date)
            .order_by(asc(Inflation.date))
            .all()
        )
        
        return [InflationResponse.model_validate(item) for item in inflation_data]