from datetime import date
from typing import List, Optional
from pydantic import BaseModel, Field

class InflationBase(BaseModel):
    """인플레이션 데이터 기본 스키마"""
    datetime: date = Field(..., alias="date", description="데이터 날짜 (연도별)")
    inflation_rate: Optional[float] = Field(None, description="인플레이션율 (%)")
    interval_type: str = Field(default="annual", description="데이터 간격")
    unit: str = Field(default="percent", description="단위")
    name: str = Field(default="Inflation - US Consumer Prices", description="데이터 설명")

class InflationResponse(InflationBase):
    """API 응답용 인플레이션 스키마"""
    
    model_config = {"from_attributes": True}

class InflationChartData(BaseModel):
    """그래프 시각화용 간소화된 스키마"""
    year: int = Field(..., description="연도")
    rate: float = Field(..., description="인플레이션율")
    
class InflationListResponse(BaseModel):
    """인플레이션 목록 응답"""
    total_count: int = Field(..., description="전체 데이터 개수")
    items: List[InflationResponse] = Field(..., description="인플레이션 데이터 목록")

class InflationChartResponse(BaseModel):
    """차트용 응답 (프론트엔드 최적화)"""
    data: List[InflationChartData] = Field(..., description="차트 데이터")
    latest_rate: Optional[float] = Field(None, description="최신 인플레이션율")
    avg_rate: Optional[float] = Field(None, description="평균 인플레이션율")
    min_rate: Optional[float] = Field(None, description="최저 인플레이션율")
    max_rate: Optional[float] = Field(None, description="최고 인플레이션율")

class InflationStatsResponse(BaseModel):
    """인플레이션 통계 정보"""
    latest_year: int = Field(..., description="최신 데이터 연도")
    latest_rate: float = Field(..., description="최신 인플레이션율")
    average_rate: float = Field(..., description="전체 평균")
    min_rate: float = Field(..., description="최저값")
    max_rate: float = Field(..., description="최고값")
    total_years: int = Field(..., description="총 데이터 연수")