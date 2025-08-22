# app/utils/timezone_utils.py
import pytz
from datetime import datetime, timedelta
from typing import Optional

# 시간대 상수
UTC = pytz.UTC
US_EASTERN = pytz.timezone('US/Eastern')
KOREA = pytz.timezone('Asia/Seoul')

class TimezoneHelper:
    """시간대 처리 유틸리티 클래스"""
    
    @staticmethod
    def now_utc() -> datetime:
        """현재 UTC 시간 반환"""
        return datetime.now(UTC)
    
    @staticmethod
    def now_us_eastern() -> datetime:
        """현재 미국 동부 시간 반환"""
        return datetime.now(US_EASTERN)
    
    @staticmethod
    def now_korea() -> datetime:
        """현재 한국 시간 반환"""
        return datetime.now(KOREA)
    
    @staticmethod
    def to_utc(dt: datetime, from_tz: str = 'US/Eastern') -> datetime:
        """특정 시간대의 datetime을 UTC로 변환"""
        if dt.tzinfo is None:
            # naive datetime인 경우 시간대 정보 추가
            source_tz = pytz.timezone(from_tz)
            dt = source_tz.localize(dt)
        return dt.astimezone(UTC)
    
    @staticmethod
    def to_us_eastern(dt: datetime) -> datetime:
        """UTC datetime을 미국 동부 시간으로 변환"""
        if dt.tzinfo is None:
            dt = UTC.localize(dt)
        return dt.astimezone(US_EASTERN)
    
    @staticmethod
    def to_korea(dt: datetime) -> datetime:
        """UTC datetime을 한국 시간으로 변환"""
        if dt.tzinfo is None:
            dt = UTC.localize(dt)
        return dt.astimezone(KOREA)
    
    @staticmethod
    def get_market_day_start_utc(date: Optional[datetime] = None) -> datetime:
        """
        미국 주식 시장 기준 특정 날짜의 시작 시간을 UTC로 반환
        
        Args:
            date: 기준 날짜 (None이면 오늘)
            
        Returns:
            datetime: 해당 날짜 00:00 EST/EDT의 UTC 시간
        """
        if date is None:
            date = TimezoneHelper.now_us_eastern()
        
        # 미국 동부 시간 기준 하루 시작 (00:00)
        market_start = US_EASTERN.localize(
            datetime(date.year, date.month, date.day, 0, 0, 0)
        )
        return market_start.astimezone(UTC)
    
    @staticmethod
    def get_previous_market_day_utc(days_back: int = 1) -> datetime:
        """
        미국 주식 시장 기준 N일 전의 시작 시간을 UTC로 반환
        
        Args:
            days_back: 며칠 전 (기본값: 1일 전)
            
        Returns:
            datetime: N일 전 00:00 EST/EDT의 UTC 시간
        """
        us_now = TimezoneHelper.now_us_eastern()
        previous_date = us_now - timedelta(days=days_back)
        return TimezoneHelper.get_market_day_start_utc(previous_date)
    
    @staticmethod
    def is_market_hours() -> bool:
        """현재 미국 주식 시장 시간인지 확인"""
        us_now = TimezoneHelper.now_us_eastern()
        
        # 주말 체크
        if us_now.weekday() >= 5:  # 5=토요일, 6=일요일
            return False
        
        # 정규 거래시간: 9:30 AM - 4:00 PM ET
        market_open = us_now.replace(hour=9, minute=30, second=0, microsecond=0)
        market_close = us_now.replace(hour=16, minute=0, second=0, microsecond=0)
        
        return market_open <= us_now <= market_close
    
    @staticmethod
    def format_for_display(dt: datetime, timezone: str = 'UTC') -> str:
        """
        datetime을 지정된 시간대로 변환하여 표시용 문자열로 포맷
        
        Args:
            dt: 변환할 datetime
            timezone: 표시할 시간대 ('UTC', 'US/Eastern', 'Asia/Seoul')
            
        Returns:
            str: 포맷된 시간 문자열
        """
        if dt.tzinfo is None:
            dt = UTC.localize(dt)
        
        if timezone == 'US/Eastern':
            target_dt = dt.astimezone(US_EASTERN)
            tz_name = 'ET'
        elif timezone == 'Asia/Seoul':
            target_dt = dt.astimezone(KOREA)
            tz_name = 'KST'
        else:
            target_dt = dt.astimezone(UTC)
            tz_name = 'UTC'
        
        return f"{target_dt.strftime('%Y-%m-%d %H:%M:%S')} {tz_name}"

# 편의 함수들
def now_utc() -> datetime:
    """현재 UTC 시간"""
    return TimezoneHelper.now_utc()

def previous_market_day_utc(days_back: int = 1) -> datetime:
    """미국 시장 기준 N일 전 시작 시간 (UTC)"""
    return TimezoneHelper.get_previous_market_day_utc(days_back)

def is_market_open() -> bool:
    """현재 미국 주식 시장이 열려있는지"""
    return TimezoneHelper.is_market_hours()
