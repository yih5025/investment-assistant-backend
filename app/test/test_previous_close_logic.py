#!/usr/bin/env python3
"""
전날 종가 계산 로직 테스트 및 분석 스크립트

이 스크립트는 현재 전날 종가 계산 로직의 문제점을 파악하고 
실제 시간 변환이 올바르게 동작하는지 검증합니다.
"""

import pytz
from datetime import datetime, timedelta
import logging

# 로깅 설정
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def analyze_previous_close_logic():
    """현재 전날 종가 계산 로직 분석"""
    
    print("=" * 80)
    print("🔍 전날 종가 계산 로직 분석")
    print("=" * 80)
    
    # 시간대 설정
    us_eastern = pytz.timezone('US/Eastern')
    korea_tz = pytz.timezone('Asia/Seoul')
    
    # 현재 시간들
    now_utc = datetime.now(pytz.UTC)
    now_korea = datetime.now(korea_tz)
    now_us = now_korea.astimezone(us_eastern)
    
    print(f"\n📅 현재 시간 정보:")
    print(f"  UTC 시간:     {now_utc.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"  한국 시간:    {now_korea.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"  미국 동부시간: {now_us.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    
    # 현재 로직 시뮬레이션
    print(f"\n🔄 현재 로직 시뮬레이션:")
    
    # Step 1: 미국 시간 기준 어제 계산
    yesterday_us = now_us - timedelta(days=1)
    print(f"  1️⃣ 미국 어제:   {yesterday_us.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    
    # Step 2: 미국 어제 23:59:59
    yesterday_end_us = yesterday_us.replace(hour=23, minute=59, second=59)
    print(f"  2️⃣ 미국 어제 종료: {yesterday_end_us.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    
    # Step 3: 한국 시간으로 변환
    yesterday_end_korea = yesterday_end_us.astimezone(korea_tz)
    print(f"  3️⃣ 한국 시간 변환: {yesterday_end_korea.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    
    # Step 4: DB 조회용 naive datetime
    db_cutoff = yesterday_end_korea.replace(tzinfo=None)
    print(f"  4️⃣ DB 조회 기준:  {db_cutoff.strftime('%Y-%m-%d %H:%M:%S')} (naive)")
    
    # 미국 시장 시간 분석
    print(f"\n📊 미국 시장 시간 분석:")
    
    # 미국 시장 개장/폐장 시간 (EST/EDT 고려)
    market_open_us = now_us.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close_us = now_us.replace(hour=16, minute=0, second=0, microsecond=0)
    
    print(f"  📈 오늘 개장:    {market_open_us.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"  📉 오늘 폐장:    {market_close_us.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    
    # 어제 폐장 시간
    yesterday_close_us = yesterday_us.replace(hour=16, minute=0, second=0, microsecond=0)
    yesterday_close_korea = yesterday_close_us.astimezone(korea_tz)
    
    print(f"  📉 어제 폐장 (미국): {yesterday_close_us.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"  📉 어제 폐장 (한국): {yesterday_close_korea.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    
    # 로직 검증
    print(f"\n✅ 로직 검증:")
    
    # 1. 시간대 변환이 올바른가?
    print(f"  1️⃣ 시간대 변환:")
    print(f"     현재 미국 시간: {now_us.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"     계산된 어제:   {yesterday_us.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    
    # 2. 실제 전날 종가 시점과 비교
    print(f"  2️⃣ 전날 종가 시점 비교:")
    print(f"     실제 어제 폐장:   {yesterday_close_us.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"     계산된 기준점:   {yesterday_end_us.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    
    is_after_close = yesterday_end_us > yesterday_close_us
    print(f"     기준점이 폐장 후인가? {is_after_close} ✅" if is_after_close else f"     기준점이 폐장 후인가? {is_after_close} ❌")
    
    # 3. 주말/공휴일 고려
    print(f"  3️⃣ 주말/공휴일 고려:")
    yesterday_weekday = yesterday_us.weekday()  # 0=월요일, 6=일요일
    print(f"     어제 요일: {['월', '화', '수', '목', '금', '토', '일'][yesterday_weekday]}")
    
    if yesterday_weekday >= 5:  # 토요일(5) 또는 일요일(6)
        print(f"     ⚠️ 어제는 주말입니다! 전날 거래일을 찾아야 합니다.")
        
        # 마지막 거래일 찾기
        last_trading_day = yesterday_us
        while last_trading_day.weekday() >= 5:
            last_trading_day = last_trading_day - timedelta(days=1)
        
        print(f"     마지막 거래일: {last_trading_day.strftime('%Y-%m-%d %a')}")
        last_close_us = last_trading_day.replace(hour=16, minute=0, second=0)
        last_close_korea = last_close_us.astimezone(korea_tz)
        print(f"     마지막 폐장 (한국): {last_close_korea.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    
    return {
        'current_logic_cutoff': db_cutoff,
        'actual_yesterday_close': yesterday_close_korea.replace(tzinfo=None),
        'is_weekend': yesterday_weekday >= 5,
        'yesterday_weekday': yesterday_weekday
    }

def test_edge_cases():
    """엣지 케이스 테스트"""
    
    print("\n" + "=" * 80)
    print("🧪 엣지 케이스 테스트")
    print("=" * 80)
    
    # 테스트 케이스들
    test_cases = [
        # 월요일 오전 (전날 종가는 금요일이어야 함)
        datetime(2024, 8, 26, 10, 0, 0),  # 월요일 10시
        # 토요일 (전날 종가는 금요일이어야 함)  
        datetime(2024, 8, 24, 15, 0, 0),  # 토요일 15시
        # 일요일 (전날 종가는 금요일이어야 함)
        datetime(2024, 8, 25, 20, 0, 0),  # 일요일 20시
    ]
    
    korea_tz = pytz.timezone('Asia/Seoul')
    us_eastern = pytz.timezone('US/Eastern')
    
    for i, test_time in enumerate(test_cases, 1):
        print(f"\n🧪 테스트 케이스 {i}: {test_time.strftime('%Y-%m-%d %H:%M:%S')} (한국시간 가정)")
        
        # 한국 시간으로 설정
        now_korea = korea_tz.localize(test_time)
        now_us = now_korea.astimezone(us_eastern)
        
        print(f"  현재 시간 (미국): {now_us.strftime('%Y-%m-%d %H:%M:%S %Z %a')}")
        
        # 현재 로직 적용
        yesterday_us = now_us - timedelta(days=1)
        yesterday_end_us = yesterday_us.replace(hour=23, minute=59, second=59)
        
        print(f"  계산된 어제:     {yesterday_us.strftime('%Y-%m-%d %H:%M:%S %Z %a')}")
        print(f"  DB 조회 기준점:  {yesterday_end_us.strftime('%Y-%m-%d %H:%M:%S %Z %a')}")
        
        # 실제 마지막 거래일 계산
        actual_last_trading_day = now_us - timedelta(days=1)
        while actual_last_trading_day.weekday() >= 5:  # 주말 제외
            actual_last_trading_day = actual_last_trading_day - timedelta(days=1)
        
        actual_close = actual_last_trading_day.replace(hour=16, minute=0, second=0)
        print(f"  실제 마지막 거래일: {actual_last_trading_day.strftime('%Y-%m-%d %Z %a')}")
        print(f"  실제 마지막 폐장:  {actual_close.strftime('%Y-%m-%d %H:%M:%S %Z %a')}")
        
        # 비교
        is_correct = yesterday_us.date() == actual_last_trading_day.date()
        print(f"  로직이 올바른가? {'✅' if is_correct else '❌'}")

if __name__ == "__main__":
    # 현재 로직 분석
    result = analyze_previous_close_logic()
    
    # 엣지 케이스 테스트
    test_edge_cases()
    
    print("\n" + "=" * 80)
    print("📋 분석 결과 요약")
    print("=" * 80)
    print("1. 현재 로직은 단순히 하루 전 23:59:59를 기준으로 함")
    print("2. 주말/공휴일을 고려하지 않음")
    print("3. 실제 시장 폐장 시간(16:00)이 아닌 자정을 기준으로 함")
    print("4. DST(일광절약시간) 변경 시점에서 문제 가능성")
    print("\n💡 개선 방안:")
    print("- 주말/공휴일 건너뛰기 로직 추가")
    print("- 실제 시장 폐장 시간(16:00) 기준으로 변경")
    print("- DST 변경 고려한 시간 계산")
