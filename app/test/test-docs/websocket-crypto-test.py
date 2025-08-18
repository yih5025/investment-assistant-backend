#!/usr/bin/env python3
"""
WebSocket Crypto 테스트 클라이언트
포트: 30888
테스트 대상: /ws/crypto 엔드포인트
"""
import asyncio
import websockets
import json
import time
from datetime import datetime

# 테스트 설정
SERVER_URL = "ws://localhost:30888"
CRYPTO_ENDPOINT = f"{SERVER_URL}/api/v1/ws/crypto"
CRYPTO_SYMBOL_ENDPOINT = f"{SERVER_URL}/api/v1/ws/crypto/KRW-BTC"  # 비트코인 테스트

async def test_crypto_all():
    """전체 암호화폐 WebSocket 테스트"""
    print("🚀 전체 암호화폐 WebSocket 테스트 시작...")
    print(f"연결 URL: {CRYPTO_ENDPOINT}")
    
    try:
        async with websockets.connect(CRYPTO_ENDPOINT) as websocket:
            print("✅ WebSocket 연결 성공!")
            
            # 연결 상태 메시지 수신
            initial_message = await websocket.recv()
            print(f"📨 초기 연결 메시지: {initial_message}")
            
            # 실시간 데이터 5개 정도 수신해보기
            message_count = 0
            start_time = time.time()
            
            while message_count < 5 and (time.time() - start_time) < 30:  # 최대 30초
                try:
                    message = await asyncio.wait_for(websocket.recv(), timeout=10.0)
                    message_count += 1
                    
                    # JSON 파싱 시도
                    try:
                        data = json.loads(message)
                        print(f"📊 메시지 #{message_count} ({datetime.now().strftime('%H:%M:%S')})")
                        print(f"   타입: {data.get('type', 'unknown')}")
                        
                        if 'data' in data:
                            crypto_data = data['data']
                            if isinstance(crypto_data, list) and crypto_data:
                                print(f"   암호화폐 개수: {len(crypto_data)}")
                                print(f"   첫 번째 예시: {crypto_data[0].get('symbol', 'N/A')} - {crypto_data[0].get('price', 'N/A')}")
                        
                        if 'timestamp' in data:
                            print(f"   타임스탬프: {data['timestamp']}")
                            
                    except json.JSONDecodeError:
                        print(f"📝 원본 메시지: {message[:200]}...")
                    
                    print("-" * 50)
                    
                except asyncio.TimeoutError:
                    print("⏰ 메시지 수신 타임아웃 (10초)")
                    break
                    
            print(f"✅ 전체 암호화폐 테스트 완료! 총 {message_count}개 메시지 수신")
            
    except websockets.exceptions.ConnectionClosed as e:
        print(f"❌ WebSocket 연결 종료: {e}")
    except Exception as e:
        print(f"❌ 전체 암호화폐 테스트 실패: {e}")

async def test_crypto_symbol():
    """특정 암호화폐 WebSocket 테스트 (KRW-BTC)"""
    print("\n🚀 특정 암호화폐(KRW-BTC) WebSocket 테스트 시작...")
    print(f"연결 URL: {CRYPTO_SYMBOL_ENDPOINT}")
    
    try:
        async with websockets.connect(CRYPTO_SYMBOL_ENDPOINT) as websocket:
            print("✅ WebSocket 연결 성공!")
            
            # 연결 상태 메시지 수신
            initial_message = await websocket.recv()
            print(f"📨 초기 연결 메시지: {initial_message}")
            
            # 실시간 데이터 3개 수신
            message_count = 0
            start_time = time.time()
            
            while message_count < 3 and (time.time() - start_time) < 20:
                try:
                    message = await asyncio.wait_for(websocket.recv(), timeout=10.0)
                    message_count += 1
                    
                    try:
                        data = json.loads(message)
                        print(f"📊 비트코인 메시지 #{message_count} ({datetime.now().strftime('%H:%M:%S')})")
                        print(f"   타입: {data.get('type', 'unknown')}")
                        
                        if 'data' in data:
                            btc_data = data['data']
                            print(f"   심볼: {btc_data.get('symbol', 'N/A')}")
                            print(f"   가격: {btc_data.get('price', 'N/A')}")
                            print(f"   변화율: {btc_data.get('change_percent', 'N/A')}%")
                        
                    except json.JSONDecodeError:
                        print(f"📝 원본 메시지: {message[:150]}...")
                    
                    print("-" * 40)
                    
                except asyncio.TimeoutError:
                    print("⏰ 메시지 수신 타임아웃 (10초)")
                    break
                    
            print(f"✅ 비트코인 테스트 완료! 총 {message_count}개 메시지 수신")
            
    except Exception as e:
        print(f"❌ 비트코인 테스트 실패: {e}")

async def test_server_status():
    """서버 상태 HTTP 엔드포인트 테스트"""
    print("\n🚀 서버 상태 확인...")
    
    try:
        import aiohttp
        
        async with aiohttp.ClientSession() as session:
            status_url = f"http://localhost:30888/api/v1/ws/status"
            
            async with session.get(status_url) as response:
                if response.status == 200:
                    data = await response.json()
                    print("📊 서버 상태:")
                    print(f"   WebSocket 서버: {data.get('websocket_server', {}).get('status', 'unknown')}")
                    print(f"   전체 연결 수: {data.get('websocket_server', {}).get('total_connections', 0)}")
                    print(f"   암호화폐 구독자: {data.get('websocket_server', {}).get('crypto_subscribers', 0)}")
                else:
                    print(f"❌ 상태 조회 실패: HTTP {response.status}")
                    
    except ImportError:
        print("⚠️ aiohttp가 설치되지 않아 HTTP 상태 테스트를 건너뜁니다.")
    except Exception as e:
        print(f"❌ 상태 조회 실패: {e}")

async def main():
    """메인 테스트 함수"""
    print("=" * 60)
    print("🧪 WebSocket Crypto 테스트 시작")
    print(f"📅 테스트 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    # 서버 상태 확인
    await test_server_status()
    
    # 전체 암호화폐 테스트
    await test_crypto_all()
    
    # 특정 암호화폐 테스트
    await test_crypto_symbol()
    
    print("\n" + "=" * 60)
    print("🏁 WebSocket Crypto 테스트 완료")
    print("=" * 60)

if __name__ == "__main__":
    asyncio.run(main())