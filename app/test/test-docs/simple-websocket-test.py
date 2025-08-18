#!/usr/bin/env python3
"""
간단한 WebSocket 연결 테스트
"""
import asyncio
import websockets
import json

async def test_websocket_connection():
    """WebSocket 연결 기본 테스트"""
    # 테스트할 URL들
    test_urls = [
        "ws://localhost:30888/api/v1/ws/crypto",
        "ws://localhost:30888/api/v1/ws/stocks/topgainers", 
        "ws://localhost:30888/api/v1/ws/dashboard"
    ]
    
    for url in test_urls:
        print(f"\n🔗 테스트 URL: {url}")
        try:
            async with websockets.connect(url, timeout=5) as websocket:
                print("  ✅ 연결 성공!")
                
                # 첫 번째 메시지 수신 시도
                try:
                    message = await asyncio.wait_for(websocket.recv(), timeout=3.0)
                    print(f"  📨 첫 메시지: {message[:100]}...")
                except asyncio.TimeoutError:
                    print("  ⏰ 첫 메시지 수신 타임아웃")
                
                break  # 하나라도 성공하면 종료
                
        except Exception as e:
            print(f"  ❌ 연결 실패: {e}")
            continue
    
    print("\n🏁 테스트 완료")

if __name__ == "__main__":
    asyncio.run(test_websocket_connection())