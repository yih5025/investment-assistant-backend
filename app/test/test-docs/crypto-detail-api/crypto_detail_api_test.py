#!/usr/bin/env python3
"""
Crypto Detail 통합 테스트 스크립트 - 분리 저장 버전
모든 crypto detail 엔드포인트를 호출하여 카테고리별 JSON 파일 생성
"""

import asyncio
import aiohttp
import json
import sys
from datetime import datetime
from typing import Dict, Any, Optional


class CryptoDetailTester:
    def __init__(self, base_url: str = "https://api.investment-assistant.site"):
        self.base_url = base_url
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def call_api(self, endpoint: str) -> Dict[str, Any]:
        """API 호출 및 응답 반환"""
        url = f"{self.base_url}{endpoint}"
        print(f"📡 Calling: {url}")
        
        try:
            async with self.session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    print(f"✅ Success: {endpoint}")
                    return {
                        "success": True,
                        "status_code": response.status,
                        "data": data,
                        "url": url
                    }
                else:
                    error_text = await response.text()
                    print(f"❌ Error {response.status}: {endpoint}")
                    return {
                        "success": False,
                        "status_code": response.status,
                        "error": error_text,
                        "url": url
                    }
        except Exception as e:
            print(f"🔥 Exception: {endpoint} - {str(e)}")
            return {
                "success": False,
                "status_code": 0,
                "error": str(e),
                "url": url
            }
    
    async def test_concept_endpoints(self, symbol: str) -> Dict[str, Any]:
        """Concept 관련 엔드포인트 테스트"""
        print(f"\n🎯 Testing Concept Endpoints for {symbol}")
        
        endpoints = [
            f"/api/v1/crypto/details/concept/{symbol}",
            f"/api/v1/crypto/details/concept/{symbol}/basic-info",
            f"/api/v1/crypto/details/concept/{symbol}/categories"
        ]
        
        results = {}
        for endpoint in endpoints:
            endpoint_name = endpoint.split('/')[-1] if endpoint.split('/')[-1] != symbol else 'main'
            results[endpoint_name] = await self.call_api(endpoint)
        
        return results
    
    async def test_ecosystem_endpoints(self, symbol: str) -> Dict[str, Any]:
        """Ecosystem 관련 엔드포인트 테스트"""
        print(f"\n🌐 Testing Ecosystem Endpoints for {symbol}")
        
        endpoints = [
            f"/api/v1/crypto/details/ecosystem/{symbol}",
            f"/api/v1/crypto/details/ecosystem/{symbol}/development",
            f"/api/v1/crypto/details/ecosystem/{symbol}/community",
            f"/api/v1/crypto/details/ecosystem/{symbol}/market",
            f"/api/v1/crypto/details/ecosystem/{symbol}/summary"
        ]
        
        results = {}
        for endpoint in endpoints:
            endpoint_name = endpoint.split('/')[-1] if endpoint.split('/')[-1] != symbol else 'main'
            results[endpoint_name] = await self.call_api(endpoint)
        
        return results
    
    async def test_investment_endpoints(self, symbol: str) -> Dict[str, Any]:
        """Investment 관련 엔드포인트 테스트"""
        print(f"\n💰 Testing Investment Endpoints for {symbol}")
        
        endpoints = [
            f"/api/v1/crypto/details/investment/{symbol}",
            f"/api/v1/crypto/details/kimchi-premium/{symbol}",
            f"/api/v1/crypto/details/kimchi-premium/{symbol}/detailed?sort_by=premium_desc&min_volume=100000",
            f"/api/v1/crypto/details/derivatives/{symbol}",
            f"/api/v1/crypto/details/kimchi-premium/{symbol}/chart"
        ]
        
        results = {}
        for endpoint in endpoints:
            # 엔드포인트 이름 추출
            if 'kimchi-premium' in endpoint and 'detailed' in endpoint:
                endpoint_name = 'kimchi_premium_detailed'
            elif 'kimchi-premium' in endpoint and 'chart' in endpoint:
                endpoint_name = 'kimchi_premium_chart'
            elif 'kimchi-premium' in endpoint:
                endpoint_name = 'kimchi_premium'
            elif 'derivatives' in endpoint:
                endpoint_name = 'derivatives'
            else:
                endpoint_name = 'main'
            
            results[endpoint_name] = await self.call_api(endpoint)
        
        return results
    
    async def test_all_endpoints(self, symbol: str) -> Dict[str, Any]:
        """모든 엔드포인트 통합 테스트"""
        print(f"🚀 Starting comprehensive test for {symbol}")
        print(f"⏰ Test started at: {datetime.now().isoformat()}")
        
        # 모든 카테고리 테스트
        concept_results = await self.test_concept_endpoints(symbol)
        ecosystem_results = await self.test_ecosystem_endpoints(symbol)
        investment_results = await self.test_investment_endpoints(symbol)
        
        # 통합 결과
        integrated_result = {
            "test_metadata": {
                "symbol": symbol.upper(),
                "test_timestamp": datetime.now().isoformat(),
                "base_url": self.base_url,
                "total_endpoints_tested": (
                    len(concept_results) + 
                    len(ecosystem_results) + 
                    len(investment_results)
                )
            },
            "concept": concept_results,
            "ecosystem": ecosystem_results,
            "investment": investment_results
        }
        
        # 성공/실패 통계
        all_results = [concept_results, ecosystem_results, investment_results]
        total_success = sum(
            sum(1 for result in category.values() if result.get('success', False))
            for category in all_results
        )
        total_tests = sum(len(category) for category in all_results)
        
        integrated_result["test_summary"] = {
            "total_endpoints": total_tests,
            "successful_endpoints": total_success,
            "failed_endpoints": total_tests - total_success,
            "success_rate": f"{(total_success/total_tests)*100:.1f}%" if total_tests > 0 else "0%"
        }
        
        return integrated_result
    
    def save_results_separately(self, result: Dict[str, Any]):
        """결과를 카테고리별 3개 파일로 분리 저장"""
        symbol = result.get('test_metadata', {}).get('symbol', 'UNKNOWN')
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # 각 카테고리별로 분리 저장
        categories = ['concept', 'ecosystem', 'investment']
        
        for category in categories:
            if category in result:
                category_data = {
                    "test_metadata": {
                        "symbol": symbol,
                        "category": category,
                        "test_timestamp": result['test_metadata']['test_timestamp'],
                        "base_url": result['test_metadata']['base_url']
                    },
                    "data": result[category],
                    "summary": {
                        "total_endpoints": len(result[category]),
                        "successful_endpoints": sum(1 for r in result[category].values() if r.get('success', False)),
                        "failed_endpoints": sum(1 for r in result[category].values() if not r.get('success', False))
                    }
                }
                
                filename = f"crypto_{category}_{symbol}_{timestamp}.json"
                
                try:
                    with open(filename, 'w', encoding='utf-8') as f:
                        json.dump(category_data, f, indent=2, ensure_ascii=False)
                    print(f"📁 {category.capitalize()} results saved to: {filename}")
                except Exception as e:
                    print(f"❌ Failed to save {category} file: {e}")
    
    def save_summary_only(self, result: Dict[str, Any]):
        """요약 정보만 별도 저장"""
        symbol = result.get('test_metadata', {}).get('symbol', 'UNKNOWN')
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"crypto_summary_{symbol}_{timestamp}.json"
        
        summary_data = {
            "test_metadata": result.get('test_metadata', {}),
            "test_summary": result.get('test_summary', {}),
            "endpoint_status": {}
        }
        
        # 각 카테고리별 엔드포인트 상태만 추출
        for category in ['concept', 'ecosystem', 'investment']:
            if category in result:
                summary_data["endpoint_status"][category] = {}
                for endpoint, endpoint_result in result[category].items():
                    summary_data["endpoint_status"][category][endpoint] = {
                        "success": endpoint_result.get('success', False),
                        "status_code": endpoint_result.get('status_code', 0),
                        "url": endpoint_result.get('url', '')
                    }
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(summary_data, f, indent=2, ensure_ascii=False)
            print(f"📁 Summary saved to: {filename}")
        except Exception as e:
            print(f"❌ Failed to save summary file: {e}")
    
    def print_summary(self, result: Dict[str, Any]):
        """테스트 결과 요약 출력"""
        print("\n" + "="*60)
        print("📊 TEST SUMMARY")
        print("="*60)
        
        metadata = result.get('test_metadata', {})
        summary = result.get('test_summary', {})
        
        print(f"🪙 Symbol: {metadata.get('symbol', 'N/A')}")
        print(f"⏰ Timestamp: {metadata.get('test_timestamp', 'N/A')}")
        print(f"🌐 Base URL: {metadata.get('base_url', 'N/A')}")
        print(f"📡 Total Endpoints: {summary.get('total_endpoints', 0)}")
        print(f"✅ Successful: {summary.get('successful_endpoints', 0)}")
        print(f"❌ Failed: {summary.get('failed_endpoints', 0)}")
        print(f"📈 Success Rate: {summary.get('success_rate', '0%')}")
        
        # 카테고리별 상세 결과
        for category_name, category_results in [
            ('Concept', result.get('concept', {})),
            ('Ecosystem', result.get('ecosystem', {})),
            ('Investment', result.get('investment', {}))
        ]:
            print(f"\n📂 {category_name} Results:")
            for endpoint, endpoint_result in category_results.items():
                status = "✅" if endpoint_result.get('success', False) else "❌"
                status_code = endpoint_result.get('status_code', 0)
                print(f"  {status} {endpoint}: {status_code}")
                
                if not endpoint_result.get('success', False):
                    error = endpoint_result.get('error', 'Unknown error')
                    print(f"    Error: {error[:100]}...")


async def main():
    """메인 실행 함수"""
    # 테스트할 심볼 설정
    test_symbol = sys.argv[1] if len(sys.argv) > 1 else "BTC"
    
    print(f"🔍 Testing Crypto Detail APIs for: {test_symbol}")
    
    async with CryptoDetailTester() as tester:
        # 모든 엔드포인트 테스트
        result = await tester.test_all_endpoints(test_symbol)
        
        # 결과 요약 출력
        tester.print_summary(result)
        
        # 카테고리별 분리 저장 (기본)
        tester.save_results_separately(result)
        
        # 요약 정보만 별도 저장
        tester.save_summary_only(result)
        
        return result


if __name__ == "__main__":
    print("🚀 Crypto Detail API Comprehensive Tester - Separate Files Version")
    print("Usage: python crypto_detail_test.py [SYMBOL]")
    print("Example: python crypto_detail_test.py BTC")
    print()
    
    try:
        result = asyncio.run(main())
        print("\n🎉 Test completed successfully!")
        print("📁 Generated files:")
        print("  - crypto_concept_[SYMBOL]_[TIMESTAMP].json")
        print("  - crypto_ecosystem_[SYMBOL]_[TIMESTAMP].json") 
        print("  - crypto_investment_[SYMBOL]_[TIMESTAMP].json")
        print("  - crypto_summary_[SYMBOL]_[TIMESTAMP].json")
    except KeyboardInterrupt:
        print("\n⚠️ Test interrupted by user")
    except Exception as e:
        print(f"\n💥 Test failed with error: {e}")
        sys.exit(1)