from sqlalchemy.orm import Session
from sqlalchemy import desc, and_, func
from typing import List, Optional, Dict, Any
from datetime import date, datetime
from decimal import Decimal

from app.models.balance_sheet_model import BalanceSheet
from app.schemas.balance_sheet_schema import (
    FinancialRatio, FinancialHealthGrade, FinancialAnalysis, 
    FinancialTrend, BalanceSheetTrends, BalanceSheetStatistics
)

class BalanceSheetService:
    """재무상태표 비즈니스 로직 서비스"""
    
    def __init__(self, db: Session):
        self.db = db
    
    # ========== 기본 CRUD 메서드 ==========
    
    def get_balance_sheets(
        self, 
        skip: int = 0, 
        limit: int = 20,
        symbol: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> List[BalanceSheet]:
        """재무상태표 목록 조회"""
        query = self.db.query(BalanceSheet)
        
        # 필터링 조건 적용
        if symbol:
            query = query.filter(BalanceSheet.symbol == symbol.upper())
        if start_date:
            query = query.filter(BalanceSheet.fiscaldateending >= start_date)
        if end_date:
            query = query.filter(BalanceSheet.fiscaldateending <= end_date)
        
        # 최신순 정렬
        query = query.order_by(desc(BalanceSheet.fiscaldateending), BalanceSheet.symbol)
        
        return query.offset(skip).limit(limit).all()
    
    def get_balance_sheet_count(
        self,
        symbol: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> int:
        """재무상태표 총 개수"""
        query = self.db.query(BalanceSheet)
        
        if symbol:
            query = query.filter(BalanceSheet.symbol == symbol.upper())
        if start_date:
            query = query.filter(BalanceSheet.fiscaldateending >= start_date)
        if end_date:
            query = query.filter(BalanceSheet.fiscaldateending <= end_date)
        
        return query.count()
    
    def get_by_symbol(self, symbol: str) -> List[BalanceSheet]:
        """특정 기업의 모든 재무상태표"""
        return self.db.query(BalanceSheet)\
            .filter(BalanceSheet.symbol == symbol.upper())\
            .order_by(desc(BalanceSheet.fiscaldateending))\
            .all()
    
    def get_latest_by_symbol(self, symbol: str) -> Optional[BalanceSheet]:
        """특정 기업의 최신 재무상태표"""
        return self.db.query(BalanceSheet)\
            .filter(BalanceSheet.symbol == symbol.upper())\
            .order_by(desc(BalanceSheet.fiscaldateending))\
            .first()
    
    def get_by_period(self, period_date: date) -> List[BalanceSheet]:
        """특정 기간의 모든 기업 재무상태표"""
        return self.db.query(BalanceSheet)\
            .filter(BalanceSheet.fiscaldateending == period_date)\
            .order_by(BalanceSheet.symbol)\
            .all()
    
    # ========== 재무비율 계산 로직 ==========
    
    def calculate_financial_ratios(self, balance_sheet: BalanceSheet) -> Dict[str, FinancialRatio]:
        """재무비율 계산"""
        if not balance_sheet:
            return {}
        
        ratios = {}
        
        # 1. 유동성 비율
        ratios.update(self._calculate_liquidity_ratios(balance_sheet))
        
        # 2. 레버리지 비율 
        ratios.update(self._calculate_leverage_ratios(balance_sheet))
        
        # 3. 효율성 비율
        ratios.update(self._calculate_efficiency_ratios(balance_sheet))
        
        return ratios
    
    def _calculate_liquidity_ratios(self, bs: BalanceSheet) -> Dict[str, FinancialRatio]:
        """유동성 비율 계산"""
        ratios = {}
        
        # 유동비율 = 유동자산 / 유동부채
        if bs.totalcurrentassets and bs.totalcurrentliabilities and bs.totalcurrentliabilities != 0:
            current_ratio = float(bs.totalcurrentassets) / float(bs.totalcurrentliabilities)
            ratios['current_ratio'] = FinancialRatio(
                name="유동비율",
                value=round(current_ratio, 2),
                status=self._get_current_ratio_status(current_ratio),
                description=self._get_current_ratio_description(current_ratio),
                benchmark=1.5
            )
        
        # 당좌비율 = (유동자산 - 재고) / 유동부채
        if (bs.totalcurrentassets and bs.inventory and 
            bs.totalcurrentliabilities and bs.totalcurrentliabilities != 0):
            quick_ratio = (float(bs.totalcurrentassets) - float(bs.inventory or 0)) / float(bs.totalcurrentliabilities)
            ratios['quick_ratio'] = FinancialRatio(
                name="당좌비율",
                value=round(quick_ratio, 2),
                status=self._get_quick_ratio_status(quick_ratio),
                description=self._get_quick_ratio_description(quick_ratio),
                benchmark=1.0
            )
        
        # 현금비율 = 현금성자산 / 유동부채
        if (bs.cashandcashequivalentsatcarryingvalue and 
            bs.totalcurrentliabilities and bs.totalcurrentliabilities != 0):
            cash_ratio = float(bs.cashandcashequivalentsatcarryingvalue) / float(bs.totalcurrentliabilities)
            ratios['cash_ratio'] = FinancialRatio(
                name="현금비율",
                value=round(cash_ratio, 2),
                status=self._get_cash_ratio_status(cash_ratio),
                description=self._get_cash_ratio_description(cash_ratio),
                benchmark=0.2
            )
        
        return ratios
    
    def _calculate_leverage_ratios(self, bs: BalanceSheet) -> Dict[str, FinancialRatio]:
        """레버리지 비율 계산"""
        ratios = {}
        
        # 부채비율 = 총부채 / 총자산
        if bs.totalliabilities and bs.totalassets and bs.totalassets != 0:
            debt_to_asset = float(bs.totalliabilities) / float(bs.totalassets)
            ratios['debt_to_asset'] = FinancialRatio(
                name="부채비율",
                value=round(debt_to_asset, 3),
                status=self._get_debt_ratio_status(debt_to_asset),
                description=self._get_debt_ratio_description(debt_to_asset),
                benchmark=0.5
            )
        
        # 부채자기자본비율 = 총부채 / 자기자본
        if (bs.totalliabilities and bs.totalshareholderequity and 
            bs.totalshareholderequity != 0):
            debt_to_equity = float(bs.totalliabilities) / float(bs.totalshareholderequity)
            ratios['debt_to_equity'] = FinancialRatio(
                name="부채자기자본비율",
                value=round(debt_to_equity, 2),
                status=self._get_debt_equity_status(debt_to_equity),
                description=self._get_debt_equity_description(debt_to_equity),
                benchmark=1.0
            )
        
        # 자기자본비율 = 자기자본 / 총자산
        if bs.totalshareholderequity and bs.totalassets and bs.totalassets != 0:
            equity_ratio = float(bs.totalshareholderequity) / float(bs.totalassets)
            ratios['equity_ratio'] = FinancialRatio(
                name="자기자본비율",
                value=round(equity_ratio, 3),
                status=self._get_equity_ratio_status(equity_ratio),
                description=self._get_equity_ratio_description(equity_ratio),
                benchmark=0.5
            )
        
        return ratios
    
    def _calculate_efficiency_ratios(self, bs: BalanceSheet) -> Dict[str, FinancialRatio]:
        """효율성 비율 계산 (제한적 - 매출액 데이터 필요)"""
        ratios = {}
        
        # 재고회전율은 매출원가가 필요하므로 제외
        # 대신 재고자산 비중 계산
        if bs.inventory and bs.totalassets and bs.totalassets != 0:
            inventory_ratio = float(bs.inventory) / float(bs.totalassets)
            ratios['inventory_ratio'] = FinancialRatio(
                name="재고자산비율",
                value=round(inventory_ratio, 3),
                status=self._get_inventory_ratio_status(inventory_ratio),
                description=self._get_inventory_ratio_description(inventory_ratio),
                benchmark=0.1
            )
        
        return ratios
    
    # ========== 상태 판정 로직 ==========
    
    def _get_current_ratio_status(self, ratio: float) -> str:
        if ratio >= 2.0: return "우수"
        elif ratio >= 1.5: return "양호"
        elif ratio >= 1.0: return "보통"
        else: return "위험"
    
    def _get_current_ratio_description(self, ratio: float) -> str:
        if ratio >= 2.0: return f"유동비율 {ratio:.2f}로 단기 지급능력이 매우 우수합니다"
        elif ratio >= 1.5: return f"유동비율 {ratio:.2f}로 단기 지급능력이 양호합니다"
        elif ratio >= 1.0: return f"유동비율 {ratio:.2f}로 단기 지급능력이 보통 수준입니다"
        else: return f"유동비율 {ratio:.2f}로 단기 지급능력에 주의가 필요합니다"
    
    def _get_quick_ratio_status(self, ratio: float) -> str:
        if ratio >= 1.0: return "우수"
        elif ratio >= 0.8: return "양호"
        elif ratio >= 0.5: return "보통"
        else: return "위험"
    
    def _get_quick_ratio_description(self, ratio: float) -> str:
        if ratio >= 1.0: return f"당좌비율 {ratio:.2f}로 즉시 현금화 가능한 자산이 충분합니다"
        elif ratio >= 0.8: return f"당좌비율 {ratio:.2f}로 단기 유동성이 양호합니다"
        elif ratio >= 0.5: return f"당좌비율 {ratio:.2f}로 단기 유동성이 보통 수준입니다"
        else: return f"당좌비율 {ratio:.2f}로 즉시 현금화 가능한 자산이 부족합니다"
    
    def _get_cash_ratio_status(self, ratio: float) -> str:
        if ratio >= 0.3: return "우수"
        elif ratio >= 0.2: return "양호"
        elif ratio >= 0.1: return "보통"
        else: return "위험"
    
    def _get_cash_ratio_description(self, ratio: float) -> str:
        if ratio >= 0.3: return f"현금비율 {ratio:.2f}로 현금 보유량이 매우 충분합니다"
        elif ratio >= 0.2: return f"현금비율 {ratio:.2f}로 적정 수준의 현금을 보유하고 있습니다"
        elif ratio >= 0.1: return f"현금비율 {ratio:.2f}로 현금 보유량이 보통 수준입니다"
        else: return f"현금비율 {ratio:.2f}로 현금 보유량 증대가 필요합니다"
    
    def _get_debt_ratio_status(self, ratio: float) -> str:
        if ratio <= 0.3: return "우수"
        elif ratio <= 0.5: return "양호"
        elif ratio <= 0.7: return "보통"
        else: return "위험"
    
    def _get_debt_ratio_description(self, ratio: float) -> str:
        percent = ratio * 100
        if ratio <= 0.3: return f"부채비율 {percent:.1f}%로 재무구조가 매우 안정적입니다"
        elif ratio <= 0.5: return f"부채비율 {percent:.1f}%로 적정 수준의 레버리지를 유지하고 있습니다"
        elif ratio <= 0.7: return f"부채비율 {percent:.1f}%로 부채 수준이 다소 높습니다"
        else: return f"부채비율 {percent:.1f}%로 부채 관리에 주의가 필요합니다"
    
    def _get_debt_equity_status(self, ratio: float) -> str:
        if ratio <= 0.5: return "우수"
        elif ratio <= 1.0: return "양호"
        elif ratio <= 2.0: return "보통"
        else: return "위험"
    
    def _get_debt_equity_description(self, ratio: float) -> str:
        if ratio <= 0.5: return f"부채자기자본비율 {ratio:.2f}로 보수적인 재무구조를 유지하고 있습니다"
        elif ratio <= 1.0: return f"부채자기자본비율 {ratio:.2f}로 적정 수준의 재무레버리지입니다"
        elif ratio <= 2.0: return f"부채자기자본비율 {ratio:.2f}로 다소 공격적인 재무구조입니다"
        else: return f"부채자기자본비율 {ratio:.2f}로 높은 재무위험을 보이고 있습니다"
    
    def _get_equity_ratio_status(self, ratio: float) -> str:
        if ratio >= 0.7: return "우수"
        elif ratio >= 0.5: return "양호"
        elif ratio >= 0.3: return "보통"
        else: return "위험"
    
    def _get_equity_ratio_description(self, ratio: float) -> str:
        percent = ratio * 100
        if ratio >= 0.7: return f"자기자본비율 {percent:.1f}%로 재무안정성이 매우 높습니다"
        elif ratio >= 0.5: return f"자기자본비율 {percent:.1f}%로 양호한 재무구조를 보이고 있습니다"
        elif ratio >= 0.3: return f"자기자본비율 {percent:.1f}%로 보통 수준의 재무구조입니다"
        else: return f"자기자본비율 {percent:.1f}%로 자본 확충이 필요할 수 있습니다"
    
    def _get_inventory_ratio_status(self, ratio: float) -> str:
        if ratio <= 0.05: return "우수"
        elif ratio <= 0.1: return "양호"
        elif ratio <= 0.2: return "보통"
        else: return "위험"
    
    def _get_inventory_ratio_description(self, ratio: float) -> str:
        percent = ratio * 100
        if ratio <= 0.05: return f"재고자산비율 {percent:.1f}%로 효율적인 재고관리를 보이고 있습니다"
        elif ratio <= 0.1: return f"재고자산비율 {percent:.1f}%로 적정 수준의 재고를 유지하고 있습니다"
        elif ratio <= 0.2: return f"재고자산비율 {percent:.1f}%로 재고 수준이 다소 높습니다"
        else: return f"재고자산비율 {percent:.1f}%로 재고관리 개선이 필요할 수 있습니다"
    
    # ========== 재무건전성 등급 계산 ==========
    
    def calculate_financial_health_grade(self, ratios: Dict[str, FinancialRatio]) -> FinancialHealthGrade:
        """재무건전성 종합 등급 계산"""
        score = 0
        max_score = 0
        
        # 각 비율별 가중치 적용
        weights = {
            'current_ratio': 25,      # 유동성 25%
            'debt_to_asset': 25,      # 레버리지 25% 
            'equity_ratio': 20,       # 자본구조 20%
            'cash_ratio': 15,         # 현금성 15%
            'quick_ratio': 10,        # 당좌성 10%
            'debt_to_equity': 5       # 기타 5%
        }
        
        for ratio_name, weight in weights.items():
            if ratio_name in ratios:
                ratio = ratios[ratio_name]
                ratio_score = self._get_ratio_score(ratio.status)
                score += ratio_score * weight / 100
                max_score += weight
        
        # 점수 정규화 (0-100)
        if max_score > 0:
            final_score = int((score / max_score) * 100)
        else:
            final_score = 0
        
        # 등급 결정
        if final_score >= 85:
            grade, status = "A+", "매우 우수"
        elif final_score >= 75:
            grade, status = "A", "우수"
        elif final_score >= 65:
            grade, status = "B+", "양호"
        elif final_score >= 55:
            grade, status = "B", "보통"
        elif final_score >= 45:
            grade, status = "C+", "주의"
        elif final_score >= 35:
            grade, status = "C", "경고"
        else:
            grade, status = "D", "위험"
        
        return FinancialHealthGrade(
            grade=grade,
            score=final_score,
            status=status
        )
    
    def _get_ratio_score(self, status: str) -> int:
        """상태별 점수 매핑"""
        status_scores = {
            "우수": 100,
            "양호": 80,
            "보통": 60,
            "위험": 20
        }
        return status_scores.get(status, 0)
    
    # ========== 재무분석 종합 ==========
    
    def analyze_financial_health(self, symbol: str) -> Optional[FinancialAnalysis]:
        """재무건전성 종합 분석"""
        # 최신 재무상태표 조회
        latest_bs = self.get_latest_by_symbol(symbol)
        if not latest_bs:
            return None
        
        # 재무비율 계산
        ratios = self.calculate_financial_ratios(latest_bs)
        if not ratios:
            return None
        
        # 재무건전성 등급 계산
        health_grade = self.calculate_financial_health_grade(ratios)
        
        # 비율 그룹화
        key_ratios = {
            "liquidity": {
                "ratios": {k: v for k, v in ratios.items() if k in ['current_ratio', 'quick_ratio', 'cash_ratio']},
                "summary": self._get_liquidity_summary(ratios)
            },
            "leverage": {
                "ratios": {k: v for k, v in ratios.items() if k in ['debt_to_asset', 'debt_to_equity', 'equity_ratio']},
                "summary": self._get_leverage_summary(ratios)
            },
            "efficiency": {
                "ratios": {k: v for k, v in ratios.items() if k in ['inventory_ratio']},
                "summary": self._get_efficiency_summary(ratios)
            }
        }
        
        # 해석 생성
        interpretation = self._generate_interpretation(symbol, latest_bs, ratios, health_grade)
        
        return FinancialAnalysis(
            symbol=symbol,
            analysis_date=latest_bs.fiscaldateending,
            financial_health=health_grade,
            key_ratios=key_ratios,
            interpretation=interpretation
        )
    
    def _get_liquidity_summary(self, ratios: Dict[str, FinancialRatio]) -> str:
        """유동성 요약"""
        if 'current_ratio' in ratios:
            status = ratios['current_ratio'].status
            value = ratios['current_ratio'].value
            if status == "우수":
                return f"유동비율 {value:.2f}로 단기 지급능력이 매우 우수합니다"
            elif status == "양호":
                return f"유동비율 {value:.2f}로 단기 지급능력이 양호합니다"
            elif status == "보통":
                return f"유동비율 {value:.2f}로 단기 지급능력이 보통 수준입니다"
            else:
                return f"유동비율 {value:.2f}로 단기 지급능력 개선이 필요합니다"
        return "유동성 분석 데이터가 부족합니다"
    
    def _get_leverage_summary(self, ratios: Dict[str, FinancialRatio]) -> str:
        """레버리지 요약"""
        if 'debt_to_asset' in ratios:
            status = ratios['debt_to_asset'].status
            value = ratios['debt_to_asset'].value * 100
            if status == "우수":
                return f"부채비율 {value:.1f}%로 재무구조가 매우 안정적입니다"
            elif status == "양호":
                return f"부채비율 {value:.1f}%로 적정 수준의 레버리지를 유지하고 있습니다"
            elif status == "보통":
                return f"부채비율 {value:.1f}%로 부채 수준이 다소 높습니다"
            else:
                return f"부채비율 {value:.1f}%로 부채 관리에 주의가 필요합니다"
        return "레버리지 분석 데이터가 부족합니다"
    
    def _get_efficiency_summary(self, ratios: Dict[str, FinancialRatio]) -> str:
        """효율성 요약"""
        if 'inventory_ratio' in ratios:
            status = ratios['inventory_ratio'].status
            value = ratios['inventory_ratio'].value * 100
            return f"재고자산비율 {value:.1f}%로 {status} 수준의 재고관리를 보이고 있습니다"
        return "효율성 분석을 위한 추가 데이터가 필요합니다"
    
    def _generate_interpretation(
        self, 
        symbol: str, 
        balance_sheet: BalanceSheet, 
        ratios: Dict[str, FinancialRatio], 
        health_grade: FinancialHealthGrade
    ) -> Dict[str, Any]:
        """재무 해석 생성"""
        
        strengths = []
        concerns = []
        
        # 강점 분석
        for ratio_name, ratio in ratios.items():
            if ratio.status in ["우수", "양호"]:
                strengths.append(ratio.description)
        
        # 우려사항 분석
        for ratio_name, ratio in ratios.items():
            if ratio.status in ["위험", "보통"]:
                concerns.append(ratio.description.replace("매우 우수합니다", "개선이 필요합니다"))
        
        # 총자산 규모 분석
        if balance_sheet.totalassets:
            assets_b = float(balance_sheet.totalassets) / 1_000_000_000
            if assets_b >= 100:
                strengths.append(f"총자산 {assets_b:.0f}십억달러 규모의 대형 기업으로 시장 지위가 안정적입니다")
            elif assets_b >= 10:
                strengths.append(f"총자산 {assets_b:.1f}십억달러 규모의 중견 기업으로 성장 잠재력이 있습니다")
        
        # 현금 보유량 분석
        if balance_sheet.cashandcashequivalentsatcarryingvalue:
            cash_b = float(balance_sheet.cashandcashequivalentsatcarryingvalue) / 1_000_000_000
            if cash_b >= 10:
                strengths.append(f"현금성 자산 {cash_b:.0f}십억달러로 충분한 유동성을 확보하고 있습니다")
        
        # 종합 요약 생성
        if health_grade.grade.startswith("A"):
            summary = f"{symbol}는 {health_grade.status}한 재무구조를 보유하고 있습니다. 안정적인 투자 대상으로 평가됩니다."
        elif health_grade.grade.startswith("B"):
            summary = f"{symbol}는 {health_grade.status}한 재무상태를 보이고 있습니다. 전반적으로 양호하나 일부 지표 모니터링이 필요합니다."
        elif health_grade.grade.startswith("C"):
            summary = f"{symbol}는 {health_grade.status} 수준의 재무상태입니다. 재무구조 개선을 위한 노력이 필요합니다."
        else:
            summary = f"{symbol}는 재무적으로 {health_grade.status}한 상태입니다. 투자 시 신중한 검토가 필요합니다."
        
        # 투자 추천
        if health_grade.score >= 75:
            recommendation = "재무안정성이 우수하여 장기 투자에 적합합니다"
        elif health_grade.score >= 55:
            recommendation = "전반적으로 양호하나 재무지표 변화 모니터링을 권장합니다"
        elif health_grade.score >= 35:
            recommendation = "재무위험이 존재하므로 단기 투자 관점에서 신중한 접근이 필요합니다"
        else:
            recommendation = "높은 재무위험으로 투자 시 각별한 주의가 필요합니다"
        
        return {
            "overall_summary": summary,
            "strengths": strengths[:3],  # 최대 3개
            "concerns": concerns[:3],    # 최대 3개
            "recommendation": recommendation
        }
    
    # ========== 시계열 트렌드 분석 ==========
    
    def get_financial_trends(self, symbol: str, periods: int = 4) -> Optional[BalanceSheetTrends]:
        """재무 트렌드 분석 (최근 N분기)"""
        balance_sheets = self.db.query(BalanceSheet)\
            .filter(BalanceSheet.symbol == symbol.upper())\
            .order_by(desc(BalanceSheet.fiscaldateending))\
            .limit(periods)\
            .all()
        
        if len(balance_sheets) < 2:
            return None
        
        trends = []
        current = balance_sheets[0]
        previous = balance_sheets[1]
        
        # 주요 지표별 트렌드 분석
        trend_metrics = [
            ('총자산', 'totalassets'),
            ('총부채', 'totalliabilities'),
            ('자기자본', 'totalshareholderequity'),
            ('유동자산', 'totalcurrentassets'),
            ('현금성자산', 'cashandcashequivalentsatcarryingvalue')
        ]
        
        for metric_name, field_name in trend_metrics:
            trend = self._calculate_trend(metric_name, current, previous, field_name)
            if trend:
                trends.append(trend)
        
        # 트렌드 요약
        summary = self._generate_trend_summary(trends)
        
        return BalanceSheetTrends(
            symbol=symbol,
            period_count=len(balance_sheets),
            trends=trends,
            summary=summary
        )
    
    def _calculate_trend(
        self, 
        metric_name: str, 
        current: BalanceSheet, 
        previous: BalanceSheet, 
        field_name: str
    ) -> Optional[FinancialTrend]:
        """개별 지표 트렌드 계산"""
        current_value = getattr(current, field_name)
        previous_value = getattr(previous, field_name)
        
        if not current_value or not previous_value:
            return None
        
        current_val = float(current_value)
        previous_val = float(previous_value)
        
        change_amount = current_val - previous_val
        change_rate = (change_amount / previous_val) * 100 if previous_val != 0 else 0
        
        # 트렌드 방향 결정
        if abs(change_rate) < 1:
            direction = "유지"
        elif change_rate > 0:
            direction = "증가"
        else:
            direction = "감소"
        
        return FinancialTrend(
            metric=metric_name,
            current_value=current_val / 1_000_000_000,  # 십억 단위
            previous_value=previous_val / 1_000_000_000,
            change_amount=change_amount / 1_000_000_000,
            change_rate=round(change_rate, 2),
            trend_direction=direction
        )
    
    def _generate_trend_summary(self, trends: List[FinancialTrend]) -> str:
        """트렌드 요약 생성"""
        if not trends:
            return "트렌드 분석 데이터가 부족합니다"
        
        increasing = [t for t in trends if t.trend_direction == "증가"]
        decreasing = [t for t in trends if t.trend_direction == "감소"]
        
        if len(increasing) > len(decreasing):
            return f"전분기 대비 {len(increasing)}개 지표 증가, {len(decreasing)}개 지표 감소로 전반적인 성장세를 보이고 있습니다"
        elif len(decreasing) > len(increasing):
            return f"전분기 대비 {len(decreasing)}개 지표 감소, {len(increasing)}개 지표 증가로 일부 조정 국면에 있습니다"
        else:
            return "전분기 대비 혼재된 트렌드를 보이며 안정적인 상태를 유지하고 있습니다"
    
    # ========== 통계 정보 ==========
    
    def get_statistics(self) -> BalanceSheetStatistics:
        """재무상태표 전체 통계"""
        # 기본 통계
        total_companies = self.db.query(BalanceSheet.symbol).distinct().count()
        latest_period = self.db.query(func.max(BalanceSheet.fiscaldateending)).scalar()
        
        # 평균 총자산 계산 (최신 분기 기준)
        avg_assets_result = self.db.query(func.avg(BalanceSheet.totalassets))\
            .filter(BalanceSheet.fiscaldateending == latest_period)\
            .scalar()
        avg_total_assets = float(avg_assets_result) / 1_000_000_000 if avg_assets_result else 0
        
        # 중간값 부채비율 계산
        debt_ratios = self.db.query(
            (BalanceSheet.totalliabilities / BalanceSheet.totalassets).label('debt_ratio')
        ).filter(
            and_(
                BalanceSheet.fiscaldateending == latest_period,
                BalanceSheet.totalassets > 0,
                BalanceSheet.totalliabilities.isnot(None)
            )
        ).all()
        
        if debt_ratios:
            ratios_list = [float(r.debt_ratio) for r in debt_ratios if r.debt_ratio]
            ratios_list.sort()
            median_debt_ratio = ratios_list[len(ratios_list) // 2] if ratios_list else 0
        else:
            median_debt_ratio = 0
        
        # 규모별 분포
        size_distribution = self._get_size_distribution(latest_period)
        
        return BalanceSheetStatistics(
            total_companies=total_companies,
            latest_period=latest_period,
            average_total_assets=round(avg_total_assets, 2),
            median_debt_ratio=round(median_debt_ratio, 3),
            size_distribution=size_distribution
        )
    
    def _get_size_distribution(self, latest_period: date) -> Dict[str, int]:
        """규모별 기업 분포"""
        companies = self.db.query(BalanceSheet.totalassets)\
            .filter(
                and_(
                    BalanceSheet.fiscaldateending == latest_period,
                    BalanceSheet.totalassets.isnot(None)
                )
            ).all()
        
        large_cap = 0  # 100B+
        mid_cap = 0    # 10B-100B
        small_cap = 0  # <10B
        
        for company in companies:
            if company.totalassets:
                assets_b = float(company.totalassets) / 1_000_000_000
                if assets_b >= 100:
                    large_cap += 1
                elif assets_b >= 10:
                    mid_cap += 1
                else:
                    small_cap += 1
        
        return {
            "large_cap": large_cap,
            "mid_cap": mid_cap,
            "small_cap": small_cap
        }