from sqlalchemy import Column, String, Date, Numeric, DateTime, text
from sqlalchemy.sql import func
from app.models.base import BaseModel 

class BalanceSheet(BaseModel):
    """
    재무상태표(대차대조표) 모델
    - 각 기업의 분기별/연도별 재무상태를 나타냄
    - Primary Key: (symbol, fiscaldateending)
    """
    __tablename__ = "balance_sheet"

    # Primary Key 구성 요소
    symbol = Column(String, primary_key=True, nullable=False, comment="기업 심볼 (예: AAPL)")
    fiscaldateending = Column(Date, primary_key=True, nullable=False, comment="회계연도 종료일")
    
    # 기본 정보
    reportedcurrency = Column(String, comment="보고 통화 (USD)")
    
    # === 자산 (Assets) ===
    totalassets = Column(Numeric, comment="총자산")
    totalcurrentassets = Column(Numeric, comment="유동자산")
    totalnoncurrentassets = Column(Numeric, comment="비유동자산")
    
    # 현금 및 현금성 자산
    cashandcashequivalentsatcarryingvalue = Column(Numeric, comment="현금 및 현금성자산")
    cashandshortterminvestments = Column(Numeric, comment="현금 및 단기투자")
    shortterminvestments = Column(Numeric, comment="단기투자")
    
    # 운영 자산
    inventory = Column(Numeric, comment="재고자산")
    currentnetreceivables = Column(Numeric, comment="매출채권")
    othercurrentassets = Column(Numeric, comment="기타 유동자산")
    
    # 고정 자산
    propertyplantequipment = Column(Numeric, comment="유형자산")
    accumulateddepreciationamortizationppe = Column(Numeric, comment="감가상각누계액")
    intangibleassets = Column(Numeric, comment="무형자산")
    intangibleassetsexcludinggoodwill = Column(Numeric, comment="영업권 제외 무형자산")
    goodwill = Column(Numeric, comment="영업권")
    investments = Column(Numeric, comment="투자자산")
    longterminvestments = Column(Numeric, comment="장기투자")
    othernoncurrentassets = Column(Numeric, comment="기타 비유동자산")
    
    # === 부채 (Liabilities) ===
    totalliabilities = Column(Numeric, comment="총부채")
    totalcurrentliabilities = Column(Numeric, comment="유동부채")
    totalnoncurrentliabilities = Column(Numeric, comment="비유동부채")
    
    # 운영 부채
    currentaccountspayable = Column(Numeric, comment="매입채무")
    deferredrevenue = Column(Numeric, comment="이연수익")
    othercurrentliabilities = Column(Numeric, comment="기타 유동부채")
    othernoncurrentliabilities = Column(Numeric, comment="기타 비유동부채")
    
    # 부채 세부
    currentdebt = Column(Numeric, comment="유동부채")
    shorttermdebt = Column(Numeric, comment="단기부채")
    longtermdebt = Column(Numeric, comment="장기부채")
    currentlongtermdebt = Column(Numeric, comment="유동장기부채")
    longtermdebtnoncurrent = Column(Numeric, comment="비유동장기부채")
    shortlongtermdebttotal = Column(Numeric, comment="단기+장기부채 총합")
    capitalleaseobligations = Column(Numeric, comment="자본리스의무")
    
    # === 자본 (Equity) ===
    totalshareholderequity = Column(Numeric, comment="총 주주자본")
    commonstock = Column(Numeric, comment="보통주")
    treasurystock = Column(Numeric, comment="자기주식")
    retainedearnings = Column(Numeric, comment="이익잉여금")
    commonstocksharesoutstanding = Column(Numeric, comment="발행주식수")
    
    # 메타데이터
    fetched_at = Column(DateTime(timezone=False), server_default=func.now(), comment="데이터 수집 시간")

    def __repr__(self):
        return f"<BalanceSheet(symbol='{self.symbol}', fiscal_date='{self.fiscaldateending}', total_assets={self.totalassets})>"
    
    @property
    def fiscal_date_str(self):
        """회계연도 종료일을 문자열로 반환"""
        return self.fiscaldateending.strftime('%Y-%m-%d') if self.fiscaldateending else None
    
    @property
    def total_assets_billions(self):
        """총자산을 십억 단위로 반환"""
        return round(float(self.totalassets) / 1_000_000_000, 2) if self.totalassets else None
    
    @property
    def market_cap_category(self):
        """시가총액 카테고리 추정 (총자산 기준)"""
        if not self.totalassets:
            return "Unknown"
        
        assets_b = float(self.totalassets) / 1_000_000_000
        if assets_b >= 100:
            return "Large Cap"
        elif assets_b >= 10:
            return "Mid Cap"
        else:
            return "Small Cap"