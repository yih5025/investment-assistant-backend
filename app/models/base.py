from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, DateTime, Integer
from datetime import datetime

# 모든 SQLAlchemy 모델이 상속받을 기본 클래스
Base = declarative_base()

class TimestampMixin:
    """
    생성일시/수정일시를 자동으로 관리하는 믹스인 클래스
    
    새로운 테이블을 만들 때 이 클래스를 상속받으면
    created_at, updated_at 컬럼이 자동으로 추가됩니다.
    
    사용 예시:
        class MyModel(Base, TimestampMixin):
            __tablename__ = "my_table"
            id = Column(Integer, primary_key=True)
            # created_at, updated_at는 자동으로 추가됨
    """
    created_at = Column(
        DateTime, 
        default=datetime.utcnow,
        nullable=False,
        comment="레코드 생성 시간"
    )
    updated_at = Column(
        DateTime, 
        default=datetime.utcnow, 
        onupdate=datetime.utcnow,
        nullable=False,
        comment="레코드 최종 수정 시간"
    )

class BaseModel(Base):
    """
    추상 기본 모델 클래스
    
    모든 테이블 모델이 상속받을 수 있는 기본 클래스입니다.
    공통 메서드들을 제공합니다.
    
    주의: __abstract__ = True이므로 실제 테이블은 생성되지 않습니다.
    """
    __abstract__ = True  # 이 클래스 자체는 테이블로 생성되지 않음
    
    def to_dict(self):
        """
        SQLAlchemy 객체를 딕셔너리로 변환
        
        JSON 직렬화나 디버깅에 유용합니다.
        
        Returns:
            dict: 객체의 모든 컬럼을 키-값 쌍으로 변환한 딕셔너리
        """
        return {
            column.name: getattr(self, column.name) 
            for column in self.__table__.columns
        }
    
    def __repr__(self):
        """
        객체의 문자열 표현
        
        디버깅할 때 객체 정보를 보기 쉽게 표시합니다.
        
        Returns:
            str: 클래스명과 주요 속성이 포함된 문자열
        """
        class_name = self.__class__.__name__
        
        # 기본키가 있으면 표시
        if hasattr(self, 'id'):
            return f"<{class_name}(id={self.id})>"
        
        # 기본키가 없으면 첫 번째 컬럼 값 표시
        if self.__table__.columns:
            first_col = list(self.__table__.columns)[0]
            first_val = getattr(self, first_col.name)
            return f"<{class_name}({first_col.name}={first_val})>"
        
        return f"<{class_name}()>"