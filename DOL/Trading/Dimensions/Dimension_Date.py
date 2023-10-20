from dataclasses import dataclass

from sqlalchemy import Column, String, Integer
from ..Base import Base


@dataclass
class Dimension_Date(Base):
    __tablename__ = 'Dimension_Date'
    __bind_key__ = 'Trading'
    __table_args__ = {'schema': 'Trading'}

    DateKey: int = Column(Integer, primary_key=True)
    DateString: str = Column(String, nullable=False)
    IsYearStart: int = Column(Integer, nullable=False)
    IsYearEnd: int = Column(Integer, nullable=False)
    Year: int = Column(Integer, nullable=False)
    IsLeapYear: int = Column(Integer, nullable=False)
    IsQuarterStart: int = Column(Integer, nullable=False)
    IsQuarterEnd: int = Column(Integer, nullable=False)
    Quarter: int = Column(Integer, nullable=False)
    Week: int = Column(Integer, nullable=False)
    IsMonthStart: int = Column(Integer, nullable=False)
    IsMonthEnd: int = Column(Integer, nullable=False)
    Month: int = Column(Integer, nullable=False)
    DayOfMonth: int = Column(Integer, nullable=False)
    Day: int = Column(Integer, nullable=False)
    DayOfYear: int = Column(Integer, nullable=False)
    IsWeekend: int = Column(Integer, nullable=False)
