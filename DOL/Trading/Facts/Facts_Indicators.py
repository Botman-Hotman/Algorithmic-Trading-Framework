from dataclasses import dataclass

from sqlalchemy import Column, String, Integer, ForeignKey, Float, BigInteger
from sqlalchemy.orm import relationship

from ..Base import Base


@dataclass
class Facts_Indicators(Base):
    __tablename__ = 'Facts_Indicators'
    __bind_key__ = 'Trading'
    __table_args__ = {'schema': 'Trading'}

    id: str = Column(String, primary_key=True)
    DateTimeKey: int = Column(BigInteger, nullable=False)
    DateKey: int = Column(BigInteger, ForeignKey('Trading.Dimension_Date.DateKey'), nullable=False)
    TimeKey: int = Column(BigInteger, ForeignKey('Trading.Dimension_Time.TimeKey'), nullable=False)
    Date = relationship("Dimension_Date")
    Time = relationship("Dimension_Time")

    GranularityKey: int = Column(Integer, ForeignKey('Trading.Dimension_Granularity.GranularityKey'), nullable=False)
    Granularity = relationship("Dimension_Granularity")

    InstrumentKey: int = Column(Integer, ForeignKey('Trading.Dimension_Instruments.InstrumentKey'), nullable=False)
    Instrument = relationship("Dimension_Instruments")

    IndicatorKey: int = Column(Integer, ForeignKey('Trading.Dimension_Indicators.IndicatorKey'), nullable=False)
    Indicator = relationship("Dimension_Indicators")

    Open: float = Column(Float)
    High: float = Column(Float)
    Low: float = Column(Float)
    Close: float = Column(Float)
    Volume: int = Column(Integer)





