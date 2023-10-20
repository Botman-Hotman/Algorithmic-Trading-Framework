from dataclasses import dataclass

from sqlalchemy import Column, Integer, String, Float, ForeignKey, BigInteger
from sqlalchemy.orm import relationship


from ..Base import Base

@dataclass
class Facts_InstrumentsDataAligned(Base):
    __tablename__ = 'Facts_InstrumentsDataAligned'
    __bind_key__ = 'Trading'
    __table_args__ = {'schema': 'Trading'}

    id: str = Column(String, primary_key=True)
    DateTimeKey: int = Column(BigInteger, nullable=False)
    DateKey: int = Column(BigInteger, ForeignKey('Trading.Dimension_Date.DateKey'), nullable=False)
    Date = relationship("Dimension_Date", foreign_keys=[DateKey])

    TimeKey: int = Column(BigInteger, ForeignKey('Trading.Dimension_Time.TimeKey'), nullable=False)
    Time = relationship("Dimension_Time", foreign_keys=[TimeKey])

    GranularityKey: int = Column(Integer, ForeignKey('Trading.Dimension_Granularity.GranularityKey'), nullable=False)
    Granularity = relationship("Dimension_Granularity", foreign_keys=[GranularityKey])

    InstrumentKey: int = Column(Integer, ForeignKey('Trading.Dimension_Instruments.InstrumentKey'), nullable=False)
    Instrument = relationship("Dimension_Instruments", foreign_keys=[InstrumentKey])

    PriceTypeKey: int = Column(Integer, ForeignKey('Trading.Dimension_PriceType.PriceTypeKey'), nullable=False)
    Price = relationship("Dimension_PriceType", foreign_keys=[PriceTypeKey])

    Open: float = Column(Float)
    High: float = Column(Float)
    Low: float = Column(Float)
    Close: float = Column(Float, nullable=False)
    Volume: int = Column(BigInteger)





