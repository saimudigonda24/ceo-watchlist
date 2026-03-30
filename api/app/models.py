from sqlalchemy import BigInteger, Text, JSON, ARRAY, ForeignKey, Numeric
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.dialects.postgresql import TIMESTAMP
from .db import Base
from sqlalchemy import Date
from datetime import date


class Company(Base):
    __tablename__ = "companies"
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    ticker: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    cik: Mapped[str | None] = mapped_column(Text)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    sector: Mapped[str | None] = mapped_column(Text)
    index_memberships: Mapped[list[str]] = mapped_column(ARRAY(Text), default=list)

class Price(Base):
    __tablename__ = "prices"
    company_id: Mapped[int] = mapped_column(ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True)
    ts: Mapped[str] = mapped_column(TIMESTAMP(timezone=True), primary_key=True)
    open: Mapped[float | None] = mapped_column(Numeric(18,6))
    high: Mapped[float | None] = mapped_column(Numeric(18,6))
    low: Mapped[float | None] = mapped_column(Numeric(18,6))
    close: Mapped[float | None] = mapped_column(Numeric(18,6))
    volume: Mapped[int | None] = mapped_column(BigInteger)

class Signal(Base):
    __tablename__ = "signals"
    company_id: Mapped[int] = mapped_column(ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True)
    ts: Mapped[str] = mapped_column(TIMESTAMP(timezone=True), primary_key=True)
    name: Mapped[str] = mapped_column(Text, primary_key=True)
    value: Mapped[float]
    meta: Mapped[dict] = mapped_column(JSON, default=dict)

class Investor(Base):
    __tablename__ = "investors"
    filer_cik: Mapped[str] = mapped_column(Text, primary_key=True)
    name: Mapped[str] = mapped_column(Text)
    style_tags: Mapped[list[str]] = mapped_column(ARRAY(Text), default=list)

class FundHolding(Base):
    __tablename__ = "fund_holdings"
    filer_cik: Mapped[str] = mapped_column(Text, ForeignKey("investors.filer_cik", ondelete="CASCADE"), primary_key=True)
    company_id: Mapped[int] = mapped_column(ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True)
    period_end: Mapped[date] = mapped_column(Date, primary_key=True)
    shares: Mapped[float | None]
    value_usd: Mapped[float | None]

