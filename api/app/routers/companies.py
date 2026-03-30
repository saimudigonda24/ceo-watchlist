from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from ..db import get_session
from .. import models, schemas

router = APIRouter(prefix="/companies", tags=["companies"])

@router.get("/", response_model=list[schemas.CompanyOut])
async def list_companies(q: str | None = None, session: AsyncSession = Depends(get_session)):
    stmt = select(models.Company)
    if q:
        stmt = stmt.where(models.Company.ticker.ilike(f"%{q}%") | models.Company.name.ilike(f"%{q}%"))
    rows = (await session.execute(stmt)).scalars().all()
    return [schemas.CompanyOut(**row.__dict__) for row in rows]

@router.post("/", response_model=schemas.CompanyOut)
async def create_company(payload: schemas.CompanyIn, session: AsyncSession = Depends(get_session)):
    c = models.Company(**payload.model_dump())
    session.add(c)
    await session.commit()
    await session.refresh(c)
    return schemas.CompanyOut(**c.__dict__)

@router.get("/{ticker}", response_model=schemas.CompanyOut)
async def get_company(ticker: str, session: AsyncSession = Depends(get_session)):
    stmt = select(models.Company).where(models.Company.ticker == ticker)
    row = (await session.execute(stmt)).scalar_one_or_none()
    if not row:
        raise HTTPException(404, "Not found")
    return schemas.CompanyOut(**row.__dict__)