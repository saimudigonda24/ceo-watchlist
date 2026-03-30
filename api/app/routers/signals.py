from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import insert, select
from ..db import get_session
from .. import models, schemas

router = APIRouter(prefix="/signals", tags=["signals"])

@router.post("/", response_model=schemas.SignalOut)
async def upsert_signal(payload: schemas.SignalIn, session: AsyncSession = Depends(get_session)):
    # simple upsert via ON CONFLICT
    stmt = insert(models.Signal).values(**payload.model_dump()).on_conflict_do_update(
        index_elements=[models.Signal.company_id, models.Signal.ts, models.Signal.name],
        set_={"value": payload.value, "meta": payload.meta},
    ).returning(models.Signal)
    row = (await session.execute(stmt)).mappings().one()
    await session.commit()
    return schemas.SignalOut(**row)

@router.get("/", response_model=list[schemas.SignalOut])
async def list_signals(company_id: int, name: str | None = None, session: AsyncSession = Depends(get_session)):
    stmt = select(models.Signal).where(models.Signal.company_id == company_id)
    if name:
        stmt = stmt.where(models.Signal.name == name)
    rows = (await session.execute(stmt)).scalars().all()
    return [schemas.SignalOut(**r.__dict__) for r in rows]