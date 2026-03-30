# api/app/scripts/load_13f_tsv_zip.py
import asyncio
import csv
import io
import os
import sys
import zipfile
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import DBAPIError
import asyncpg

# --- Use your app's existing DB/session/models ---
from app.db import SessionLocal as async_session_maker
from app import models


def parse_date(dt: str):
    # SUBMISSION.tsv date format like "31-DEC-2024"
    return datetime.strptime(dt, "%d-%b-%Y").date()


async def retry_deadlock(coro, *args, **kwargs):
    """
    Retry a DB operation a few times when Postgres reports a deadlock.
    Use like: await retry_deadlock(session.execute, stmt)
              await retry_deadlock(session.flush)
              await retry_deadlock(session.commit)
    """
    backoffs = [0.2, 0.5, 1.0, 2.0]
    for i, delay in enumerate(backoffs):
        try:
            return await coro(*args, **kwargs)
        except DBAPIError as e:
            msg = str(getattr(e, "orig", e)).lower()
            if "deadlock detected" in msg:
                if i == len(backoffs) - 1:
                    raise
                await asyncio.sleep(delay)
                continue
            raise
        except asyncpg.exceptions.DeadlockDetectedError:
            if i == len(backoffs) - 1:
                raise
            await asyncio.sleep(delay)
            continue


async def upsert_company(session: AsyncSession, ticker: str, name: str) -> int:
    """
    Atomic upsert to avoid deadlocks:
      - insert (ticker, name)
      - on conflict (ticker) update name only if it changed
      - return id in one round trip
    """
    stmt = (
        pg_insert(models.Company)
        .values(ticker=ticker, name=name or ticker)
        .on_conflict_do_update(
            index_elements=[models.Company.ticker],
            set_={"name": (name or ticker)},
            where=models.Company.name.is_distinct_from(name or ticker),
        )
        .returning(models.Company.id)
    )
    res = await retry_deadlock(session.execute, stmt)
    company_id = res.scalar_one_or_none()
    if company_id is not None:
        return company_id

    # (Very unlikely) fallback: select id if no row returned
    res = await session.execute(select(models.Company.id).where(models.Company.ticker == ticker))
    return res.scalar_one()


async def upsert_investor(session: AsyncSession, filer_cik: str, name: str | None) -> None:
    res = await session.execute(select(models.Investor).where(models.Investor.filer_cik == filer_cik))
    row = res.scalar_one_or_none()
    if row:
        if name and row.name != name:
            row.name = name
            await retry_deadlock(session.flush)
        return
    session.add(models.Investor(filer_cik=filer_cik, name=name or f"CIK {filer_cik}"))
    await retry_deadlock(session.flush)


async def insert_holding(
    session: AsyncSession,
    filer_cik: str,
    company_id: int,
    period_end,
    shares: int,
    value_usd: int,
):
    insert_stmt = pg_insert(models.FundHolding).values(
        filer_cik=filer_cik,
        company_id=company_id,
        period_end=period_end,
        shares=shares,
        value_usd=value_usd,
    )

    upsert_stmt = insert_stmt.on_conflict_do_update(
        index_elements=["filer_cik", "company_id", "period_end"],
        set_={
            "shares": insert_stmt.excluded.shares,
            "value_usd": insert_stmt.excluded.value_usd,
        },
    )

    await retry_deadlock(session.execute, upsert_stmt)


def normalize_ticker_from_row(issuer: str, cusip: str) -> str:
    """
    Bulk 13F TSVs don’t include tickers. Use CUSIP as a stable key for Company.ticker.
    Fallback: a slug from NAMEOFISSUER.
    """
    if cusip and cusip.strip():
        return cusip.strip()[:10]
    slug = "".join(ch for ch in (issuer or "").upper() if ch.isalnum())[:10]
    return slug or "UNKNOWN"


async def main(path: str):
    if not os.path.exists(path):
        print(f"File not found: {path}", file=sys.stderr)
        sys.exit(1)

    with zipfile.ZipFile(path, "r") as z:
        # Ensure required tables exist
        required = ["SUBMISSION.tsv", "INFOTABLE.tsv"]
        for req in required:
            if req not in z.namelist():
                print(f"ZIP missing {req}", file=sys.stderr)
                sys.exit(2)

        # Build accession -> (cik, period_end) map
        with z.open("SUBMISSION.tsv") as f:
            sub_reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8", errors="ignore"), delimiter="\t")
            acc_map = {}
            for row in sub_reader:
                acc = row["ACCESSION_NUMBER"]
                cik = row["CIK"].lstrip("0")
                period = parse_date(row["PERIODOFREPORT"])
                acc_map[acc] = (cik, period)

        # Optional: filer names by accession (from COVERPAGE)
        filer_name_by_acc = {}
        if "COVERPAGE.tsv" in z.namelist():
            with z.open("COVERPAGE.tsv") as f:
                cov_reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8", errors="ignore"), delimiter="\t")
                for row in cov_reader:
                    acc = row["ACCESSION_NUMBER"]
                    name = row.get("FILINGMANAGER_NAME") or ""
                    filer_name_by_acc[acc] = name

        inserted = 0
        skipped = 0

        async with async_session_maker() as session:
            batch = 0
            with z.open("INFOTABLE.tsv") as f:
                info_reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8", errors="ignore"), delimiter="\t")
                for row in info_reader:
                    acc = row["ACCESSION_NUMBER"]
                    if acc not in acc_map:
                        skipped += 1
                        continue

                    cik, period_end = acc_map[acc]
                    issuer = (row.get("NAMEOFISSUER") or "").strip()
                    cusip = (row.get("CUSIP") or "").strip()
                    value_str = (row.get("VALUE") or "0").replace(",", "")
                    shares_str = (row.get("SSHPRNAMT") or "0").replace(",", "")

                    # Parse integers defensively
                    try:
                        value = int(float(value_str))
                    except Exception:
                        value = 0
                    try:
                        shares = int(float(shares_str))
                    except Exception:
                        shares = 0

                    await upsert_investor(session, cik, filer_name_by_acc.get(acc))

                    ticker = normalize_ticker_from_row(issuer or "UNKNOWN", cusip)
                    company_id = await upsert_company(session, ticker=ticker, name=issuer or ticker)

                    await insert_holding(
                        session,
                        filer_cik=cik,
                        company_id=company_id,
                        period_end=period_end,
                        shares=shares,
                        value_usd=value,  # TSV appears to be dollars
                    )

                    inserted += 1
                    batch += 1
                    if batch >= 500:
                        await retry_deadlock(session.commit)
                        batch = 0

            # Final commit
            await retry_deadlock(session.commit)

        print(f"Inserted holdings: {inserted}, skipped: {skipped}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python -m app.scripts.load_13f_tsv_zip /data/13f/01dec2024-28feb2025_form13f.zip")
        sys.exit(64)
    asyncio.run(main(sys.argv[1]))