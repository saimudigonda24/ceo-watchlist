import os, re
from datetime import datetime, date
import httpx, xmltodict
from httpx import HTTPStatusError, RequestError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert as pg_insert
from ..db import SessionLocal
from .. import models
import re

DIR_HREF_RE = re.compile(r'href="([^"]+)"', re.IGNORECASE)

async def list_dir_candidates(client: httpx.AsyncClient, base: str) -> list[str]:
    """
    Fetch the HTML index page for a filing directory and return file names found in hrefs.
    Works even when index.json is missing.
    """
    # ensure trailing slash
    url = base if base.endswith("/") else base + "/"
    try:
        r = await _get(client, url)
        html = r.text
        names = []
        for href in DIR_HREF_RE.findall(html):
            # only files within same directory
            if "/" in href.strip("/"):
                continue
            names.append(href)
        return list(dict.fromkeys(names))  # dedupe, keep order
    except Exception:
        return []

SEC_BASE = "https://data.sec.gov"
UA = os.getenv("SEC_USER_AGENT", "youremail@example.com")
HEADERS = {"User-Agent": UA, "Accept-Encoding": "gzip, deflate"}

def cik_normalize(cik: str) -> str:
    return re.sub(r"\D", "", cik or "").zfill(10)

async def _get(client: httpx.AsyncClient, url: str) -> httpx.Response:
    r = await client.get(url, headers=HEADERS, timeout=60)
    r.raise_for_status()
    return r

async def get_company_submissions(client: httpx.AsyncClient, cik: str):
    cik10 = cik_normalize(cik)
    r = await _get(client, f"{SEC_BASE}/submissions/CIK{cik10}.json")
    return r.json()

def _first_key(d: dict, *keys):
    for k in keys:
        if k in d:
            return d[k]
    return None

async def latest_13f_meta(client: httpx.AsyncClient, cik: str, max_filings: int = 1):
    """
    Builds best-guess URLs using submissions JSON even when index.json is absent.
    Returns [{url, accession, reportDate, primaryDocument, base}].
    """
    subs = await get_company_submissions(client, cik)
    recent = subs.get("filings", {}).get("recent", {})
    forms   = recent.get("form", [])
    accs    = recent.get("accessionNumber", [])
    prims   = recent.get("primaryDocument", [])
    reports = recent.get("reportDate", [])
    out = []
    for form, acc, primary_doc, repdate in zip(forms, accs, prims, reports):
        if form not in ("13F-HR", "13F-HR/A"):
            continue
        acc_nodashes = acc.replace("-", "")
        base = f"{SEC_BASE}/Archives/edgar/data/{int(cik_normalize(cik))}/{acc_nodashes}"
        # Start with a common info-table filename; we’ll fall back if needed
        candidate = "form13fInfoTable.xml"
        out.append({
            "url": f"{base}/{candidate}",
            "accession": acc,
            "reportDate": repdate,
            "primaryDocument": primary_doc,  # e.g. primary_doc.txt or subfolder/file
            "base": base
        })
        if len(out) >= max_filings:
            break
    return out

def extract_info_table_from_text(txt: str) -> str | None:
    """
    Extract embedded <informationTable>...</informationTable> XML from a TXT/HTML filing.
    """
    m = re.search(r"<informationTable\b.*?</informationTable>", txt, flags=re.DOTALL | re.IGNORECASE)
    if m:
        return m.group(0)
    return None

def parse_13f_xml(xml_text: str):
    data = xmltodict.parse(xml_text)
    info = _first_key(data, "informationTable", "r13f:informationTable") or data
    rows = _first_key(info, "infoTable", "r13f:infoTable") or []
    if isinstance(rows, dict):
        rows = [rows]

    def get(row, *names):
        v = _first_key(row, *names)
        if v is None:
            v = _first_key(row, *[f"r13f:{n}" for n in names])
        return v

    out = []
    for row in rows:
        name = (get(row, "nameOfIssuer") or "").strip()
        cusip = (get(row, "cusip") or "").strip()
        value = get(row, "value")
        try:
            value = float(value) * 1000.0 if value is not None else None
        except:
            value = None
        shrs = get(row, "shrsOrPrnAmt") or {}
        if isinstance(shrs, str):
            shrs = {}
        shares = _first_key(shrs, "sshPrnamt", "r13f:sshPrnamt")
        try:
            shares = float(shares) if shares is not None else None
        except:
            shares = None
        ticker = (get(row, "issuerTradingSymbol") or "").strip().upper()
        out.append({"name": name, "cusip": cusip, "ticker": ticker, "shares": shares, "value_usd": value})
    return out

async def upsert_investor(session: AsyncSession, filer_cik: str, name: str):
    stmt = pg_insert(models.Investor).values(filer_cik=filer_cik, name=name).on_conflict_do_nothing()
    await session.execute(stmt)

async def get_or_create_company_id(session: AsyncSession, ticker: str) -> int | None:
    if not ticker:
        return None
    t = ticker.upper()
    q = select(models.Company.id).where(models.Company.ticker == t)
    cid = (await session.execute(q)).scalar_one_or_none()
    if cid:
        return cid
    stmt = pg_insert(models.Company).values(ticker=t, name=t).on_conflict_do_nothing().returning(models.Company.id)
    res = (await session.execute(stmt)).scalar_one_or_none()
    if res:
        return res
    return (await session.execute(q)).scalar_one_or_none()

async def save_holdings(session: AsyncSession, filer_cik: str, period_end: date, holdings: list[dict]):
    for h in holdings:
        cid = await get_or_create_company_id(session, h.get("ticker"))
        if not cid:
            continue
        stmt = pg_insert(models.FundHolding).values(
            filer_cik=filer_cik,
            company_id=cid,
            period_end=period_end,
            shares=h.get("shares"),
            value_usd=h.get("value_usd"),
        ).on_conflict_do_nothing()
        await session.execute(stmt)

async def fetch_info_table_xml(client: httpx.AsyncClient, base: str, primary_doc: str) -> str | None:
    """
    Try exact-case XML names in root and known subfolders.
    If missing, try directory listing HTML to discover filenames.
    If still missing, fetch primary doc (and .txt/.htm/.html variants) and extract embedded XML.
    """
    candidates_root = [
        "form13fInfoTable.xml", "form13FInfoTable.xml",
        "informationtable.xml", "InformationTable.xml",
        "infoTable.xml", "infotable.xml",
        "form13f_infotable.xml", "form13F_infotable.xml",
        "form13fInformationTable.xml", "form13FInformationTable.xml",
    ]
    subfolders = ["xslForm13F_X01", "xslForm13F_X02", "xslForm13F_X03",
                  "XSLForm13F_X01", "XSLForm13F_X02", "XSLForm13F_X03"]

    # 1) Direct root candidates
    for fn in candidates_root:
        try:
            return (await _get(client, f"{base}/{fn}")).text
        except HTTPStatusError as e:
            if e.response.status_code != 404:
                raise

    # 2) Subfolder candidates
    base_root = "/".join(base.split("/")[:-1])
    for folder in subfolders:
        for fn in candidates_root:
            try:
                return (await _get(client, f"{base_root}/{folder}/{fn}")).text
            except HTTPStatusError as e:
                if e.response.status_code != 404:
                    raise

    # 3) Directory listing discovery (root, then common subfolders)
    discovered = await list_dir_candidates(client, base)
    # if we saw any files, try any XML with "info" or "table" in name
    for name in discovered:
        low = name.lower()
        if low.endswith(".xml") and ("info" in low or "table" in low):
            try:
                return (await _get(client, f"{base}/{name}")).text
            except HTTPStatusError as e:
                if e.response.status_code != 404:
                    raise

    for folder in subfolders:
        sub_base = f"{base_root}/{folder}"
        found = await list_dir_candidates(client, sub_base)
        for name in found:
            low = name.lower()
            if low.endswith(".xml") and ("info" in low or "table" in low):
                try:
                    return (await _get(client, f"{sub_base}/{name}")).text
                except HTTPStatusError as e:
                    if e.response.status_code != 404:
                        raise

    # 4) Primary document and its common variants (.txt, .htm, .html) -> extract <informationTable>
    primaries = [primary_doc]
    if primary_doc.lower().endswith(".xml"):
        primaries += [
            primary_doc[:-4] + "txt",
            primary_doc[:-4] + "htm",
            primary_doc[:-4] + "html",
        ]
    else:
        # also try adding .txt/.htm/.html if no extension given
        primaries += [primary_doc + ".txt", primary_doc + ".htm", primary_doc + ".html"]

    tried = set()
    for p in primaries:
        if p in tried:
            continue
        tried.add(p)
        try:
            txt = (await _get(client, f"{base}/{p}")).text
            xml = extract_info_table_from_text(txt)
            if xml:
                return xml
        except HTTPStatusError as e:
            if e.response.status_code != 404:
                raise

    # 5) As a last resort, try *any* XML we saw in listings, even if name doesn’t contain 'info'/'table'
    for name in discovered:
        if name.lower().endswith(".xml"):
            try:
                return (await _get(client, f"{base}/{name}")).text
            except HTTPStatusError as e:
                if e.response.status_code != 404:
                    raise
    for folder in subfolders:
        sub_base = f"{base_root}/{folder}"
        found = await list_dir_candidates(client, sub_base)
        for name in found:
            if name.lower().endswith(".xml"):
                try:
                    return (await _get(client, f"{sub_base}/{name}")).text
                except HTTPStatusError as e:
                    if e.response.status_code != 404:
                        raise

    return None

async def ingest_latest_13f(filer_cik: str, filer_name: str = ""):
    filer_cik = cik_normalize(filer_cik)
    try:
        async with httpx.AsyncClient() as client, SessionLocal() as session:
            await upsert_investor(session, filer_cik, filer_name or f"Filer {filer_cik}")
            metas = await latest_13f_meta(client, filer_cik, max_filings=1)
            if not metas:
                await session.commit()
                return {"status": "no_13f_found", "reason": "No recent 13F-HR for this CIK"}

            meta = metas[0]
            base = meta["base"]
            primary_doc = meta.get("primaryDocument") or "primary_doc.txt"

            xml_text = await fetch_info_table_xml(client, base, primary_doc)
            if not xml_text:
                return {"status": "error", "code": 404, "reason": "Info table XML not found in filing"}

            holdings = parse_13f_xml(xml_text)

            # period_end from meta if present
            try:
                rep = meta.get("reportDate")
                period_end = datetime.strptime(rep, "%Y-%m-%d").date() if rep else datetime.utcnow().date()
            except Exception:
                period_end = datetime.utcnow().date()

            await save_holdings(session, filer_cik, period_end, holdings)
            await session.commit()
            return {"status": "ok", "count": len(holdings), "period_end": str(period_end)}
    except HTTPStatusError as e:
        code = e.response.status_code
        if code == 403:
            return {"status": "error", "code": 403, "reason": "SEC blocked request. Set SEC_USER_AGENT to your email and rebuild the API container."}
        return {"status": "error", "code": code, "reason": str(e)}
    except RequestError as e:
        return {"status": "error", "reason": f"Network error: {e}"}
    except Exception as e:
        return {"status": "error", "reason": f"Unexpected: {e.__class__.__name__}: {e}"}