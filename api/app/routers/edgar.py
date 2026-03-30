from fastapi import APIRouter
from ..ingestion.edgar_13f import latest_13f_meta, cik_normalize
import httpx

router = APIRouter(prefix="/edgar", tags=["edgar"])

@router.get("/peek13f")
async def peek_13f(cik: str):
    async with httpx.AsyncClient() as client:
        metas = await latest_13f_meta(client, cik, max_filings=1)
    return metas

@router.post("/ingest13f")
async def ingest_13f(cik: str, name: str = ""):
    from ..ingestion.edgar_13f import ingest_latest_13f
    return await ingest_latest_13f(cik, name)

# --- add this block ---
from fastapi import APIRouter
import httpx
from ..ingestion.edgar_13f import (
    SEC_BASE, cik_normalize, get_company_submissions, _get
)

router = APIRouter(prefix="/edgar", tags=["edgar"])

@router.get("/debug13f")
async def debug_13f(cik: str):
    """
    Probe common info-table paths (root + subfolders) for the latest 13F.
    Returns a dict {candidate_url: http_status_or_error}.
    """
    results = {}
    async with httpx.AsyncClient() as client:
        subs = await get_company_submissions(client, cik)
        recent = subs.get("filings", {}).get("recent", {})
        for form, acc, primary_doc, report_date in zip(
            recent.get("form", []),
            recent.get("accessionNumber", []),
            recent.get("primaryDocument", []),
            recent.get("reportDate", []),
        ):
            if form not in ("13F-HR", "13F-HR/A"):
                continue
            base = f"{SEC_BASE}/Archives/edgar/data/{int(cik_normalize(cik))}/{acc.replace('-','')}"
            candidates_root = [
                "form13fInfoTable.xml", "form13FInfoTable.xml",  # case variants
                "informationtable.xml", "InformationTable.xml",
                "infoTable.xml", "infotable.xml",
                "form13f_infotable.xml", "form13F_infotable.xml",
                "form13fInformationTable.xml", "form13FInformationTable.xml",
            ]
            # try primary document itself too
            probes = [f"{base}/{primary_doc}"] + [f"{base}/{fn}" for fn in candidates_root]

            # if primary_doc is inside a subfolder, also probe there and in common xslForm folders
            subfolders = ["xslForm13F_X01", "xslForm13F_X02", "xslForm13F_X03",
                          "XSLForm13F_X01", "XSLForm13F_X02", "XSLForm13F_X03"]
            base_root = "/".join(base.split("/")[:-1])

            for folder in subfolders:
                probes += [f"{base_root}/{folder}/{fn}" for fn in candidates_root]

            # run probes
            statuses = {}
            for url in probes:
                try:
                    r = await _get(client, url)
                    statuses[url] = r.status_code
                except Exception as e:
                    statuses[url] = str(e)
            results[acc] = {
                "reportDate": report_date,
                "primaryDocument": primary_doc,
                "statuses": statuses,
            }
            break  # only the most recent 13F
    return results

from fastapi import APIRouter
import httpx
from ..ingestion.edgar_13f import (
    SEC_BASE, cik_normalize, get_company_submissions, _get, ingest_latest_13f
)

router = APIRouter(prefix="/edgar", tags=["edgar"])

@router.get("/debug13f")
async def debug_13f(cik: str):
    results = {}
    async with httpx.AsyncClient() as client:
        subs = await get_company_submissions(client, cik)
        recent = subs.get("filings", {}).get("recent", {})
        for form, acc, primary_doc, report_date in zip(
            recent.get("form", []),
            recent.get("accessionNumber", []),
            recent.get("primaryDocument", []),
            recent.get("reportDate", []),
        ):
            if form not in ("13F-HR", "13F-HR/A"):
                continue
            base = f"{SEC_BASE}/Archives/edgar/data/{int(cik_normalize(cik))}/{acc.replace('-','')}"
            candidates_root = [
                "form13fInfoTable.xml", "form13FInfoTable.xml",
                "informationtable.xml", "InformationTable.xml",
                "infoTable.xml", "infotable.xml",
                "form13f_infotable.xml", "form13F_infotable.xml",
                "form13fInformationTable.xml", "form13FInformationTable.xml",
            ]
            subfolders = ["xslForm13F_X01","xslForm13F_X02","xslForm13F_X03",
                          "XSLForm13F_X01","XSLForm13F_X02","XSLForm13F_X03"]
            base_root = "/".join(base.split("/")[:-1])

            probes = [f"{base}/{primary_doc}"] + [f"{base}/{fn}" for fn in candidates_root]
            for folder in subfolders:
                probes += [f"{base_root}/{folder}/{fn}" for fn in candidates_root]

            statuses = {}
            for url in probes:
                try:
                    r = await _get(httpx.AsyncClient(), url)  # small shortcut
                    statuses[url] = r.status_code
                except Exception as e:
                    statuses[url] = str(e)
            results[acc] = {"reportDate": report_date, "primaryDocument": primary_doc, "statuses": statuses}
            break
    return results

@router.post("/ingest13f")
async def ingest_13f(cik: str, name: str = ""):
    return await ingest_latest_13f(cik, name)