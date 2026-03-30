# add at top
from fastapi import APIRouter, Query
from typing import List, Dict, Any
from ..core.discovery_engine import build_watchlist

router = APIRouter(prefix="/leaders", tags=["leaders"])

@router.get("/watchlist")
def get_watchlist(
    k: int = Query(25, ge=1, le=200),
    min_prob: float = Query(0.55, ge=0.0, le=1.0),
    role: str = Query("CEO"),
    unique_by: str = Query("ticker"),
    verbose: int = Query(0),  # 0 = trader-clean, 1 = full
):
    items: List[Dict[str, Any]] = build_watchlist(
        k=k, min_prob=min_prob, role=role, unique_by=unique_by
    )

    if verbose:
        return {"items": items}

    # Trader-clean: keep only the essentials + your note/freshness
    keep = {
        "person","company","ticker","role","tenure_start","sector",
        "composite_score","emergence_boost","note",
        "latest_price_date","latest_insider_date"
    }
    cleaned = [{k: v for k, v in d.items() if k in keep} for d in items]
    return {"items": cleaned}

@router.get("/export")
def export_watchlist_csv(
    k: int = Query(50, ge=1, le=200),
    min_prob: float = Query(0.55, ge=0.0, le=0.99),
    role: str = Query("CEO"),
    unique_by: str = Query("ticker"),
):
    items = build_watchlist(k=k, min_prob=min_prob, role=role, unique_by=unique_by)
    cols = ["person","company","ticker","role","tenure_start","leadership_score","trajectory_score","why_watch"]
    buf = StringIO()
    writer = csv.DictWriter(buf, fieldnames=cols, extrasaction="ignore")
    writer.writeheader()
    for row in items:
        writer.writerow(row)
    buf.seek(0)
    return StreamingResponse(buf, media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="leadership_watchlist_k{k}_min{min_prob:.2f}.csv"'})

# --- snapshots API ---
from typing import List, Dict
import sqlite3, os, json
from fastapi import Query

@router.get("/snapshots")
def list_snapshots(limit: int = Query(10, ge=1, le=200)):
    DB = os.environ.get("FEATURES_DB", "./data/gold/ceo_watchlist.db")
    with sqlite3.connect(DB) as con:
        rows = con.execute("""
            SELECT snapshot_ts, params_json, count
            FROM watchlist_snapshots
            ORDER BY snapshot_ts DESC
            LIMIT ?
        """, (limit,)).fetchall()
    return [
        {"snapshot_ts": ts, "params": json.loads(params), "count": cnt}
        for (ts, params, cnt) in rows
    ]

@router.get("/snapshots/{snapshot_ts}")
def read_snapshot(snapshot_ts: str):
    DB = os.environ.get("FEATURES_DB", "./data/gold/ceo_watchlist.db")
    with sqlite3.connect(DB) as con:
        row = con.execute("""
            SELECT snapshot_ts, params_json, count, items_json
            FROM watchlist_snapshots
            WHERE snapshot_ts = ?
        """, (snapshot_ts,)).fetchone()
    if not row:
        return {"error": "snapshot not found"}
    ts, params, cnt, items = row
    return {
        "snapshot_ts": ts,
        "params": json.loads(params),
        "count": cnt,
        "items": json.loads(items),
    }

from fastapi.responses import StreamingResponse
from io import StringIO
import csv

@router.get("/snapshots/{snapshot_ts}/export")
def export_snapshot_csv(snapshot_ts: str):
    DB = os.environ.get("FEATURES_DB", "./data/gold/ceo_watchlist.db")
    with sqlite3.connect(DB) as con:
        row = con.execute("""
            SELECT items_json FROM watchlist_snapshots
            WHERE snapshot_ts = ?
        """, (snapshot_ts,)).fetchone()
    if not row:
        return {"error": "snapshot not found"}
    items = json.loads(row[0])

    cols = [
        "person","company","ticker","role","tenure_start",
        "leadership_score","trajectory_score","why_watch"
    ]
    buf = StringIO()
    writer = csv.DictWriter(buf, fieldnames=cols, extrasaction="ignore")
    writer.writeheader()
    for it in items:
        writer.writerow(it)
    buf.seek(0)
    return StreamingResponse(
        buf,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename=\"watchlist_snapshot_{snapshot_ts}.csv\"'}
    )