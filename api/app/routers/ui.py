# api/app/routers/ui.py
from fastapi import APIRouter, Query, Response
from ..core.discovery_engine import build_watchlist
import csv
from io import StringIO
from io import BytesIO
import pandas as pd


router = APIRouter(prefix="/leaders", tags=["ui"])

@router.get("/watchlist.csv")
def watchlist_csv(
    k: int = Query(50, ge=1, le=500),
    min_prob: float = Query(0.0, ge=0.0, le=1.0),
    role: str = Query("any"),
    unique_by: str = Query("ticker"),
):
    items = build_watchlist(k=k, min_prob=min_prob, role=role, unique_by=unique_by)
    # choose trader-friendly columns
    cols = [
        "person","company","ticker","role","sector","tenure_start",
        "composite_score","emergence_boost","latest_price_date","latest_insider_date","note"
    ]
    buf = StringIO()
    w = csv.DictWriter(buf, fieldnames=cols, extrasaction="ignore")
    w.writeheader()
    for it in items:
        w.writerow(it)
    data = buf.getvalue()
    return Response(
        data,
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="watchlist.csv"'}
    )

@router.get("/watchlist.xlsx")
def watchlist_xlsx(
    k: int = Query(50, ge=1, le=500),
    min_prob: float = Query(0.0, ge=0.0, le=1.0),
    role: str = Query("any"),
    unique_by: str = Query("ticker"),
):
    items = build_watchlist(k=k, min_prob=min_prob, role=role, unique_by=unique_by)
    cols = [
        "person","company","ticker","role","sector","tenure_start",
        "composite_score","emergence_boost","latest_price_date","latest_insider_date","note"
    ]
    df = pd.DataFrame(items)[cols] if items else pd.DataFrame(columns=cols)

    # Try to write a real .xlsx (preferred). If openpyxl/xlsxwriter isn’t installed, fall back to CSV-but-name-.xls
    try:
        bio = BytesIO()
        with pd.ExcelWriter(bio, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Watchlist")
        data = bio.getvalue()
        return Response(
            content=data,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": 'attachment; filename="watchlist.xlsx"'}
        )
    except Exception:
        # Fallback: Excel will still open this fine
        csv_bytes = df.to_csv(index=False).encode("utf-8")
        return Response(
            content=csv_bytes,
            media_type="application/vnd.ms-excel",
            headers={"Content-Disposition": 'attachment; filename="watchlist.xls"'}
        )

_HTML_HEAD = """<!doctype html><html><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>CEO Watchlist</title>
<style>
body{font:14px/1.45 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial}
h1{font-size:18px;margin:12px 0}
table{width:100%;border-collapse:collapse}
th,td{padding:8px 10px;border-bottom:1px solid #eee;vertical-align:top}
th{background:#fafafa;text-align:left;position:sticky;top:0}
.badge{font-size:12px;padding:2px 6px;border-radius:4px;background:#efefef}
.small{font-size:12px;color:#333}
.right{text-align:right}
.row:hover{background:#fcfcff}
.meta{color:#666;font-size:12px;margin:6px 0 12px}
</style></head><body>"""

def _fmt(x):
    try: return f"{float(x):.3f}"
    except: return x or ""

@router.get("/watchlist/html")
def watchlist_html(
    k: int = Query(25, ge=1, le=400),
    min_prob: float = Query(0.0, ge=0.0, le=1.0),
    role: str = Query("any"),
    unique_by: str = Query("ticker"),
    refresh_s: int = Query(180, ge=15, le=3600),
):
    items = build_watchlist(k=k, min_prob=min_prob, role=role, unique_by=unique_by)

    # Build unique picklists from current data
    sectors = sorted({(it.get("sector") or "Unknown") for it in items})
    roles   = sorted({(it.get("role")   or "CEO")     for it in items})
    # Tenure year buckets
    tenure_years = sorted({
        pd.to_datetime(it.get("tenure_start")).year
        for it in items
        if it.get("tenure_start")
    })

    html = [
        _HTML_HEAD,
        "<h1>CEO Watchlist</h1>",
        f"<div class='meta'>k={k} • min_prob={min_prob} • unique_by={unique_by} • auto-refresh: <span id='refresh_cd'>{refresh_s}</span>s</div>",
        f"<div class='meta'><a href='/leaders/watchlist.csv?k={k}&min_prob={min_prob}&role={role}&unique_by={unique_by}'>Download CSV</a> • "
        f"<a href='/leaders/watchlist.xlsx?k={k}&min_prob={min_prob}&role={role}&unique_by={unique_by}'>Open in Excel</a> • "
        "<a href='#' id='btn_clear'>Clear filters</a> • <span id='count_badge'>…</span></div>",

        # Header CSS
        "<style>",
        "thead th{position:sticky;top:0;background:#fafafa;z-index:1}",
        "th .hwrap{display:flex;align-items:center;gap:6px}",
        "th select{font-size:12px;padding:2px;border:1px solid #ddd;border-radius:4px;background:#fff}",
        ".flt-arrow{font-size:10px;color:#888}",
        ".th-sort{cursor:pointer;user-select:none}",
        ".th-sort .sort-ind{font-size:10px;color:#888;margin-left:4px}",
        ".th-active{background:#e7f1ff}",
        ".th-active .flt-arrow{color:#0066cc}",
        ".th-active .sort-ind{color:#0066cc}",
        "</style>",

        "<table><thead><tr>",
            "<th class='th-sort' data-sort='ceo'><div class='hwrap'>CEO <span class='sort-ind'>↕</span></div></th>",
            "<th class='th-sort' data-sort='company'><div class='hwrap'>Company (Ticker) <span class='sort-ind'>↕</span></div></th>",
            # Tenure start + dropdown (by year)
            "<th class='th-sort' data-sort='tenure'><div class='hwrap'>Tenure start "
                "<select id='flt_tenure'><option value='__all__'>All</option>" +
                "".join(f"<option value='{y}'>{y}</option>" for y in tenure_years) +
            "</select><span class='flt-arrow'>▼</span><span class='sort-ind'>↕</span></div></th>",
            # Sector + dropdown
            "<th class='th-sort' data-sort='sector'><div class='hwrap'>Sector "
                "<select id='flt_sector'><option value='__all__'>All</option>" +
                "".join(f"<option value='{s}'>{s}</option>" for s in sectors) +
            "</select><span class='flt-arrow'>▼</span><span class='sort-ind'>↕</span></div></th>",
            # Role + dropdown
            "<th class='th-sort' data-sort='role'><div class='hwrap'>Role "
                "<select id='flt_role'><option value='__all__'>All</option>" +
                "".join(f"<option value='{r}'>{r}</option>" for r in roles) +
            "</select><span class='flt-arrow'>▼</span><span class='sort-ind'>↕</span></div></th>",
            # Composite + buckets
            "<th class='th-sort right' data-sort='comp'><div class='hwrap'>Composite "
                "<select id='flt_comp'>"
                    "<option value='all'>All</option>"
                    "<option value='0.90'>&ge; 0.90</option>"
                    "<option value='0.80'>&ge; 0.80</option>"
                    "<option value='0.70'>&ge; 0.70</option>"
                    "<option value='0.60'>&ge; 0.60</option>"
                "</select><span class='flt-arrow'>▼</span><span class='sort-ind'>↕</span></div></th>",
            # Emergence + buckets
            "<th class='th-sort right' data-sort='emerg'><div class='hwrap'>Emergence "
                "<select id='flt_emerg'>"
                    "<option value='all'>All</option>"
                    "<option value='0.30'>&ge; 0.30</option>"
                    "<option value='0.20'>&ge; 0.20</option>"
                    "<option value='0.10'>&ge; 0.10</option>"
                "</select><span class='flt-arrow'>▼</span><span class='sort-ind'>↕</span></div></th>",
            "<th class='th-sort' data-sort='fresh'><div class='hwrap'>Freshness <span class='sort-ind'>↕</span></div></th>",
            "<th class='th-sort' data-sort='note'><div class='hwrap'>Note <span class='sort-ind'>↕</span></div></th>",
        "</tr></thead><tbody id='tbl_body'>"
    ]

    # Rows
    def _iso(d):
        try:
            return str(pd.to_datetime(d).date())
        except Exception:
            return ""
    for it in items:
        person = it.get("person","")
        company = it.get("company","")
        ticker = it.get("ticker","")
        tenure = it.get("tenure_start","")
        sector = it.get("sector","Unknown")
        role_v = it.get("role","CEO")
        comp = _fmt(it.get("composite_score"))
        emerg = _fmt(it.get("emergence_boost"))
        fresh_px = it.get("latest_price_date") or "—"
        fresh_in = it.get("latest_insider_date") or "—"
        fresh_label = f"px: {fresh_px} • ins: {fresh_in}"
        note = it.get("note","")

        html.append(
            f"<tr class='row' "
            f"data-ceo='{person}' "
            f"data-company='{company}' data-ticker='{ticker}' "
            f"data-tenure='{tenure}' data-tenureiso='{_iso(tenure)}' data-year='{(pd.to_datetime(tenure).year if tenure else '')}' "
            f"data-sector='{sector}' data-role='{role_v}' "
            f"data-comp='{comp}' data-emerg='{emerg}' "
            f"data-freshpx='{_iso(fresh_px)}' data-freshin='{_iso(fresh_in)}' "
            f"data-note='{note}'>"
            f"<td><strong>{person}</strong></td>"
            f"<td>{company} <span class='badge'>{ticker}</span></td>"
            f"<td class='small'>{tenure}</td>"
            f"<td>{sector}</td>"
            f"<td>{role_v}</td>"
            f"<td class='right'><strong>{comp}</strong></td>"
            f"<td class='right'>{emerg}</td>"
            f"<td class='small'>{fresh_label}</td>"
            f"<td class='small'>{note}</td>"
            "</tr>"
        )

    # --- JS: NOT an f-string; we concat refresh ms separately to avoid Python touching ${} ---
    script = """
<script>
(function(){
  const $ = (sel, ctx=document) => ctx.querySelector(sel);
  const $$ = (sel, ctx=document) => Array.from(ctx.querySelectorAll(sel));
  const rows = $$("#tbl_body tr.row");
  const filters = ["flt_tenure","flt_sector","flt_role","flt_comp","flt_emerg"];
  const els = Object.fromEntries(filters.map(id => [id, document.getElementById(id)]));

  function applyFilters(){
    const vals = Object.fromEntries(filters.map(id => [id, els[id].value]));
    let shown = 0;
    rows.forEach(tr => {
      const year = tr.dataset.year || "";
      const sector = tr.dataset.sector || "Unknown";
      const role = tr.dataset.role || "CEO";
      const comp = parseFloat(tr.dataset.comp || "0");
      const emerg = parseFloat(tr.dataset.emerg || "0");

      let ok = true;
      if(vals.flt_tenure !== "__all__" && String(year) !== String(vals.flt_tenure)) ok = false;
      if(ok && vals.flt_sector !== "__all__" && sector !== vals.flt_sector) ok = false;
      if(ok && vals.flt_role   !== "__all__" && role   !== vals.flt_role)   ok = false;
      if(ok && vals.flt_comp   !== "all"     && !(comp  >= parseFloat(vals.flt_comp))) ok = false;
      if(ok && vals.flt_emerg  !== "all"     && !(emerg >= parseFloat(vals.flt_emerg))) ok = false;

      tr.style.display = ok ? "" : "none";
      if(ok) shown++;
    });
    // Save
    filters.forEach(id => sessionStorage.setItem(id, els[id].value));
    // Header highlight if active
    filters.forEach(id => {
      const active = (els[id].value !== "__all__" && els[id].value !== "all");
      const th = els[id].closest("th");
      th.classList.toggle("th-active", active);
    });
    // Count badge
    const total = rows.length;
    $("#count_badge").textContent = `showing ${shown} of ${total}`;
  }

  // Restore saved filters (if options still exist)
  filters.forEach(id => {
    const saved = sessionStorage.getItem(id);
    const el = els[id];
    if(saved && [...el.options].some(o => o.value===saved)) el.value = saved;
  });
  filters.forEach(id => els[id].addEventListener("change", applyFilters));

  // Sorting
  const thSorts = $$(".th-sort");
  function sortBy(key, dir){
    const tuples = rows.map(tr => {
      switch(key){
        case "ceo":     return [ (tr.dataset.ceo||"").toLowerCase(), tr ];
        case "company": return [ ((tr.dataset.company||"") + " " + (tr.dataset.ticker||"")).toLowerCase(), tr ];
        case "tenure":  return [ tr.dataset.tenureiso || "", tr ];
        case "sector":  return [ (tr.dataset.sector||"").toLowerCase(), tr ];
        case "role":    return [ (tr.dataset.role||"").toLowerCase(), tr ];
        case "comp":    return [ parseFloat(tr.dataset.comp || "0"), tr ];
        case "emerg":   return [ parseFloat(tr.dataset.emerg || "0"), tr ];
        case "fresh":   return [ tr.dataset.freshpx || "", tr ];
        case "note":    return [ (tr.dataset.note||"").toLowerCase(), tr ];
        default:        return [ 0, tr ];
      }
    });
    tuples.sort((a,b) => {
      if(a[0] < b[0]) return dir === "asc" ? -1 : 1;
      if(a[0] > b[0]) return dir === "asc" ?  1 : -1;
      return 0;
    });
    const tbody = $("#tbl_body");
    tuples.forEach(([,tr]) => tbody.appendChild(tr));
    // Save sort state
    sessionStorage.setItem("sort_key", key);
    sessionStorage.setItem("sort_dir", dir);
    // Update indicators
    thSorts.forEach(th => th.classList.remove("th-active"));
    const active = document.querySelector(".th-sort[data-sort='"+key+"']");
    if(active) active.classList.add("th-active");
    thSorts.forEach(th => {
      const ind = $(".sort-ind", th);
      if(ind) ind.textContent = "↕";
    });
    const ind = $(".sort-ind", document.querySelector(".th-sort[data-sort='"+key+"']"));
    if(ind) ind.textContent = (dir === "asc" ? "▲" : "▼");
  }
  thSorts.forEach(th => th.addEventListener("click", () => {
    const key = th.dataset.sort;
    const curK = sessionStorage.getItem("sort_key");
    const curD = sessionStorage.getItem("sort_dir") || "desc";
    const nextD = (curK === key && curD === "desc") ? "asc" : "desc";
    sortBy(key, nextD);
  }));
  (function(){
    const k = sessionStorage.getItem("sort_key") || "comp";
    const d = sessionStorage.getItem("sort_dir") || "desc";
    sortBy(k, d);
  })();

  // Initial filter & count
  applyFilters();

  // Clear filters
  document.getElementById("btn_clear").addEventListener("click", (e) => {
    e.preventDefault();
    els.flt_tenure.value = "__all__";
    els.flt_sector.value = "__all__";
    els.flt_role.value   = "__all__";
    els.flt_comp.value   = "all";
    els.flt_emerg.value  = "all";
    applyFilters();
  });

  // Auto-refresh with countdown
  let left = """ + str(refresh_s) + """;
  const cd = document.getElementById("refresh_cd");
  const iv = setInterval(() => {
    left -= 1;
    if(left <= 0){
      clearInterval(iv);
      window.location.reload();
    } else {
      cd.textContent = left;
    }
  }, 1000);
})();
</script>
"""
    html.append("</tbody></table>")
    html.append(script)
    html.append("</body></html>")
    return Response("".join(html), media_type="text/html")

from pathlib import Path

@router.get("/snapshots")
def list_snapshots():
    root = Path("data/snapshots")
    rows = []
    if root.exists():
        for p in sorted(root.glob("watchlist_*.csv")):
            rows.append((p.name, p.stat().st_size))
    html = [_HTML_HEAD, "<h1>Daily Snapshots</h1>", "<table><thead><tr><th>File</th><th class='right'>Size</th></tr></thead><tbody>"]
    for name, size in rows:
        html += [f"<tr><td><a href='/static/snapshots/{name}'>{name}</a></td>",
                 f"<td class='right'>{size:,}</td></tr>"]
    html.append("</tbody></table></body></html>")
    return Response("".join(html), media_type="text/html")