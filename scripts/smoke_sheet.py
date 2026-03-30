import os, pandas as pd, gspread
from gspread_dataframe import set_with_dataframe

sheet_id = os.environ["GSHEET_ID"]
tab = os.environ.get("GSHEET_TAB","Potential Dudes")

# user OAuth (no service account):
gc = gspread.oauth()   # uses ./credentials.json; will create ./token.json on first run
sh = gc.open_by_key(sheet_id)

try:
    ws = sh.worksheet(tab)
    ws.clear()
except gspread.exceptions.WorksheetNotFound:
    ws = sh.add_worksheet(title=tab, rows="1000", cols="20")

df = pd.DataFrame([{"Hello":"World","OK":True}])
set_with_dataframe(ws, df, include_index=False, include_column_header=True, resize=True)
ws.freeze(rows=1)
print("✅ Wrote a test row to the sheet/tab.")