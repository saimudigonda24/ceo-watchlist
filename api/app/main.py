from fastapi import FastAPI
from .routers import watchlist
from .routers import health, companies, signals, edgar, watchlist
from .routers import ui  # <— add

app = FastAPI(title="CEO Watchlist API", version="0.1.0")

# Mount only what we need right now
app.include_router(health.router)
app.include_router(companies.router)
app.include_router(signals.router)
app.include_router(edgar.router)
app.include_router(watchlist.router)
app.include_router(ui.router)  # <-- add

@app.get("/health")
def health():
    return {"status": "ok"}

from fastapi.staticfiles import StaticFiles
# mount after app creation
app.mount("/static/snapshots", StaticFiles(directory="data/snapshots"), name="snapshots")