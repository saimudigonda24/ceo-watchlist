# --- config ---
PYTHON   ?= python
UVICORN  ?= uvicorn
APP      ?= api.app.main:app
PORT     ?= 8000
HOST     ?= 0.0.0.0
ENV_FILE ?= .env

# --- help ---
.PHONY: help
help:
	@echo "make dev        - run API with autoreload (uses .env)"
	@echo "make kill       - free up port 8000"
	@echo "make refresh    - rebuild tenure_features"
	@echo "make snapshot   - save a watchlist snapshot"
	@echo "make watchlist  - curl current watchlist JSON"

# --- dev server ---
.PHONY: dev
dev:
	$(UVICORN) --env-file $(ENV_FILE) $(APP) --reload --host $(HOST) --port $(PORT)

.PHONY: kill
kill:
	- kill $$(lsof -ti :$(PORT)) 2>/dev/null || true
	- kill -9 $$(lsof -ti :$(PORT)) 2>/dev/null || true

# --- data pipeline ---
.PHONY: refresh
refresh:
	$(PYTHON) api/app/scripts/refresh_features.py

.PHONY: snapshot
snapshot:
	$(PYTHON) -m api.app.scripts.snapshot_watchlist

# --- quick API check ---
CURL ?= curl -fsS
BASE ?= http://localhost:$(PORT)

.PHONY: watchlist
watchlist:
	$(CURL) "$(BASE)/leaders/watchlist?k=10&role=CEO&unique_by=ticker&min_prob=0.0" | jq .

.PHONY: refresh-daemon
refresh-daemon:
	FEATURES_DB="$(PWD)/data/gold/ceo_watchlist.db" $(PYTHON) api/app/scripts/auto_refresh.py

.PHONY: daily
daily:
	FEATURES_DB="$(PWD)/data/gold/ceo_watchlist.db" $(PYTHON) api/app/scripts/daily_refresh.py