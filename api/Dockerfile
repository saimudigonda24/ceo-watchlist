FROM python:3.11-slim

WORKDIR /app

# System deps (gcc for some wheels), then clean up
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
  && rm -rf /var/lib/apt/lists/*

# Copy API requirements from the repo root (build context) into image
COPY api/requirements.txt /app/requirements.txt

# Speed up pip, then install deps
RUN pip install -U pip && pip install --no-cache-dir -r /app/requirements.txt

# Copy the API source
COPY api/ /app/

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]