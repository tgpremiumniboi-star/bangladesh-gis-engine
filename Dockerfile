FROM python:3.11-slim

WORKDIR /app

# Install system dependencies for psycopg2 and geospatial libraries
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application directories
COPY backend/ ./backend/
COPY frontend/ ./frontend/
COPY frontend_maplibre/ ./frontend_maplibre/
COPY data/ ./data/

# Set environment
ENV PYTHONUNBUFFERED=1
ENV PORT=8001

EXPOSE 8001

CMD ["sh", "-c", "uvicorn backend.api_pg:app --host 0.0.0.0 --port ${PORT:-8001} --workers 2"]
