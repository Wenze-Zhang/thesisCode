FROM python:3.11-slim

WORKDIR /app

# Build deps for compiled Python packages (e.g. psycopg2, lxml)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Default entrypoint: run the Metadata Enricher.
CMD ["python", "-u", "enricher.py"]
