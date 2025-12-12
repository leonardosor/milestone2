FROM python:3.11-slim

WORKDIR /app

# Install system dependencies (including GDAL for GeoPandas)
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    libgdal-dev \
    gdal-bin \
    postgresql-client \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set GDAL environment variables
ENV GDAL_CONFIG=/usr/bin/gdal-config
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ ./src/
COPY config/ ./config/
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

# Create necessary directories
RUN mkdir -p /app/logs /app/data/tiger

# Set Python path to include src directory
ENV PYTHONPATH=/app/src:/app

ENTRYPOINT ["./entrypoint.sh"]
CMD ["run-etl"]
