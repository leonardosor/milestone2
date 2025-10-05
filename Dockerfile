# syntax=docker/dockerfile:1.6
FROM python:3.11-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    TZ=UTC

# System dependencies for geopandas, shapely, fiona, pyproj, rtree, psycopg2, dbt
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       build-essential \
       gdal-bin \
       libgdal-dev \
       libproj-dev \
       proj-bin \
       libgeos-dev \
       libspatialindex-dev \
       libpq-dev \
       ca-certificates \
       wget \
       curl \
       git \
       postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Set GDAL / PROJ env variables (Python wheels often detect automatically but set for safety)
ENV GDAL_DATA=/usr/share/gdal \
    PROJ_LIB=/usr/share/proj

WORKDIR /app

# Requirements first (leverage caching)
COPY requirements.txt ./
RUN pip install --upgrade pip \
    && pip install -r requirements.txt \
    && pip check || true

# dbt specific requirements (if separate)
COPY dbt_project/requirements_dbt.txt dbt_project/requirements_dbt.txt
RUN pip install -r dbt_project/requirements_dbt.txt || true

# Copy project files
COPY . .

# Ensure entrypoint script is executable
RUN chmod +x docker/entrypoint.sh

VOLUME ["/app/logs", "/app/outputs", "/app/tiger_data"]

EXPOSE 8080

ENTRYPOINT ["/app/docker/entrypoint.sh"]
CMD ["run-etl"]
