# Dockerfile

FROM apache/airflow:2.10.2-python3.11

# Install Python dependencies for MySQL and PostgreSQL connectors
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Set Airflow user as root for simplicity in local dev (avoid permission issues with mounts)
USER root

# Optional: Install any OS-level deps if needed (e.g., for pandas or other libs)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow