FROM apache/airflow:2.10.1-python3.12

USER airflow

# Set working directory
WORKDIR /opt/airflow

# Upgrade pip
RUN pip install --upgrade pip

# Install requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt