# Use the official Airflow image as the base image
FROM apache/airflow:2.8.1-python3.11
# Install any additional dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt