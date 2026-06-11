FROM python:3.11-slim

WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy service source code
COPY generators/ ./generators/
COPY kafka_producer/ ./kafka_producer/
COPY schemas/ ./schemas/
COPY main.py .

CMD ["python", "main.py"]
