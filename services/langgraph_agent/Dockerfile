FROM python:3.11-slim

WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy shared schemas (copied during build by deploy.sh)
COPY shared/schemas/ ./schemas/

# Copy service source code
COPY main.py .

CMD ["python", "main.py"]
