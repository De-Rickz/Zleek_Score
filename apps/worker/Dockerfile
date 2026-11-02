FROM python:3.11-slim

RUN useradd --create-home --shell /bin/bash worker
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

USER worker
WORKDIR /app/ingestion
CMD ["python", "main.py"]
