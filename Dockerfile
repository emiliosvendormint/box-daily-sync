FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY box_daily_sync.py .

ENTRYPOINT ["python", "box_daily_sync.py"]
