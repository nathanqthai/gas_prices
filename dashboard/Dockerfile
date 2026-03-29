FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY dashboard.py .
COPY templates/ templates/
COPY static/ static/

ENV DB_PATH=/app/gas_prices.db
EXPOSE 8080

CMD ["python", "dashboard.py"]
