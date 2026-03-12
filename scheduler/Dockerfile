FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY build_snapshot.py .
COPY fetch_downtimes.py .
COPY fetch_controles.py .
COPY ingest_gmail.py .
COPY parser_parte_diario.py .
COPY autorizar_gmail.py .

CMD ["python", "build_snapshot.py"]
