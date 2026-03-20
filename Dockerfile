FROM python:3.9-slim as builder

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

FROM python:3.9-slim

WORKDIR /app

COPY --from=builder /install /usr/local

COPY data_analysis.py .
COPY All_Diets.csv .

RUN useradd -m appuser
USER appuser

CMD ["python", "data_analysis.py"]