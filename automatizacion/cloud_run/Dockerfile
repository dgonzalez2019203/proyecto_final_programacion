# Imagen de la API de NovaBank para Cloud Run (opcional: la demo corre en local).
# Construye la base SQLite dentro de la imagen para que el servicio sea autocontenido.
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY pipeline/ pipeline/
COPY api/ api/
COPY tablero/ tablero/
RUN python pipeline/ejecutar_pipeline.py
ENV PORT=8080
CMD ["sh", "-c", "uvicorn api.main:app --host 0.0.0.0 --port ${PORT}"]
