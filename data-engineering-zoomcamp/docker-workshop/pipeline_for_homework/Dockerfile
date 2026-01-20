FROM python:3.11-slim

WORKDIR /app

# install uv (binario)
RUN pip install --no-cache-dir uv

# copia solo i file di dipendenze prima (per cache)
COPY pyproject.toml uv.lock /app/

# crea venv e installa deps dal lock
RUN uv venv /app/.venv \
    && . /app/.venv/bin/activate \
    && uv sync --frozen

# copia il codice dopo
COPY ingest_data.py /app

ENV PATH="/app/.venv/bin:$PATH"

CMD ["python", "ingest_data.py"]
