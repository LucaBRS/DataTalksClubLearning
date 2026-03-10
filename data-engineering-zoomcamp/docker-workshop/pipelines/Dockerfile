FROM python:3.13.11-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/


# creation of the directory in docker image
WORKDIR /code
# because i did install uv when i set the env of python to the one below i do no need to do ENTRYPOINT ["uv","run","python","pipeline.py"]
ENV PATH="/code/.venv/bin:$PATH"
# RUN pip install pandas pyarrow
# we are using uv so we need to copy the pyproject.toml
COPY pyproject.toml .python-version uv.lock ./

RUN uv sync --locked

COPY ingest_data.py .

ENTRYPOINT ["python","ingest_data.py"]