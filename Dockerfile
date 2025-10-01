# Stage 1: Builder
FROM python:3.8-slim AS builder

WORKDIR /src

RUN apt-get update && apt-get install -y curl build-essential && rm -rf /var/lib/apt/lists/*

ARG POETRY_VERSION=1.3.2
RUN curl -sSL https://install.python-poetry.org/ | python3 - --version "${POETRY_VERSION}"
ENV PATH="/root/.local/bin:${PATH}"

COPY pyproject.toml poetry.lock* ./
RUN poetry install --no-dev --no-root

COPY . .
#RUN poetry export -f requirements.txt --output requirements.txt --without-hashes --without-urls -o requirements.txt

# Stage 2: Runtime
FROM python:3.8-slim

WORKDIR /src

COPY --from=builder /root/.cache/pypoetry/virtualenvs /venv
ENV PATH="/venv/bin:$PATH"

COPY *.py ./

ENTRYPOINT ["python"]
CMD ["/src/jobs.py"]
