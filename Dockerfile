FROM python:3.8-slim

WORKDIR /src

# Install curl and build dependencies
RUN apt-get update && \
    apt-get install -y curl build-essential && \
    rm -rf /var/lib/apt/lists/*

# Install Poetry
ARG POETRY_VERSION=1.3.2
RUN curl -sSL https://install.python-poetry.org/ | python3 - --version "${POETRY_VERSION}"
ENV PATH="/root/.local/bin:$PATH"

# Copy dependency files
COPY pyproject.toml poetry.lock* ./

# Install all dependencies (including boto3, kubernetes, etc.)
RUN poetry install --no-interaction --no-ansi

# Copy source code
COPY *.py ./

# Run your script via poetry
ENTRYPOINT ["poetry", "run", "python"]
CMD ["/src/jobs.py"]

