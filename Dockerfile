FROM python:3.8

WORKDIR /src

ENV PATH="/root/.poetry/bin:${PATH}"

ARG POETRY_VERSION
ENV POETRY_VERSION="${POETRY_VERSION:-1.3.2}"
RUN curl -sSL https://install.python-poetry.org/ \
    | python3 - --version "${POETRY_VERSION}"

ENV PATH="${PATH}:/root/.local/bin"


COPY pyproject.toml ./
COPY *.py ./

RUN poetry install
ENTRYPOINT ["poetry", "run"]
CMD ["/src/jobs.py"]