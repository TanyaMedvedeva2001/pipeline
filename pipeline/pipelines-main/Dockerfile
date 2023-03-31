FROM python:3.10-alpine

ENV PIP_DEFAULT_TIMEOUT=100 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    POETRY_VERSION=1.4.1

WORKDIR /app
COPY poetry.lock pyproject.toml /app/

RUN apk add --no-cache gcc libffi-dev musl-dev python3-dev
RUN pip install poetry==${POETRY_VERSION} 
RUN poetry config virtualenvs.create false
RUN poetry install --no-interaction --no-ansi

COPY . /app/
RUN pip install -e .

CMD ["pipelines"]

# docker build -t pipelines .
# docker run pipelines
