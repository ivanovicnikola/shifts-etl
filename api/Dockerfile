FROM python:3.10-slim

# Install gcc and necessary build tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libffi-dev \
    build-essential \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV PYTHONFAULTHANDLER=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=random \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    POETRY_VERSION=1.8.4

RUN pip install "poetry==$POETRY_VERSION"

WORKDIR /api

COPY poetry.lock pyproject.toml /api/

RUN POETRY_VIRTUALENVS_CREATE=false \
    poetry install --only main --no-root --no-interaction --no-ansi

COPY ./app /api/app

CMD ["uvicorn", "app.main:app", "--host=0.0.0.0"]
