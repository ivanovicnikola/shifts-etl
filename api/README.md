# Shifts API

Expose generated shifts data as a REST endpoint using FastAPI.

## Getting Started

Install dependencies inside virtual environment with

```bash
$ poetry install
```

Start shifts API with

```bash
$ uvicorn app.main:app
```

### Docker

Build docker image with

```bash
$ docker build . -t smartcat-interviews/de-01-api
```

and run container with

```bash
$ docker run -it --rm -p 8000:8000 smartcat-interviews/de-01-api
```

## OpenAPI Specs

After you start server, endpoints OpenAPI specs can be found at
[http://localhost:8000/docs](http://localhost:8000/docs) or
[http://localhost:8000/redoc](http://localhost:8000/redoc).
