FROM python:3.11-slim

# --- dépendances système ---
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# --- installer uv ---
RUN pip install uv

# --- évite les builds Rust inutiles (important pour tokenizers) ---
ENV UV_PYTHON_PREFER_BINARY=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

WORKDIR /app

# --- copier dépendances en premier (cache Docker propre) ---
COPY pyproject.toml uv.lock ./

# --- install deps ---
RUN uv sync

# --- copier le code ensuite ---
COPY . .

# --- commande par défaut ---
CMD ["uv", "run", "pipelines/run.py"]
