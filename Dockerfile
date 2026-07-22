FROM python:3.12-slim

RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    libxcb1 \
    libxcb-shm0 \
    libxcb-xfixes0 \
    libglib2.0-0 \
    libgl1 \
    libfreetype6-dev \
    libpng-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install uv

WORKDIR /app

COPY pyproject.toml uv.lock ./

RUN uv sync

COPY . .

ENV PYTHONPATH=/app

# Exécution locale (`docker run`). Sur Clever Cloud (Task), la commande est
# pilotée par la variable d'environnement CC_RUN_COMMAND et remplace ce CMD.
CMD ["uv", "run", "python", "-m", "pipelines.run"]