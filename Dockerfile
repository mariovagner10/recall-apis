# Dockerfile na raiz do projeto
FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /app

# Dependências do sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    libpq-dev \
    curl \
    && python -m pip install --upgrade pip \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copia requirements.txt
COPY backend/requirements.txt .

# Instala NumPy primeiro para evitar conflitos com Pandas
RUN pip install --no-cache-dir numpy==1.27.6 \
    && pip install --no-cache-dir -r requirements.txt

# Copia todo o código do backend
COPY backend/ .

EXPOSE 8000

# Comando padrão para rodar FastAPI
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
