# Dockerfile para backend Python FastAPI
FROM python:3.11-slim

# Evita prompts durante instalação
ENV DEBIAN_FRONTEND=noninteractive

# Configura o diretório de trabalho
WORKDIR /app

# Atualiza pip e instala dependências do sistema
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

# Instala dependências Python
RUN pip install --no-cache-dir -r requirements.txt

# Copia todo o código do backend
COPY backend/ .

# Expõe porta do FastAPI
EXPOSE 8000

# Comando default para rodar a aplicação
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
