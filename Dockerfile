# Use Python 3.11 slim
FROM python:3.11-slim

# Define diretório de trabalho
WORKDIR /app

# Instala dependências do sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copia todo o backend para /app
COPY backend/ . 

# Instala dependências Python
RUN pip install --no-cache-dir "numpy>=1.26,<2" \
    && pip install --no-cache-dir -r requirements.txt

# Expõe a porta
EXPOSE 8000

# Comando para iniciar o Uvicorn
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
