# Use Python 3.11 slim
FROM python:3.11-slim

# Define o diretório de trabalho
WORKDIR /app

# Instala dependências do sistema necessárias
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        gcc \
        libpq-dev \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Copia requirements.txt
COPY backend/requirements.txt .

# Instala NumPy compatível com Python 3.11 primeiro para evitar conflito com Pandas
RUN pip install --no-cache-dir "numpy>=1.26,<2" \
    && pip install --no-cache-dir -r requirements.txt

# Copia todo o código da aplicação
COPY backend/ ./app

# Expõe a porta da aplicação
EXPOSE 8000

# Comando para rodar o Uvicorn
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
