FROM mcr.microsoft.com/vscode/devcontainers/python:3.11

# Instalar dependencias de sistema necesarias para adaptadores de dbt (Postgres/Git)
RUN apt-get update && apt-get install -y \
    git \
    libpq-dev \
    && apt-get clean

# Actualizar pip e instalar dbt
RUN pip install --upgrade pip

# Seteamos el directorio de trabajo
WORKDIR /workspaces/voltio-dbt