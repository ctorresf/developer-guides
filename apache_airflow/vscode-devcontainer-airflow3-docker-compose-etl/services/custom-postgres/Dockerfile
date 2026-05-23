FROM postgres:18
# Adding pg_parquet: https://github.com/CrunchyData/pg_parquet/tree/v0.5.1

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    build-essential \
    pkg-config \
    libssl-dev \
    git \
    postgresql-server-dev-18

# Install rustup
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Install cargo-pgrx
RUN cargo install cargo-pgrx --version 0.16.1 --locked
# configure pgrx
RUN cargo pgrx init --pg18 $(which pg_config)

# Install pg_parquet
RUN git clone https://github.com/CrunchyData/pg_parquet.git
RUN cd pg_parquet && cargo pgrx install --release --features pg18

# create the extension in the database
#psql> "CREATE EXTENSION pg_parquet;"
# Cambiar al usuario postgres por seguridad antes de lanzar el servicio
USER postgres
# 👇 Modifica o agrega el CMD final para incluir el parámetro de configuración
CMD ["postgres", "-c", "shared_preload_libraries=pg_parquet"]