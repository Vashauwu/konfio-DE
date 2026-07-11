FROM python:3.11-slim-bookworm

# --- Dependencias de sistema: Java (requerido por Spark) y curl (para bajar el jar de Iceberg) ---
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless curl procps && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

WORKDIR /app

# --- Dependencias de Python ---
COPY requirements.txt requirements-dev.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# --- Runtime de Iceberg para Spark ---
# Se descarga en build time (no en runtime) para que el contenedor pueda
# ejecutarse offline una vez construido, y para evitar la latencia/fragilidad
# de resolver el paquete vía Ivy cada vez que arranca Spark.
ARG ICEBERG_VERSION=1.5.2
ARG SPARK_SCALA_VERSION=3.5_2.12
RUN mkdir -p /opt/spark-jars && \
    curl -fsSL -o /opt/spark-jars/iceberg-spark-runtime.jar \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_SCALA_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_SCALA_VERSION}-${ICEBERG_VERSION}.jar"

ENV ICEBERG_JAR_PATH=/opt/spark-jars/iceberg-spark-runtime.jar

# --- Código del pipeline ---
COPY config/ ./config/
COPY src/ ./src/
COPY tests/ ./tests/
COPY data/ ./data/

# Carpetas generadas por el pipeline (se pueden montar como volumen)
RUN mkdir -p /app/warehouse /app/events /app/reports

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

CMD ["python", "-m", "src.main"]
