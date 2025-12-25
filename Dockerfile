FROM apache/airflow:2.8.1-python3.11

USER root

# Установка системных зависимостей
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        gcc \
        python3-dev \
        libpq-dev \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Копируем и устанавливаем Python зависимости
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Копируем скрипты
COPY --chown=airflow:root scripts/ /opt/airflow/scripts/

