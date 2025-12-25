# Airflow + PostgreSQL Docker Setup

Проект содержит Docker Compose конфигурацию для запуска Apache Airflow с PostgreSQL.

## Структура проекта

```
.
├── docker-compose.yml    # Docker Compose конфигурация
├── Dockerfile           # Кастомный образ Airflow
├── requirements.txt     # Python зависимости
├── .env                 # Переменные окружения
├── dags/                # DAG файлы Airflow
├── logs/                # Логи Airflow
├── plugins/             # Плагины Airflow
└── scripts/
    └── test_postgres_connection.py  # Скрипт проверки подключения
```

## Быстрый старт

### 1. Запуск сервисов

```bash
# Сборка и запуск всех контейнеров
docker-compose up -d --build

# Просмотр логов
docker-compose logs -f
```

### 2. Доступ к сервисам

- **Airflow Web UI**: http://localhost:8080
  - Логин: `airflow`
  - Пароль: `airflow`

- **PostgreSQL**: localhost:5433
  - User: `airflow`
  - Password: `airflow`
  - Database: `airflow`

### 3. Проверка подключения к PostgreSQL

```bash
# Запуск скрипта проверки из контейнера Airflow
docker-compose exec airflow-webserver python /opt/airflow/scripts/test_postgres_connection.py
```

## Полезные команды

```bash
# Остановка всех контейнеров
docker-compose down

# Полная очистка (включая volumes)
docker-compose down -v

# Просмотр статуса контейнеров
docker-compose ps

# Вход в контейнер Airflow
docker-compose exec airflow-webserver bash

# Подключение к PostgreSQL
docker-compose exec postgres psql -U airflow -d airflow
```

## Переменные окружения

Основные переменные можно настроить в файле `.env`:

| Переменная | Описание | По умолчанию |
|------------|----------|--------------|
| AIRFLOW_UID | UID пользователя Airflow | 50000 |
| _AIRFLOW_WWW_USER_USERNAME | Логин админа | airflow |
| _AIRFLOW_WWW_USER_PASSWORD | Пароль админа | airflow |

