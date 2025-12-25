#!/usr/bin/env python3
"""
Скрипт для тестирования подключения к PostgreSQL из контейнера Airflow.
Выполняет тестовый SELECT запрос и выводит результат.
"""

import os
import sys
import psycopg2
from psycopg2 import OperationalError


def get_connection_params():
    """Получение параметров подключения из переменных окружения."""
    return {
        "host": os.environ.get("TEST_POSTGRES_HOST", "postgres"),
        "port": int(os.environ.get("TEST_POSTGRES_PORT", 5432)),
        "user": os.environ.get("TEST_POSTGRES_USER", "airflow"),
        "password": os.environ.get("TEST_POSTGRES_PASSWORD", "airflow"),
        "database": os.environ.get("TEST_POSTGRES_DB", "airflow"),
    }


def test_connection():
    """Тестирование подключения к PostgreSQL."""
    params = get_connection_params()
    
    print("=" * 60)
    print("Тестирование подключения к PostgreSQL")
    print("=" * 60)
    print(f"\nПараметры подключения:")
    print(f"  Host:     {params['host']}")
    print(f"  Port:     {params['port']}")
    print(f"  User:     {params['user']}")
    print(f"  Database: {params['database']}")
    print()
    
    try:
        # Подключение к PostgreSQL
        print("Подключение к базе данных...")
        connection = psycopg2.connect(
            host=params["host"],
            port=params["port"],
            user=params["user"],
            password=params["password"],
            database=params["database"],
            connect_timeout=10
        )
        
        print("✓ Подключение успешно установлено!")
        print()
        
        # Создание курсора
        cursor = connection.cursor()
        
        # Тест 1: Проверка версии PostgreSQL
        print("Тест 1: Получение версии PostgreSQL")
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        print(f"  ✓ Версия: {version}")
        print()
        
        # Тест 2: Проверка текущего времени
        print("Тест 2: Получение текущего времени сервера")
        cursor.execute("SELECT NOW();")
        current_time = cursor.fetchone()[0]
        print(f"  ✓ Время сервера: {current_time}")
        print()
        
        # Тест 3: Проверка списка баз данных
        print("Тест 3: Список доступных баз данных")
        cursor.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")
        databases = cursor.fetchall()
        for db in databases:
            print(f"  - {db[0]}")
        print()
        
        # Тест 4: Проверка текущего пользователя
        print("Тест 4: Информация о текущем пользователе")
        cursor.execute("SELECT current_user, current_database();")
        user_info = cursor.fetchone()
        print(f"  ✓ Текущий пользователь: {user_info[0]}")
        print(f"  ✓ Текущая база данных: {user_info[1]}")
        print()
        
        # Тест 5: Создание тестовой таблицы и вставка данных
        print("Тест 5: Создание тестовой таблицы и операции CRUD")
        
        # Создание таблицы
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS test_connection (
                id SERIAL PRIMARY KEY,
                message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        connection.commit()
        print("  ✓ Таблица test_connection создана/существует")
        
        # Вставка тестовой записи
        cursor.execute(
            "INSERT INTO test_connection (message) VALUES (%s) RETURNING id;",
            ("Тестовое подключение из Airflow",)
        )
        inserted_id = cursor.fetchone()[0]
        connection.commit()
        print(f"  ✓ Вставлена запись с ID: {inserted_id}")
        
        # Чтение данных
        cursor.execute("SELECT id, message, created_at FROM test_connection ORDER BY id DESC LIMIT 5;")
        rows = cursor.fetchall()
        print("  ✓ Последние 5 записей:")
        for row in rows:
            print(f"      ID: {row[0]}, Message: {row[1]}, Created: {row[2]}")
        print()
        
        # Закрытие соединения
        cursor.close()
        connection.close()
        
        print("=" * 60)
        print("✓ Все тесты пройдены успешно!")
        print("✓ PostgreSQL доступен из контейнера Airflow")
        print("=" * 60)
        
        return True
        
    except OperationalError as e:
        print(f"\n✗ Ошибка подключения к PostgreSQL:")
        print(f"  {e}")
        print("\nВозможные причины:")
        print("  - PostgreSQL контейнер не запущен")
        print("  - Неверные параметры подключения")
        print("  - Сетевые проблемы между контейнерами")
        return False
        
    except Exception as e:
        print(f"\n✗ Неожиданная ошибка:")
        print(f"  {e}")
        return False


if __name__ == "__main__":
    success = test_connection()
    sys.exit(0 if success else 1)

