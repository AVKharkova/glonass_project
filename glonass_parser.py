import os
import requests
import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Загрузка переменных окружения из .env файла
load_dotenv()

# Получение конфигурации из .env
API_URL = os.getenv('API_URL')
API_LOGIN = os.getenv('API_LOGIN')
API_PASSWORD = os.getenv('API_PASSWORD')

DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

def setup_database():
    """Создает таблицы в БД, если они не существуют."""
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
        )
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS vehicles (
                    id INT PRIMARY KEY,
                    guid UUID UNIQUE,
                    name VARCHAR(255),
                    imei VARCHAR(50) UNIQUE,
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS vehicle_tracks (
                    id BIGSERIAL PRIMARY KEY,
                    vehicle_id INT NOT NULL,
                    "timestamp" TIMESTAMP WITH TIME ZONE NOT NULL,
                    latitude DECIMAL(9, 6) NOT NULL,
                    longitude DECIMAL(9, 6) NOT NULL,
                    speed INT,
                    altitude INT,
                    course INT,
                    voltage DECIMAL(5, 2),
                    params_json JSONB,
                    CONSTRAINT fk_vehicle
                        FOREIGN KEY(vehicle_id) 
                        REFERENCES vehicles(id)
                        ON DELETE CASCADE
                );
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_vehicle_tracks_vehicle_id_timestamp 
                ON vehicle_tracks (vehicle_id, "timestamp" DESC);
            """)
        conn.commit()
        logging.info("База данных успешно настроена.")
    except psycopg2.Error as e:
        logging.error(f"Ошибка при настройке базы данных: {e}")
        raise
    finally:
        if conn:
            conn.close()

def get_api_token():
    """1. Аутентификация и получение токена."""
    auth_url = f"{API_URL}/api/v3/auth/login"
    payload = {"login": API_LOGIN, "password": API_PASSWORD}
    try:
        response = requests.post(auth_url, json=payload)
        response.raise_for_status()  # Вызовет исключение для кодов 4xx/5xx
        token = response.json().get('AuthId')
        logging.info("Токен авторизации успешно получен.")
        return token
    except requests.exceptions.RequestException as e:
        logging.error(f"Ошибка при аутентификации в API: {e}")
        return None

def get_all_vehicles(token):
    """2. Получение списка всех ТС."""
    vehicles_url = f"{API_URL}/api/v3/vehicles/find"
    headers = {"X-Auth": token, "Content-Type": "application/json"}
    try:
        response = requests.post(vehicles_url, headers=headers, json={})
        response.raise_for_status()
        vehicles = response.json()
        logging.info(f"Получено {len(vehicles)} транспортных средств.")
        return vehicles
    except requests.exceptions.RequestException as e:
        logging.error(f"Ошибка при получении списка ТС: {e}")
        return []

def sync_vehicles_to_db(vehicles_data):
    """Сохранение/обновление списка ТС в БД (используя UPSERT)."""
    if not vehicles_data:
        return
        
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )
    with conn.cursor() as cur:
        # Подготовка данных для вставки
        insert_data = [
            (
                v.get('vehicleId'),
                v.get('vehicleGuid'),
                v.get('name'),
                v.get('imei'),
                datetime.now(timezone.utc)
            ) for v in vehicles_data
        ]

        # Запрос UPSERT (INSERT ... ON CONFLICT ...)
        upsert_query = """
            INSERT INTO vehicles (id, guid, name, imei, updated_at)
            VALUES %s
            ON CONFLICT (id) DO UPDATE SET
                guid = EXCLUDED.guid,
                name = EXCLUDED.name,
                imei = EXCLUDED.imei,
                updated_at = EXCLUDED.updated_at;
        """
        psycopg2.extras.execute_values(cur, upsert_query, insert_data)
    conn.commit()
    conn.close()
    logging.info(f"Справочник ТС синхронизирован с базой данных.")

def fetch_and_save_tracks(token, vehicle_id):
    """3 & 4. Запрос треков за последние 24 часа и сохранение в БД."""
    tracks_url = f"{API_URL}/api/v3/terminalMessages"
    headers = {"X-Auth": token, "Content-Type": "application/json"}
    
    # Определяем период: последние 24 часа
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=24)
    
    # Форматируем время для API
    time_format = "%Y-%m-%dT%H:%M:%S.%fZ"
    payload = {
        "vehicleId": vehicle_id,
        "from": start_time.strftime(time_format),
        "to": end_time.strftime(time_format)
    }
    
    try:
        response = requests.post(tracks_url, headers=headers, json=payload)
        response.raise_for_status()
        
        # API возвращает список, в котором один элемент с ключом "messages"
        messages = response.json()[0].get("messages", [])
        if not messages:
            logging.info(f"Для ТС ID {vehicle_id} нет новых треков за последние 24 часа.")
            return

        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
        )
        with conn.cursor() as cur:
            # Подготовка данных для массовой вставки
            track_data = [
                (
                    vehicle_id,
                    m.get('serverTime'),
                    m.get('latitude'),
                    m.get('longitude'),
                    m.get('speed'),
                    m.get('altitude'),
                    m.get('course'),
                    m.get('voltage'),
                    psycopg2.extras.Json(m.get('parameters', {}))
                ) for m in messages
            ]
            
            # Массовая вставка
            insert_query = """
                INSERT INTO vehicle_tracks (
                    vehicle_id, "timestamp", latitude, longitude, speed, altitude, course, voltage, params_json
                ) VALUES %s;
            """
            psycopg2.extras.execute_values(cur, insert_query, track_data)
        
        conn.commit()
        conn.close()
        logging.info(f"Для ТС ID {vehicle_id} сохранено {len(messages)} точек трека.")

    except requests.exceptions.RequestException as e:
        logging.error(f"Ошибка при получении треков для ТС ID {vehicle_id}: {e}")
    except psycopg2.Error as e:
        logging.error(f"Ошибка при сохранении треков для ТС ID {vehicle_id} в БД: {e}")

def main():
    """Главная функция-оркестратор."""
    logging.info("Запуск скрипта парсера ГЛОНАССSoft...")
    
    # 1. Настройка БД
    setup_database()
    
    # 2. Аутентификация
    token = get_api_token()
    if not token:
        logging.critical("Не удалось получить токен API. Выполнение прервано.")
        return

    # 3. Получение и синхронизация списка ТС
    vehicles = get_all_vehicles(token)
    if not vehicles:
        logging.warning("Список ТС пуст. Дальнейшее выполнение невозможно.")
        return
    sync_vehicles_to_db(vehicles)

    # 4. Получение и сохранение треков для каждого ТС
    for vehicle in vehicles:
        vehicle_id = vehicle.get('vehicleId')
        if vehicle_id is not None:
            logging.info(f"--- Обработка ТС: {vehicle.get('name')} (ID: {vehicle_id}) ---")
            fetch_and_save_tracks(token, vehicle_id)
    
    logging.info("Работа скрипта парсера завершена.")


if __name__ == "__main__":
    main()
