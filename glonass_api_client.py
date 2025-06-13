import os
import json
import copy
import time
import logging
import requests
import sqlite3
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from typing import Type, List, Optional, Any, Dict, Union

# Для PostgreSQL
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection as SQLAConnection
import psycopg2

# Импортируем Pydantic модели
try:
    from glonass_schemas import (
        APIBaseModel,
        DeviceTypeSchema,
        SensorTypeSchema,
        VehicleMileageMotohoursDataSchema,
        MileageMotohoursPeriodSchema,
        VehicleFuelConsumptionDataSchema,
        FuelConsumptionPeriodSchema,
        VehicleFuelInOutDataSchema,
        FuelEventSchema,
        VehicleMoveStopDataSchema,
        MoveEventSchema,
        StopEventSchema,
        LastDataObjectSchema,
        GeozoneInfoSchema,
        DriverInfoSchema,
        AuthLoginResponseSchema,
        VehicleListItemSchema,
        VehicleDetailResponseSchema,
        VehicleCustomFieldSchema,
        VehicleCountersSchema,
        VehicleSensorSchema,
        VehicleSensorGradeTableSchema,
        VehicleSensorGradeSchema,
        VehicleDriverSchema,
        VehicleStatusHistoryItemSchema,
        VehicleCMSV6ParametersSchema,
        VehicleCommandTemplateSchema,
        VehicleInspectionTaskSchema,
    )
    from pydantic import ValidationError, BaseModel as PydanticBaseModel
except ImportError as e:
    print(f"Ошибка: Не удалось импортировать модели: {e}")
    print(
        "Убедитесь, что файл с моделями (например, glonass_schemas.py) существует и доступен, и содержит все необходимые классы."
    )
    exit(1)

# --- Конфигурация ---
load_dotenv()

API_LOGIN = os.getenv("API_LOGIN")
API_PASSWORD = os.getenv("API_PASSWORD")
BASE_URL = os.getenv("API_BASE_URL", "https://hosting.glonasssoft.ru/api/v3")

REQUEST_DELAY_SECONDS = 5
DETAIL_REQUEST_DELAY_SECONDS = 7
MAX_RETRIES = 5
RETRY_DELAY_BASE_SECONDS = 10
DAYS_FOR_REPORTS = 30
API_TIMEOUT_SECONDS = 120

LOG_FILE = "api_calls_ru_validated_db.log"

# --- Настройки БД ---
DB_TYPE = os.getenv("DB_TYPE", "sqlite").lower()
SQLITE_DB_FILE = os.getenv("SQLITE_DB_FILE", "glonass_data.sqlite")

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", 5432))
PG_DB_NAME = os.getenv("PG_DB_NAME", "glonass_db")
PG_USER = os.getenv("PG_USER", "glonass_user")
PG_PASSWORD = os.getenv("PG_PASSWORD", "glonass_password")


# --- Настройка логирования ---
logger = logging.getLogger("GlonassAPIClientDB")
logger.setLevel(logging.INFO)
fh = logging.FileHandler(LOG_FILE, mode="w", encoding="utf-8")
fh.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
fh.setFormatter(formatter)
ch.setFormatter(formatter)
logger.addHandler(fh)
logger.addHandler(ch)

# Глобальный словарь для маппинга имен типов сенсоров на их ID
SENSOR_TYPE_NAME_TO_ID_MAP: Dict[str, int] = {}

# Тип соединения, которое может возвращать get_db_connection
DBConnection = Union[sqlite3.Connection, SQLAConnection]


# --- Вспомогательные функции для генерации SQL (глобальные) ---
def get_pk_autoincrement_sql(col_name: str = "id") -> str:
    if DB_TYPE == "sqlite":
        return f"{col_name} INTEGER PRIMARY KEY AUTOINCREMENT"
    else:  # PostgreSQL
        return f"{col_name} BIGSERIAL PRIMARY KEY"


def get_text_sql_type() -> str:
    return "TEXT"


def get_boolean_sql_type() -> str:
    return "INTEGER"


def quote_identifier(name: str) -> str:
    return f'"{name}"' if DB_TYPE == "postgres" else name


# --- Функции работы с БД ---
def get_db_connection() -> Optional[DBConnection]:
    """Устанавливает соединение с БД SQLite или PostgreSQL."""
    conn = None
    if DB_TYPE == "sqlite":
        try:
            conn = sqlite3.connect(SQLITE_DB_FILE)
            conn.row_factory = sqlite3.Row
            logger.debug(f"Установлено соединение с SQLite: {SQLITE_DB_FILE}")
            conn.execute("PRAGMA foreign_keys = ON")
            return conn
        except sqlite3.Error as e:
            logger.error(f"Ошибка соединения с SQLite: {e}")
            return None
    elif DB_TYPE == "postgres":
        try:
            db_url = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB_NAME}"
            engine = create_engine(db_url)
            conn = engine.connect()
            logger.debug(
                f"Установлено соединение с PostgreSQL: {PG_DB_NAME}@{PG_HOST}:{PG_PORT}"
            )
            return conn
        except psycopg2.Error as e:
            logger.error(f"Ошибка соединения с PostgreSQL: {e}")
            return None
        except Exception as e:
            logger.error(
                f"Неизвестная ошибка при соединении с PostgreSQL: {e}")
            return None
    else:
        logger.error(f"Неизвестный тип базы данных в DB_TYPE: {DB_TYPE}")
        return None


def create_tables(conn: DBConnection):
    """
    Создает таблицы в БД, если они не существуют.
    Адаптирует SQL синтаксис под SQLite или PostgreSQL.
    """
    if not conn:
        return

    is_sqlite = isinstance(conn, sqlite3.Connection)

    try:
        # Справочники (должны создаваться и заполняться ПЕРВЫМИ)
        conn.execute(
            text(
                f"""CREATE TABLE IF NOT EXISTS {quote_identifier('device_types')} ({quote_identifier('deviceTypeId')} INTEGER PRIMARY KEY, {quote_identifier('deviceTypeName')} {get_text_sql_type()}, {quote_identifier('retrieved_at')} {get_text_sql_type()} NOT NULL)"""
            )
        )
        conn.execute(
            text(
                f"""CREATE TABLE IF NOT EXISTS {quote_identifier('sensor_types')} ({quote_identifier('id')} INTEGER PRIMARY KEY, {quote_identifier('name')} {get_text_sql_type()} UNIQUE, {quote_identifier('description')} {get_text_sql_type()}, {quote_identifier('retrieved_at')} {get_text_sql_type()} NOT NULL)"""
            )
        )
        conn.execute(
            text(
                f"""CREATE TABLE IF NOT EXISTS {quote_identifier('drivers')} ({quote_identifier('id')} {get_text_sql_type()} PRIMARY KEY, {quote_identifier('name')} {get_text_sql_type()}, {quote_identifier('description')} {get_text_sql_type()}, {quote_identifier('hiredate')} {get_text_sql_type()}, {quote_identifier('chopdate')} {get_text_sql_type()}, {quote_identifier('exclusive')} {get_boolean_sql_type()}, {quote_identifier('parentId')} {get_text_sql_type()}, {quote_identifier('deleted')} {get_boolean_sql_type()}, {quote_identifier('retrieved_at')} {get_text_sql_type()} NOT NULL)"""
            )
        )

        # Основная таблица ТС
        conn.execute(
            text(
                f"""
            CREATE TABLE IF NOT EXISTS {quote_identifier('vehicle_details')} (
                {quote_identifier('vehicleId')} INTEGER PRIMARY KEY, {quote_identifier('vehicleGuid')} {get_text_sql_type()}, {quote_identifier('name')} {get_text_sql_type()}, {quote_identifier('imei')} {get_text_sql_type()},
                {quote_identifier('deviceTypeId')} INTEGER, {quote_identifier('deviceTypeName')} {get_text_sql_type()}, {quote_identifier('sim1')} {get_text_sql_type()}, {quote_identifier('sim2')} {get_text_sql_type()},
                {quote_identifier('parentId')} {get_text_sql_type()}, {quote_identifier('parentName')} {get_text_sql_type()}, {quote_identifier('modelId')} {get_text_sql_type()}, {quote_identifier('modelName')} {get_text_sql_type()},
                {quote_identifier('unitId')} {get_text_sql_type()}, {quote_identifier('unitName')} {get_text_sql_type()}, {quote_identifier('status')} INTEGER, {quote_identifier('createdAt')} {get_text_sql_type()},
                {quote_identifier('consumptionPer100Km')} {get_text_sql_type()}, {quote_identifier('consumptionPerHour')} {get_text_sql_type()}, {quote_identifier('locationByCellId')} {get_boolean_sql_type()},
                {quote_identifier('counter_mileage')} REAL, {quote_identifier('counter_motohours')} REAL,
                {quote_identifier('counter_mileageTime')} {get_text_sql_type()}, {quote_identifier('counter_motohoursTime')} {get_text_sql_type()},
                {quote_identifier('showLineTrackWhenNoCoords')} {get_boolean_sql_type()}, {quote_identifier('IsSackEnabled')} {get_boolean_sql_type()}, {quote_identifier('consumptionIdle')} {get_text_sql_type()},
                {quote_identifier('consumptionPer100KmSeasonal')} REAL, {quote_identifier('consumptionPerHourSeasonal')} REAL, {quote_identifier('consumptionIdleSeasonal')} REAL,
                {quote_identifier('consumptionPer100KmSeasonalBegin')} {get_text_sql_type()}, {quote_identifier('consumptionPer100KmSeasonalEnd')} {get_text_sql_type()},
                {quote_identifier('consumptionPerHourSeasonalBegin')} {get_text_sql_type()}, {quote_identifier('consumptionPerHourSeasonalEnd')} {get_text_sql_type()},
                {quote_identifier('consumptionIdleSeasonalBegin')} {get_text_sql_type()}, {quote_identifier('consumptionIdleSeasonalEnd')} {get_text_sql_type()},
                {quote_identifier('mileageCalcMethod')} {get_text_sql_type()}, {quote_identifier('mileageCoeff')} REAL, {quote_identifier('dottedLineTrackWhenNoCoords')} {get_boolean_sql_type()},
                {quote_identifier('highlightSensorGuid')} {get_text_sql_type()}, {quote_identifier('motohoursCalcMethod')} {get_text_sql_type()},
                {quote_identifier('retrieved_at')} {get_text_sql_type()} NOT NULL,
                FOREIGN KEY ({quote_identifier('deviceTypeId')}) REFERENCES {quote_identifier('device_types')}({quote_identifier('deviceTypeId')}) ON DELETE SET NULL ON UPDATE CASCADE
            )
        """
            )
        )
        # Связанные с vehicle_details таблицы
        conn.execute(
            text(
                f"""CREATE TABLE IF NOT EXISTS {quote_identifier('vehicle_custom_fields_detail')} ({get_pk_autoincrement_sql()}, {quote_identifier('vehicleId')} INTEGER NOT NULL, {quote_identifier('custom_field_id')} {get_text_sql_type()}, {quote_identifier('name')} {get_text_sql_type()}, {quote_identifier('value_text')} {get_text_sql_type()}, {quote_identifier('forTooltip')} {get_boolean_sql_type()}, {quote_identifier('retrieved_at')} {get_text_sql_type()} NOT NULL, FOREIGN KEY ({quote_identifier('vehicleId')}) REFERENCES {quote_identifier('vehicle_details')}({quote_identifier('vehicleId')}) ON DELETE CASCADE, UNIQUE({quote_identifier('vehicleId')}, {quote_identifier('custom_field_id')}))"""
            )
        )

        conn.execute(
            text(
                f"""
            CREATE TABLE IF NOT EXISTS {quote_identifier('vehicle_sensors_detail')} (
                {get_pk_autoincrement_sql()},
                {quote_identifier('vehicleId')} INTEGER NOT NULL,
                {quote_identifier('sensor_id')} {get_text_sql_type()},
                {quote_identifier('name')} {get_text_sql_type()},
                {quote_identifier('type_str')} {get_text_sql_type()},
                {quote_identifier('sensor_type_id')} INTEGER,
                {quote_identifier('inputType')} {get_text_sql_type()},
                {quote_identifier('inputNumber')} INTEGER,
                {quote_identifier('pseudonym')} {get_text_sql_type()},
                {quote_identifier('isInverted')} {get_boolean_sql_type()},
                {quote_identifier('disabled')} {get_boolean_sql_type()},
                {quote_identifier('showInTooltip')} {get_boolean_sql_type()},
                {quote_identifier('showLastValid')} {get_boolean_sql_type()},
                {quote_identifier('gradeType')} {get_text_sql_type()},
                {quote_identifier('gradesTables_json')} {get_text_sql_type()},
                {quote_identifier('kind')} {get_text_sql_type()},
                {quote_identifier('color')} {get_text_sql_type()},
                {quote_identifier('showAsDutOnGraph')} {get_boolean_sql_type()},
                {quote_identifier('showWithoutIgn')} {get_boolean_sql_type()},
                {quote_identifier('agrFunction')} {get_text_sql_type()},
                {quote_identifier('expr')} {get_text_sql_type()},
                {quote_identifier('customParams_json')} {get_text_sql_type()},
                {quote_identifier('summaryMaxValue_text')} {get_text_sql_type()},
                {quote_identifier('valueIntervals_json')} {get_text_sql_type()},
                {quote_identifier('disableEmissionsValidation')} {get_boolean_sql_type()},
                {quote_identifier('unitOfMeasure')} INTEGER,
                {quote_identifier('medianDegree')} INTEGER,
                {quote_identifier('retrieved_at')} {get_text_sql_type()} NOT NULL,
                FOREIGN KEY ({quote_identifier('vehicleId')}) REFERENCES {quote_identifier('vehicle_details')}({quote_identifier('vehicleId')}) ON DELETE CASCADE,
                FOREIGN KEY ({quote_identifier('sensor_type_id')}) REFERENCES {quote_identifier('sensor_types')}({quote_identifier('id')}) ON DELETE SET NULL ON UPDATE CASCADE,
                UNIQUE({quote_identifier('vehicleId')}, {quote_identifier('sensor_id')})
            )
        """
            )
        )
        conn.execute(
            text(
                f"""CREATE TABLE IF NOT EXISTS {quote_identifier('vehicle_drivers_assigned')} ({get_pk_autoincrement_sql()}, {quote_identifier('vehicleId')} INTEGER NOT NULL, {quote_identifier('driver_id')} {get_text_sql_type()}, {quote_identifier('name')} {get_text_sql_type()}, {quote_identifier('isDefault')} {get_boolean_sql_type()}, {quote_identifier('retrieved_at')} {get_text_sql_type()} NOT NULL, FOREIGN KEY ({quote_identifier('vehicleId')}) REFERENCES {quote_identifier('vehicle_details')}({quote_identifier('vehicleId')}) ON DELETE CASCADE, UNIQUE({quote_identifier('vehicleId')}, {quote_identifier('driver_id')}))"""
            )
        )

        conn.execute(
            text(
                f"""
            CREATE TABLE IF NOT EXISTS {quote_identifier('vehicle_status_history_items')} (
                {get_pk_autoincrement_sql()}, {quote_identifier('vehicleId')} INTEGER NOT NULL,
                {quote_identifier('status')} INTEGER, {quote_identifier('date')} {get_text_sql_type()}, {quote_identifier('description')} {get_text_sql_type()}, {quote_identifier('additionalInfo')} {get_text_sql_type()},
                {quote_identifier('retrieved_at')} {get_text_sql_type()} NOT NULL,
                UNIQUE({quote_identifier('vehicleId')}, {quote_identifier('date')}, {quote_identifier('status')}, {quote_identifier('description')}, {quote_identifier('additionalInfo')}),
                FOREIGN KEY ({quote_identifier('vehicleId')}) REFERENCES {quote_identifier('vehicle_details')}({quote_identifier('vehicleId')}) ON DELETE CASCADE
            )
        """
            )
        )
        conn.execute(
            text(
                f"""CREATE TABLE IF NOT EXISTS {quote_identifier('vehicle_cmsv6_params')}({quote_identifier('vehicleId')} INTEGER PRIMARY KEY, {quote_identifier('cms_id')} {get_text_sql_type()}, {quote_identifier('enabled')} {get_boolean_sql_type()}, {quote_identifier('host')} {get_text_sql_type()}, {quote_identifier('login')} {get_text_sql_type()}, {quote_identifier('password')} {get_text_sql_type()}, {quote_identifier('retrieved_at')} {get_text_sql_type()} NOT NULL, FOREIGN KEY ({quote_identifier('vehicleId')}) REFERENCES {quote_identifier('vehicle_details')}({quote_identifier('vehicleId')}) ON DELETE CASCADE)"""
            )
        )
        conn.execute(
            text(
                f"""CREATE TABLE IF NOT EXISTS {quote_identifier('vehicle_command_templates')}({get_pk_autoincrement_sql()}, {quote_identifier('vehicleId')} INTEGER NOT NULL, {quote_identifier('command_template_id')} {get_text_sql_type()} NOT NULL, {quote_identifier('name')} {get_text_sql_type()}, {quote_identifier('command')} {get_text_sql_type()}, {quote_identifier('retries')} INTEGER, {quote_identifier('retrieved_at')} {get_text_sql_type()} NOT NULL, FOREIGN KEY ({quote_identifier('vehicleId')}) REFERENCES {quote_identifier('vehicle_details')}({quote_identifier('vehicleId')}) ON DELETE CASCADE, UNIQUE ({quote_identifier('vehicleId')}, {quote_identifier('command_template_id')}))"""
            )
        )
        conn.execute(
            text(
                f"""CREATE TABLE IF NOT EXISTS {quote_identifier('vehicle_inspection_tasks')}({get_pk_autoincrement_sql()}, {quote_identifier('vehicleId')} INTEGER NOT NULL, {quote_identifier('task_id')} {get_text_sql_type()} NOT NULL, {quote_identifier('enabled')} {get_boolean_sql_type()}, {quote_identifier('name')} {get_text_sql_type()}, {quote_identifier('description')} {get_text_sql_type()}, {quote_identifier('mileageCondition')} REAL, {quote_identifier('lastMileage')} REAL, {quote_identifier('motohoursCondition')} REAL, {quote_identifier('lastMotohours')} REAL, {quote_identifier('periodicCondition')} INTEGER, {quote_identifier('kind')} {get_text_sql_type()}, {quote_identifier('lastInspectionDate')} {get_text_sql_type()}, {quote_identifier('maxQuantity')} INTEGER, {quote_identifier('retrieved_at')} {get_text_sql_type()} NOT NULL, FOREIGN KEY ({quote_identifier('vehicleId')}) REFERENCES {quote_identifier('vehicle_details')}({quote_identifier('vehicleId')}) ON DELETE CASCADE, UNIQUE ({quote_identifier('vehicleId')}, {quote_identifier('task_id')}))"""
            )
        )

        # Таблицы отчетов, ссылающиеся на vehicle_details
        conn.execute(
            text(
                f"""CREATE TABLE IF NOT EXISTS {quote_identifier('last_data')} ({quote_identifier('vehicleId')} INTEGER PRIMARY KEY, {quote_identifier('vehicleGuid')} {get_text_sql_type()}, {quote_identifier('vehicleNumber')} {get_text_sql_type()}, {quote_identifier('receiveTime')} {get_text_sql_type()}, {quote_identifier('recordTime')} {get_text_sql_type()}, {quote_identifier('state')} INTEGER, {quote_identifier('speed')} REAL, {quote_identifier('course')} INTEGER, {quote_identifier('latitude')} REAL, {quote_identifier('longitude')} REAL, {quote_identifier('address')} {get_text_sql_type()}, {quote_identifier('geozones')} {get_text_sql_type()}, {quote_identifier('retrieved_at')} {get_text_sql_type()} NOT NULL, FOREIGN KEY ({quote_identifier('vehicleId')}) REFERENCES {quote_identifier('vehicle_details')}({quote_identifier('vehicleId')}) ON DELETE CASCADE)"""
            )
        )
        conn.execute(
            text(
                f"""CREATE TABLE IF NOT EXISTS {quote_identifier('mileage_motohours')} ({get_pk_autoincrement_sql()}, {quote_identifier('vehicleId')} INTEGER, {quote_identifier('period_start')} {get_text_sql_type()}, {quote_identifier('period_end')} {get_text_sql_type()}, {quote_identifier('mileage')} REAL, {quote_identifier('mileageBegin')} REAL, {quote_identifier('mileageEnd')} REAL, {quote_identifier('motohours')} REAL, {quote_identifier('motohoursBegin')} REAL, {quote_identifier('motohoursEnd')} REAL, {quote_identifier('idlingTime')} REAL, {quote_identifier('retrieved_at')} {get_text_sql_type()} NOT NULL, FOREIGN KEY ({quote_identifier('vehicleId')}) REFERENCES {quote_identifier('vehicle_details')}({quote_identifier('vehicleId')}) ON DELETE CASCADE, UNIQUE({quote_identifier('vehicleId')}, {quote_identifier('period_start')}, {quote_identifier('period_end')}))"""
            )
        )
        conn.execute(
            text(
                f"""CREATE TABLE IF NOT EXISTS {quote_identifier('fuel_consumption')} ({get_pk_autoincrement_sql()}, {quote_identifier('vehicleId')} INTEGER, {quote_identifier('period_start')} {get_text_sql_type()}, {quote_identifier('period_end')} {get_text_sql_type()}, {quote_identifier('fuelLevelStart')} REAL, {quote_identifier('fuelLevelEnd')} REAL, {quote_identifier('fuelTankLevelStart')} REAL, {quote_identifier('fuelTankLevelEnd')} REAL, {quote_identifier('fuelConsumption')} REAL, {quote_identifier('fuelConsumptionMove')} REAL, {quote_identifier('fuelConsumptionFactTank')} REAL, {quote_identifier('retrieved_at')} {get_text_sql_type()} NOT NULL, FOREIGN KEY ({quote_identifier('vehicleId')}) REFERENCES {quote_identifier('vehicle_details')}({quote_identifier('vehicleId')}) ON DELETE CASCADE, UNIQUE({quote_identifier('vehicleId')}, {quote_identifier('period_start')}, {quote_identifier('period_end')}))"""
            )
        )
        conn.execute(
            text(
                f"""CREATE TABLE IF NOT EXISTS {quote_identifier('fuel_events')} ({get_pk_autoincrement_sql()}, {quote_identifier('vehicleId')} INTEGER, {quote_identifier('report_period_start')} {get_text_sql_type()}, {quote_identifier('report_period_end')} {get_text_sql_type()}, {quote_identifier('vehicleModel')} {get_text_sql_type()}, {quote_identifier('event_type')} {get_text_sql_type()}, {quote_identifier('event_startDate')} {get_text_sql_type()}, {quote_identifier('event_endDate')} {get_text_sql_type()}, {quote_identifier('valueFuel')} REAL, {quote_identifier('fuelStart')} REAL, {quote_identifier('fuelEnd')} REAL, {quote_identifier('retrieved_at')} {get_text_sql_type()} NOT NULL, FOREIGN KEY ({quote_identifier('vehicleId')}) REFERENCES {quote_identifier('vehicle_details')}({quote_identifier('vehicleId')}) ON DELETE CASCADE, UNIQUE({quote_identifier('vehicleId')}, {quote_identifier('event_startDate')}, {quote_identifier('event_type')}))"""
            )
        )
        conn.execute(
            text(
                f"""CREATE TABLE IF NOT EXISTS {quote_identifier('move_events')} ({get_pk_autoincrement_sql()}, {quote_identifier('vehicleId')} INTEGER, {quote_identifier('mileage')} REAL, {quote_identifier('eventId')} INTEGER, {quote_identifier('eventName')} {get_text_sql_type()}, {quote_identifier('event_start')} {get_text_sql_type()}, {quote_identifier('event_end')} {get_text_sql_type()}, {quote_identifier('duration')} INTEGER, {quote_identifier('retrieved_at')} {get_text_sql_type()} NOT NULL, FOREIGN KEY ({quote_identifier('vehicleId')}) REFERENCES {quote_identifier('vehicle_details')}({quote_identifier('vehicleId')}) ON DELETE CASCADE, UNIQUE({quote_identifier('vehicleId')}, {quote_identifier('event_start')}, {quote_identifier('eventId')}))"""
            )
        )
        conn.execute(
            text(
                f"""CREATE TABLE IF NOT EXISTS {quote_identifier('stop_events')} ({get_pk_autoincrement_sql()}, {quote_identifier('vehicleId')} INTEGER, {quote_identifier('address')} {get_text_sql_type()}, {quote_identifier('eventId')} INTEGER, {quote_identifier('eventName')} {get_text_sql_type()}, {quote_identifier('event_start')} {get_text_sql_type()}, {quote_identifier('event_end')} {get_text_sql_type()}, {quote_identifier('duration')} INTEGER, {quote_identifier('retrieved_at')} {get_text_sql_type()} NOT NULL, FOREIGN KEY ({quote_identifier('vehicleId')}) REFERENCES {quote_identifier('vehicle_details')}({quote_identifier('vehicleId')}) ON DELETE CASCADE, UNIQUE({quote_identifier('vehicleId')}, {quote_identifier('event_start')}, {quote_identifier('eventId')}))"""
            )
        )

        if is_sqlite:
            conn.commit()
        logger.info("Таблицы в БД успешно созданы/проверены.")
    except Exception as e:
        logger.error(f"Ошибка при создании таблиц в БД: {e}")
        raise  # Перевыбрасываем исключение


def insert_data(conn: DBConnection, table_name: str, data: dict):
    if not conn or not data:
        return
    data_copy = data.copy()
    data_copy["retrieved_at"] = datetime.now(timezone.utc).isoformat()

    is_sqlite = isinstance(conn, sqlite3.Connection)

    quoted_table_name = quote_identifier(table_name)
    columns = ", ".join(quote_identifier(col) for col in data_copy.keys())

    # Адаптация INSERT/REPLACE для PostgreSQL (используя ON CONFLICT)
    if not is_sqlite:
        unique_cols_map = {  # Карта уникальных колонок для ON CONFLICT
            "device_types": ["deviceTypeId"],
            "sensor_types": ["id"],
            "drivers": ["id"],
            "vehicle_details": ["vehicleId"],
            "vehicle_custom_fields_detail": ["vehicleId", "custom_field_id"],
            "vehicle_sensors_detail": ["vehicleId", "sensor_id"],
            "vehicle_drivers_assigned": ["vehicleId", "driver_id"],
            "vehicle_status_history_items": [
                "vehicleId",
                "date",
                "status",
                "description",
                "additionalInfo",
            ],
            "vehicle_cmsv6_params": ["vehicleId"],
            "vehicle_command_templates": ["vehicleId", "command_template_id"],
            "vehicle_inspection_tasks": ["vehicleId", "task_id"],
            "last_data": ["vehicleId"],
            "mileage_motohours": ["vehicleId", "period_start", "period_end"],
            "fuel_consumption": ["vehicleId", "period_start", "period_end"],
            "fuel_events": ["vehicleId", "event_startDate", "event_type"],
            "move_events": ["vehicleId", "event_start", "eventId"],
            "stop_events": ["vehicleId", "event_start", "eventId"],
        }

        unique_cols = unique_cols_map.get(table_name)

        if unique_cols:
            conflict_cols = ", ".join(quote_identifier(col)
                                      for col in unique_cols)
            # Исключаем PK из списка обновляемых полей
            update_set_parts = [
                f"{quote_identifier(k)} = EXCLUDED.{quote_identifier(k)}"
                for k in data_copy.keys()
                if k not in unique_cols
            ]

            if (
                not update_set_parts
            ):  # Если обновлять нечего, кроме PK/UNIQUE (например, таблица справочника)
                sql = f"INSERT INTO {quoted_table_name} ({columns}) VALUES ({', '.join(f':{k}' for k in data_copy.keys())}) ON CONFLICT ({conflict_cols}) DO NOTHING"
            else:
                update_set = ", ".join(update_set_parts)
                sql = f"INSERT INTO {quoted_table_name} ({columns}) VALUES ({', '.join(f':{k}' for k in data_copy.keys())}) ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_set}"

            # для SQLAlchemy `text()` с именованными параметрами,
            # значения передаются словарем
            params_to_execute = data_copy

        else:  # Если нет уникальных ключей, то просто INSERT (без ON CONFLICT)
            sql = f"INSERT INTO {quoted_table_name} ({columns}) VALUES ({', '.join(f':{k}' for k in data_copy.keys())})"
            logger.warning(
                f"Таблица {table_name} не имеет определённых уникальных ключей для ON CONFLICT. Используется простой INSERT. Возможны ошибки дублирования."
            )
            params_to_execute = data_copy

    else:  # SQLite (INSERT OR REPLACE)
        placeholders = ", ".join("?" * len(data_copy))
        sql = f"INSERT OR REPLACE INTO {quoted_table_name} ({columns}) VALUES ({placeholders})"
        # Для SQLite значения передаются списком
        params_to_execute = list(data_copy.values())

    try:
        if is_sqlite:
            cursor = conn.cursor()
            cursor.execute(
                sql, params_to_execute
            )  
            conn.commit()
        else:  # PostgreSQL (SQLAlchemy)
            conn.execute(text(sql), params_to_execute)
            conn.commit()

        log_id_val = next(
            (
                data_copy[key]
                for key in [
                    "id",
                    "vehicleId",
                    "deviceTypeId",
                    "custom_field_id",
                    "sensor_id",
                    "driver_id",
                    "command_template_id",
                    "task_id",
                ]
                if key in data_copy
            ),
            "N/A",
        )
        logger.debug(
            f"Данные успешно вставлены/обновлены в таблицу {table_name} (ID/ключ: {log_id_val})"
        )
    except Exception as e:
        logger.error(
            f"Ошибка при вставке данных в таблицу {table_name}: {e}\nSQL: {sql}\nДанные: {data_copy}"
        )
        if not is_sqlite:
            try:
                conn.rollback()
            except Exception as rb_e:
                logger.error(f"Ошибка при rollback транзакции: {rb_e}")


# --- Вспомогательные функции для API запросов ---
def pretty_print_json(data: Any) -> str:
    if isinstance(data, (dict, list)):
        try:
            return json.dumps(data, indent=2, ensure_ascii=False)
        except TypeError:
            return str(data)
    return str(data)


def make_api_request(
    method: str,
    endpoint_path: str,
    token: Optional[str] = None,
    json_data: Optional[Union[dict, list]] = None,
    params: Optional[dict] = None,
    response_model: Optional[Type[PydanticBaseModel]] = None,
    response_list_model: Optional[Type[PydanticBaseModel]] = None,
    current_retries: int = 0,
) -> Optional[Union[PydanticBaseModel, List[PydanticBaseModel], dict, list, str]]:
    url = f"{BASE_URL}{endpoint_path}"
    headers = {"Content-Type": "application/json"}
    if token:
        headers["X-Auth"] = token
    log_message_req = f"Запрос {method} {url}\nЗаголовки: {pretty_print_json(headers)}"
    if json_data is not None:
        log_message_req += f"\nТело запроса: {pretty_print_json(json_data)}"
    if params:
        log_message_req += f"\nПараметры URL: {pretty_print_json(params)}"
    logger.info(log_message_req)
    try:
        response = requests.request(
            method,
            url,
            headers=headers,
            json=json_data,
            params=params,
            timeout=API_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        raw_response_data: Any = None
        try:
            raw_response_data = response.json()
            log_message_resp_body = pretty_print_json(raw_response_data)
        except json.JSONDecodeError:
            raw_response_data = response.text
            log_message_resp_body = (
                f"(Не JSON): {response.text[:500]}..."
                if response.text
                else "(Пустой ответ)"
            )
        logger.info(
            f"Ответ от {method} {url}\nСтатус: {response.status_code}\nТело ответа: {log_message_resp_body}"
        )
        if raw_response_data is not None and (
                response_model or response_list_model):
            try:
                if response_list_model:
                    if not isinstance(raw_response_data, list):
                        logger.error(
                            f"Ожидался список для {response_list_model.__name__}, получен {type(raw_response_data)}. Ответ: {str(raw_response_data)[:200]}"
                        )
                        return None
                    validated_data = [
                        response_list_model.model_validate(item)
                        for item in raw_response_data
                    ]
                    logger.info(
                        f"Ответ успешно валидирован с использованием списка {response_list_model.__name__} ({len(validated_data)} элементов)."
                    )
                    return validated_data
                elif response_model:
                    validated_data = response_model.model_validate(
                        raw_response_data)
                    logger.info(
                        f"Ответ успешно валидирован с использованием {response_model.__name__}."
                    )
                    return validated_data
            except ValidationError as e:
                logger.error(
                    f"Ошибка валидации Pydantic для {method} {url}:\n{e}")
                problematic_data_excerpt = str(
                    e.errors(include_input=True))[:1000]
                logger.debug(
                    f"Проблемные данные (часть): {problematic_data_excerpt}")
                return None
        return raw_response_data
    except requests.exceptions.HTTPError as e:
        logger.error(
            f"HTTPError для {method} {url}: {e.response.status_code} - {e.response.text[:500]}"
        )
        if e.response.status_code == 429 and current_retries < MAX_RETRIES:
            retry_after_header = e.response.headers.get("Retry-After")
            wait_time = RETRY_DELAY_BASE_SECONDS * (2**current_retries)
            if retry_after_header and retry_after_header.isdigit():
                wait_time = max(wait_time, int(retry_after_header))
            logger.warning(
                f"Получен статус 429. Попытка {current_retries + 1}/{MAX_RETRIES}. Ожидание {wait_time} секунд..."
            )
            time.sleep(wait_time)
            return make_api_request(
                method,
                endpoint_path,
                token,
                json_data,
                params,
                response_model,
                response_list_model,
                current_retries + 1,
            )
        elif e.response.status_code == 429:
            logger.error(
                f"Статус 429. Превышено макс. кол-во попыток ({MAX_RETRIES}).")
    except requests.exceptions.Timeout:
        logger.error(
            f"TimeoutError для {method} {url} (timeout={API_TIMEOUT_SECONDS}s)"
        )
    except requests.exceptions.RequestException as e:
        logger.error(f"RequestException для {method} {url}: {e}")
    return None


# --- Функции логики приложения ---
def authenticate() -> Optional[str]:
    """Выполняет аутентификацию и возвращает токен."""
    logger.info("Попытка аутентификации...")
    payload = {"login": API_LOGIN, "password": API_PASSWORD}
    response_data = make_api_request(
        "POST", "/auth/login", json_data=payload, response_model=AuthLoginResponseSchema
    )
    if response_data and isinstance(response_data, AuthLoginResponseSchema):
        logger.info(
            f"Аутентификация успешна. Пользователь: {response_data.User}")
        return response_data.AuthId
    logger.error(
        "Аутентификация не удалась или ответ не соответствует модели.")
    return None


def get_all_vehicles_with_data(token: str) -> List[VehicleListItemSchema]:
    """Получает список ВСЕХ доступных транспортных средств с их основными данными."""
    logger.info("Запрос списка ВСЕХ доступных транспортных средств с данными...")
    find_payload_for_all_vehicles: Dict[str, Any] = {}
    response_data = make_api_request(
        method="POST",
        endpoint_path="/vehicles/find",
        token=token,
        json_data=find_payload_for_all_vehicles,
        response_list_model=VehicleListItemSchema,
    )
    if response_data and isinstance(response_data, list):
        valid_vehicles = [
            item for item in response_data if isinstance(item, VehicleListItemSchema)
        ]
        if len(valid_vehicles) != len(response_data):
            logger.warning(
                f"Не все элементы в ответе /vehicles/find являются VehicleListItemSchema. Получено {len(valid_vehicles)} из {len(response_data)}."
            )
        logger.info(f"Получено {len(valid_vehicles)} ТС из /vehicles/find.")
        return valid_vehicles
    logger.error(
        f"Не удалось получить или валидировать список ТС из /vehicles/find. Ответ: {type(response_data)}"
    )
    return []


def save_vehicle_detail_data(
    conn: DBConnection, detail_data: VehicleDetailResponseSchema
):
    """Сохраняет детальную информацию о ТС и связанные с ней данные."""
    if not conn or not detail_data:
        return
    main_data = {
        "vehicleId": detail_data.vehicleId,
        "vehicleGuid": detail_data.vehicleGuid,
        "name": detail_data.name,
        "imei": detail_data.imei,
        "deviceTypeId": detail_data.deviceTypeId,
        "deviceTypeName": detail_data.deviceTypeName,
        "sim1": detail_data.sim1,
        "sim2": detail_data.sim2,
        "parentId": detail_data.parentId,
        "parentName": detail_data.parentName,
        "modelId": detail_data.modelId,
        "modelName": detail_data.modelName,
        "unitId": detail_data.unitId,
        "unitName": detail_data.unitName,
        "status": detail_data.status,
        "createdAt": detail_data.createdAt,
        "consumptionPer100Km": (
            str(detail_data.consumptionPer100Km)
            if detail_data.consumptionPer100Km is not None
            else None
        ),
        "consumptionPerHour": (
            str(detail_data.consumptionPerHour)
            if detail_data.consumptionPerHour is not None
            else None
        ),
        "locationByCellId": 1 if detail_data.locationByCellId else 0,
        "showLineTrackWhenNoCoords": 1 if detail_data.showLineTrackWhenNoCoords else 0,
        "IsSackEnabled": 1 if detail_data.IsSackEnabled else 0,
        "consumptionIdle": (
            str(detail_data.consumptionIdle)
            if detail_data.consumptionIdle is not None
            else None
        ),
        "consumptionPer100KmSeasonal": detail_data.consumptionPer100KmSeasonal,
        "consumptionPerHourSeasonal": detail_data.consumptionPerHourSeasonal,
        "consumptionIdleSeasonal": detail_data.consumptionIdleSeasonal,
        "consumptionPer100KmSeasonalBegin": detail_data.consumptionPer100KmSeasonalBegin,
        "consumptionPer100KmSeasonalEnd": detail_data.consumptionPer100KmSeasonalEnd,
        "consumptionPerHourSeasonalBegin": detail_data.consumptionPerHourSeasonalBegin,
        "consumptionPerHourSeasonalEnd": detail_data.consumptionPerHourSeasonalEnd,
        "consumptionIdleSeasonalBegin": detail_data.consumptionIdleSeasonalBegin,
        "consumptionIdleSeasonalEnd": detail_data.consumptionIdleSeasonalEnd,
        "mileageCalcMethod": str(detail_data.mileageCalcMethod),
        "mileageCoeff": detail_data.mileageCoeff,
        "dottedLineTrackWhenNoCoords": (
            1 if detail_data.dottedLineTrackWhenNoCoords else 0
        ),
        "highlightSensorGuid": detail_data.highlightSensorGuid,
        "motohoursCalcMethod": str(detail_data.motohoursCalcMethod),
    }
    if detail_data.counters:
        main_data["counter_mileage"] = detail_data.counters.mileage
        main_data["counter_motohours"] = detail_data.counters.motohours
        main_data["counter_mileageTime"] = detail_data.counters.mileageTime
        main_data["counter_motohoursTime"] = detail_data.counters.motohoursTime
    insert_data(conn, "vehicle_details", main_data)

    if detail_data.customFields:
        for cf in detail_data.customFields:
            cf_data = {
                "vehicleId": detail_data.vehicleId,
                "custom_field_id": cf.id,
                "name": cf.name,
                "value_text": (
                    json.dumps(cf.value, ensure_ascii=False)
                    if cf.value is not None
                    else None
                ),
                "forTooltip": 1 if cf.forTooltip else 0,
            }
            insert_data(conn, "vehicle_custom_fields_detail", cf_data)

    if detail_data.sensors:
        for sensor in detail_data.sensors:
            sensor_type_id_fk = None
            if (
                isinstance(sensor.type, str)
                and sensor.type in SENSOR_TYPE_NAME_TO_ID_MAP
            ):
                sensor_type_id_fk = SENSOR_TYPE_NAME_TO_ID_MAP[sensor.type]
            elif isinstance(sensor.type, int):  # Если вдруг API вернет числовой ID типа
                sensor_type_id_fk = sensor.type

            if sensor_type_id_fk is None and sensor.type is not None:
                logger.warning(
                    f"Не удалось найти ID для типа сенсора '{sensor.type}' в справочнике sensor_types для vehicleId {detail_data.vehicleId}, sensor_id {sensor.id}. sensor_type_id будет NULL."
                )

            sensor_data = {
                "vehicleId": detail_data.vehicleId,
                "sensor_id": sensor.id,
                "name": sensor.name,
                "type_str": str(sensor.type) if sensor.type is not None else None,
                "sensor_type_id": sensor_type_id_fk,
                "inputType": str(sensor.inputType),
                "inputNumber": sensor.inputNumber,
                "pseudonym": sensor.pseudonym,
                "isInverted": 1 if sensor.isInverted else 0,
                "disabled": 1 if sensor.disabled else 0,
                "showInTooltip": 1 if sensor.showInTooltip else 0,
                "showLastValid": 1 if sensor.showLastValid else 0,
                "gradeType": str(sensor.gradeType),
                "gradesTables_json": (
                    json.dumps(
                        [gt.model_dump() for gt in sensor.gradesTables],
                        ensure_ascii=False,
                    )
                    if sensor.gradesTables
                    else None
                ),
                "kind": sensor.kind,
                "color": sensor.color,
                "showAsDutOnGraph": 1 if sensor.showAsDutOnGraph else 0,
                "showWithoutIgn": 1 if sensor.showWithoutIgn else 0,
                "agrFunction": sensor.agrFunction,
                "expr": sensor.expr,
                "customParams_json": (
                    json.dumps(sensor.customParams, ensure_ascii=False)
                    if sensor.customParams
                    else None
                ),
                "summaryMaxValue_text": (
                    str(sensor.summaryMaxValue)
                    if sensor.summaryMaxValue is not None
                    else None
                ),
                "valueIntervals_json": (
                    json.dumps(sensor.valueIntervals, ensure_ascii=False)
                    if sensor.valueIntervals
                    else None
                ),
                "disableEmissionsValidation": (
                    1 if sensor.disableEmissionsValidation else 0
                ),
                "unitOfMeasure": sensor.unitOfMeasure,
                "medianDegree": sensor.medianDegree,
            }
            insert_data(conn, "vehicle_sensors_detail", sensor_data)

    if detail_data.drivers:
        for driver_assigned in detail_data.drivers:
            driver_data = {
                "vehicleId": detail_data.vehicleId,
                "driver_id": driver_assigned.id,
                "name": driver_assigned.name,
                "isDefault": 1 if driver_assigned.isDefault else 0,
            }
            insert_data(conn, "vehicle_drivers_assigned", driver_data)
    if detail_data.statusHistory:
        for history_item in detail_data.statusHistory:
            history_data = {
                "vehicleId": detail_data.vehicleId,
                "status": history_item.status,
                "date": history_item.date,
                "description": history_item.description,
                "additionalInfo": history_item.additionalInfo,
            }
            insert_data(conn, "vehicle_status_history_items", history_data)
    if detail_data.cmsv6Parameters:
        cms_data = detail_data.cmsv6Parameters.model_dump(exclude_none=True)
        cms_data["vehicleId"] = detail_data.vehicleId
        cms_data["cms_id"] = cms_data.pop("id", None)
        cms_data["enabled"] = 1 if cms_data.get("enabled") else 0
        insert_data(conn, "vehicle_cmsv6_params", cms_data)
    if detail_data.commandTemplates:
        for template in detail_data.commandTemplates:
            tpl_data = template.model_dump(exclude_none=True)
            tpl_data["vehicleId"] = detail_data.vehicleId
            tpl_data["command_template_id"] = tpl_data.pop("id")
            insert_data(conn, "vehicle_command_templates", tpl_data)
    if detail_data.inspectionTasks:
        for task in detail_data.inspectionTasks:
            task_data = task.model_dump(exclude_none=True)
            task_data["vehicleId"] = detail_data.vehicleId
            task_data["task_id"] = task_data.pop("id")
            task_data["enabled"] = 1 if task_data.get("enabled") else 0
            insert_data(conn, "vehicle_inspection_tasks", task_data)


# --- Основное выполнение ---
if __name__ == "__main__":
    # Удаление старого файла БД только для SQLite
    if DB_TYPE == "sqlite" and os.path.exists(SQLITE_DB_FILE):
        try:
            os.remove(SQLITE_DB_FILE)
            logger.info(f"Старый файл SQLite БД {SQLITE_DB_FILE} удален.")
        except OSError as e:
            logger.error(
                f"Не удалось удалить старый файл SQLite БД {SQLITE_DB_FILE}: {e}"
            )

    db_conn = get_db_connection()
    if db_conn:
        try:
            create_tables(db_conn)
        except Exception as e_create:
            logger.error(
                f"Критическая ошибка при создании таблиц: {e_create}. Выполнение прервано."
            )
            if DB_TYPE == "postgres":
                db_conn.close()
            elif DB_TYPE == "sqlite":
                db_conn.close()
            exit(1)
    else:
        logger.error("Не удалось подключиться к БД. Завершение работы.")
        exit(1)

    if not API_LOGIN or not API_PASSWORD:
        logger.error(
            "API_LOGIN и API_PASSWORD должны быть установлены в .env.")
        if DB_TYPE == "postgres":
            db_conn.close()
        elif DB_TYPE == "sqlite":
            db_conn.close()
        exit(1)

    auth_token = authenticate()
    if auth_token:
        logger.info(
            f"Ожидание {REQUEST_DELAY_SECONDS}с после аутентификации...")
        time.sleep(REQUEST_DELAY_SECONDS)
    else:
        logger.error("Невозможно продолжить без токена.")
        if DB_TYPE == "postgres":
            db_conn.close()
        elif DB_TYPE == "sqlite":
            db_conn.close()
        exit(1)

    logger.info("--- Этап 1: Загрузка справочников ---")
    device_types_resp = make_api_request(
        "GET", "/devices/types", token=auth_token, response_list_model=DeviceTypeSchema
    )
    if device_types_resp and isinstance(device_types_resp, list) and db_conn:
        for dt in device_types_resp:
            if isinstance(dt, DeviceTypeSchema):
                insert_data(
                    db_conn,
                    "device_types",
                    dt.model_dump(
                        exclude_none=True))
        logger.info(f"Загружено {len(device_types_resp)} типов устройств.")
    else:
        logger.warning("Не удалось загрузить типы устройств.")
    time.sleep(REQUEST_DELAY_SECONDS)

    sensor_types_resp = make_api_request(
        "GET", "/sensors/types", token=auth_token, response_list_model=SensorTypeSchema
    )
    if sensor_types_resp and isinstance(sensor_types_resp, list) and db_conn:
        SENSOR_TYPE_NAME_TO_ID_MAP.clear()
        for st_item in sensor_types_resp:
            if isinstance(st_item, SensorTypeSchema):
                insert_data(
                    db_conn, "sensor_types", st_item.model_dump(
                        exclude_none=True)
                )
                if st_item.name and st_item.id is not None:
                    SENSOR_TYPE_NAME_TO_ID_MAP[st_item.name] = st_item.id
        logger.info(
            f"Загружено {len(sensor_types_resp)} типов датчиков. Карта имен создана."
        )
    else:
        logger.warning("Не удалось загрузить типы датчиков.")
    time.sleep(REQUEST_DELAY_SECONDS)
    logger.info("--- Этап 1: Загрузка справочников завершена ---")

    all_vehicles_data_list: List[VehicleListItemSchema] = get_all_vehicles_with_data(
        token=auth_token
    )
    active_vehicle_ids: List[int] = [
        v.vehicleId for v in all_vehicles_data_list if v.vehicleId is not None
    ]
    all_parent_ids_from_vehicles = set()
    for v_data in all_vehicles_data_list:
        if v_data.parentId:
            all_parent_ids_from_vehicles.add(v_data.parentId)

    if not active_vehicle_ids:
        logger.warning(
            "ID ТС не получены. Запросы, требующие ID, будут пропущены.")

    if active_vehicle_ids and db_conn:
        logger.info(
            f"--- Этап 2: Обработка детальной информации для {len(active_vehicle_ids)} ТС ---"
        )
        for i, v_id in enumerate(active_vehicle_ids):
            logger.info(
                f"Запрос деталей для ТС ID: {v_id} ({i+1}/{len(active_vehicle_ids)})"
            )
            detail_response = make_api_request(
                method="GET",
                endpoint_path=f"/vehicles/{v_id}",
                token=auth_token,
                response_model=VehicleDetailResponseSchema,
            )
            if detail_response and isinstance(
                detail_response, VehicleDetailResponseSchema
            ):
                save_vehicle_detail_data(db_conn, detail_response)
                if detail_response.parentId:
                    all_parent_ids_from_vehicles.add(detail_response.parentId)
            else:
                logger.warning(
                    f"Не удалось получить или валидировать детальную информацию для ТС ID: {v_id}"
                )
            logger.info(
                f"Ожидание {DETAIL_REQUEST_DELAY_SECONDS}с после деталей ТС ID: {v_id}..."
            )
            time.sleep(DETAIL_REQUEST_DELAY_SECONDS)
        logger.info(
            f"--- Этап 2: Завершена обработка деталей. Уникальных parentId: {len(all_parent_ids_from_vehicles)} ---"
        )

    now_utc = datetime.now(timezone.utc)
    to_time_utc_dt = now_utc
    from_time_utc_dt = now_utc - timedelta(days=DAYS_FOR_REPORTS)
    to_time_iso_str = to_time_utc_dt.isoformat().replace("+00:00", "Z")
    from_time_iso_str = from_time_utc_dt.isoformat().replace("+00:00", "Z")
    logger.info(
        f"Период для отчетов: с {from_time_iso_str} по {to_time_iso_str}")
    sampling_daily_seconds = 24 * 60 * 60

    api_calls_templates = [
        {
            "method": "POST",
            "path": "/vehicles/mileageAndMotohours",
            "json_data_template": {
                "sampling": sampling_daily_seconds,
                "vehicleIds": [],
                "from": from_time_iso_str,
                "to": to_time_iso_str,
                "timezone": 0,
            },
            "params": None,
            "description": f"Пробег и моточасы ({DAYS_FOR_REPORTS}д)",
            "response_list_model": VehicleMileageMotohoursDataSchema,
            "db_table": "mileage_motohours",
            "requires_vehicle_ids": True,
        },
        {
            "method": "POST",
            "path": "/vehicles/fuelConsumption",
            "json_data_template": {
                "sampling": sampling_daily_seconds,
                "vehicleIds": [],
                "from": from_time_iso_str,
                "to": to_time_iso_str,
                "timezone": 0,
            },
            "params": None,
            "description": f"Расход топлива ({DAYS_FOR_REPORTS}д)",
            "response_list_model": VehicleFuelConsumptionDataSchema,
            "db_table": "fuel_consumption",
            "requires_vehicle_ids": True,
        },
        {
            "method": "POST",
            "path": "/vehicles/fuelInOut",
            "json_data_template": {
                "vehicleIds": [],
                "from": from_time_iso_str,
                "to": to_time_iso_str,
                "timezone": 0,
            },
            "params": None,
            "description": f"Заправки и сливы ({DAYS_FOR_REPORTS}д)",
            "response_list_model": VehicleFuelInOutDataSchema,
            "db_table": "fuel_events",
            "requires_vehicle_ids": True,
        },
        {
            "method": "POST",
            "path": "/vehicles/moveStop",
            "json_data_template": {
                "vehicleIds": [],
                "from": from_time_iso_str,
                "to": to_time_iso_str,
                "timezone": 0,
            },
            "params": None,
            "description": f"Движение и стоянки ({DAYS_FOR_REPORTS}д)",
            "response_list_model": VehicleMoveStopDataSchema,
            "requires_vehicle_ids": True,
        },
        {
            "method": "POST",
            "path": "/vehicles/getlastdata",
            "json_data_template": [],
            "params": None,
            "description": "Последние данные объекта",
            "response_list_model": LastDataObjectSchema,
            "db_table": "last_data",
            "requires_vehicle_ids": True,
            "is_body_list_of_ids": True,
        },
    ]
    if all_parent_ids_from_vehicles:
        logger.info(
            f"Будет {len(all_parent_ids_from_vehicles)} запросов /Drivers/find."
        )
        for p_id in all_parent_ids_from_vehicles:
            api_calls_templates.append(
                {
                    "method": "POST",
                    "path": "/Drivers/find",
                    "json_data_template": {"parentId": p_id},
                    "params": None,
                    "description": f"Водители для клиента {p_id}",
                    "response_list_model": DriverInfoSchema,
                    "db_table": "drivers",
                }
            )
    else:
        logger.warning("Нет parentId для запроса /Drivers/find.")

    logger.info(
        f"--- Этап 3: Загрузка отчетов и данных по списку ТС ({len(api_calls_templates)} задач) ---"
    )
    for call_template in api_calls_templates:
        description = call_template.get(
            "description", f"{call_template['method']} {call_template['path']}"
        )
        logger.info(f"--- Обработка: {description} ---")
        actual_json_data: Optional[Union[dict, list]] = None
        template_data = call_template.get("json_data_template")
        if call_template.get("requires_vehicle_ids"):
            if not active_vehicle_ids:
                logger.warning(f"Пропуск {call_template['path']}, нет ID ТС.")
                time.sleep(REQUEST_DELAY_SECONDS)
                continue
            if template_data is not None:
                actual_json_data = (
                    copy.deepcopy(template_data)
                    if isinstance(template_data, dict)
                    else list(template_data)
                )
                if call_template.get("is_body_list_of_ids"):
                    actual_json_data = active_vehicle_ids
                elif (
                    isinstance(actual_json_data, dict)
                    and "vehicleIds" in actual_json_data
                ):
                    actual_json_data["vehicleIds"] = active_vehicle_ids
                else:
                    logger.error(
                        f"Шаблон для {call_template['path']} некорректен. Пропуск."
                    )
                    continue
            else:
                logger.error(
                    f"Для {call_template['path']} requires_vehicle_ids=True, но json_data_template не задан. Пропуск."
                )
                continue
        elif template_data is not None:
            actual_json_data = (
                copy.deepcopy(template_data)
                if isinstance(template_data, dict)
                else list(template_data)
            )
        validated_response = make_api_request(
            call_template["method"],
            call_template["path"],
            token=auth_token,
            json_data=actual_json_data,
            params=call_template.get("params"),
            response_model=call_template.get("response_model"),
            response_list_model=call_template.get("response_list_model"),
        )
        if validated_response is not None and db_conn:
            db_table_name = call_template.get("db_table")
            if db_table_name:
                response_list_to_process = (
                    validated_response
                    if isinstance(validated_response, list)
                    else [validated_response]
                )
                for item_from_response in response_list_to_process:
                    if not isinstance(item_from_response, PydanticBaseModel):
                        logger.warning(
                            f"Элемент для {db_table_name} не Pydantic ({type(item_from_response)}), пропуск: {str(item_from_response)[:100]}"
                        )
                        continue
                    item_dict_for_db = item_from_response.model_dump(
                        exclude_none=True, by_alias=True
                    )
                    if db_table_name == "mileage_motohours" and isinstance(
                        item_from_response, VehicleMileageMotohoursDataSchema
                    ):
                        if item_from_response.periods:
                            for period in item_from_response.periods:
                                period_data = period.model_dump(
                                    exclude_none=True)
                                period_data["vehicleId"] = item_from_response.vehicleId
                                period_data["period_start"] = period_data.pop(
                                    "start", None
                                )
                                period_data["period_end"] = period_data.pop(
                                    "end", None)
                                insert_data(
                                    db_conn, db_table_name, period_data)
                    elif db_table_name == "fuel_consumption" and isinstance(
                        item_from_response, VehicleFuelConsumptionDataSchema
                    ):
                        if item_from_response.periods:
                            for period in item_from_response.periods:
                                period_data = period.model_dump(
                                    exclude_none=True)
                                period_data["vehicleId"] = item_from_response.vehicleId
                                period_data["period_start"] = period_data.pop(
                                    "start", None
                                )
                                period_data["period_end"] = period_data.pop(
                                    "end", None)
                                insert_data(
                                    db_conn, db_table_name, period_data)
                    elif db_table_name == "fuel_events" and isinstance(
                        item_from_response, VehicleFuelInOutDataSchema
                    ):
                        if item_from_response.fuels:
                            for fuel_event_obj in item_from_response.fuels:
                                event_data = fuel_event_obj.model_dump(
                                    exclude_none=True
                                )
                                event_data["report_period_start"] = (
                                    item_from_response.start
                                )
                                event_data["report_period_end"] = item_from_response.end
                                event_data["vehicleId"] = item_from_response.vehicleId
                                event_data["vehicleModel"] = item_from_response.model
                                event_data["event_type"] = event_data.pop(
                                    "event", None)
                                event_data["event_startDate"] = event_data.pop(
                                    "startDate", None
                                )
                                event_data["event_endDate"] = event_data.pop(
                                    "endDate", None
                                )
                                insert_data(db_conn, db_table_name, event_data)
                    elif call_template["path"] == "/vehicles/moveStop" and isinstance(
                        item_from_response, VehicleMoveStopDataSchema
                    ):
                        if item_from_response.moves:
                            for move in item_from_response.moves:
                                move_data = move.model_dump(exclude_none=True)
                                move_data["vehicleId"] = item_from_response.vehicleId
                                move_data["event_start"] = move_data.pop(
                                    "start", None)
                                move_data["event_end"] = move_data.pop(
                                    "end", None)
                                insert_data(db_conn, "move_events", move_data)
                        if item_from_response.stops:
                            for stop in item_from_response.stops:
                                stop_data = stop.model_dump(exclude_none=True)
                                stop_data["vehicleId"] = item_from_response.vehicleId
                                stop_data["event_start"] = stop_data.pop(
                                    "start", None)
                                stop_data["event_end"] = stop_data.pop(
                                    "end", None)
                                insert_data(db_conn, "stop_events", stop_data)
                    elif db_table_name == "last_data" and isinstance(
                        item_from_response, LastDataObjectSchema
                    ):
                        current_item_dict = item_from_response.model_dump(
                            exclude_none=True
                        )
                        if item_from_response.geozones:
                            current_item_dict["geozones"] = json.dumps(
                                [gz.model_dump()
                                 for gz in item_from_response.geozones],
                                ensure_ascii=False,
                            )
                        else:
                            current_item_dict["geozones"] = None
                        insert_data(db_conn, db_table_name, current_item_dict)
                    elif db_table_name == "drivers" and isinstance(
                        item_from_response, DriverInfoSchema
                    ):
                        current_item_dict = item_from_response.model_dump(
                            exclude_none=True
                        )
                        current_item_dict["exclusive"] = (
                            1 if current_item_dict.get("exclusive") else 0
                        )
                        current_item_dict["deleted"] = (
                            1 if current_item_dict.get("deleted") else 0
                        )
                        insert_data(db_conn, db_table_name, current_item_dict)
            else:
                logger.debug(
                    f"Для {description} не указана таблица БД, данные не сохраняются."
                )
        elif validated_response is None and db_conn:
            logger.warning(
                f"Не удалось получить или валидировать ответ для {description}. Данные не сохраняются."
            )
        elif not db_conn:
            logger.error(
                f"Нет соединения с БД, данные для {description} не могут быть сохранены."
            )
        logger.info(
            f"Ожидание {REQUEST_DELAY_SECONDS}с после API вызова {description}..."
        )
        time.sleep(REQUEST_DELAY_SECONDS)

    logger.info("Все API-вызовы из основного цикла обработаны.")
    if db_conn:
        try:
            db_conn.close()
            logger.info(f"Соединение с БД закрыто.")
        except Exception as e:
            logger.error(f"Ошибка при закрытии соединения с БД: {e}")
