import os
import json
import time
import logging
import requests
import sqlite3
from datetime import datetime, timedelta, timezone 
from dotenv import load_dotenv
from typing import Type, List, Optional, Any, Dict, Union

# Импортируем Pydantic модели
try:
    from glonass_schemas import ( 
        APIBaseModel, DeviceTypeSchema, SensorTypeSchema,
        VehicleMileageMotohoursDataSchema, MileageMotohoursPeriodSchema,
        VehicleFuelConsumptionDataSchema, FuelConsumptionPeriodSchema,
        VehicleFuelInOutDataSchema, FuelEventSchema,
        VehicleMoveStopDataSchema, MoveEventSchema, StopEventSchema,
        LastDataObjectSchema, GeozoneInfoSchema,
        DriverInfoSchema, AuthLoginResponseSchema,
        VehicleListItemSchema, 
        VehicleDetailResponseSchema, VehicleCustomFieldSchema, VehicleCountersSchema,
        VehicleSensorSchema, VehicleSensorGradeTableSchema, VehicleSensorGradeSchema,
        VehicleDriverSchema, VehicleStatusHistoryItemSchema,
        VehicleCMSV6ParametersSchema, VehicleCommandTemplateSchema, VehicleInspectionTaskSchema 
    )
    from pydantic import ValidationError, BaseModel as PydanticBaseModel
except ImportError as e:
    print(f"Ошибка: Не удалось импортировать модели: {e}")
    print("Убедитесь, что файл с моделями (например, glonass_schemas.py) существует и доступен, и содержит все необходимые классы.")
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
DB_FILE = "glonass_data.sqlite"

# --- Настройка логирования ---
logger = logging.getLogger("GlonassAPIClientDB")
logger.setLevel(logging.INFO)
fh = logging.FileHandler(LOG_FILE, mode='w', encoding='utf-8')
fh.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
logger.addHandler(fh)
logger.addHandler(ch)

# Глобальный словарь для маппинга имен типов сенсоров на их ID
SENSOR_TYPE_NAME_TO_ID_MAP: Dict[str, int] = {}

# --- Функции для работы с БД ---
def get_db_connection() -> Optional[sqlite3.Connection]:
    conn = None
    try:
        conn = sqlite3.connect(DB_FILE)
        conn.row_factory = sqlite3.Row 
        logger.debug(f"Установлено соединение с БД: {DB_FILE}")
    except sqlite3.Error as e:
        logger.error(f"Ошибка соединения с БД SQLite: {e}")
    return conn

def create_tables(conn: sqlite3.Connection):
    if not conn: return
    try:
        cursor = conn.cursor()
        cursor.execute("""CREATE TABLE IF NOT EXISTS device_types (deviceTypeId INTEGER PRIMARY KEY, deviceTypeName TEXT, retrieved_at TEXT NOT NULL)""")
        cursor.execute("""CREATE TABLE IF NOT EXISTS sensor_types (id INTEGER PRIMARY KEY, name TEXT UNIQUE, description TEXT, retrieved_at TEXT NOT NULL)""") # Добавил UNIQUE для name
        cursor.execute("""CREATE TABLE IF NOT EXISTS drivers (id TEXT PRIMARY KEY, name TEXT, description TEXT, hiredate TEXT, chopdate TEXT, exclusive INTEGER, parentId TEXT, deleted INTEGER, retrieved_at TEXT NOT NULL)""")
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS vehicle_details (
                vehicleId INTEGER PRIMARY KEY, vehicleGuid TEXT, name TEXT, imei TEXT,
                deviceTypeId INTEGER, deviceTypeName TEXT, sim1 TEXT, sim2 TEXT,
                parentId TEXT, parentName TEXT, modelId TEXT, modelName TEXT,
                unitId TEXT, unitName TEXT, status INTEGER, createdAt TEXT,
                consumptionPer100Km TEXT, consumptionPerHour TEXT, locationByCellId INTEGER,
                counter_mileage REAL, counter_motohours REAL,
                counter_mileageTime TEXT, counter_motohoursTime TEXT,
                showLineTrackWhenNoCoords INTEGER, IsSackEnabled INTEGER, consumptionIdle TEXT,
                consumptionPer100KmSeasonal REAL, consumptionPerHourSeasonal REAL, consumptionIdleSeasonal REAL,
                consumptionPer100KmSeasonalBegin TEXT, consumptionPer100KmSeasonalEnd TEXT,
                consumptionPerHourSeasonalBegin TEXT, consumptionPerHourSeasonalEnd TEXT,
                consumptionIdleSeasonalBegin TEXT, consumptionIdleSeasonalEnd TEXT,
                mileageCalcMethod TEXT, mileageCoeff REAL, dottedLineTrackWhenNoCoords INTEGER,
                highlightSensorGuid TEXT, motohoursCalcMethod TEXT,
                retrieved_at TEXT NOT NULL,
                FOREIGN KEY (deviceTypeId) REFERENCES device_types(deviceTypeId) ON DELETE SET NULL ON UPDATE CASCADE
            )
        """)
        cursor.execute("""CREATE TABLE IF NOT EXISTS vehicle_custom_fields_detail (id INTEGER PRIMARY KEY AUTOINCREMENT, vehicleId INTEGER NOT NULL, custom_field_id TEXT, name TEXT, value_text TEXT, forTooltip INTEGER, retrieved_at TEXT NOT NULL, FOREIGN KEY (vehicleId) REFERENCES vehicle_details(vehicleId) ON DELETE CASCADE, UNIQUE(vehicleId, custom_field_id))""")
        
        # ИЗМЕНЕНО: Добавлено sensor_type_id для FK и type_str для оригинальной строки
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS vehicle_sensors_detail (
                id INTEGER PRIMARY KEY AUTOINCREMENT, 
                vehicleId INTEGER NOT NULL, 
                sensor_id TEXT, 
                name TEXT, 
                type_str TEXT, -- Оригинальное строковое значение типа от API /vehicles/{id}
                sensor_type_id INTEGER, -- Внешний ключ на sensor_types.id
                inputType TEXT, -- inputType тоже может быть строкой, оставляем TEXT
                inputNumber INTEGER, 
                pseudonym TEXT, 
                isInverted INTEGER, 
                disabled INTEGER, 
                showInTooltip INTEGER, 
                showLastValid INTEGER, 
                gradeType TEXT, -- gradeType также может быть строкой, оставляем TEXT
                gradesTables_json TEXT, 
                kind TEXT, 
                color TEXT, 
                showAsDutOnGraph INTEGER, 
                showWithoutIgn INTEGER, 
                agrFunction TEXT, 
                expr TEXT, 
                customParams_json TEXT, 
                summaryMaxValue_text TEXT, 
                valueIntervals_json TEXT, 
                disableEmissionsValidation INTEGER, 
                unitOfMeasure INTEGER, 
                medianDegree INTEGER, 
                retrieved_at TEXT NOT NULL, 
                FOREIGN KEY (vehicleId) REFERENCES vehicle_details(vehicleId) ON DELETE CASCADE, 
                FOREIGN KEY (sensor_type_id) REFERENCES sensor_types(id) ON DELETE SET NULL ON UPDATE CASCADE, -- Новый FK
                UNIQUE(vehicleId, sensor_id)
            )
        """)
        cursor.execute("""CREATE TABLE IF NOT EXISTS vehicle_drivers_assigned (id INTEGER PRIMARY KEY AUTOINCREMENT, vehicleId INTEGER NOT NULL, driver_id TEXT, name TEXT, isDefault INTEGER, retrieved_at TEXT NOT NULL, FOREIGN KEY (vehicleId) REFERENCES vehicle_details(vehicleId) ON DELETE CASCADE, UNIQUE(vehicleId, driver_id))""")
        cursor.execute("""CREATE TABLE IF NOT EXISTS vehicle_status_history_items (id INTEGER PRIMARY KEY AUTOINCREMENT, vehicleId INTEGER NOT NULL, status INTEGER, date TEXT, description TEXT, additionalInfo TEXT, retrieved_at TEXT NOT NULL, FOREIGN KEY (vehicleId) REFERENCES vehicle_details(vehicleId) ON DELETE CASCADE, UNIQUE(vehicleId, date, status, description, additionalInfo) )""")
        cursor.execute("""CREATE TABLE IF NOT EXISTS vehicle_cmsv6_params(vehicleId INTEGER PRIMARY KEY, cms_id TEXT, enabled INTEGER, host TEXT, login TEXT, password TEXT, retrieved_at TEXT NOT NULL, FOREIGN KEY (vehicleId) REFERENCES vehicle_details(vehicleId) ON DELETE CASCADE)""")
        cursor.execute("""CREATE TABLE IF NOT EXISTS vehicle_command_templates(auto_id INTEGER PRIMARY KEY AUTOINCREMENT, vehicleId INTEGER NOT NULL, command_template_id TEXT NOT NULL, name TEXT, command TEXT, retries INTEGER, retrieved_at TEXT NOT NULL, FOREIGN KEY (vehicleId) REFERENCES vehicle_details(vehicleId) ON DELETE CASCADE, UNIQUE (vehicleId, command_template_id))""")
        cursor.execute("""CREATE TABLE IF NOT EXISTS vehicle_inspection_tasks(auto_id INTEGER PRIMARY KEY AUTOINCREMENT, vehicleId INTEGER NOT NULL, task_id TEXT NOT NULL, enabled INTEGER, name TEXT, description TEXT, mileageCondition REAL, lastMileage REAL, motohoursCondition REAL, lastMotohours REAL, periodicCondition INTEGER, kind TEXT, lastInspectionDate TEXT, maxQuantity INTEGER, retrieved_at TEXT NOT NULL, FOREIGN KEY (vehicleId) REFERENCES vehicle_details(vehicleId) ON DELETE CASCADE, UNIQUE (vehicleId, task_id))""")
        cursor.execute("""CREATE TABLE IF NOT EXISTS last_data (vehicleId INTEGER PRIMARY KEY, vehicleGuid TEXT, vehicleNumber TEXT, receiveTime TEXT, recordTime TEXT, state INTEGER, speed REAL, course INTEGER, latitude REAL, longitude REAL, address TEXT, geozones TEXT, retrieved_at TEXT NOT NULL, FOREIGN KEY (vehicleId) REFERENCES vehicle_details(vehicleId) ON DELETE CASCADE)""")
        cursor.execute("""CREATE TABLE IF NOT EXISTS mileage_motohours (id INTEGER PRIMARY KEY AUTOINCREMENT, vehicleId INTEGER, period_start TEXT, period_end TEXT, mileage REAL, mileageBegin REAL, mileageEnd REAL, motohours REAL, motohoursBegin REAL, motohoursEnd REAL, idlingTime REAL, retrieved_at TEXT NOT NULL, FOREIGN KEY (vehicleId) REFERENCES vehicle_details(vehicleId) ON DELETE CASCADE, UNIQUE(vehicleId, period_start, period_end))""")
        cursor.execute("""CREATE TABLE IF NOT EXISTS fuel_consumption (id INTEGER PRIMARY KEY AUTOINCREMENT, vehicleId INTEGER, period_start TEXT, period_end TEXT, fuelLevelStart REAL, fuelLevelEnd REAL, fuelTankLevelStart REAL, fuelTankLevelEnd REAL, fuelConsumption REAL, fuelConsumptionMove REAL, fuelConsumptionFactTank REAL, retrieved_at TEXT NOT NULL, FOREIGN KEY (vehicleId) REFERENCES vehicle_details(vehicleId) ON DELETE CASCADE, UNIQUE(vehicleId, period_start, period_end))""")
        cursor.execute("""CREATE TABLE IF NOT EXISTS fuel_events (id INTEGER PRIMARY KEY AUTOINCREMENT, vehicleId INTEGER, report_period_start TEXT, report_period_end TEXT, vehicleModel TEXT, event_type TEXT, event_startDate TEXT, event_endDate TEXT, valueFuel REAL, fuelStart REAL, fuelEnd REAL, retrieved_at TEXT NOT NULL, FOREIGN KEY (vehicleId) REFERENCES vehicle_details(vehicleId) ON DELETE CASCADE, UNIQUE(vehicleId, event_startDate, event_type))""")
        cursor.execute("""CREATE TABLE IF NOT EXISTS move_events (id INTEGER PRIMARY KEY AUTOINCREMENT, vehicleId INTEGER, mileage REAL, eventId INTEGER, eventName TEXT, event_start TEXT, event_end TEXT, duration INTEGER, retrieved_at TEXT NOT NULL, FOREIGN KEY (vehicleId) REFERENCES vehicle_details(vehicleId) ON DELETE CASCADE, UNIQUE(vehicleId, event_start, eventId))""")
        cursor.execute("""CREATE TABLE IF NOT EXISTS stop_events (id INTEGER PRIMARY KEY AUTOINCREMENT, vehicleId INTEGER, address TEXT, eventId INTEGER, eventName TEXT, event_start TEXT, event_end TEXT, duration INTEGER, retrieved_at TEXT NOT NULL, FOREIGN KEY (vehicleId) REFERENCES vehicle_details(vehicleId) ON DELETE CASCADE, UNIQUE(vehicleId, event_start, eventId))""")
        conn.commit()
        logger.info("Таблицы в БД успешно созданы/проверены.")
    except sqlite3.Error as e:
        logger.error(f"Ошибка при создании таблиц в БД: {e}")
        raise 

def insert_data(conn: sqlite3.Connection, table_name: str, data: dict): # Без изменений
    if not conn or not data: return
    data_copy = data.copy() 
    data_copy['retrieved_at'] = datetime.now(timezone.utc).isoformat() 
    columns = ', '.join(f'"{col}"' for col in data_copy.keys()) 
    placeholders = ', '.join('?' * len(data_copy))
    sql = f"INSERT OR REPLACE INTO {table_name} ({columns}) VALUES ({placeholders})"
    try:
        cursor = conn.cursor()
        cursor.execute(sql, list(data_copy.values()))
        conn.commit()
        log_id_val = next((data_copy[key] for key in ['id', 'vehicleId', 'deviceTypeId', 'custom_field_id', 'sensor_id', 'driver_id', 'command_template_id', 'task_id'] if key in data_copy), 'N/A')
        logger.debug(f"Данные успешно вставлены/обновлены в таблицу {table_name} (ID/ключ: {log_id_val})")
    except sqlite3.Error as e:
        logger.error(f"Ошибка sqlite3 при вставке данных в таблицу {table_name}: {e}\nSQL: {sql}\nДанные: {data_copy}")
    except Exception as ex:
        logger.error(f"Непредвиденная ошибка при вставке в {table_name}: {ex}\nДанные: {data_copy}")

def pretty_print_json(data: Any) -> str: # Без изменений
    if isinstance(data, (dict, list)):
        try: return json.dumps(data, indent=2, ensure_ascii=False)
        except TypeError: return str(data) 
    return str(data)

def make_api_request( # Без изменений
    method: str, endpoint_path: str, token: Optional[str] = None,
    json_data: Optional[Union[dict, list]] = None, params: Optional[dict] = None,
    response_model: Optional[Type[PydanticBaseModel]] = None,
    response_list_model: Optional[Type[PydanticBaseModel]] = None,
    current_retries: int = 0
) -> Optional[Union[PydanticBaseModel, List[PydanticBaseModel], dict, list, str]]:
    url = f"{BASE_URL}{endpoint_path}"
    headers = {"Content-Type": "application/json"}
    if token: headers["X-Auth"] = token
    log_message_req = f"Запрос {method} {url}\nЗаголовки: {pretty_print_json(headers)}"
    if json_data is not None: log_message_req += f"\nТело запроса: {pretty_print_json(json_data)}"
    if params: log_message_req += f"\nПараметры URL: {pretty_print_json(params)}"
    logger.info(log_message_req)
    try:
        response = requests.request(method, url, headers=headers, json=json_data, params=params, timeout=API_TIMEOUT_SECONDS) 
        response.raise_for_status()
        raw_response_data: Any = None
        try:
            raw_response_data = response.json()
            log_message_resp_body = pretty_print_json(raw_response_data)
        except json.JSONDecodeError:
            raw_response_data = response.text 
            log_message_resp_body = f"(Не JSON): {response.text[:500]}..." if response.text else "(Пустой ответ)"
        logger.info(f"Ответ от {method} {url}\nСтатус: {response.status_code}\nТело ответа: {log_message_resp_body}")
        if raw_response_data is not None and (response_model or response_list_model):
            try:
                if response_list_model: 
                    if not isinstance(raw_response_data, list):
                        logger.error(f"Ожидался список для {response_list_model.__name__}, получен {type(raw_response_data)}. Ответ: {str(raw_response_data)[:200]}")
                        return None
                    validated_data = [response_list_model.model_validate(item) for item in raw_response_data]
                    logger.info(f"Ответ успешно валидирован с использованием списка {response_list_model.__name__} ({len(validated_data)} элементов).")
                    return validated_data
                elif response_model: 
                    validated_data = response_model.model_validate(raw_response_data)
                    logger.info(f"Ответ успешно валидирован с использованием {response_model.__name__}.")
                    return validated_data
            except ValidationError as e:
                logger.error(f"Ошибка валидации Pydantic для {method} {url}:\n{e}")
                problematic_data_excerpt = str(e.errors(include_input=True))[:1000]
                logger.debug(f"Проблемные данные (часть): {problematic_data_excerpt}")
                return None 
        return raw_response_data 
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTPError для {method} {url}: {e.response.status_code} - {e.response.text[:500]}")
        if e.response.status_code == 429 and current_retries < MAX_RETRIES:
            retry_after_header = e.response.headers.get("Retry-After")
            wait_time = RETRY_DELAY_BASE_SECONDS * (2 ** current_retries) 
            if retry_after_header and retry_after_header.isdigit(): wait_time = max(wait_time, int(retry_after_header)) 
            logger.warning(f"Получен статус 429. Попытка {current_retries + 1}/{MAX_RETRIES}. Ожидание {wait_time} секунд...")
            time.sleep(wait_time)
            return make_api_request(method, endpoint_path, token, json_data, params, response_model, response_list_model, current_retries + 1)
        elif e.response.status_code == 429: logger.error(f"Статус 429. Превышено макс. кол-во попыток ({MAX_RETRIES}).")
    except requests.exceptions.Timeout: logger.error(f"TimeoutError для {method} {url} (timeout={API_TIMEOUT_SECONDS}s)")
    except requests.exceptions.RequestException as e: logger.error(f"RequestException для {method} {url}: {e}")
    return None

def authenticate() -> Optional[str]: # Без изменений
    logger.info("Попытка аутентификации...")
    payload = {"login": API_LOGIN, "password": API_PASSWORD}
    response_data = make_api_request("POST", "/auth/login", json_data=payload, response_model=AuthLoginResponseSchema)
    if response_data and isinstance(response_data, AuthLoginResponseSchema):
        logger.info(f"Аутентификация успешна. Пользователь: {response_data.User}")
        return response_data.AuthId
    logger.error("Аутентификация не удалась или ответ не соответствует модели.")
    return None

def get_all_vehicles_with_data(token: str) -> List[VehicleListItemSchema]: # Без изменений
    logger.info("Запрос списка ВСЕХ доступных транспортных средств с данными...")
    find_payload_for_all_vehicles: Dict[str, Any] = {} 
    response_data = make_api_request(method="POST", endpoint_path="/vehicles/find", token=token, json_data=find_payload_for_all_vehicles, response_list_model=VehicleListItemSchema)
    if response_data and isinstance(response_data, list):
        valid_vehicles = [item for item in response_data if isinstance(item, VehicleListItemSchema)]
        if len(valid_vehicles) != len(response_data):
            logger.warning(f"Не все элементы в ответе /vehicles/find являются VehicleListItemSchema. Получено {len(valid_vehicles)} из {len(response_data)}.")
        logger.info(f"Получено {len(valid_vehicles)} ТС из /vehicles/find.")
        return valid_vehicles
    logger.error(f"Не удалось получить или валидировать список ТС из /vehicles/find. Ответ: {type(response_data)}")
    return []

def save_vehicle_detail_data(conn: sqlite3.Connection, detail_data: VehicleDetailResponseSchema):
    if not conn or not detail_data: return
    main_data = {
        "vehicleId": detail_data.vehicleId, "vehicleGuid": detail_data.vehicleGuid, "name": detail_data.name,
        "imei": detail_data.imei, "deviceTypeId": detail_data.deviceTypeId, "deviceTypeName": detail_data.deviceTypeName,
        "sim1": detail_data.sim1, "sim2": detail_data.sim2, "parentId": detail_data.parentId,
        "parentName": detail_data.parentName, "modelId": detail_data.modelId, "modelName": detail_data.modelName,
        "unitId": detail_data.unitId, "unitName": detail_data.unitName, "status": detail_data.status,
        "createdAt": detail_data.createdAt,
        "consumptionPer100Km": str(detail_data.consumptionPer100Km) if detail_data.consumptionPer100Km is not None else None,
        "consumptionPerHour": str(detail_data.consumptionPerHour) if detail_data.consumptionPerHour is not None else None,
        "locationByCellId": 1 if detail_data.locationByCellId else 0,
        "showLineTrackWhenNoCoords": 1 if detail_data.showLineTrackWhenNoCoords else 0,
        "IsSackEnabled": 1 if detail_data.IsSackEnabled else 0,
        "consumptionIdle": str(detail_data.consumptionIdle) if detail_data.consumptionIdle is not None else None,
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
        "dottedLineTrackWhenNoCoords": 1 if detail_data.dottedLineTrackWhenNoCoords else 0,
        "highlightSensorGuid": detail_data.highlightSensorGuid,
        "motohoursCalcMethod": str(detail_data.motohoursCalcMethod)
    }
    if detail_data.counters:
        main_data["counter_mileage"] = detail_data.counters.mileage
        main_data["counter_motohours"] = detail_data.counters.motohours
        main_data["counter_mileageTime"] = detail_data.counters.mileageTime
        main_data["counter_motohoursTime"] = detail_data.counters.motohoursTime
    insert_data(conn, "vehicle_details", main_data)

    if detail_data.customFields:
        for cf in detail_data.customFields:
            cf_data = {"vehicleId": detail_data.vehicleId, "custom_field_id": cf.id, "name": cf.name, "value_text": json.dumps(cf.value, ensure_ascii=False) if cf.value is not None else None, "forTooltip": 1 if cf.forTooltip else 0}
            insert_data(conn, "vehicle_custom_fields_detail", cf_data)
    
    if detail_data.sensors:
        for sensor in detail_data.sensors:
            sensor_type_id_fk = None
            if isinstance(sensor.type, str) and sensor.type in SENSOR_TYPE_NAME_TO_ID_MAP:
                sensor_type_id_fk = SENSOR_TYPE_NAME_TO_ID_MAP[sensor.type]
            elif isinstance(sensor.type, int): # Если вдруг API вернет числовой ID типа
                sensor_type_id_fk = sensor.type
            
            if sensor_type_id_fk is None and sensor.type is not None:
                 logger.warning(f"Не удалось найти ID для типа сенсора '{sensor.type}' в справочнике sensor_types для vehicleId {detail_data.vehicleId}, sensor_id {sensor.id}. sensor_type_id будет NULL.")

            sensor_data = {
                "vehicleId": detail_data.vehicleId, 
                "sensor_id": sensor.id, 
                "name": sensor.name, 
                "type_str": str(sensor.type) if sensor.type is not None else None, # Сохраняем оригинальную строку
                "sensor_type_id": sensor_type_id_fk, # Сохраняем найденный FK ID
                "inputType": str(sensor.inputType), 
                "inputNumber": sensor.inputNumber, 
                "pseudonym": sensor.pseudonym,
                "isInverted": 1 if sensor.isInverted else 0, 
                "disabled": 1 if sensor.disabled else 0,
                "showInTooltip": 1 if sensor.showInTooltip else 0, 
                "showLastValid": 1 if sensor.showLastValid else 0,
                "gradeType": str(sensor.gradeType), 
                "gradesTables_json": json.dumps([gt.model_dump() for gt in sensor.gradesTables], ensure_ascii=False) if sensor.gradesTables else None,
                "kind": sensor.kind, 
                "color": sensor.color, 
                "showAsDutOnGraph": 1 if sensor.showAsDutOnGraph else 0,
                "showWithoutIgn": 1 if sensor.showWithoutIgn else 0, 
                "agrFunction": sensor.agrFunction, 
                "expr": sensor.expr,
                "customParams_json": json.dumps(sensor.customParams, ensure_ascii=False) if sensor.customParams else None,
                "summaryMaxValue_text": str(sensor.summaryMaxValue) if sensor.summaryMaxValue is not None else None,
                "valueIntervals_json": json.dumps(sensor.valueIntervals, ensure_ascii=False) if sensor.valueIntervals else None,
                "disableEmissionsValidation": 1 if sensor.disableEmissionsValidation else 0,
                "unitOfMeasure": sensor.unitOfMeasure, 
                "medianDegree": sensor.medianDegree
            }
            insert_data(conn, "vehicle_sensors_detail", sensor_data)

    if detail_data.drivers:
        for driver_assigned in detail_data.drivers:
            driver_data = {"vehicleId": detail_data.vehicleId, "driver_id": driver_assigned.id, "name": driver_assigned.name, "isDefault": 1 if driver_assigned.isDefault else 0}
            insert_data(conn, "vehicle_drivers_assigned", driver_data)
    if detail_data.statusHistory:
        for history_item in detail_data.statusHistory:
            history_data = {"vehicleId": detail_data.vehicleId, "status": history_item.status, "date": history_item.date, "description": history_item.description, "additionalInfo": history_item.additionalInfo }
            insert_data(conn, "vehicle_status_history_items", history_data)
    if detail_data.cmsv6Parameters:
        cms_data = detail_data.cmsv6Parameters.model_dump(exclude_none=True); cms_data["vehicleId"] = detail_data.vehicleId
        cms_data["cms_id"] = cms_data.pop("id", None); cms_data["enabled"] = 1 if cms_data.get("enabled") else 0
        insert_data(conn, "vehicle_cmsv6_params", cms_data)
    if detail_data.commandTemplates:
        for template in detail_data.commandTemplates:
            tpl_data = template.model_dump(exclude_none=True); tpl_data["vehicleId"] = detail_data.vehicleId
            tpl_data["command_template_id"] = tpl_data.pop("id")
            insert_data(conn, "vehicle_command_templates", tpl_data)
    if detail_data.inspectionTasks:
        for task in detail_data.inspectionTasks:
            task_data = task.model_dump(exclude_none=True); task_data["vehicleId"] = detail_data.vehicleId
            task_data["task_id"] = task_data.pop("id"); task_data["enabled"] = 1 if task_data.get("enabled") else 0
            insert_data(conn, "vehicle_inspection_tasks", task_data)

# --- Основное выполнение ---
if __name__ == "__main__":
    if os.path.exists(DB_FILE):
        try: os.remove(DB_FILE); logger.info(f"Старый файл БД {DB_FILE} удален.")
        except OSError as e: logger.error(f"Не удалось удалить старый файл БД {DB_FILE}: {e}")
    
    db_conn = get_db_connection()
    if db_conn: 
        db_conn.execute("PRAGMA foreign_keys = ON")
        try: create_tables(db_conn)
        except Exception as e_create:
            logger.error(f"Критическая ошибка при создании таблиц: {e_create}. Выполнение прервано.")
            db_conn.close(); exit(1)
    else: logger.error("Не удалось подключиться к БД. Завершение работы."); exit(1)

    if not API_LOGIN or not API_PASSWORD:
        logger.error("API_LOGIN и API_PASSWORD должны быть установлены в .env.")
        if db_conn: db_conn.close(); exit(1)

    auth_token = authenticate()
    if auth_token: logger.info(f"Ожидание {REQUEST_DELAY_SECONDS}с после аутентификации..."); time.sleep(REQUEST_DELAY_SECONDS) 
    else:
        logger.error("Невозможно продолжить без токена."); 
        if db_conn: db_conn.close()
        exit(1)

    logger.info("--- Этап 1: Загрузка справочников ---")
    device_types_resp = make_api_request("GET", "/devices/types", token=auth_token, response_list_model=DeviceTypeSchema)
    if device_types_resp and isinstance(device_types_resp, list) and db_conn:
        for dt in device_types_resp:
            if isinstance(dt, DeviceTypeSchema): insert_data(db_conn, "device_types", dt.model_dump(exclude_none=True))
        logger.info(f"Загружено {len(device_types_resp)} типов устройств.")
    else: logger.warning("Не удалось загрузить типы устройств.")
    time.sleep(REQUEST_DELAY_SECONDS)

    sensor_types_resp = make_api_request("GET", "/sensors/types", token=auth_token, response_list_model=SensorTypeSchema)
    if sensor_types_resp and isinstance(sensor_types_resp, list) and db_conn:
        SENSOR_TYPE_NAME_TO_ID_MAP.clear() # Очищаем на случай повторного запуска без перезапуска скрипта
        for st_item in sensor_types_resp:
            if isinstance(st_item, SensorTypeSchema):
                insert_data(db_conn, "sensor_types", st_item.model_dump(exclude_none=True))
                if st_item.name and st_item.id is not None: # Заполняем маппинг
                    SENSOR_TYPE_NAME_TO_ID_MAP[st_item.name] = st_item.id
        logger.info(f"Загружено {len(sensor_types_resp)} типов датчиков. Карта имен создана.")
    else: logger.warning("Не удалось загрузить типы датчиков.")
    time.sleep(REQUEST_DELAY_SECONDS)
    logger.info("--- Этап 1: Загрузка справочников завершена ---")

    all_vehicles_data_list: List[VehicleListItemSchema] = get_all_vehicles_with_data(token=auth_token)
    active_vehicle_ids: List[int] = [v.vehicleId for v in all_vehicles_data_list if v.vehicleId is not None]
    all_parent_ids_from_vehicles = set()
    for v_data in all_vehicles_data_list:
        if v_data.parentId: all_parent_ids_from_vehicles.add(v_data.parentId)

    if not active_vehicle_ids: logger.warning("ID ТС не получены. Запросы, требующие ID, будут пропущены.")
    
    if active_vehicle_ids and db_conn:
        logger.info(f"--- Этап 2: Обработка детальной информации для {len(active_vehicle_ids)} ТС ---")
        for i, v_id in enumerate(active_vehicle_ids):
            logger.info(f"Запрос деталей для ТС ID: {v_id} ({i+1}/{len(active_vehicle_ids)})")
            detail_response = make_api_request(method="GET", endpoint_path=f"/vehicles/{v_id}", token=auth_token, response_model=VehicleDetailResponseSchema)
            if detail_response and isinstance(detail_response, VehicleDetailResponseSchema):
                save_vehicle_detail_data(db_conn, detail_response)
                if detail_response.parentId: all_parent_ids_from_vehicles.add(detail_response.parentId) 
            else: logger.warning(f"Не удалось получить или валидировать детальную информацию для ТС ID: {v_id}")
            logger.info(f"Ожидание {DETAIL_REQUEST_DELAY_SECONDS}с после деталей ТС ID: {v_id}..."); time.sleep(DETAIL_REQUEST_DELAY_SECONDS) 
        logger.info(f"--- Этап 2: Завершена обработка деталей. Уникальных parentId: {len(all_parent_ids_from_vehicles)} ---")

    now_utc = datetime.now(timezone.utc); to_time_utc_dt = now_utc 
    from_time_utc_dt = now_utc - timedelta(days=DAYS_FOR_REPORTS)
    to_time_iso_str = to_time_utc_dt.isoformat().replace("+00:00", "Z")
    from_time_iso_str = from_time_utc_dt.isoformat().replace("+00:00", "Z")
    logger.info(f"Период для отчетов: с {from_time_iso_str} по {to_time_iso_str}")
    sampling_daily_seconds = 24 * 60 * 60 

    api_calls_templates = [
        {"method": "POST", "path": "/vehicles/mileageAndMotohours", "json_data_template": {"sampling": sampling_daily_seconds, "vehicleIds": [], "from": from_time_iso_str, "to": to_time_iso_str, "timezone": 0}, "params": None, "description": f"Пробег и моточасы ({DAYS_FOR_REPORTS}д)", "response_list_model": VehicleMileageMotohoursDataSchema, "db_table": "mileage_motohours", "requires_vehicle_ids": True},
        {"method": "POST", "path": "/vehicles/fuelConsumption", "json_data_template": {"sampling": sampling_daily_seconds, "vehicleIds": [], "from": from_time_iso_str, "to": to_time_iso_str, "timezone": 0}, "params": None, "description": f"Расход топлива ({DAYS_FOR_REPORTS}д)", "response_list_model": VehicleFuelConsumptionDataSchema, "db_table": "fuel_consumption", "requires_vehicle_ids": True},
        {"method": "POST", "path": "/vehicles/fuelInOut", "json_data_template": {"vehicleIds": [], "from": from_time_iso_str, "to": to_time_iso_str, "timezone": 0}, "params": None, "description": f"Заправки и сливы ({DAYS_FOR_REPORTS}д)", "response_list_model": VehicleFuelInOutDataSchema, "db_table": "fuel_events", "requires_vehicle_ids": True},
        {"method": "POST", "path": "/vehicles/moveStop", "json_data_template": {"vehicleIds": [], "from": from_time_iso_str, "to": to_time_iso_str, "timezone": 0}, "params": None, "description": f"Движение и стоянки ({DAYS_FOR_REPORTS}д)", "response_list_model": VehicleMoveStopDataSchema, "requires_vehicle_ids": True },
        {"method": "POST", "path": "/vehicles/getlastdata", "json_data_template": [], "params": None, "description": "Последние данные объекта", "response_list_model": LastDataObjectSchema, "db_table": "last_data", "requires_vehicle_ids": True, "is_body_list_of_ids": True}
    ]
    if all_parent_ids_from_vehicles:
        logger.info(f"Будет {len(all_parent_ids_from_vehicles)} запросов /Drivers/find.")
        for p_id in all_parent_ids_from_vehicles:
            api_calls_templates.append({"method": "POST", "path": "/Drivers/find", "json_data_template": {"parentId": p_id}, "params": None, "description": f"Водители для клиента {p_id}", "response_list_model": DriverInfoSchema, "db_table": "drivers"})
    else: logger.warning("Нет parentId для запроса /Drivers/find.")

    logger.info(f"--- Этап 3: Загрузка отчетов и данных по списку ТС ({len(api_calls_templates)} задач) ---")
    for call_template in api_calls_templates:
        description = call_template.get("description", f"{call_template['method']} {call_template['path']}")
        logger.info(f"--- Обработка: {description} ---")
        actual_json_data: Optional[Union[dict, list]] = None
        template_data = call_template.get("json_data_template")
        if call_template.get("requires_vehicle_ids"):
            if not active_vehicle_ids:
                logger.warning(f"Пропуск {call_template['path']}, нет ID ТС."); time.sleep(REQUEST_DELAY_SECONDS); continue
            if template_data is not None:
                actual_json_data = template_data.copy() if isinstance(template_data, dict) else list(template_data) 
                if call_template.get("is_body_list_of_ids"): actual_json_data = active_vehicle_ids
                elif isinstance(actual_json_data, dict) and "vehicleIds" in actual_json_data: actual_json_data["vehicleIds"] = active_vehicle_ids 
                else: logger.error(f"Шаблон для {call_template['path']} некорректен. Пропуск."); continue
            else: logger.error(f"Для {call_template['path']} requires_vehicle_ids=True, но json_data_template не задан. Пропуск."); continue
        elif template_data is not None: actual_json_data = template_data.copy() if isinstance(template_data, dict) else list(template_data)
        validated_response = make_api_request(call_template["method"], call_template["path"], token=auth_token, json_data=actual_json_data, params=call_template.get("params"), response_model=call_template.get("response_model"), response_list_model=call_template.get("response_list_model"))
        if validated_response is not None and db_conn:
            db_table_name = call_template.get("db_table")
            if db_table_name:
                response_list_to_process = validated_response if isinstance(validated_response, list) else [validated_response]
                for item_from_response in response_list_to_process:
                    if not isinstance(item_from_response, PydanticBaseModel):
                        logger.warning(f"Элемент для {db_table_name} не Pydantic ({type(item_from_response)}), пропуск: {str(item_from_response)[:100]}"); continue
                    item_dict_for_db = item_from_response.model_dump(exclude_none=True, by_alias=True)
                    if db_table_name == "mileage_motohours" and isinstance(item_from_response, VehicleMileageMotohoursDataSchema):
                        if item_from_response.periods:
                            for period in item_from_response.periods:
                                period_data = period.model_dump(exclude_none=True); period_data['vehicleId'] = item_from_response.vehicleId
                                period_data['period_start'] = period_data.pop('start', None); period_data['period_end'] = period_data.pop('end', None)
                                insert_data(db_conn, db_table_name, period_data)
                    elif db_table_name == "fuel_consumption" and isinstance(item_from_response, VehicleFuelConsumptionDataSchema):
                        if item_from_response.periods:
                            for period in item_from_response.periods:
                                period_data = period.model_dump(exclude_none=True); period_data['vehicleId'] = item_from_response.vehicleId
                                period_data['period_start'] = period_data.pop('start', None); period_data['period_end'] = period_data.pop('end', None)
                                insert_data(db_conn, db_table_name, period_data)
                    elif db_table_name == "fuel_events" and isinstance(item_from_response, VehicleFuelInOutDataSchema):
                        if item_from_response.fuels:
                            for fuel_event_obj in item_from_response.fuels: 
                                event_data = fuel_event_obj.model_dump(exclude_none=True); event_data['report_period_start'] = item_from_response.start; event_data['report_period_end'] = item_from_response.end
                                event_data['vehicleId'] = item_from_response.vehicleId; event_data['vehicleModel'] = item_from_response.model
                                event_data['event_type'] = event_data.pop('event', None); event_data['event_startDate'] = event_data.pop('startDate', None); event_data['event_endDate'] = event_data.pop('endDate', None)
                                insert_data(db_conn, db_table_name, event_data)
                    elif call_template['path'] == "/vehicles/moveStop" and isinstance(item_from_response, VehicleMoveStopDataSchema): 
                        if item_from_response.moves:
                            for move in item_from_response.moves:
                                move_data = move.model_dump(exclude_none=True); move_data['vehicleId'] = item_from_response.vehicleId
                                move_data['event_start'] = move_data.pop('start', None); move_data['event_end'] = move_data.pop('end', None)
                                insert_data(db_conn, "move_events", move_data)
                        if item_from_response.stops:
                            for stop in item_from_response.stops:
                                stop_data = stop.model_dump(exclude_none=True); stop_data['vehicleId'] = item_from_response.vehicleId
                                stop_data['event_start'] = stop_data.pop('start', None); stop_data['event_end'] = stop_data.pop('end', None)
                                insert_data(db_conn, "stop_events", stop_data)
                    elif db_table_name == "last_data" and isinstance(item_from_response, LastDataObjectSchema):
                        current_item_dict = item_from_response.model_dump(exclude_none=True) 
                        if item_from_response.geozones: current_item_dict['geozones'] = json.dumps([gz.model_dump() for gz in item_from_response.geozones], ensure_ascii=False)
                        else: current_item_dict['geozones'] = None
                        insert_data(db_conn, db_table_name, current_item_dict)
                    elif db_table_name == "drivers" and isinstance(item_from_response, DriverInfoSchema): # Этот блок вызывается для каждого parentId
                        current_item_dict = item_from_response.model_dump(exclude_none=True)
                        current_item_dict['exclusive'] = 1 if current_item_dict.get('exclusive') else 0; current_item_dict['deleted'] = 1 if current_item_dict.get('deleted') else 0
                        insert_data(db_conn, db_table_name, current_item_dict)
            else: logger.debug(f"Для {description} не указана таблица БД, данные не сохраняются.")
        elif validated_response is None and db_conn: logger.warning(f"Не удалось получить или валидировать ответ для {description}. Данные не сохраняются.")
        elif not db_conn: logger.error(f"Нет соединения с БД, данные для {description} не могут быть сохранены.")
        logger.info(f"Ожидание {REQUEST_DELAY_SECONDS}с после API вызова {description}..."); time.sleep(REQUEST_DELAY_SECONDS) 
    
    logger.info("Все API-вызовы из основного цикла обработаны.")
    if db_conn:
        try: db_conn.close(); logger.info(f"Соединение с БД {DB_FILE} закрыто.")
        except sqlite3.Error as e: logger.error(f"Ошибка при закрытии соединения с БД: {e}")
