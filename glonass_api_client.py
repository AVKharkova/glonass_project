import os
import json
import time
import logging
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
from typing import Type, List, Optional

# Импортируем Pydantic модели
try:
    from glonass_schemas import (
        APIBaseModel, DeviceTypeSchema, SensorTypeSchema,
        VehicleMileageMotohoursDataSchema, VehicleFuelConsumptionDataSchema,
        VehicleFuelInOutDataSchema, VehicleMoveStopDataSchema,
        LastDataObjectSchema, DriverInfoSchema, AuthLoginResponseSchema
    )
    from pydantic import ValidationError
except ImportError:
    print("Ошибка: Не удалось импортировать модели из glonass_schemas.py.")
    print("Убедитесь, что файл существует и доступен.")
    exit(1)

# Конфигурация
load_dotenv()

API_LOGIN = os.getenv("API_LOGIN")
API_PASSWORD = os.getenv("API_PASSWORD")
BASE_URL = os.getenv("API_BASE_URL", "https://hosting.glonasssoft.ru/api/v3")

VEHICLE_ID_1 = os.getenv("VEHICLE_ID_1")
VEHICLE_ID_2 = os.getenv("VEHICLE_ID_2")
CLIENT_PARENT_ID = os.getenv("CLIENT_PARENT_ID")

TEST_VEHICLE_IDS = []
if VEHICLE_ID_1:
    try:
        TEST_VEHICLE_IDS.append(int(VEHICLE_ID_1))
    except ValueError:
        print(f"Предупреждение: VEHICLE_ID_1 ('{VEHICLE_ID_1}') не является числом.")
if VEHICLE_ID_2:
    try:
        TEST_VEHICLE_IDS.append(int(VEHICLE_ID_2))
    except ValueError:
        print(f"Предупреждение: VEHICLE_ID_2 ('{VEHICLE_ID_2}') не является числом.")

if not TEST_VEHICLE_IDS:
    TEST_VEHICLE_IDS = [0]

REQUEST_DELAY_SECONDS = 1
LOG_FILE = "api_calls_ru_validated.log"

# Настройка логирования
logger = logging.getLogger("GlonassAPIClientRUValidated")
logger.setLevel(logging.INFO)
fh = logging.FileHandler(LOG_FILE, mode='w', encoding='utf-8')
fh.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
logger.addHandler(fh)
logger.addHandler(ch)


def pretty_print_json(data):
    """Форматирует данные в JSON с отступами."""
    if isinstance(data, (dict, list)):
        return json.dumps(data, indent=2, ensure_ascii=False)
    return str(data)


def make_api_request(
    method: str,
    endpoint_path: str,
    token: Optional[str] = None,
    json_data: Optional[dict] = None,
    params: Optional[dict] = None,
    response_model: Optional[Type[APIBaseModel]] = None,
    response_list_model: Optional[Type[APIBaseModel]] = None
):
    """
    Выполняет API-запрос, логирует детали и валидирует ответ.

    :param method: HTTP-метод.
    :param endpoint_path: Путь к эндпоинту API.
    :param token: Токен авторизации (X-Auth).
    :param json_data: Данные для тела запроса.
    :param params: Параметры URL.
    :param response_model: Pydantic модель для одиночного объекта.
    :param response_list_model: Pydantic модель для списка объектов.
    :return: Валидированный объект/список или None при ошибке.
    """
    url = f"{BASE_URL}{endpoint_path}"
    headers = {"Content-Type": "application/json"}
    if token:
        headers["X-Auth"] = token

    log_message = f"Запрос {method} {url}\nЗаголовки: {pretty_print_json(headers)}"
    if json_data:
        log_message += f"\nТело запроса: {pretty_print_json(json_data)}"
    if params:
        log_message += f"\nПараметры URL: {pretty_print_json(params)}"
    logger.info(log_message)

    try:
        response = requests.request(method, url, headers=headers, json=json_data,
                                   params=params, timeout=30)
        response.raise_for_status()

        try:
            raw_response_json = response.json()
            log_message_resp = pretty_print_json(raw_response_json)
        except json.JSONDecodeError:
            raw_response_json = response.text
            log_message_resp = f"(Не JSON): {response.text[:500]}..."

        logger.info(f"Ответ от {method} {url}\nСтатус: {response.status_code}\n"
                    f"Тело ответа: {log_message_resp}")

        if raw_response_json and (response_model or response_list_model):
            try:
                if response_list_model:
                    validated_data = [response_list_model.model_validate(item)
                                      for item in raw_response_json]
                    logger.info(f"Валидирован список {response_list_model.__name__}.")
                    return validated_data
                elif response_model:
                    validated_data = response_model.model_validate(raw_response_json)
                    logger.info(f"Валидирован {response_model.__name__}.")
                    return validated_data
            except ValidationError as e:
                logger.error(f"Ошибка валидации для {method} {url}:\n{e}")
                return None

        return raw_response_json

    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTPError для {method} {url}: {e.response.status_code} - "
                     f"{e.response.text}")
    except requests.exceptions.RequestException as e:
        logger.error(f"RequestException для {method} {url}: {e}")
    return None


def authenticate():
    """Аутентификация в API."""
    logger.info("Попытка аутентификации...")
    payload = {"login": API_LOGIN, "password": API_PASSWORD}
    response_data = make_api_request("POST", "/auth/login", json_data=payload,
                                     response_model=AuthLoginResponseSchema)
    if response_data and isinstance(response_data, AuthLoginResponseSchema):
        logger.info(f"Аутентификация успешна. Пользователь: {response_data.User}")
        return response_data.AuthId
    logger.error("Аутентификация не удалась или ответ не соответствует модели.")
    return None


if __name__ == "__main__":
    if not API_LOGIN or not API_PASSWORD:
        logger.error("Переменные API_LOGIN и API_PASSWORD должны быть в .env.")
        exit(1)

    auth_token = authenticate()
    time.sleep(REQUEST_DELAY_SECONDS)

    if auth_token:
        logger.info(f"Токен авторизации: {auth_token[:10]}...")

        now = datetime.utcnow()
        from_time_str = (now - timedelta(hours=1)).isoformat() + "Z"
        to_time_str = now.isoformat() + "Z"

        api_calls = [
            {
                "method": "GET",
                "path": "/devices/types",
                "data": None,
                "params": None,
                "description": "Получение списка типов устройств",
                "response_list_model": DeviceTypeSchema
            },
            {
                "method": "GET",
                "path": "/sensors/types",
                "data": None,
                "params": None,
                "description": "Получение списка типов датчиков",
                "response_list_model": SensorTypeSchema
            },
            {
                "method": "POST",
                "path": "/vehicles/mileageAndMotohours",
                "data": {
                    "sampling": 3600,
                    "vehicleIds": TEST_VEHICLE_IDS,
                    "from": from_time_str,
                    "to": to_time_str,
                    "timezone": 3
                },
                "params": None,
                "description": "Данные о пробеге и моточасах",
                "response_list_model": VehicleMileageMotohoursDataSchema
            },
            {
                "method": "POST",
                "path": "/vehicles/fuelConsumption",
                "data": {
                    "sampling": 3600,
                    "vehicleIds": TEST_VEHICLE_IDS,
                    "from": from_time_str,
                    "to": to_time_str,
                    "timezone": 3
                },
                "params": None,
                "description": "Данные о расходе топлива",
                "response_list_model": VehicleFuelConsumptionDataSchema
            },
            {
                "method": "POST",
                "path": "/vehicles/fuelInOut",
                "data": {
                    "vehicleIds": TEST_VEHICLE_IDS,
                    "from": from_time_str,
                    "to": to_time_str,
                    "timezone": 3
                },
                "params": None,
                "description": "Данные о заправках и сливах",
                "response_list_model": VehicleFuelInOutDataSchema
            },
            {
                "method": "POST",
                "path": "/vehicles/moveStop",
                "data": {
                    "vehicleIds": TEST_VEHICLE_IDS,
                    "from": from_time_str,
                    "to": to_time_str,
                    "timezone": 3
                },
                "params": None,
                "description": "Данные по движению и стоянкам",
                "response_list_model": VehicleMoveStopDataSchema
            },
            {
                "method": "POST",
                "path": "/vehicles/getlastdata",
                "data": TEST_VEHICLE_IDS,
                "params": None,
                "description": "Последние данные объекта",
                "response_list_model": LastDataObjectSchema
            }
        ]

        if CLIENT_PARENT_ID:
            api_calls.append({
                "method": "POST",
                "path": "/Drivers/find",
                "data": {"parentId": CLIENT_PARENT_ID},
                "params": None,
                "description": "Получение списка водителей клиента",
                "response_list_model": DriverInfoSchema
            })
        else:
            logger.warning("CLIENT_PARENT_ID не установлен, пропуск /Drivers/find.")

        for call_info in api_calls:
            description = call_info.get("description",
                                       f"{call_info['method']} {call_info['path']}")
            logger.info(f"--- Обработка: {description} ---")

            validated_response = make_api_request(
                call_info["method"],
                call_info["path"],
                token=auth_token,
                json_data=call_info["data"],
                params=call_info["params"],
                response_model=call_info.get("response_model"),
                response_list_model=call_info.get("response_list_model")
            )

            if validated_response is not None:
                logger.info(f"Валидированный ответ для {description}: "
                            f"{type(validated_response)}")
                if isinstance(validated_response, list) and validated_response:
                    logger.info(f"Тип первого элемента: "
                                f"{type(validated_response[0])}")
                elif not isinstance(validated_response, list):
                    logger.info(f"Тип ответа: {type(validated_response)}")
            else:
                logger.warning(f"Не удалось получить/валидировать ответ для "
                               f"{description}.")

            logger.info(f"Ожидание {REQUEST_DELAY_SECONDS} секунд(ы)...")
            time.sleep(REQUEST_DELAY_SECONDS)

        logger.info("Все API-вызовы обработаны.")
    else:
        logger.error("Невозможно продолжить без токена аутентификации.")
