from datetime import datetime
from decimal import Decimal
from typing import Optional, List, Dict, Union, Any
from uuid import UUID
from pydantic import BaseModel, Field, ConfigDict, validator

# Общая конфигурация для моделей
class APIBaseModel(BaseModel):
    model_config = ConfigDict(from_attributes=True, extra='ignore')

# --- POST /api/v3/auth/login ---

class AuthLoginRequestSchema(APIBaseModel):
    """
    Модель для запроса авторизации.
    Описание: Используется для передачи логина и пароля пользователя для получения токена.
    Эндпоинт: POST /api/v3/auth/login
    :param login: Логин пользователя
    :param password: Пароль пользователя
    """
    login: str = Field(description="Логин пользователя")
    password: str = Field(description="Пароль пользователя")

class AuthLoginResponseSchema(APIBaseModel):
    """
    Модель для ответа при успешной авторизации.
    Описание: Содержит токен авторизации и имя пользователя.
    Эндпоинт: POST /api/v3/auth/login
    :param AuthId: Токен авторизации (X-Auth)
    :param User: Имя пользователя
    """
    AuthId: str = Field(description="Токен авторизации (X-Auth)")
    User: str = Field(description="Имя пользователя")

# --- GET /api/v3/devices/types ---

class DeviceTypeSchema(APIBaseModel):
    """
    Модель описывает тип телематического устройства, установленного на транспортном средстве.
    Эндпоинт: GET /api/v3/devices/types (ожидается список таких объектов)
    :param deviceTypeId: ID типа устройства
    :param deviceTypeName: Название типа устройства
    """
    deviceTypeId: Optional[str] = Field(None, description="ID типа устройства")
    deviceTypeName: Optional[str] = Field(None, description="Название типа устройства")

# Тип ответа для GET /api/v3/devices/types: List[DeviceTypeSchema]


# --- GET /api/v3/sensors/types ---
# (Переопределение или подтверждение на основе предоставленного в этом запросе формата)

class SensorTypeSchema(APIBaseModel):
    """
    Модель описывает тип датчика, используемого на транспортном средстве или в системе.
    Эндпоинт: GET /api/v3/sensors/types (ожидается список таких объектов)
    :param id: ID типа датчика
    :param name: Название типа датчика
    :param description: Описание типа датчика
    """
    id: Optional[str] = Field(None, description="ID типа датчика")
    name: Optional[str] = Field(None, description="Название")
    description: Optional[str] = Field(None, description="Описание")

# Тип ответа для GET /api/v3/sensors/types: List[SensorTypeSchema]

# --- POST /api/v3/vehicles/mileageAndMotohours ---

class MileageMotohoursRequestSchema(APIBaseModel):
    """
    Модель для запроса данных о пробеге и моточасах.
    Эндпоинт: POST /api/v3/vehicles/mileageAndMotohours
    :param sampling: Частота дискретизации в секундах, минимум 60 секунд.
    :param vehicleIds: Список ID объектов.
    :param from_datetime: Начало периода (в Pydantic v2 'from' - зарезервированное слово, используем alias или другое имя).
    :param to_datetime: Окончание периода (в Pydantic v2 'to' - зарезервированное слово, используем alias или другое имя).
    :param timezone: Временная зона. Если не указана, то по умолчанию UTC+3.
    """
    sampling: int = Field(description="Частота дискретизации в секундах, минимум 60 секунд.")
    vehicleIds: List[int] = Field(description="Список ID объектов.")
    # Используем Field alias для 'from' и 'to', т.к. 'from' - ключевое слово в Python
    from_datetime: str = Field(alias="from", description="Начало периода (строка в формате ISO datetime)")
    to_datetime: str = Field(alias="to", description="Окончание периода (строка в формате ISO datetime)")
    timezone: Optional[int] = Field(0, description="Временная зона. Если не указана, то по умолчанию UTC+3.")

class MileageMotohoursPeriodSchema(APIBaseModel):
    """
    Модель для периода данных о пробеге и моточасах.
    :param start: Начало периода (строка в формате ISO datetime)
    :param end: Окончание периода (строка в формате ISO datetime)
    :param mileage: Пробег за период, километры
    :param mileageBegin: Пробег на начало периода, километры
    :param mileageEnd: Пробег на окончание периода, километры
    :param motohours: Моточасы за период, секунды
    :param motohoursBegin: Моточасы на начало периода, секунды
    :param motohoursEnd: Моточасы на окончание периода, секунды
    :param idlingTime: Холостой ход за период, секунды
    """
    start: Optional[str] = Field(None, description="Начало периода")
    end: Optional[str] = Field(None, description="Окончание периода")
    mileage: Optional[float] = Field(None, description="Пробег за период, километры")
    mileageBegin: Optional[float] = Field(None, description="Пробег на начало периода, километры")
    mileageEnd: Optional[float] = Field(None, description="Пробег на окончание периода, километры")
    motohours: Optional[float] = Field(None, description="Моточасы за период, секунды")
    motohoursBegin: Optional[float] = Field(None, description="Моточасы на начало периода, секунды")
    motohoursEnd: Optional[float] = Field(None, description="Моточасы на окончание периода, секунды")
    idlingTime: Optional[float] = Field(None, description="Холостой ход за период, секунды")

class VehicleMileageMotohoursDataSchema(APIBaseModel):
    """
    Модель для данных о пробеге и моточасах по одному ТС.
    :param vehicleId: ID объекта
    :param name: Имя ТС
    :param periods: Список периодов с данными (список MileageMotohoursPeriodSchema)
    """
    vehicleId: Optional[int] = Field(None, description="ID объекта")
    name: Optional[str] = Field(None, description="Имя ТС")
    periods: Optional[List[MileageMotohoursPeriodSchema]] = Field(None, description="Периоды")

# Тип ответа для POST /api/v3/vehicles/mileageAndMotohours: List[VehicleMileageMotohoursDataSchema]


# --- POST /api/v3/vehicles/fuelConsumption ---

class FuelConsumptionRequestSchema(APIBaseModel):
    """
    Модель для запроса данных о расходе топлива.
    Эндпоинт: POST /api/v3/vehicles/fuelConsumption
    :param sampling: Частота дискретизации в секундах, минимум 60 секунд.
    :param vehicleIds: Список ID объектов.
    :param from_datetime: Начало периода (в Pydantic v2 'from' - зарезервированное слово, используем alias или другое имя).
    :param to_datetime: Окончание периода (в Pydantic v2 'to' - зарезервированное слово, используем alias или другое имя).
    :param timezone: Временная зона. Если не указана, то по умолчанию UTC+3.
    """
    sampling: int = Field(description="Частота дискретизации в секундах, минимум 60 секунд.")
    vehicleIds: List[int] = Field(description="Список ID объектов.")
    from_datetime: str = Field(alias="from", description="Начало периода (строка в формате ISO datetime)")
    to_datetime: str = Field(alias="to", description="Окончание периода (строка в формате ISO datetime)")
    timezone: Optional[int] = Field(0, description="Временная зона. Если не указана, то по умолчанию UTC+3.")

class FuelConsumptionPeriodSchema(APIBaseModel):
    """
    Модель для периода данных о расходе топлива.
    :param start: Начало периода (строка в формате ISO datetime)
    :param end: Окончание периода (строка в формате ISO datetime)
    :param fuelLevelStart: Уровень топлива на начало периода
    :param fuelLevelEnd: Уровень топлива на конец периода
    :param fuelTankLevelStart: Уровень топлива в цистерне на начало периода
    :param fuelTankLevelEnd: Уровень топлива в цистерне на конец периода
    :param fuelConsumption: Расход топлива
    :param fuelConsumptionMove: Расход топлива в движении
    :param fuelConsumptionFactTank: Фактический расход топлива в цистерне
    """
    start: Optional[str] = Field(None, description="Начало периода")
    end: Optional[str] = Field(None, description="Окончание периода")
    fuelLevelStart: Optional[float] = Field(None, description="Уровень топлива на начало периода")
    fuelLevelEnd: Optional[float] = Field(None, description="Уровень топлива на конец периода")
    fuelTankLevelStart: Optional[float] = Field(None, description="Уровень топлива в цистерне на начало периода")
    fuelTankLevelEnd: Optional[float] = Field(None, description="Уровень топлива в цистерне на конец периода")
    fuelConsumption: Optional[float] = Field(None, description="Расход топлива")
    fuelConsumptionMove: Optional[float] = Field(None, description="Расход топлива в движении")
    fuelConsumptionFactTank: Optional[float] = Field(None, description="Фактический расход топлива в цистерне")

class VehicleFuelConsumptionDataSchema(APIBaseModel):
    """
    Модель для данных о расходе топлива по одному ТС.
    :param vehicleId: ID объекта
    :param name: Имя ТС
    :param periods: Список периодов с данными (список FuelConsumptionPeriodSchema)
    """
    vehicleId: Optional[int] = Field(None, description="ID объекта")
    name: Optional[str] = Field(None, description="Имя ТС")
    periods: Optional[List[FuelConsumptionPeriodSchema]] = Field(None, description="Периоды")

# Тип ответа для POST /api/v3/vehicles/fuelConsumption: List[VehicleFuelConsumptionDataSchema]


# ++ --- POST /api/v3/vehicles/fuelInOut ---

class FuelInOutRequestSchema(APIBaseModel):
    """
    Модель для запроса данных о заправках и сливах.
    Эндпоинт: POST /api/v3/vehicles/fuelInOut
    :param vehicleIds: Список ID объектов.
    :param from_datetime: Дата и время начало запроса (в Pydantic v2 'from' - зарезервированное слово).
    :param to_datetime: Дата и время окончания запроса (в Pydantic v2 'to' - зарезервированное слово).
    :param timezone: Временная зона. Если не указана, то по умолчанию UTC+3.
    """
    vehicleIds: List[int] = Field(description="Список ID объектов.")
    from_datetime: str = Field(alias="from", description="Дата и время начало запроса (строка в формате ISO datetime)")
    to_datetime: str = Field(alias="to", description="Дата и время окончания запроса (строка в формате ISO datetime)")
    timezone: Optional[int] = Field(0, description="Временная зона. Если не указана, то по умолчанию UTC+3.")

class FuelEventSchema(APIBaseModel):
    """
    Модель для события заправки/слива или другого топливного события.
    :param event: Тип события (числовое представление)
    :param startDate: Начало события (строка в формате ISO datetime)
    :param endDate: Окончание события (строка в формате ISO datetime)
    :param valueFuel: Количество (объем топлива)
    :param fuelStart: Уровень топлива на начало события
    :param fuelEnd: Уровень топлива на конец события
    """
    event: Optional[int] = Field(None, description="Тип события")
    startDate: Optional[str] = Field(None, description="Начало")
    endDate: Optional[str] = Field(None, description="Окончание")
    valueFuel: Optional[float] = Field(None, description="Количество")
    fuelStart: Optional[float] = Field(None, description="Уровень топлива на начало события")
    fuelEnd: Optional[float] = Field(None, description="Уровень топливо на конец события")

class VehicleFuelInOutDataSchema(APIBaseModel):
    """
    Модель для данных о заправках и сливах по одному ТС за период.
    :param start: Начало периода отчета (строка в формате ISO datetime)
    :param end: Окончание периода отчета (строка в формате ISO datetime)
    :param vehicleId: ID объекта
    :param name: Имя объекта
    :param model: Модель объекта
    :param fuels: Массив данным по заправкам и сливам (список FuelEventSchema)
    """
    start: Optional[str] = Field(None, description="Начало периода")
    end: Optional[str] = Field(None, description="Окончание периода")
    vehicleId: Optional[int] = Field(None, description="ID объекта")
    name: Optional[str] = Field(None, description="Имя объекта")
    model: Optional[str] = Field(None, description="Модель объекта")
    fuels: Optional[List[FuelEventSchema]] = Field(None, description="Массив данным по заправкам и сливам")

# Тип ответа для POST /api/v3/vehicles/fuelInOut: List[VehicleFuelInOutDataSchema]


# ++ --- POST /api/v3/vehicles/moveStop ---

class MoveStopRequestSchema(APIBaseModel):
    """
    Модель для запроса данных по событиям движения и стоянок.
    Эндпоинт: POST /api/v3/vehicles/moveStop
    :param vehicleIds: Список ID объектов.
    :param from_datetime: Дата и время начало запроса (в Pydantic v2 'from' - зарезервированное слово).
    :param to_datetime: Дата и время окончания запроса (в Pydantic v2 'to' - зарезервированное слово).
    :param timezone: Временная зона. Если не указана, то по умолчанию UTC+3.
    """
    vehicleIds: List[int] = Field(description="Список ID объектов.")
    from_datetime: str = Field(alias="from", description="Дата и время начало запроса (строка в формате ISO datetime)")
    to_datetime: str = Field(alias="to", description="Дата и время окончания запроса (строка в формате ISO datetime)")
    timezone: Optional[int] = Field(0, description="Временная зона. Если не указана, то по умолчанию UTC+3.")

class MoveEventSchema(APIBaseModel):
    """
    Модель для события движения.
    :param mileage: Пробег, км
    :param eventId: Идентификатор события
    :param eventName: Название события
    :param start: Дата и время начало события (строка в формате ISO datetime)
    :param end: Дата и время окончания события (строка в формате ISO datetime)
    :param duration: Продолжительность события, секунд
    """
    mileage: Optional[float] = Field(None, description="Пробег, км")
    eventId: Optional[int] = Field(None, description="Идентификатор события")
    eventName: Optional[str] = Field(None, description="Название события")
    start: Optional[str] = Field(None, description="Дата и время начало события")
    end: Optional[str] = Field(None, description="Дата и время окончания события")
    duration: Optional[int] = Field(None, description="Продолжительность события, секунд")

class StopEventSchema(APIBaseModel):
    """
    Модель для события стоянки.
    :param address: Адрес события
    :param eventId: Идентификатор события
    :param eventName: Название события
    :param start: Дата и время начало события (строка в формате ISO datetime)
    :param end: Дата и время окончания события (строка в формате ISO datetime)
    :param duration: Продолжительность события, секунд
    """
    address: Optional[str] = Field(None, description="Адрес события Улица, Дом, Город, Регион, Страна, Координаты")
    eventId: Optional[int] = Field(None, description="Идентификатор события")
    eventName: Optional[str] = Field(None, description="Название события")
    start: Optional[str] = Field(None, description="Дата и время начало события")
    end: Optional[str] = Field(None, description="Дата и время окончания события")
    duration: Optional[int] = Field(None, description="Продолжительность события, секунд")

class VehicleMoveStopDataSchema(APIBaseModel):
    """
    Модель для данных о событиях движения и стоянок по одному ТС.
    :param vehicleId: ID объекта
    :param vehicleName: Имя объекта
    :param moves: Список событий движения (список MoveEventSchema)
    :param stops: Список событий стоянок (список StopEventSchema)
    """
    vehicleId: Optional[int] = Field(None, description="ID объекта")
    vehicleName: Optional[str] = Field(None, description="Имя объекта")
    moves: Optional[List[MoveEventSchema]] = Field(None, description="Событие движения")
    stops: Optional[List[StopEventSchema]] = Field(None, description="События стоянок")

# Тип ответа для POST /api/v3/vehicles/moveStop: List[VehicleMoveStopDataSchema]


# +++ --- POST /api/v3/vehicles/getlastdata ---

# Тело запроса: List[int] - список ID объектов. Отдельная Pydantic модель для такого простого запроса не обязательна,
# можно напрямую использовать List[int] в аннотации типа FastAPI/другого фреймворка.
# Если бы требовалась модель:
# class GetLastDataRequestSchema(APIBaseModel):
#     vehicle_ids: List[int] # или просто передавать список как тело

class GeozoneInfoSchema(APIBaseModel):
    """
    Модель для информации о геозоне.
    :param id: ID геозоны
    :param name: Название геозоны
    """
    id: Optional[int] = Field(None, description="ID геозоны")
    name: Optional[str] = Field(None, description="Название геозоны")

class LastDataObjectSchema(APIBaseModel):
    """
    Модель для последних данных по объекту.
    Эндпоинт: POST /api/v3/vehicles/getlastdata (ожидается список таких объектов, если в запросе несколько ID)
    :param vehicleId: Идентификатор объекта.
    :param vehicleGuid: Идентификатор объекта (GUID).
    :param vehicleNumber: Номер объекта.
    :param receiveTime: Время получения записи на сервере (строка в формате ISO datetime).
    :param recordTime: Время записи от устройства (строка в формате ISO datetime).
    :param state: Состояние объекта в мониторинге: нет данных - 0; отключена - 1; остановка - 2; стоянка - 3; в движении - 4.
    :param speed: Скорость объекта.
    :param course: Курс движения [0;360].
    :param latitude: Широта [-90°;90°].
    :param longitude: Долгота [-180°;180°].
    :param address: Адрес.
    :param geozones: Сведения по геозонам (список GeozoneInfoSchema).
    """
    vehicleId: Optional[int] = Field(None, description="Идентификатор объекта.")
    vehicleGuid: Optional[str] = Field(None, description="Идентификатор объекта.")
    vehicleNumber: Optional[str] = Field(None, description="Номер объекта.")
    receiveTime: Optional[str] = Field(None, description="Время получения записи на сервере")
    recordTime: Optional[str] = Field(None, description="Время записи от устройства")
    state: Optional[int] = Field(None, description="Определяет состояния объекта в мониторинге")
    speed: Optional[float] = Field(None, description="Скорость объекта.")
    course: Optional[int] = Field(None, description="Курс движения [0;360]")
    latitude: Optional[float] = Field(None, description="Широта [-90°;90°].")
    longitude: Optional[float] = Field(None, description="Долгота [-180°;180°].")
    address: Optional[str] = Field(None, description="Адрес.")
    geozones: Optional[List[GeozoneInfoSchema]] = Field(None, description="Сведения по геозонам.")

# Тип ответа для POST /api/v3/vehicles/getlastdata: List[LastDataObjectSchema]
# (Несмотря на пример одного объекта, запрос массива ID обычно подразумевает массив ответов)


# --- POST /api/v3/Drivers/find ---

class FindDriversRequestSchema(APIBaseModel):
    """
    Модель для запроса списка водителей клиента.
    Эндпоинт: POST /api/v3/Drivers/find
    :param parentId: ID клиента-родителя (GUID)
    """
    parentId: str = Field(description="ID клиента-родителя")

class DriverInfoSchema(APIBaseModel):
    """
    Модель для информации о водителе.
    :param name: Наименование водителя
    :param description: Описание
    :param hiredate: Дата найма (строка в формате ISO datetime)
    :param chopdate: Дата увольнения (строка в формате ISO datetime)
    :param exclusive: Признак "Исключительный" (true/false)
    :param id: ID водителя (GUID)
    :param parentId: Guid родителя (клиента, которому принадлежит водитель)
    :param deleted: Признак удаления (true/false)
    """
    name: Optional[str] = Field(None, description="Определяет наименование")
    description: Optional[str] = Field(None, description="Определяет описание")
    hiredate: Optional[str] = Field(None, description="Определяет дату найма")
    chopdate: Optional[str] = Field(None, description="Определяет дату увольнения")
    exclusive: Optional[bool] = Field(None, description="Определяет признак \"Исключительный\"")
    id: Optional[str] = Field(None, description="ID водителя")
    parentId: Optional[str] = Field(None, description="Guid родителя")
    deleted: Optional[bool] = Field(None, description="Признак удаления")

# Тип ответа для POST /api/v3/Drivers/find: List[DriverInfoSchema]

# --- GET /api/v3/vehicles/{vehicleId} (выборочные параметры) ---

class VehicleCustomFieldSchema(APIBaseModel):
    """
    Модель для произвольного поля объекта (сокращенная).
    :param id: ID поля
    :param name: Имя поля
    :param value: Значение поля (может быть строкой, числом, булевым)
    :param forTooltip: Отображать в подсказке
    """
    id: Optional[str] = Field(None, description="ID поля")
    name: Optional[str] = Field(None, description="Имя поля")
    value: Optional[Any] = Field(None, description="Значение поля")
    forTooltip: Optional[bool] = Field(None, description="Отображать в подсказке")

class VehicleCountersSchema(APIBaseModel):
    """
    Модель для счетчиков объекта (пробег, моточасы).
    :param mileage: Текущий пробег (в метрах, если не указано иное)
    :param motohours: Текущие моточасы (в секундах, если не указано иное)
    :param mileageTime: Дата пересчета последнего пробега (строка в формате ISO)
    :param motohoursTime: Дата пересчета последних моточасов (строка в формате ISO)
    """
    mileage: Optional[float] = Field(None, description="Текущий пробег (в метрах, если не указано иное)")
    motohours: Optional[float] = Field(None, description="Текущие моточасы (в секундах, если не указано иное)")
    mileageTime: Optional[str] = Field(None, description="Дата пересчета последнего пробега")
    motohoursTime: Optional[str] = Field(None, description="Дата пересчета последних моточасов")

class VehicleSensorGradeSchema(APIBaseModel):
    """
    Модель для точки тарировки датчика.
    :param input: Входящее значение
    :param output: Выходящее значение
    """
    input: Optional[float] = Field(None, description="Входящее значение")
    output: Optional[float] = Field(None, description="Выходящее значение")

class VehicleSensorGradeTableSchema(APIBaseModel):
    """
    Модель для таблицы тарировки датчика.
    :param grades: Список точек тарировки (модель VehicleSensorGradeSchema)
    :param relevanceTime: Дата/время начала применения тарировочной таблицы (строка в формате ISO)
    """
    grades: Optional[List[VehicleSensorGradeSchema]] = Field(None, description="Таблица тарировки")
    relevanceTime: Optional[str] = Field(None, description="Дата/время начала применения тарировочной таблицы")

class VehicleSensorSchema(APIBaseModel):
    """
    Модель для датчика объекта (сокращенная, важные параметры).
    :param id: ID датчика
    :param name: Имя датчика
    :param type: Тип датчика (числовое представление)
    :param inputType: Тип входа (числовое представление)
    :param pseudonym: Псевдоним датчика
    :param showInTooltip: Отображать в подсказке (true/false)
    :param showLastValid: Отображать последнее валидное значение (true/false)
    :param gradeType: Тип тарировки (числовое представление)
    :param gradesTables: Массив таблиц тарировки (список моделей VehicleSensorGradeTableSchema)
    """
    id: Optional[str] = Field(None, description="ID датчика")
    name: Optional[str] = Field(None, description="Имя датчика")
    type: Optional[int] = Field(None, description="Тип датчика (числовое представление)")
    inputType: Optional[int] = Field(None, description="Тип входа (числовое представление)")
    pseudonym: Optional[str] = Field(None, description="Псевдоним")
    showInTooltip: Optional[bool] = Field(None, description="Отображать в подсказке")
    showLastValid: Optional[bool] = Field(None, description="Отображать последнее валидное значение")
    gradeType: Optional[int] = Field(None, description="Тип тарировки")
    gradesTables: Optional[List[VehicleSensorGradeTableSchema]] = Field(None, description="Массив таблиц тарировки")

class VehicleDriverSchema(APIBaseModel):
    """
    Модель для водителя, назначенного на объект (сокращенная).
    :param id: ID водителя
    :param name: Наименование водителя
    :param isDefault: Является ли водителем по умолчанию (true/false)
    """
    id: Optional[str] = Field(None, description="ID водителя")
    name: Optional[str] = Field(None, description="Наименование водителя")
    isDefault: Optional[bool] = Field(None, description="По умолчанию")

class VehicleStatusHistoryItemSchema(APIBaseModel):
    """
    Модель для элемента истории статусов объекта.
    :param status: ID статуса (числовое представление)
    :param date: Дата статуса (строка в формате ISO)
    :param description: Описание статуса (комментарий)
    """
    status: Optional[int] = Field(None, description="ID статуса")
    date: Optional[str] = Field(None, description="Дата статуса")
    description: Optional[str] = Field(None, description="Описание статуса (комментарий)")

class VehicleDetailResponseSchema(APIBaseModel):
    """
    Модель для детальной информации об объекте (только самые важные параметры).
    Описание: Содержит основную информацию из карточки объекта.
    Эндпоинт: GET /api/v3/vehicles/{vehicleId}
    :param vehicleGuid: Guid ТС
    :param vehicleId: ID объекта
    :param name: Имя объекта
    :param imei: IMEI объекта
    :param deviceTypeId: ID типа устройства
    :param deviceTypeName: Название типа устройства
    :param sim1: Номер SIM1
    :param sim2: Номер SIM2
    :param parentId: ID клиента-родителя
    :param parentName: Наименование клиента-родителя
    :param modelId: ID модели объекта
    :param modelName: Имя модели объекта
    :param unitId: ID подразделения
    :param unitName: Наименование подразделения
    :param status: Статус объекта (числовое представление)
    :param createdAt: Дата создания объекта (строка в формате ISO)
    :param customFields: Список произвольных полей (список моделей VehicleCustomFieldSchema)
    :param consumptionPer100Km: Расход топлива на 100 км (число или строка)
    :param consumptionPerHour: Расход топлива за моточас (число или строка)
    :param counters: Счетчики (пробег, моточасы) (модель VehicleCountersSchema)
    :param sensors: Список датчиков (список моделей VehicleSensorSchema)
    :param drivers: Список водителей (список моделей VehicleDriverSchema)
    :param statusHistory: История статусов (список моделей VehicleStatusHistoryItemSchema)
    :param locationByCellId: Определение местоположения по данным LBS (true/false)
    """
    vehicleGuid: Optional[str] = Field(None, description="Guid ТС")
    vehicleId: str = Field(description="ID объекта")
    name: Optional[str] = Field(None, description="Имя объекта")
    imei: Optional[str] = Field(None, description="IMEI объекта")
    deviceTypeId: Optional[int] = Field(None, description="ID типа устройства")
    deviceTypeName: Optional[str] = Field(None, description="Название типа устройства")
    sim1: Optional[str] = Field(None, description="Номер SIM1")
    sim2: Optional[str] = Field(None, description="Номер SIM2")
    parentId: Optional[str] = Field(None, description="ID клиента-родителя")
    parentName: Optional[str] = Field(None, description="Наименование клиента-родителя")
    modelId: Optional[str] = Field(None, description="ID модели объекта")
    modelName: Optional[str] = Field(None, description="Имя модели объекта")
    unitId: Optional[str] = Field(None, description="ID подразделения")
    unitName: Optional[str] = Field(None, description="Наименование подразделения")
    status: Optional[int] = Field(None, description="Статус объекта (числовое представление)")
    createdAt: Optional[str] = Field(None, description="Дата создания объекта")

    customFields: Optional[List[VehicleCustomFieldSchema]] = Field(None, description="Произвольные поля")
    
    consumptionPer100Km: Optional[Union[float, str]] = Field(None, description="Расход топлива на 100 км")
    consumptionPerHour: Optional[Union[float, str]] = Field(None, description="Расход топлива за моточас")

    counters: Optional[VehicleCountersSchema] = Field(None, description="Счетчики (пробег, моточасы)")
    sensors: Optional[List[VehicleSensorSchema]] = Field(None, description="Датчики")
    drivers: Optional[List[VehicleDriverSchema]] = Field(None, description="Водители")
    statusHistory: Optional[List[VehicleStatusHistoryItemSchema]] = Field(None, description="История статусов (последние/важные)")
    
    locationByCellId: Optional[bool] = Field(None, description="Определение местоположения по данным LBS")


# --- POST /api/v3/vehicles/find ---

class VehicleFindRequestSchema(APIBaseModel):
    """
    Модель для запроса поиска объектов.
    Описание: Используется для фильтрации списка объектов.
    Эндпоинт: POST /api/v3/vehicles/find
    :param vehicleId: ID ТС (для точного совпадения, integer)
    :param name: Имя ТС (для частичного совпадения, string)
    :param imei: IMEI (для частичного совпадения, string)
    :param sim: Номер телефона (для частичного совпадения, string)
    :param deviceTypeId: ID типа устройства (для точного совпадения, short/int)
    :param unitId: ID подразделения (для точного совпадения, GUID string)
    :param unitName: Название подразделения (для частичного совпадения, string)
    :param customFields: Поиск по содержимому произвольных полей ТС (string)
    :param vehicleGroupId: ID группы ТС (для точного совпадения, GUID string)
    :param vehicleGroupName: Название группы ТС (для частичного совпадения, string)
    :param parentId: Идентификатор клиента-родителя, для которого надо получить данные (GUID string)
    """
    vehicleId: Optional[int] = Field(None, description="ID ТС (для точного совпадения)")
    name: Optional[str] = Field(None, description="Имя ТС (для частичного совпадения)")
    imei: Optional[str] = Field(None, description="IMEI (для частичного совпадения)")
    sim: Optional[str] = Field(None, description="Номер телефона (для частичного совпадения)")
    deviceTypeId: Optional[int] = Field(None, description="ID типа устройства (для точного совпадения)")
    unitId: Optional[str] = Field(None, description="ID подразделения (для точного совпадения)")
    unitName: Optional[str] = Field(None, description="Название подразделения (для частичного совпадения)")
    customFields: Optional[str] = Field(None, description="Поиск по содержимому произвольных полей ТС")
    vehicleGroupId: Optional[str] = Field(None, description="ID группы ТС (для точного совпадения)")
    vehicleGroupName: Optional[str] = Field(None, description="Название группы ТС (для частичного совпадения)")
    parentId: Optional[str] = Field(None, description="Идентификатор клиента-родителя (для точного совпадения)")


class VehicleListCustomFieldItemSchema(APIBaseModel):
    """
    Модель для произвольного поля в списке объектов.
    :param id: Идентификатор произвольного поля
    :param name: Имя произвольного поля
    :param value: Значение произвольного поля (может быть строкой, числом, булевым)
    :param forClient: Право на отображение у клиента (true/false)
    :param forTooltip: Право на отображение в подсказке (true/false)
    :param forReport: Право на отображение в отчетах (true/false)
    """
    id: Optional[str] = Field(None, description="Идентификатор произвольного поля")
    name: Optional[str] = Field(None, description="Имя произвольного поля")
    value: Optional[Any] = Field(None, description="Значение произвольного поля")
    forClient: Optional[bool] = Field(None, description="Право на отображение у клиента")
    forTooltip: Optional[bool] = Field(None, description="Право на отображение в подсказке")
    forReport: Optional[bool] = Field(None, description="Право на отображение в отчетах")

class VehicleListGroupItemSchema(APIBaseModel):
    """
    Модель для группы ТС в списке объектов.
    :param id: System.Guid идентификатор элемента сущности (группы)
    :param name: Наименование группы ТС
    """
    id: Optional[str] = Field(None, description="System.Guid идентификатор элемента сущности")
    name: Optional[str] = Field(None, description="Наименование группы ТС")

class VehicleListItemSchema(APIBaseModel):
    """
    Модель для одного объекта в списке ответа поиска.
    Описание: Содержит основные параметры объекта.
    :param vehicleGuid: Guid ТС
    :param vehicleId: Идентификатор объекта
    :param name: Наименование объекта
    :param imei: IMEI объекта
    :param deviceTypeId: Идентификатор типа устройства
    :param deviceTypeName: Наименование типа устройства
    :param sim1: Первый номер телефона
    :param sim2: Второй номер телефона
    :param parentId: Идентификатор клиента-родителя
    :param parentName: Наименование клиента-родителя
    :param modelId: Идентификатор модели объекта
    :param modelName: Наименование модели объекта
    :param unitId: Идентификатор подразделения
    :param unitName: Наименование подразделения
    :param status: Статус объекта (числовое или строковое представление)
    :param createdAt: Дата создания объектов (строка в формате ISO)
    :param customFields: Список произвольных полей (список моделей VehicleListCustomFieldItemSchema)
    :param vehicleGroups: Группы ТС (список моделей VehicleListGroupItemSchema)
    """
    vehicleGuid: Optional[str] = Field(None, description="Guid ТС")
    vehicleId: Optional[str] = Field(None, description="Идентификатор объекта")
    name: Optional[str] = Field(None, description="Наименование объекта")
    imei: Optional[str] = Field(None, description="IMEI объекта")
    deviceTypeId: Optional[str] = Field(None, description="Идентификатор типа устройства")
    deviceTypeName: Optional[str] = Field(None, description="Наименование типа устройства")
    sim1: Optional[str] = Field(None, description="Первый номер телефона")
    sim2: Optional[str] = Field(None, description="Второй номер телефона")
    parentId: Optional[str] = Field(None, description="Идентификатор клиента-родителя")
    parentName: Optional[str] = Field(None, description="Наименование клиента-родителя")
    modelId: Optional[str] = Field(None, description="Идентификатор модели объекта")
    modelName: Optional[str] = Field(None, description="Наименование модели объекта")
    unitId: Optional[str] = Field(None, description="Идентификатор подразделения")
    unitName: Optional[str] = Field(None, description="Наименование подразделения")
    status: Optional[Union[str, int]] = Field(None, description="Статус объекта")
    createdAt: Optional[str] = Field(None, description="Дата создания объектов")
    customFields: Optional[List[VehicleListCustomFieldItemSchema]] = Field(None, description="Произвольные поля")
    vehicleGroups: Optional[List[VehicleListGroupItemSchema]] = Field(None, description="Группы ТС")

# Ответ для эндпоинта /api/v3/vehicles/find это список объектов VehicleFindResponseSchema = List[VehicleListItemSchema]

# Пример использования (для проверки)
if __name__ == '__main__':
    # 1. Login Request
    login_data = {"login": "testuser", "password": "testuser"}
    login_req = AuthLoginRequestSchema(**login_data)
    print("Login Request Docstring:\n", AuthLoginRequestSchema.__doc__)
    print("Login Request Model:", login_req.model_dump_json(indent=2))

    # 1. Login Response
    login_resp_data = {"AuthId": "93c6jg79-b88b-4a35-a2d0-70dg9jc2898b", "User": "testuser"}
    login_resp = AuthLoginResponseSchema(**login_resp_data)
    print("\nLogin Response Docstring:\n", AuthLoginResponseSchema.__doc__)
    print("Login Response Model:", login_resp.model_dump_json(indent=2))

    # 2. Vehicle Detail Response (примерные данные на основе "важных полей")
    vehicle_detail_data = {
        "vehicleGuid": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
        "vehicleId": "12345",
        "name": "КамАЗ 65115",
        "imei": "123456789012345",
        "deviceTypeName": "Teltonika FMB920",
        "status": 1,
        "counters": {
            "mileage": 125000.5,
            "motohours": 3500.2,
            "mileageTime": "2023-05-22T09:57:39.562Z",
            "motohoursTime": "2023-05-22T09:57:39.562Z"
        },
        "sensors": [
            {
                "id": "sensor-1",
                "name": "Уровень топлива",
                "type": 1,
                "showInTooltip": True
            }
        ],
        "drivers": [
            {
                "id": "driver-1",
                "name": "Иванов И.И."
            }
        ],
        "customFields": [
            {"id": "cf-001", "name": "Цвет", "value": "Синий", "forTooltip": True}
        ],
        "statusHistory": [
            {"status": 1, "date": "2023-01-01T10:00:00Z", "description": "Активирован"}
        ]
    }
    vehicle_detail_resp = VehicleDetailResponseSchema(**vehicle_detail_data)
    print("\nVehicle Detail Response Docstring:\n", VehicleDetailResponseSchema.__doc__)
    print("Vehicle Detail Response Model:", vehicle_detail_resp.model_dump_json(indent=2, exclude_none=True))

    # 3. Vehicle Find Request
    find_req_data = {
        "name": "КамАЗ",
        "parentId": "client-abc"
    }
    find_req = VehicleFindRequestSchema(**find_req_data)
    print("\nVehicle Find Request Docstring:\n", VehicleFindRequestSchema.__doc__)
    print("Vehicle Find Request Model:", find_req.model_dump_json(indent=2, exclude_none=True))

    # 3. Vehicle Find Response
    find_resp_data = [
        {
            "vehicleGuid": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
            "vehicleId": "12345",
            "name": "КамАЗ 65115",
            "imei": "123456789012345",
            "deviceTypeName": "Teltonika FMB920",
            "status": "1",
            "customFields": [
                {"id": "cf1", "name": "Гаражный номер", "value": "ГН-001", "forTooltip": True}
            ],
            "vehicleGroups": [
                {"id": "vg1", "name": "Тяжелая техника"}
            ]
        }
    ]
    parsed_find_resp = [VehicleListItemSchema(**item) for item in find_resp_data]
    if parsed_find_resp:
        print("\nVehicle List Item Schema Docstring (for first item in response):\n", VehicleListItemSchema.__doc__)
        print("Vehicle Find Response (parsed item 0):", parsed_find_resp[0].model_dump_json(indent=2, exclude_none=True))