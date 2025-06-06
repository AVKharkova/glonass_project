-- Таблица для хранения информации о транспортных средствах
CREATE TABLE IF NOT EXISTS vehicles (
    id INT PRIMARY KEY,
    guid UUID UNIQUE,
    name VARCHAR(255),
    imei VARCHAR(50) UNIQUE,
    -- Добавьте другие поля при необходимости
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Таблица для хранения сырых данных треков
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
    params_json JSONB, -- Для хранения всех дополнительных параметров
    
    -- Связь с таблицей vehicles
    CONSTRAINT fk_vehicle
        FOREIGN KEY(vehicle_id) 
        REFERENCES vehicles(id)
        ON DELETE CASCADE
);

-- Индексы для ускорения запросов
CREATE INDEX IF NOT EXISTS idx_vehicle_tracks_vehicle_id_timestamp ON vehicle_tracks (vehicle_id, "timestamp" DESC);