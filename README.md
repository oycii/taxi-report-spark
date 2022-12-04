# Приложение демострации работы с rdd и dataframe

## Инициализация структуры БД

DROP DATABASE IF EXISTS taxi;
CREATE DATABASE taxi;


-- public.taxi_report definition

-- Drop table

-- DROP TABLE public.taxi_report;

CREATE TABLE public.taxi_report (
distance int4 NULL,
"count(distance)" int8 NOT NULL,
"min(trip_distance)" float8 NULL,
"max(trip_distance)" float8 NULL,
"avg(trip_distance)" float8 NULL,
"stddev_samp(trip_distance)" float8 NULL,
from_distance_km int4 NULL,
to_distance_km int4 NULL
);

## Переход в docker с postgress
cd $HOME/projects/otus/otus-hadoop-homework
docker-compose up

## Запуск sql tools
cd $HOME/tools/dbeaver
./dbeaver