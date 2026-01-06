# Загрузка данных в ClickHouse с помощью Python скрпита

## Конфигурация

В этом репозитории находится **Docker Compose конфигурация** для запуска кластера ClickHouse с репликацией и шардингом через Zookeeper.
Также в файле main.py находится скрипт, который создает 2 таблицы на кластере - ReplicatedMergeTree, Distributed и загружает данные.
Количество вставляемых данных настраивается.

---

## 🏗 Архитектура кластера

- **Ноды ClickHouse:** 4  
  - Шард 1: clickhouse-01, clickhouse-02 (2 реплики)  
  - Шард 2: clickhouse-03, clickhouse-04 (2 реплики)  
- **Ноды Zookeeper:** 3 (для хранения метаданных и координации репликации)  
- **Имя кластера:** `analytics_cluster`

**Порты:**

| Сервис          | HTTP  | TCP  |
|-----------------|-------|------|
| clickhouse-01   | 8123  | 9000 |
| clickhouse-02   | 8124  | 9000 |
| clickhouse-03   | 8125  | 9000 |
| clickhouse-04   | 8126  | 9000 |
| zookeeper1      | 2181  | -    |
| zookeeper2      | 2182  | -    |
| zookeeper3      | 2183  | -    |

---

## 📁 Конфигурационные файлы

- **`cluster.xml`** – описывает шардирование, реплики и ноды Zookeeper.  
- **`users.xml`** – содержит настройки пользователей ClickHouse и права доступа. 
- **`macros.xml`** – содержит настройки макросов для использования в DDL. 

Файлы должны быть смонтированы в контейнеры как указано в `docker-compose.yml`.

---

## Запуск

1. Клонируем репозиторий:

```bash
git clone https://github.com/KimAleksey/Loading-data-into-cluster-ClickHouse.git
cd Loading-data-into-cluster-ClickHouse
```

2. Запускаем кластер:

```bash
docker compose up -d
```

3. Подключаемся к ClickHouse в DBeaver:

```text
Driver: ClickHouse
Host: localhost
Port: 8123
DB: default
Username: default
Password: default
```

Также доступны ноды на других портах:
- 8123
- 8124
- 8125
- 8126

4. Установить зависимости

```bash
pip install --upgrade pip && \
pip install uv && \
uv venv activate && \
uv install
```

5. Запустить скрипт по загрузке данных

```bash
python3 main.py
```

Примерный лог загрузки:
```text
2026-01-06 23:02:10,092 | INFO | root | Start - Connecting to ClickHouse.
2026-01-06 23:02:10,136 | INFO | root | End - Connected to ClickHouse.
2026-01-06 23:02:10,136 | INFO | root | Start - Creating ReplicatedMergeTree table default.table_shard in ClickHouse.
2026-01-06 23:02:10,216 | INFO | root | End - Table default.table_shard already existed.
2026-01-06 23:02:10,216 | INFO | root | Start - Creating Distributed table default.table_distr in ClickHouse.
2026-01-06 23:02:10,283 | INFO | root | End - Table default.table_distr already existed.
2026-01-06 23:02:10,283 | INFO | root | Star - Generating 10000000 rows.
2026-01-06 23:02:12,115 | INFO | root | End - Generating 10000000 rows. Execution time: 1.8325748443603516 seconds.
2026-01-06 23:02:12,116 | INFO | root | Start - Generating DataFrame for 10000000 rows.
2026-01-06 23:02:13,748 | INFO | root | End - Generating DataFrame for 10000000 rows. Execution time: 1.6320159435272217 seconds.
2026-01-06 23:02:13,748 | INFO | root | Start - Loading DataFrame for 10000000 rows into ClickHouse.
2026-01-06 23:02:15,211 | INFO | root | 15000171 rows inserted. 75001095 bytes loaded.
2026-01-06 23:02:15,212 | INFO | root | End - Loading DataFrame for 10000000 rows into ClickHouse. Execution time: 1.463447093963623 seconds.
2026-01-06 23:02:15,212 | INFO | root | Total execution time: 5.119751930236816 seconds.
```

6. Проверить что данные загружены в ClickHouse

```sql
SELECT count(*) FROM table_shard; -- Кол-во записей на шарде (реплике).

SELECT count(*) FROM table_distr; -- Кол-во записей на всех шардах.

SELECT shardNum(), hostName(), count(*)
FROM table_distr
GROUP BY 1,2
ORDER BY 1,2; -- Распределение данных по шардам.
```

## ⚠️ Важные моменты

- Кластер использует ReplicatedMergeTree, чтобы включить репликацию.
- Zookeeper нужен для координации реплик; 3 ноды обеспечивают кворум и отказоустойчивость.


## 📌 Полезные ссылки
- [Документация ClickHouse: Интеграция Python](https://clickhouse.com/docs/integrations/python)
- [Документация ClickHouse: Репликация](https://clickhouse.com/docs/ru/engines/table-engines/mergetree-family/replication/)
- [Документация ClickHouse: Distributed Tables](https://clickhouse.com/docs/ru/engines/table-engines/special/distributed/)
- [Документация Zookeeper](https://chatgpt.com/c/695233e7-1ca8-8325-9892-916ef941fcc6#:~:text=ClickHouse%3A%20Distributed%20Tables-,%D0%94%D0%BE%D0%BA%D1%83%D0%BC%D0%B5%D0%BD%D1%82%D0%B0%D1%86%D0%B8%D1%8F,-Zookeeper)