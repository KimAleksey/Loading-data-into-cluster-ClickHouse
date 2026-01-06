import clickhouse_connect
import logging
import random
import time

from pandas import DataFrame

# Конфигурация логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

def main():
    # Подключение к CH - clickhouse-01
    start_time = time.time()
    try:
        logging.info("Start - Connecting to ClickHouse.")
        client = clickhouse_connect.get_client(
            host="localhost",
            port=8123,
            database="default",
            user="default",
            password="default",
        )
        logging.info("End - Connected to ClickHouse.")
    except Exception as error:
        raise RuntimeError(error)

    # Создание Replicated таблицы на кластере
    try:
        logging.info("Start - Creating ReplicatedMergeTree table default.table_shard in ClickHouse.")
        check_query = """
            SELECT count()
            FROM system.tables
            WHERE database = 'default' AND name = 'table_shard'
            """
        exists_before = client.query(check_query).result_rows[0][0] > 0

        client.query(
            """
                CREATE TABLE IF NOT EXISTS `default`.table_shard ON CLUSTER analytics_cluster
                (
                    x UInt32,
                    y FixedString(1)
                )
                ENGINE = ReplicatedMergeTree(  
                    '/clickhouse/tables/{shard}/{uuid}',  
                    '{replica}'
                )
                ORDER BY x
                SETTINGS index_granularity = 8192;
            """
        )

        exists_after = client.query(check_query).result_rows[0][0] > 0
        if not exists_before and exists_after:
            logging.info("End - Table default.table_shard was successfully created.")
        elif exists_before:
            logging.info("End - Table default.table_shard already existed.")
        else:
            logging.error("End - Table default.table_shard does not exist after CREATE — something went wrong!")
    except Exception as error:
        raise RuntimeError(error)

    # Создание Distributed таблицы на кластере
    try:
        logging.info("Start - Creating Distributed table default.table_distr in ClickHouse.")
        check_query = """
            SELECT count()
            FROM system.tables
            WHERE database = 'default' AND name = 'table_distr'
            """
        exists_before = client.query(check_query).result_rows[0][0] > 0
        client.query(
            """
                CREATE TABLE IF NOT EXISTS `default`.table_distr ON CLUSTER analytics_cluster
                (
                    x UInt32,
                    y FixedString(1)
                )
                ENGINE = Distributed(
                    'analytics_cluster',
                    'default',
                    'table_shard',
                    cityHash64(x)
                );
            """
        )

        exists_after = client.query(check_query).result_rows[0][0] > 0
        if not exists_before and exists_after:
            logging.info("End - Table default.table_distr was successfully created.")
        elif exists_before:
            logging.info("End - Table default.table_distr already existed.")
        else:
            logging.error("End - Table default.table_distr does not exist after CREATE — something went wrong!")
    except Exception as error:
        raise RuntimeError(error)

    # Генерация данных
    rows_number = 10000000
    data = {}
    start = time.time()
    logging.info(f"Star - Generating {rows_number} rows.")
    for i in range(rows_number):
        data[i] = chr(random.randint(0, 26) + 97)
    end = time.time()
    logging.info(f"End - Generating {rows_number} rows. Execution time: {end - start} seconds.")

    # Создание DataFrame
    start = time.time()
    logging.info(f"Start - Generating DataFrame for {rows_number} rows.")
    df = DataFrame(list(data.items()), columns=["x", "y"])
    end = time.time()
    logging.info(f"End - Generating DataFrame for {rows_number} rows. Execution time: {end - start} seconds.")

    # Запись в CH
    try:
        start = time.time()
        logging.info(f"Start - Loading DataFrame for {rows_number} rows into ClickHouse.")
        query_summary = client.insert_df(
            database="default",
            table="table_distr",
            df=df,
        )
        end = time.time()
        logging.info(f"{query_summary.written_rows} rows inserted. {query_summary.written_bytes()} bytes loaded.")
        logging.info(f"End - Loading DataFrame for {rows_number} rows into ClickHouse. Execution time: {end - start} seconds.")
    except Exception as error:
        raise RuntimeError(error)

    # Общее время выполнения скрипта
    end_time = time.time()
    logging.info(f"Total execution time: {end_time - start_time} seconds.")

if __name__ == "__main__":
    main()