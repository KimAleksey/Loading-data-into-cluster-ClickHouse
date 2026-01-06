import random

import clickhouse_connect

from pandas import DataFrame

def main():
    try:
        client = clickhouse_connect.get_client(
            host="localhost",
            port=8123,
            user="default",
            password="default",
        )
    except Exception as error:
        raise RuntimeError(error)

    data = {}

    for i in range(1000000):
        data[i] = chr(random.randint(0, 26) + 97)

    df = DataFrame(list(data.items()), columns=["x", "y"])

    client.insert_df(
        database="default",
        table="table_distr",
        df=df,
    )

if __name__ == "__main__":
    main()