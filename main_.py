import psycopg2


def mysql_qvery(conn, qvery):
    cursor = conn.cursor()
    cursor.execute(qvery)
    records = cursor.fetchall()

    for row in records:
        print(row)

    cursor.close()


def mysql_connect():
    conn = psycopg2.connect(dbname='order_swap', user='postgres',
                            password='delay159', host='192.168.0.175')
    return conn


if __name__ == "__main__":
    conn = mysql_connect()

    try:
        qvery = """
        COPY orders (id, added_at, departure_at, code_shop, franch_inn, weight, volume, width, depth, height)
        FROM '\\\\192.168.0.198\\tbp\\temp_order_csv_test\\orders\\1_SQL_CSV_TBP_orders_20210304164247.csv'
        WITH (FORMAT csv,
        HEADER TRUE,
        DELIMITER '|'
        );"""

        # qvery = "select version();"
        mysql_qvery(conn, qvery)
    except:
        print('error')
    else:
        conn.close()
    finally:
        conn.close()