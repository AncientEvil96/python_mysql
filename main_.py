import psycopg2


def pssql_qvery(conn, qwery):
    cursor = conn.cursor()
    cursor.execute(qwery)
    conn.commit()
    # records = cursor.fetchall()

    # print(cursor.description)
    # for row in records:
    #     print(row)

    # cursor.close()


def mysql_connect():
    conn = psycopg2.connect(dbname='order_swap_test', user='postgres', password='delay159', host='192.168.0.175')
    return conn


if __name__ == "__main__":
    conn = mysql_connect()
    way_file = '\\\\192.168.0.198\\tbp\\temp_order_csv_test\\orders\\1_SQL_CSV_TBP_orders_20210304164247.csv'
    way_file = way_file.replace('\\', '\\\\')
    i = 0
    try:
        while i < 20:
            i += 1
            qwery = f"""
            COPY orders (id, added_at, departure_at, code_shop, franch_inn, weight, volume, width, depth, height)
            FROM '{way_file}'
            WITH (FORMAT csv,
            HEADER TRUE,
            DELIMITER '|'
            );
            """


            # qwery = 'select * from orders'

            # qwery = 'select version();'
            pssql_qvery(conn, qwery)
            print(f'iteraion {i}')
    except Exception as err:
        print(f'error: {err}')
    else:
        conn.close()
    finally:
        conn.close()
