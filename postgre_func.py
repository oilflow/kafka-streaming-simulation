import psycopg2


def connect_postgres(pg_host, pg_port, pg_user, pg_password, pg_dbname):
    try:
        conn = psycopg2.connect(
            host=pg_host,
            port=pg_port,
            user=pg_user,
            password=pg_password,
            dbname=pg_dbname
        )
        return conn
    except Exception as msg:
        print(f"Unable to connect to the database: {msg}")


def create_table(conn, query):
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            conn.commit()
    except Exception as msg:
        print(f"Couldn't create table: {msg}")


def insert_user(user_data, conn, query):
    try:
        with conn.cursor() as cursor:
            insert_query = query
            cursor.execute(insert_query, user_data)
            conn.commit()
    except Exception as msg:
        print(f"Failed to insert user: {msg}")
        conn.rollback()
