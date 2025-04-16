from db.mysql_conn import get_mysql_connection
from db.postgres_conn import get_postgres_connection
from db.mongo_conn import get_mongo_connection
from db.cassandra_conn import get_cassandra_session
from utils.data_converter import convert_users_data
from uuid import uuid4
from rich import print


def fetch_users_from(source):
    if source == "mysql":
        conn = get_mysql_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT name, email FROM users")
        data = cursor.fetchall()
        cursor.close()
        conn.close()
        return convert_users_data(data, "mysql")

    elif source == "postgres":
        conn = get_postgres_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT name, email FROM users")
        data = cursor.fetchall()
        cursor.close()
        conn.close()
        return convert_users_data(data, "postgres")

    elif source == "mongodb":
        db = get_mongo_connection()
        data = list(db.users.find({}, {"_id": 0}))
        return convert_users_data(data, "mongodb")

    elif source == "cassandra":
        session = get_cassandra_session()
        result = session.execute("SELECT name, email FROM users")
        session.shutdown()
        return convert_users_data(result, "cassandra")

    else:
        raise ValueError(f"Sumber database '{source}' tidak dikenali.")


def insert_users_to(target, data):
    if target == "mysql":
        conn = get_mysql_connection()
        cursor = conn.cursor()
        cursor.executemany("INSERT INTO users (name, email) VALUES (%s, %s)",
                           [(u["name"], u["email"]) for u in data])
        conn.commit()
        cursor.close()
        conn.close()

    elif target == "postgres":
        conn = get_postgres_connection()
        cursor = conn.cursor()
        cursor.executemany("INSERT INTO users (name, email) VALUES (%s, %s)",
                           [(u["name"], u["email"]) for u in data])
        conn.commit()
        cursor.close()
        conn.close()

    elif target == "mongodb":
        db = get_mongo_connection()
        db.users.insert_many(data)

    elif target == "cassandra":
        session = get_cassandra_session()
        for u in data:
            session.execute("""
                INSERT INTO users (id, name, email) VALUES (%s, %s, %s)
            """, (uuid4(), u["name"], u["email"]))
        session.shutdown()

    else:
        raise ValueError(f"Target database '{target}' tidak dikenali.")
