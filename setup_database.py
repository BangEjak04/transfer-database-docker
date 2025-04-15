import mysql.connector
import psycopg2
import psycopg2.extensions
from pymongo import MongoClient
from cassandra.cluster import Cluster


#### MYSQL ####
def check_or_create_mysql():
    try:
        conn = mysql.connector.connect(
            host="localhost",
            port=3306,
            user="root",
            password="root"
        )
        cursor = conn.cursor()
        cursor.execute("SHOW DATABASES LIKE 'unity_db'")
        result = cursor.fetchone()

        if result:
            print("✅ [MySQL] Database 'unity_db' sudah ada.")
        else:
            cursor.execute("CREATE DATABASE unity_db")
            print("🆕 [MySQL] Database 'unity_db' berhasil dibuat.")

        cursor.close()
        conn.close()
    except Exception as e:
        print("❌ [MySQL] Error:", e)


#### POSTGRESQL ####
def check_or_create_postgres():
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5433,
            user="postgres",
            password="root",
        )
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM pg_database WHERE datname='unity_db'")
        exists = cursor.fetchone()

        if exists:
            print("✅ [PostgreSQL] Database 'unity_db' sudah ada.")
        else:
            cursor.execute("CREATE DATABASE unity_db")
            print("🆕 [PostgreSQL] Database 'unity_db' berhasil dibuat.")

        cursor.close()
        conn.close()
    except Exception as e:
        print("❌ [PostgreSQL] Error:", e)


#### MONGODB ####
def check_or_create_mongodb():
    try:
        client = MongoClient("mongodb://localhost:27017/")
        if "unity_db" in client.list_database_names():
            print("✅ [MongoDB] Database 'unity_db' sudah ada.")
        else:
            # buat dummy koleksi untuk trigger pembuatan db
            db = client["unity_db"]
            db.create_collection("dummy_collection")
            print("🆕 [MongoDB] Database 'unity_db' berhasil dibuat (dengan koleksi dummy).")

        client.close()
    except Exception as e:
        print("❌ [MongoDB] Error:", e)


#### CASSANDRA ####
def check_or_create_cassandra():
    try:
        cluster = Cluster(['localhost'])  # Ganti dengan IP kalau container beda network
        session = cluster.connect()

        keyspaces = session.execute("SELECT keyspace_name FROM system_schema.keyspaces")
        if 'unity_db' in [row.keyspace_name for row in keyspaces]:
            print("✅ [Cassandra] Keyspace 'unity_db' sudah ada.")
        else:
            session.execute("""
                CREATE KEYSPACE unity_db WITH replication = {
                    'class': 'SimpleStrategy',
                    'replication_factor': 1
                }
            """)
            print("🆕 [Cassandra] Keyspace 'unity_db' berhasil dibuat.")

        cluster.shutdown()
    except Exception as e:
        print("❌ [Cassandra] Error:", e)



#### MAIN FUNCTION ####
if __name__ == "__main__":
    check_or_create_mysql()
    check_or_create_postgres()
    check_or_create_mongodb()
    check_or_create_cassandra()