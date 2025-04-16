from db.mysql_conn import get_mysql_connection
from db.postgres_conn import get_postgres_connection
from db.mongo_conn import get_mongo_connection
from db.cassandra_conn import get_cassandra_session
from rich import print


def build_mysql_users():
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(100)
            )
        """)
        conn.commit()
        print("[green]✅ MySQL: Tabel users berhasil dibuat[/green]")
    except Exception as e:
        print(f"[red]❌ MySQL: Gagal membuat tabel users[/red]\n{e}")
    finally:
        cursor.close()
        conn.close()


def build_postgres_users():
    try:
        conn = get_postgres_connection()
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(100)
            )
        """)
        conn.commit()
        print("[green]✅ PostgreSQL: Tabel users berhasil dibuat[/green]")
    except Exception as e:
        print(f"[red]❌ PostgreSQL: Gagal membuat tabel users[/red]\n{e}")
    finally:
        cursor.close()
        conn.close()


def build_mongodb_users():
    try:
        db = get_mongo_connection()
        if "users" not in db.list_collection_names():
            db.create_collection("users")
            print("[green]✅ MongoDB: Koleksi users berhasil dibuat[/green]")
        else:
            print("[yellow]⚠️ MongoDB: Koleksi users sudah ada[/yellow]")
    except Exception as e:
        print(f"[red]❌ MongoDB: Gagal membuat koleksi users[/red]\n{e}")


def build_cassandra_users():
    try:
        session = get_cassandra_session()
        session.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id UUID PRIMARY KEY,
                name TEXT,
                email TEXT
            )
        """)
        print("[green]✅ Cassandra: Tabel users berhasil dibuat[/green]")
        session.shutdown()
    except Exception as e:
        print(f"[red]❌ Cassandra: Gagal membuat tabel users[/red]\n{e}")
