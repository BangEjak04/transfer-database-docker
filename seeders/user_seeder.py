from db.mysql_conn import get_mysql_connection
from db.postgres_conn import get_postgres_connection
from db.mongo_conn import get_mongo_connection
from db.cassandra_conn import get_cassandra_session
from uuid import uuid4
from rich import print


USERS = [
    {"name": "Alice", "email": "alice@mail.com"},
    {"name": "Bob", "email": "bob@mail.com"},
    {"name": "Charlie", "email": "charlie@mail.com"},
]


def seed_mysql_users():
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor()
        cursor.executemany(
            "INSERT INTO users (name, email) VALUES (%s, %s)",
            [(u["name"], u["email"]) for u in USERS],
        )
        conn.commit()
        print("[green]✅ MySQL: Berhasil menambahkan data users[/green]")
    except Exception as e:
        print(f"[red]❌ MySQL: Gagal seeding users[/red]\n{e}")
    finally:
        cursor.close()
        conn.close()


def seed_postgres_users():
    try:
        conn = get_postgres_connection()
        cursor = conn.cursor()
        cursor.executemany(
            "INSERT INTO users (name, email) VALUES (%s, %s)",
            [(u["name"], u["email"]) for u in USERS],
        )
        conn.commit()
        print("[green]✅ PostgreSQL: Berhasil menambahkan data users[/green]")
    except Exception as e:
        print(f"[red]❌ PostgreSQL: Gagal seeding users[/red]\n{e}")
    finally:
        cursor.close()
        conn.close()


def seed_mongodb_users():
    try:
        db = get_mongo_connection()
        result = db.users.insert_many(USERS)
        print(f"[green]✅ MongoDB: {len(result.inserted_ids)} user ditambahkan[/green]")
    except Exception as e:
        print(f"[red]❌ MongoDB: Gagal seeding users[/red]\n{e}")


def seed_cassandra_users():
    try:
        session = get_cassandra_session()
        for user in USERS:
            session.execute(
                """
                INSERT INTO users (id, name, email) VALUES (%s, %s, %s)
                """,
                (uuid4(), user["name"], user["email"]),
            )
        print(f"[green]✅ Cassandra: {len(USERS)} user ditambahkan[/green]")
        session.shutdown()
    except Exception as e:
        print(f"[red]❌ Cassandra: Gagal seeding users[/red]\n{e}")
