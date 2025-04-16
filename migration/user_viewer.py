from db.mysql_conn import get_mysql_connection
from db.postgres_conn import get_postgres_connection
from db.mongo_conn import get_mongo_connection
from db.cassandra_conn import get_cassandra_session
from rich.console import Console
from rich.table import Table

console = Console()


def view_mysql_users():
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT id, name, email FROM users")
        users = cursor.fetchall()

        table = Table(title="MySQL - Users")
        table.add_column("ID", justify="center")
        table.add_column("Name", justify="left")
        table.add_column("Email", justify="left")

        for user in users:
            table.add_row(str(user[0]), user[1], user[2])

        console.print(table)
        cursor.close()
        conn.close()
    except Exception as e:
        console.print(f"[red]❌ Gagal mengambil data dari MySQL: {e}[/red]")


def view_postgres_users():
    try:
        conn = get_postgres_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT id, name, email FROM users")
        users = cursor.fetchall()

        table = Table(title="PostgreSQL - Users")
        table.add_column("ID", justify="center")
        table.add_column("Name", justify="left")
        table.add_column("Email", justify="left")

        for user in users:
            table.add_row(str(user[0]), user[1], user[2])

        console.print(table)
        cursor.close()
        conn.close()
    except Exception as e:
        console.print(f"[red]❌ Gagal mengambil data dari PostgreSQL: {e}[/red]")


def view_mongodb_users():
    try:
        db = get_mongo_connection()
        users = db.users.find({}, {"_id": 0})

        table = Table(title="MongoDB - Users")
        table.add_column("Name", justify="left")
        table.add_column("Email", justify="left")

        for user in users:
            table.add_row(user["name"], user["email"])

        console.print(table)
    except Exception as e:
        console.print(f"[red]❌ Gagal mengambil data dari MongoDB: {e}[/red]")


def view_cassandra_users():
    try:
        session = get_cassandra_session()
        result = session.execute("SELECT id, name, email FROM users")

        table = Table(title="Cassandra - Users")
        table.add_column("ID", justify="center")
        table.add_column("Name", justify="left")
        table.add_column("Email", justify="left")

        for row in result:
            table.add_row(str(row.id), row.name, row.email)

        console.print(table)
        session.shutdown()
    except Exception as e:
        console.print(f"[red]❌ Gagal mengambil data dari Cassandra: {e}[/red]")
