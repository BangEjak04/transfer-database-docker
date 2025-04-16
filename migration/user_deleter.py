from db.mysql_conn import get_mysql_connection
from db.postgres_conn import get_postgres_connection
from db.mongo_conn import get_mongo_connection
from db.cassandra_conn import get_cassandra_session
from rich.console import Console
from rich.prompt import Confirm

console = Console()


def delete_mysql_users():
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor()

        # Konfirmasi penghapusan
        if Confirm.ask("Apakah Anda yakin ingin menghapus semua data users di MySQL?"):
            cursor.execute("DELETE FROM users")
            conn.commit()
            console.print("[green]✅ Data users MySQL berhasil dihapus.[/green]")
        cursor.close()
        conn.close()
    except Exception as e:
        console.print(f"[red]❌ Gagal menghapus data di MySQL: {e}[/red]")


def delete_postgres_users():
    try:
        conn = get_postgres_connection()
        cursor = conn.cursor()

        # Konfirmasi penghapusan
        if Confirm.ask("Apakah Anda yakin ingin menghapus semua data users di PostgreSQL?"):
            cursor.execute("DELETE FROM users")
            conn.commit()
            console.print("[green]✅ Data users PostgreSQL berhasil dihapus.[/green]")
        cursor.close()
        conn.close()
    except Exception as e:
        console.print(f"[red]❌ Gagal menghapus data di PostgreSQL: {e}[/red]")


def delete_mongodb_users():
    try:
        db = get_mongo_connection()

        # Konfirmasi penghapusan
        if Confirm.ask("Apakah Anda yakin ingin menghapus semua data users di MongoDB?"):
            db.users.delete_many({})
            console.print("[green]✅ Data users MongoDB berhasil dihapus.[/green]")
    except Exception as e:
        console.print(f"[red]❌ Gagal menghapus data di MongoDB: {e}[/red]")


def delete_cassandra_users():
    try:
        session = get_cassandra_session()

        # Konfirmasi penghapusan
        if Confirm.ask("Apakah Anda yakin ingin menghapus semua data users di Cassandra?"):
            session.execute("DELETE FROM users")
            console.print("[green]✅ Data users Cassandra berhasil dihapus.[/green]")
        session.shutdown()
    except Exception as e:
        console.print(f"[red]❌ Gagal menghapus data di Cassandra: {e}[/red]")
