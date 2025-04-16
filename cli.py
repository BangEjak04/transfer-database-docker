import typer
from rich.console import Console
from rich.prompt import Prompt

from db.mysql_conn import get_mysql_connection
from db.postgres_conn import get_postgres_connection
from db.mongo_conn import get_mongo_connection
from db.cassandra_conn import get_cassandra_session
from builders.user_builder import (
    build_mysql_users,
    build_postgres_users,
    build_mongodb_users,
    build_cassandra_users,
)
from seeders.user_seeder import (
    seed_mysql_users,
    seed_postgres_users,
    seed_mongodb_users,
    seed_cassandra_users,
)
from migration.user_migrator import fetch_users_from, insert_users_to
from migration.user_viewer import view_mysql_users, view_postgres_users, view_mongodb_users, view_cassandra_users
from migration.user_deleter import delete_mysql_users, delete_postgres_users, delete_mongodb_users, delete_cassandra_users

app = typer.Typer(help="🧪 CLI untuk test koneksi database")
console = Console()


@app.command("mysql")
def test_mysql():
    """Cek koneksi MYSQL"""
    try:
        conn = get_mysql_connection()
        conn.close()
        print("[green]✅ MySQL: Koneksi berhasil[/green]")
    except Exception as e:
        print(f"[red]❌ MySQL: Gagal koneksi[/red]\n{e}")


@app.command("postgres")
def test_postgres():
    """Cek koneksi Postgres"""
    try:
        conn = get_postgres_connection()
        conn.close()
        print("[green]✅ PostgreSQL: Koneksi berhasil[/green]")
    except Exception as e:
        print(f"[red]❌ PostgreSQL: Gagal koneksi[/red]\n{e}")


@app.command("mongodb")
def test_mongodb():
    """Cek koneksi MongoDB"""
    try:
        db = get_mongo_connection()
        _ = db.list_collection_names()
        print("[green]✅ MongoDB: Koneksi berhasil[/green]")
    except Exception as e:
        print(f"[red]❌ MongoDB: Gagal koneksi[/red]\n{e}")


@app.command("cassandra")
def test_cassandra():
    """Cek koneksi Cassandra"""
    try:
        session = get_cassandra_session()
        session.execute("SELECT release_version FROM system.local")
        session.shutdown()
        print("[green]✅ Cassandra: Koneksi berhasil[/green]")
    except Exception as e:
        print(f"[red]❌ Cassandra: Gagal koneksi[/red]\n{e}")


@app.command("all")
def test_all():
    """Cek koneksi ke semua database"""
    test_mysql()
    test_postgres()
    test_mongodb()
    test_cassandra()


@app.command("build-users")
def build_users():
    """Membuat struktur tabel/collection 'users' di semua database"""
    build_mysql_users()
    build_postgres_users()
    build_mongodb_users()
    build_cassandra_users()


@app.command("seed-users")
def seed_users():
    """Isi data dummy ke tabel/collection users"""
    seed_mysql_users()
    seed_postgres_users()
    seed_mongodb_users()
    seed_cassandra_users()


@app.command("migrate-users")
def migrate_users(
    source: str = typer.Option(..., "--from", help="Database sumber (mysql, postgres, mongodb, cassandra)"),
    target: str = typer.Option(..., "--to", help="Database tujuan (mysql, postgres, mongodb, cassandra)"),
):
    """Migrasi data users dari satu DB ke DB lain"""
    if source == target:
        print("[red]❌ Sumber dan tujuan tidak boleh sama[/red]")
        raise typer.Exit()

    print(f"[cyan]🔄 Migrasi users dari {source} ke {target}...[/cyan]")
    data = fetch_users_from(source)
    if not data:
        print(f"[yellow]⚠️ Tidak ada data users di {source}[/yellow]")
        return

    insert_users_to(target, data)
    print(f"[green]✅ Migrasi berhasil: {len(data)} user dimasukkan ke {target}[/green]")


@app.command("view-users")
def view_users(
    database: str = typer.Option(..., "--database", help="Nama database untuk melihat data (mysql, postgres, mongodb, cassandra)")
):
    """Melihat isi data users di database tertentu"""
    if database == "mysql":
        view_mysql_users()
    elif database == "postgres":
        view_postgres_users()
    elif database == "mongodb":
        view_mongodb_users()
    elif database == "cassandra":
        view_cassandra_users()
    else:
        print(f"[red]❌ Database '{database}' tidak dikenali[/red]")


@app.command("delete-users")
def delete_users(
    database: str = typer.Option(..., "--database", help="Nama database untuk menghapus data (mysql, postgres, mongodb, cassandra)")
):
    """Menghapus data users di database tertentu"""
    if database == "mysql":
        delete_mysql_users()
    elif database == "postgres":
        delete_postgres_users()
    elif database == "mongodb":
        delete_mongodb_users()
    elif database == "cassandra":
        delete_cassandra_users()
    else:
        print(f"[red]❌ Database '{database}' tidak dikenali[/red]")


if __name__ == "__main__":
    app()
