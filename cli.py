import os
import typer
import json
import csv
from rich.console import Console
from rich.table import Table
from tabulate import tabulate
from db.mysql import MySQLDB
from db.postgresql import PostgreSQLDB
from db.mongodb import MongoDB
from db.cassandra import CassandraDB
from db.transfer import interactive_transfer
from dotenv import load_dotenv
from utils.validation import is_valid_schema_name



load_dotenv()

app = typer.Typer(help="🧪 CLI untuk manajemen database")

DEFAULT_SCHEMA_NAME = os.getenv("DEFAULT_SCHEMA_NAME", "warehouse_db")



def get_db_handler(db: str):
    if db == "mysql":
        return MySQLDB()
    elif db == "postgres":
        return PostgreSQLDB()
    elif db == "mongo":
        return MongoDB()
    elif db == "cassandra":
        return CassandraDB()
    else:
        raise typer.BadParameter("Unsupported DB type. Use mysql/postgres/mongo/cassandra.")

@app.command("schema:create")
def create_schema(
    db: str = typer.Option(...),
    name: str = typer.Option(DEFAULT_SCHEMA_NAME)
):
    if not is_valid_schema_name(name):
        typer.echo(f"❌ Nama schema '{name}' tidak valid.")
        raise typer.Exit()

    confirm = typer.confirm(f"Apakah kamu yakin ingin membuat schema '{name}' di {db}?")
    if not confirm:
        typer.echo("Pembuatan dibatalkan.")
        raise typer.Exit()

    db_handler = get_db_handler(db)
    try:
        if name in db_handler.read_schemas():  # ← pengecekan duplikasi
            typer.echo(f"⚠️ Schema '{name}' sudah ada di {db}.")
        else:
            db_handler.create_schema(name)
            typer.echo(f"✅ Schema '{name}' berhasil dibuat di {db}.")
    except Exception as e:
        typer.echo(f"❌ Gagal membuat schema: {e}")

@app.command("schema:read")
def read_schemas(
    db: str = typer.Option(...),
):
    db_handler = get_db_handler(db)
    try:
        schemas = db_handler.read_schemas()
        
        console = Console()  # Inisialisasi console hanya sekali

        if schemas:
            table = Table(title=f"Daftar Schema di {db.upper()}")
            table.add_column("No", style="cyan", justify="right")
            table.add_column("Schema Name", style="green")

            for i, s in enumerate(schemas, start=1):
                table.add_row(str(i), s)

            console.print(table)
        else:
            console.print(f"📭 Tidak ada schema di {db}.", style="yellow")

    except Exception as e:
        console.print(f"❌ Gagal membaca schema: {e}", style="bold red")

@app.command("schema:delete")
def delete_schema(
    db: str = typer.Option(...),
    name: str = typer.Option(DEFAULT_SCHEMA_NAME)
):
    if not is_valid_schema_name(name):
        typer.echo(f"❌ Nama schema '{name}' tidak valid.")
        raise typer.Exit()

    confirm = typer.confirm(f"Apakah kamu yakin ingin menghapus schema '{name}' dari {db}?")
    if not confirm:
        typer.echo("Penghapusan dibatalkan.")
        raise typer.Exit()

    db_handler = get_db_handler(db)
    try:
        if name not in db_handler.read_schemas():
            typer.echo(f"⚠️ Schema '{name}' tidak ditemukan di {db}.")
        else:
            db_handler.delete_schema(name)
            typer.echo(f"✅ Schema '{name}' berhasil dihapus dari {db}.")
    except Exception as e:
        typer.echo(f"❌ Gagal menghapus schema: {e}")

@app.command("table:create")
def create_table(
    db: str = typer.Option(..., help="Jenis database (mysql/postgres/dll)"),
    schema: str = typer.Option(DEFAULT_SCHEMA_NAME, help="Nama schema yang digunakan"),
    table: str = typer.Option(..., help="Nama tabel yang akan dibuat"),
    columns: str = typer.Option(None, help='Struktur kolom SQL, contoh: "id INT PRIMARY KEY, name VARCHAR(100)"'),
    columns_json: str = typer.Option(None, help='Struktur kolom dalam format JSON: [{"name": "id", "type": "INT"}]'),
):
    if not columns and not columns_json:
        typer.echo("❌ Harus menyertakan --columns atau --columns-json")
        raise typer.Exit(code=1)

    db_handler = get_db_handler(db)

    if columns_json:
        try:
            parsed_columns = json.loads(columns_json)
            if not isinstance(parsed_columns, list):
                raise ValueError
        except Exception:
            typer.echo("❌ Format columns-json tidak valid. Harus berupa JSON list of objects.")
            raise typer.Exit(code=1)
        
        typer.echo(f"📦 Membuat tabel '{table}' dengan struktur JSON:")
        for col in parsed_columns:
            typer.echo(f" - {col['name']}: {col['type']}")
        
        if not typer.confirm("Lanjutkan?"):
            typer.echo("❌ Dibatalkan.")
            raise typer.Exit()

        db_handler.create_table(schema, table, parsed_columns)
        typer.echo(f"✅ Tabel '{table}' berhasil dibuat di schema '{schema}' ({db})")

    else:
        typer.echo(f"📦 Membuat tabel '{table}' dengan kolom:\n{columns}")
        if not typer.confirm("Lanjutkan?"):
            typer.echo("❌ Dibatalkan.")
            raise typer.Exit()

        db_handler.create_table(schema, table, columns)
        typer.echo(f"✅ Tabel '{table}' berhasil dibuat di schema '{schema}' ({db})")


@app.command("table:read")
def read_tables(
    db: str = typer.Option(..., help="Jenis database (mysql/postgres/dll)"),
    schema: str = typer.Option(DEFAULT_SCHEMA_NAME, help="Nama schema yang digunakan")
):
    db_handler = get_db_handler(db)
    tables = db_handler.read_tables(schema)

    if not tables:
        print("📭 Tidak ada tabel di schema ini.")
        return

    for table_name in tables:
        typer.echo(f"\n📦 Tabel: {table_name}")
        structure = db_handler.describe_table(schema, table_name)
        if structure:
            print(tabulate(structure, headers="keys", tablefmt="fancy_grid"))
        else:
            typer.echo("❌ Gagal mengambil struktur tabel atau tabel kosong.")

@app.command("table:delete")
def delete_table(db: str = typer.Option(..., help="Jenis database (mysql/postgres/dll)"), 
                 schema: str = typer.Option(DEFAULT_SCHEMA_NAME, help="Nama schema yang digunakan"), 
                 table: str = typer.Option(..., help="Nama tabel yang akan dihapus")):
    confirm = typer.confirm(f"Yakin ingin menghapus tabel '{table}' dari schema '{schema}' di {db}?")
    if not confirm:
        typer.echo("❌ Penghapusan dibatalkan.")
        raise typer.Exit()
    db_handler = get_db_handler(db)
    db_handler.delete_table(schema, table)

@app.command("table:create-data")
def create_data(
    db: str = typer.Option(..., help="Jenis database"),
    schema: str = typer.Option(DEFAULT_SCHEMA_NAME, help="Nama schema"),
    table: str = typer.Option(..., help="Nama tabel"),
    data: str = typer.Option(None, help="Data dalam format JSON (opsional, jika tidak diberikan akan interaktif)"),
):
    db_handler = get_db_handler(db)

    if data:
        try:
            row_data = json.loads(data)
        except json.JSONDecodeError:
            print("❌ Format JSON tidak valid.")
            raise typer.Exit()
    else:
        # Ambil struktur kolom dari tabel
        columns = db_handler.describe_table(schema, table)
        if not columns:
            print("❌ Struktur tabel tidak ditemukan.")
            raise typer.Exit()

        row_data = {}
        for col in columns:
            col_name = col['Column']
            extra = col.get('Extra', '')

            # ⛔ Skip kolom auto_increment
            if 'auto_increment' in extra.lower():
                continue

            value = typer.prompt(f"📝 Masukkan nilai untuk kolom '{col_name}'", default="")
            row_data[col_name] = value

    success = db_handler.insert_data(schema, table, row_data)
    if success:
        print(f"✅ Data berhasil ditambahkan ke tabel '{table}'")
    else:
        print(f"❌ Gagal menambahkan data ke tabel '{table}'")

@app.command("table:read-data")
def read_data(
    db: str = typer.Option(..., help="Jenis database"),
    schema: str = typer.Option(DEFAULT_SCHEMA_NAME, help="Nama schema"),
    table: str = typer.Option(..., help="Nama tabel"),
):
    db_handler = get_db_handler(db)
    console = Console()

    # Ambil data dari tabel
    rows = db_handler.read_data(schema, table)
    if not rows:
        print(f"📭 Tidak ada data di tabel '{table}'")
        raise typer.Exit()

    # Ambil nama kolom dari struktur tabel
    columns = db_handler.describe_table(schema, table)
    column_names = [col['Column'] for col in columns]

    # Membuat tabel menggunakan rich
    table = Table(show_header=True, header_style="bold magenta")
    
    # Menambahkan kolom header
    for column in column_names:
        table.add_column(column, style="dim")

    # Menambahkan baris data
    for row in rows:
        table.add_row(*map(str, row))

    # Tampilkan tabel
    console.print(table)

@app.command("table:update-data")
def update_data(
    db: str = typer.Option(..., help="Jenis database"),
    schema: str = typer.Option(DEFAULT_SCHEMA_NAME, help="Nama schema"),
    table: str = typer.Option(..., help="Nama tabel"),
    row_id: int = typer.Option(..., help="ID baris yang ingin diperbarui")
):
    db_handler = get_db_handler(db)
    console = Console()

    # Ambil data dari tabel
    rows = db_handler.read_data(schema, table)
    if not rows:
        print(f"📭 Tidak ada data di tabel '{table}'")
        raise typer.Exit()

    # Tampilkan data yang ada
    table_data = Table(show_header=True, header_style="bold magenta")
    columns = db_handler.describe_table(schema, table)
    column_names = [col['Column'] for col in columns]

    # Menambahkan kolom header
    for column in column_names:
        table_data.add_column(column, style="dim")

    # Menambahkan baris data
    for row in rows:
        table_data.add_row(*map(str, row))

    # Tampilkan data untuk memilih baris
    console.print(table_data)
    print(f"\n📝 Pilih kolom yang ingin diperbarui di baris dengan ID {row_id}")

    # Meminta input untuk kolom dan nilai yang ingin diubah
    column_to_update = typer.prompt("Masukkan nama kolom yang ingin diperbarui")
    new_value = typer.prompt(f"Masukkan nilai baru untuk kolom '{column_to_update}'")

    # Memperbarui data pada tabel
    db_handler.update_data(schema, table, row_id, column_to_update, new_value)
    print(f"✅ Data pada kolom '{column_to_update}' berhasil diperbarui.")

@app.command("table:delete-data")
def delete_data(
    db: str = typer.Option(..., help="Jenis database"),
    schema: str = typer.Option(DEFAULT_SCHEMA_NAME, help="Nama schema"),
    table: str = typer.Option(..., help="Nama tabel"),
    row_id: int = typer.Option(..., help="ID baris yang ingin dihapus")
):
    db_handler = get_db_handler(db)
    console = Console()

    # Ambil data dan tampilkan sebagai tabel
    rows = db_handler.read_data(schema, table)
    if not rows:
        print(f"📭 Tidak ada data di tabel '{table}'")
        raise typer.Exit()

    columns = db_handler.describe_table(schema, table)
    column_names = [col['Column'] for col in columns]

    rich_table = Table(show_header=True, header_style="bold magenta")
    for column in column_names:
        rich_table.add_column(column)

    for row in rows:
        rich_table.add_row(*map(str, row))

    console.print(rich_table)

    confirm = typer.confirm(f"⚠️ Yakin ingin menghapus data dengan ID {row_id} dari tabel '{table}'?")
    if not confirm:
        print("❌ Aksi dibatalkan.")
        raise typer.Exit()

    success = db_handler.delete_data(schema, table, row_id)
    if success:
        print(f"🗑️ Data dengan ID {row_id} berhasil dihapus dari tabel '{table}'.")
    else:
        print("❌ Gagal menghapus data.")

@app.command("table:search")
def search_data(
    db: str = typer.Option(..., help="Jenis database"),
    schema: str = typer.Option(DEFAULT_SCHEMA_NAME, help="Nama schema"),
    table: str = typer.Option(..., help="Nama tabel"),
    column: str = typer.Option(..., help="Kolom yang ingin dicari"),
    keyword: str = typer.Option(..., help="Kata kunci pencarian")
):
    db_handler = get_db_handler(db)
    console = Console()

    results = db_handler.search_data(schema, table, column, keyword)
    if not results:
        print("📭 Tidak ada hasil ditemukan.")
        raise typer.Exit()

    # Ambil struktur kolom
    columns = db_handler.describe_table(schema, table)
    column_names = [col['Column'] for col in columns]

    table_display = Table(show_header=True, header_style="bold cyan")
    for col in column_names:
        table_display.add_column(col)

    for row in results:
        table_display.add_row(*map(str, row))

    console.print(table_display)

@app.command("table:export")
def export_data(
    db: str = typer.Option(..., help="Jenis database"),
    schema: str = typer.Option(DEFAULT_SCHEMA_NAME, help="Nama schema"),
    table: str = typer.Option(..., help="Nama tabel"),
    output: str = typer.Option(None, help="Nama file output CSV (opsional)")
):
    db_handler = get_db_handler(db)

    rows = db_handler.read_data(schema, table)
    if not rows:
        print("📭 Tidak ada data di tabel ini.")
        raise typer.Exit()

    columns = db_handler.describe_table(schema, table)
    column_names = [col['Column'] for col in columns]

    filename = output or f"export_{table}.csv"

    try:
        with open(filename, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(column_names)
            for row in rows:
                writer.writerow(row)
        print(f"📦 Data dari tabel '{table}' berhasil diekspor ke file '{filename}'.")
    except Exception as e:
        print("❌ Error saat ekspor:", e)

@app.command("table:import")
def import_data(
    db: str = typer.Option(..., help="Jenis database"),
    schema: str = typer.Option(DEFAULT_SCHEMA_NAME, help="Nama schema"),
    table: str = typer.Option(..., help="Nama tabel"),
    file: str = typer.Option(..., help="Path ke file CSV yang ingin diimport")
):
    db_handler = get_db_handler(db)

    if not os.path.exists(file):
        print("❌ File tidak ditemukan:", file)
        raise typer.Exit()

    try:
        with open(file, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            data = list(reader)

        if not data:
            print("📭 File CSV kosong.")
            raise typer.Exit()

        success_count = 0
        for row in data:
            cleaned_row = {k: (v if v != "" else None) for k, v in row.items()}
            result = db_handler.insert_data(schema, table, cleaned_row)
            if result:
                success_count += 1

        print(f"✅ Berhasil mengimpor {success_count} dari {len(data)} baris ke tabel '{table}'.")
    except Exception as e:
        print("❌ Gagal mengimpor data:", e)

@app.command("transfer:data")
def transfer_data():
    """Transfer data dari satu DB ke DB lain"""
    interactive_transfer()

if __name__ == "__main__":
    app()