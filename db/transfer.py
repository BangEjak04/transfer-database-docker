# db_transfer.py
from getpass import getpass
import importlib
from rich.prompt import Prompt
from rich.console import Console

console = Console()

def prompt_db_config(role):
    db_type = Prompt.ask(f"[cyan]{role} database type[/cyan]", choices=["mysql", "postgres", "mongodb", "cassandra"])
    host = Prompt.ask(f"[cyan]{role} host[/cyan]", default="localhost")
    port = Prompt.ask(f"[cyan]{role} port[/cyan]", default="3306" if db_type == "mysql" else "5432" if db_type == "postgres" else "27017" if db_type == "mongodb" else "9042")
    user = Prompt.ask(f"[cyan]{role} username[/cyan]", default="root" if db_type in ["mysql", "postgres"] else "")
    password = getpass(f"{role} password: ") if db_type in ["mysql", "postgres"] else None
    return {
        "db_type": db_type,
        "host": host,
        "port": int(port),
        "user": user,
        "password": password
    }

def get_handler_from_config(config):
    module = importlib.import_module(f"db.{config['db_type']}")
    handler_class = getattr(module, f"{config['db_type'].capitalize()}DB")
    return handler_class(config)

def interactive_transfer():
    console.rule("[bold green]Database Source Configuration")
    source_config = prompt_db_config("Source")
    
    console.rule("[bold green]Database Target Configuration")
    target_config = prompt_db_config("Target")

    source_schema = Prompt.ask("[yellow]Source schema/database name[/yellow]")
    source_table = Prompt.ask("[yellow]Source table/collection name[/yellow]")

    target_schema = Prompt.ask("[yellow]Target schema/database name[/yellow]", default=source_schema)
    target_table = Prompt.ask("[yellow]Target table/collection name[/yellow]", default=source_table)

    # Create handler for source and target
    source_handler = get_handler_from_config(source_config)
    target_handler = get_handler_from_config(target_config)

    # Baca data dari source
    data = source_handler.read_data(source_schema, source_table)
    if not data:
        console.print("❌ Tidak ada data untuk ditransfer.", style="bold red")
        return

    # Cek dan buat schema/tabel jika belum ada
    try:
        target_handler.create_schema(target_schema)
    except:
        pass  # Jika DB seperti MongoDB tidak punya konsep schema

    try:
        # Ambil struktur kolom dari source
        columns = source_handler.describe_table(source_schema, source_table)
        if columns:
            target_handler.create_table(target_schema, target_table, columns)
    except Exception as e:
        console.print(f"[red]❗ Gagal membuat tabel: {e}[/red]")

    # Masukkan data ke target
    for row in data:
        try:
            row_dict = row if isinstance(row, dict) else dict(row)
            target_handler.insert_data(target_schema, target_table, row_dict)
        except Exception as e:
            console.print(f"[red]❗ Gagal insert data: {e}[/red]")

    console.print(f"[green]✅ Transfer selesai. {len(data)} data berhasil ditransfer.[/green]")
