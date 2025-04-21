import os
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from dotenv import load_dotenv
from .base import DatabaseHandler

load_dotenv()

class CassandraDB(DatabaseHandler):
    def __init__(self):
        self.cluster = Cluster(
            contact_points=[os.getenv("CASSANDRA_HOST", "127.0.0.1")],
            port=int(os.getenv("CASSANDRA_PORT", 9042))
        )
        self.session = self.cluster.connect()

    def create_schema(self, name: str):
        self.session.execute(f"CREATE KEYSPACE IF NOT EXISTS {name} WITH replication = {{'class':'SimpleStrategy', 'replication_factor':1}}")

    def read_schemas(self):
        rows = self.session.execute("SELECT keyspace_name FROM system_schema.keyspaces")
        ignored = [
            "system", "system_auth", "system_schema",
            "system_distributed", "system_traces"
        ]
        return [row.keyspace_name for row in rows if row.keyspace_name not in ignored]

    def delete_schema(self, name: str):
        self.session.execute(f"DROP KEYSPACE IF EXISTS {name}")

    def create_table(self, schema: str, table_name: str, columns):
        self.session.set_keyspace(schema)

        if isinstance(columns, str):
            query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})"
        elif isinstance(columns, list):
            column_defs = []
            for col in columns:
                name = col.get("name")
                tipe = col.get("type")
                if not name or not tipe:
                    raise ValueError("Setiap kolom harus memiliki 'name' dan 'type'")
                column_defs.append(f"{name} {tipe}")
            query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(column_defs)})"
        else:
            raise TypeError("Kolom harus berupa string atau list of dict.")
        
        self.session.execute(query)

    def read_tables(self, schema: str):
        rows = self.session.execute(f"SELECT table_name FROM system_schema.tables WHERE keyspace_name = '{schema}'")
        return [row.table_name for row in rows]

    def describe_table(self, schema: str, table: str):
        rows = self.session.execute(f"""
            SELECT column_name, kind, type 
            FROM system_schema.columns 
            WHERE keyspace_name = '{schema}' AND table_name = '{table}'
        """)
        return [{"Column": r.column_name, "Kind": r.kind, "Type": r.type} for r in rows]

    def delete_table(self, schema: str, table: str):
        self.session.set_keyspace(schema)
        self.session.execute(f"DROP TABLE IF EXISTS {table}")

    def insert_data(self, schema: str, table: str, data: dict) -> bool:
        self.session.set_keyspace(schema)
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['%s'] * len(data))
        values = tuple(data.values())
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        try:
            self.session.execute(query, values)
            return True
        except Exception as e:
            print("❌ Error:", e)
            return False

    def read_data(self, schema: str, table: str):
        self.session.set_keyspace(schema)
        try:
            rows = self.session.execute(f"SELECT * FROM {table}")
            return list(rows)
        except Exception as e:
            print("❌ Error:", e)
            return []

    def update_data(self, schema: str, table: str, row_id: str, column: str, new_value: str):
        # Cassandra tidak mendukung UPDATE berdasarkan id yang tidak menjadi PRIMARY KEY
        print("⚠️ UPDATE hanya didukung jika kolom target adalah bagian dari PRIMARY KEY.")

    def delete_data(self, schema: str, table: str, row_id: str) -> bool:
        print("⚠️ DELETE membutuhkan PRIMARY KEY lengkap untuk dieksekusi di Cassandra.")
        return False

    def search_data(self, schema: str, table: str, column: str, keyword: str):
        print("🔍 Pencarian LIKE belum didukung secara langsung di Cassandra.")
        return []
