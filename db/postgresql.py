import os
import psycopg2
from dotenv import load_dotenv
from .base import DatabaseHandler

load_dotenv()

class PostgreSQLDB(DatabaseHandler):
    def __init__(self):
        self.conn = psycopg2.connect(
            host=os.getenv("POSTGRESQL_HOST"),
            user=os.getenv("POSTGRESQL_USER"),
            password=os.getenv("POSTGRESQL_PASSWORD"),
            database=os.getenv("POSTGRESQL_DATABASE"),
            port=int(os.getenv("POSTGRESQL_PORT", 5432))
        )
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()
    
    def create_schema(self, name: str) -> bool:
        self.cursor.execute(f"CREATE SCHEMA {name}")
        self.conn.commit()
    
    def read_schemas(self) -> list[str]:
        self.cursor.execute("""
            SELECT schema_name 
            FROM information_schema.schemata
            WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'pg_temp_1', 'pg_toast_temp_1', 'public')
            ORDER BY schema_name
        """)
        schemas = [row[0] for row in self.cursor.fetchall()]
        
        # Jangan cetak schema di sini, cukup kembalikan list-nya saja
        return schemas

    def delete_schema(self, name: str) -> bool:
        self.cursor.execute(f"DROP SCHEMA IF EXISTS {name} CASCADE")
        self.conn.commit()
    
    def create_table(self, schema: str, table_name: str, columns: list[dict]):
        if isinstance(columns, str):
            columns_def = columns
        elif isinstance(columns, list):
            column_defs = []
            for col in columns:
                col_name = col.get("name")
                col_type = col.get("type")
                if not col_name or not col_type:
                    raise ValueError("Each column must have 'name' and 'type'")
                column_defs.append(f"{col_name} {col_type}")
            columns_def = ", ".join(column_defs)
        else:
            raise TypeError("The 'columns' parameter must be a string or list of dict.")

        query = f"CREATE TABLE IF NOT EXISTS {schema}.{table_name} ({columns_def})"
        self.cursor.execute(query)
        self.conn.commit()

    def read_tables(self, schema: str):
        self.cursor.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema = %s", (schema,))
        tables = [row[0] for row in self.cursor.fetchall()]
        return tables

    def describe_table(self, schema: str, table: str):
        query = """
            SELECT 
                column_name AS "Column",
                data_type AS "Type",
                is_nullable AS "Nullable",
                column_default AS "Default"
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
        """
        self.cursor.execute(query, (schema, table))
        columns = [desc[0] for desc in self.cursor.description]
        rows = self.cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]
    
    def delete_table(self, schema: str, table: str):
        self.cursor.execute(f"DROP TABLE IF EXISTS {schema}.{table}")
        self.conn.commit()
        print(f"✅ Tabel '{table}' berhasil dihapus di schema '{schema}'.")
    
    def insert_data(self, schema: str, table: str, data: dict) -> bool:
        try:
            columns = ', '.join(f'"{k}"' for k in data.keys())
            placeholders = ', '.join(['%s'] * len(data))
            values = tuple(data.values())

            query = f"INSERT INTO {schema}.{table} ({columns}) VALUES ({placeholders})"
            self.cursor.execute(query, values)
            self.conn.commit()
            return True
        except Exception as e:
            print("❌ Error:", e)
            self.conn.rollback()
            return False
    
    def read_data(self, schema: str, table: str):
        try:
            self.cursor.execute(f"SELECT * FROM {schema}.{table}")
            rows = self.cursor.fetchall()
            return rows
        except Exception as e:
            print("❌ Error:", e)
            return []

    def update_data(self, schema: str, table: str, row_id: int, column: str, new_value: str):
        try:
            query = f"UPDATE {schema}.{table} SET {column} = %s WHERE id = %s"
            self.cursor.execute(query, (new_value, row_id))
            self.conn.commit()
        except Exception as e:
            print("❌ Error:", e)
            self.conn.rollback()

    def delete_data(self, schema: str, table: str, row_id: int) -> bool:
        try:
            query = f"DELETE FROM {schema}.{table} WHERE id = %s"
            self.cursor.execute(query, (row_id,))
            self.conn.commit()
            return self.cursor.rowcount > 0
        except Exception as e:
            print("❌ Error:", e)
            self.conn.rollback()
            return False

    def search_data(self, schema: str, table: str, column: str, keyword: str) -> list:
        try:
            query = f"SELECT * FROM {schema}.{table} WHERE {column} LIKE %s"
            like_pattern = f"%{keyword}%"
            self.cursor.execute(query, (like_pattern,))
            return self.cursor.fetchall()
        except Exception as e:
            print("❌ Error:", e)
            return []

