import os
import pymysql
from dotenv import load_dotenv
from .base import DatabaseHandler

load_dotenv()

class MySQLDB(DatabaseHandler):
    def __init__(self):
        self.conn = pymysql.connect(
            host=os.getenv("MYSQL_HOST", "localhost"),
            user=os.getenv("MYSQL_USER", "root"),
            password=os.getenv("MYSQL_PASSWORD", ""),
            port=int(os.getenv("MYSQL_PORT", 3306)),
        )
        self.cursor = self.conn.cursor()
        
    def create_schema(self, name: str):
        self.cursor.execute(f"CREATE DATABASE IF NOT EXISTS {name}")
        self.conn.commit()
        
    def read_schemas(self):
        self.cursor.execute("SHOW DATABASES")
        result = [row[0] for row in self.cursor.fetchall()]
        ignored = ["information_schema", "mysql", "performance_schema", "sys"]
        return [r for r in result if r not in ignored]

    def delete_schema(self, name: str):
        self.cursor.execute(f"DROP DATABASE IF EXISTS {name}")
        self.conn.commit()
        
    def create_table(self, schema: str, table_name: str, columns):
        cursor = self.conn.cursor()
        cursor.execute(f"USE {schema}")

        if isinstance(columns, str):
            columns_def = columns
        elif isinstance(columns, list):
            column_defs = []
            for col in columns:
                col_name = col.get("name")
                col_type = col.get("type")
                if not col_name or not col_type:
                    raise ValueError("Setiap kolom harus punya 'name' dan 'type'")
                column_defs.append(f"{col_name} {col_type}")
            columns_def = ", ".join(column_defs)
        else:
            raise TypeError("Parameter 'columns' harus string atau list of dict.")

        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_def})"
        cursor.execute(query)
        self.conn.commit()

    def read_tables(self, schema: str):
        cursor = self.conn.cursor()
        cursor.execute(f"USE {schema}")
        cursor.execute("SHOW TABLES")
        results = cursor.fetchall()

        return [t[0] for t in results]
    
    def describe_table(self, schema: str, table: str) -> list[dict]:
        query = """
            SELECT 
                COLUMN_NAME AS 'Column',
                COLUMN_TYPE AS 'Type',
                IS_NULLABLE AS 'Nullable',
                COLUMN_KEY AS 'Key',
                COLUMN_DEFAULT AS 'Default',
                EXTRA AS 'Extra'
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        """
        self.cursor.execute(query, (schema, table))
        columns = [col[0] for col in self.cursor.description]
        rows = self.cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]

    def delete_table(self, schema: str, table: str):
        cursor = self.conn.cursor()
        self.cursor.execute(f"DROP TABLE IF EXISTS {schema}.{table}")
        self.conn.commit()
        print(f"✅ Tabel '{table}' berhasil dihapus di schema '{schema}'.")
    
    def insert_data(self, schema: str, table: str, data: dict) -> bool:
        try:
            columns = ', '.join(f"`{col}`" for col in data.keys())
            placeholders = ', '.join(['%s'] * len(data))
            values = list(data.values())
            query = f"INSERT INTO `{schema}`.`{table}` ({columns}) VALUES ({placeholders})"
            self.cursor.execute(query, values)
            self.conn.commit()
            return True
        except Exception as e:
            print("❌ Error:", e)
            return False
    
    def read_data(self, schema: str, table: str) -> list:
        try:
            self.cursor.execute(f"USE `{schema}`;")
            self.cursor.execute(f"SELECT * FROM `{table}`;")
            rows = self.cursor.fetchall()
            return rows
        except Exception as e:
            print("❌ Error:", e)
            return []
    
    def update_data(self, schema: str, table: str, row_id: int, column: str, new_value: str):
        try:
            # Menggunakan query untuk memperbarui data
            query = f"UPDATE `{table}` SET `{column}` = %s WHERE id = %s"
            self.cursor.execute(query, (new_value, row_id))
            self.conn.commit()
        except Exception as e:
            print("❌ Error:", e)
            self.conn.rollback()
    
    def delete_data(self, schema: str, table: str, row_id: int) -> bool:
        try:
            query = f"DELETE FROM `{table}` WHERE id = %s"
            self.cursor.execute(query, (row_id,))
            self.conn.commit()
            return self.cursor.rowcount > 0
        except Exception as e:
            print("❌ Error:", e)
            self.conn.rollback()
            return False

    def search_data(self, schema: str, table: str, column: str, keyword: str) -> list:
        try:
            query = f"SELECT * FROM `{schema}`.`{table}` WHERE `{column}` LIKE %s"
            like_pattern = f"%{keyword}%"
            self.cursor.execute(query, (like_pattern,))
            return self.cursor.fetchall()
        except Exception as e:
            print("❌ Error:", e)
            return []
