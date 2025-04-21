# Implementasi MongoDB untuk DatabaseHandler
import os
from dotenv import load_dotenv
from pymongo import MongoClient
from bson.objectid import ObjectId
from .base import DatabaseHandler

load_dotenv()

class MongoDB(DatabaseHandler):
    def __init__(self):
        self.client = MongoClient(
            os.getenv("MONGODB_URI", "mongodb://localhost:27017/"),
        )

    def create_schema(self, name: str):
        self.client[name]

    def read_schemas(self):
        system_dbs = {"admin", "config", "local"}
        databases = self.client.list_database_names()
        return [db for db in databases if db not in system_dbs]


    def delete_schema(self, name: str):
        self.client.drop_database(name)

    def create_table(self, schema: str, table_name: str, columns):
        db = self.client[schema]
        db.create_collection(table_name)

    def read_tables(self, schema: str):
        db = self.client[schema]
        return db.list_collection_names()

    def describe_table(self, schema: str, table: str) -> list[dict]:
        db = self.client[schema]
        sample = db[table].find_one()
        if sample:
            return [{"Field": key, "Type": str(type(value).__name__)} for key, value in sample.items()]
        return []

    def delete_table(self, schema: str, table: str):
        db = self.client[schema]
        db.drop_collection(table)

    def insert_data(self, schema: str, table: str, data: dict) -> bool:
        try:
            db = self.client[schema]
            db[table].insert_one(data)
            return True
        except Exception as e:
            print("❌ Error:", e)
            return False

    def read_data(self, schema: str, table: str) -> list:
        db = self.client[schema]
        return list(db[table].find())

    def update_data(self, schema: str, table: str, row_id: str, column: str, new_value: str):
        db = self.client[schema]
        db[table].update_one({"_id": ObjectId(row_id)}, {"$set": {column: new_value}})

    def delete_data(self, schema: str, table: str, row_id: str) -> bool:
        db = self.client[schema]
        result = db[table].delete_one({"_id": ObjectId(row_id)})
        return result.deleted_count > 0

    def search_data(self, schema: str, table: str, column: str, keyword: str) -> list:
        db = self.client[schema]
        query = {column: {"$regex": keyword, "$options": "i"}}
        return list(db[table].find(query))

