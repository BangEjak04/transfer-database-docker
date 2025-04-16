from pymongo import MongoClient
from db.config import MONGO_URI, MONGO_DB

def get_mongo_connection():
    client = MongoClient(MONGO_URI)
    return client[MONGO_DB]
