from abc import ABC, abstractmethod

class DatabaseHandler(ABC):
    @abstractmethod
    def create_schema(self, name: str): pass
    
    @abstractmethod
    def read_schemas(self): pass
    
    @abstractmethod
    def delete_schema(self, name: str): pass
    
    @abstractmethod
    def create_table(self, schema: str, table_name: str, columns): pass