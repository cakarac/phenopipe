from abc import ABC, abstractmethod
from pydantic import BaseModel

class QueryConnection(BaseModel,ABC):
    @abstractmethod
    def get_query(self):
        ...