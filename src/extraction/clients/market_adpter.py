from abc import ABC, abstractmethod

class MarketDataProvider(ABC):
    @abstractmethod
    def _master_instrument(self, timeout:int):
        pass

    @abstractmethod
    def extract_data(self, instrument:str):
        pass