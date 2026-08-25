from abc import ABC, abstractmethod

class ClientRegistry:
    _adapters = {}

    @classmethod
    def register(cls, name: str):
        def decorator(adapter_cls):
            cls._adapters[name] = adapter_cls
            return adapter_cls
        return decorator

    @classmethod
    def get_adapter(cls, name: str):
        adapter_cls = cls._adapters.get(name)
        if adapter_cls is None:
            raise ValueError(f"No adapter registered under the name '{name}'")
        return adapter_cls