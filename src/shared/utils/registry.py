class Registry:
    def __init__(self):
        self._items = {}

    def register(self, name: str):
        def decorator(cls):
            if name in self._items:
                raise ValueError(f"'{name}' is already registered")
            self._items[name] = cls
            return cls
        return decorator

    def get(self, name: str):
        try:
            return self._items[name]
        except KeyError:
            raise KeyError(f"No implementation registered for '{name}'. "f"Available: {list(self._items)}")