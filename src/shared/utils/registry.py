import logging

logger = logging.getLogger(__name__)


class Registry:
    def __init__(self):
        self._items = {}
        logger.debug("Registry initialized")

    def register(self, name: str):
        def decorator(cls):
            if name in self._items:
                logger.error("Duplicate registration attempted", extra={"name": name, "existing": self._items[name].__name__, "new": cls.__name__})
                raise ValueError(f"'{name}' is already registered")
            self._items[name] = cls
            logger.debug("Registered implementation", extra={"name": name, "class": cls.__name__})
            return cls
        return decorator

    def get(self, name: str):
        try:
            logger.debug("Retrieving implementation", extra={"name": name})
            return self._items[name]
        except KeyError:
            available = list(self._items.keys())
            logger.error("No implementation registered", extra={"name": name, "available": available})
            raise KeyError(f"No implementation registered for '{name}'. Available: {available}")