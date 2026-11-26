from shared.observability import get_logger

logger = get_logger(__name__)


class Registry:
    def __init__(self):
        self._items = {}
        logger.debug("Registry initialized")

    def register(self, name: str):
        def decorator(cls):
            if name in self._items:
                logger.error(f"Duplicate registration attempted: reg_name={name}, existing={self._items[name].__name__}, new={cls.__name__}")
                raise ValueError(f"'{name}' is already registered")
            self._items[name] = cls
            logger.debug(f"Registered implementation: reg_name={name}, class={cls.__name__}")
            return cls
        return decorator

    def get(self, name: str):
        try:
            logger.debug(f"Retrieving implementation: reg_name={name}")
            return self._items[name]
        except KeyError:
            available = list(self._items.keys())
            logger.error(f"No implementation registered: reg_name={name}, available={available}")
            raise KeyError(f"No implementation registered for '{name}'. Available: {available}")