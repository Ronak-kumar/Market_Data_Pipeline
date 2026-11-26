import importlib
import pkgutil
from types import ModuleType
from shared.observability import get_logger

logger = get_logger(__name__)


def discover_modules(package: ModuleType, *, exclude: set[str] | None = None,) -> None:
    logger.debug(f"Starting module discovery: package={package.__name__}")
    exclude = exclude or set()
    discovered = []

    for module in pkgutil.iter_modules(package.__path__):
        module_name = module.name

        if module_name.startswith("_"):
            continue

        if module_name in exclude:
            continue

        importlib.import_module(f"{package.__name__}.{module_name}")
        discovered.append(module_name)

    logger.info(f"Module discovery completed: package={package.__name__}, discovered_modules={discovered}")