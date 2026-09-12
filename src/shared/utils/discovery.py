import importlib
import pkgutil
from types import ModuleType


def discover_modules(package: ModuleType, *, exclude: set[str] | None = None,) -> None:

    exclude = exclude or set()

    for module in pkgutil.iter_modules(package.__path__):

        module_name = module.name

        if module_name.startswith("_"):
            continue

        if module_name in exclude:
            continue

        importlib.import_module(f"{package.__name__}.{module_name}")