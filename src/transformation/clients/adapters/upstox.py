from pathlib import WindowsPath
from transformation.clients import client_registry
from transformation.clients import DataTransformer

@client_registry.register('upstox')
class UpstoxTransformationAdapter(DataTransformer):
    def __init__(self):
        pass

