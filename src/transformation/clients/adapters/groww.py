from transformation.clients import client_registry
from transformation.clients import DataTransformer

@client_registry.register('groww')
class GrowwTransformationAdapter(DataTransformer):
    def __init__(self):
        pass