from transformation.clients import transformer_registry
from transformation.clients import DataTransformer

@transformer_registry.register('groww')
class GrowwTransformationAdapter(DataTransformer):
    def __init__(self):
        pass