from .base import Invalidator


class NullInvalidator(Invalidator):
    def __init__(self, *args, **kwargs):
        pass

    def add_to_flush_list(self, mapping):
        return
