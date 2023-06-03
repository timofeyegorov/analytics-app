from typing import Dict, Any


class ContextTemplate(dict):
    def __call__(self, name: str, value: Any):
        self.update({name: value})
