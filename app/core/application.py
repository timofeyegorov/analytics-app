from flask import Flask


class Application:
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        try:
            if isinstance(args[0], Flask):
                args[0].extensions.update({self.__class__.__name__: self})
        except IndexError:
            pass

    def ext(self, name: str) -> "Application":
        return self.extensions.get(name)
