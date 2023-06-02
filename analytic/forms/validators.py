from wtforms.validators import DataRequired as DataRequiredWT


class DataRequired(DataRequiredWT):
    def __init__(self, *args, **kwargs):
        kwargs.setdefault("message", "Обязательно поле")
        super().__init__(*args, **kwargs)
