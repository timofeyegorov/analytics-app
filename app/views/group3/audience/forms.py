from app import forms

from . import serializers, data


class AudienceBaseForm(forms.FilterForm):
    template = "group3/audience/form.html"

    tab = forms.fields.HiddenField()
    date_start = forms.fields.DateField(label="С даты")
    date_end = forms.fields.DateField(label="По дату")
    utm = forms.fields.SelectField(
        choices=[("", "--- Выберите тип ---")] + data.UTMEnum.choices(), multivalue=True
    )
    utm_value = forms.fields.StringField(
        placeholder="Введите значение", multivalue=True
    )
    update = forms.fields.SubmitField(placeholder="Обновить", is_action=True)
    reset = forms.fields.ResetField(placeholder="Сбросить", is_action=True)

    def validate_on_submit(self, *args, **kwargs) -> bool:
        is_valid = super().validate_on_submit(*args, **kwargs)
        if is_valid:
            date_start = self.serializer.date_start
            date_end = self.serializer.date_end
            if date_start and date_end and date_start > date_end:
                is_valid = False
                self.set_error(
                    "Начальная дата должна быть меньше или равна конечной даты."
                )
        return is_valid


class AbsoluteForm(AudienceBaseForm):
    action = "group3:audience:absolute"
    serializer_class = serializers.AbsoluteSerializer


class RelativeForm(AudienceBaseForm):
    action = "group3:audience:relative"
    serializer_class = serializers.RelativeSerializer
