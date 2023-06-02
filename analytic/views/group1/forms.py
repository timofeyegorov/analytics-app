from analytic import forms

from .serializers import SegmentsSerializer, TurnoverSerializer


class SegmentsForm(forms.FilterForm):
    action = "segments"
    serializer_class = SegmentsSerializer
    template = "group1/segments/form.html"

    date_start = forms.fields.DateField(label="С даты")
    date_end = forms.fields.DateField(label="По дату")
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
                    "Начальная дата должна быть меньше или равна конечно даты."
                )
        return is_valid


class TurnoverForm(forms.FilterForm):
    action = "turnover"
    serializer_class = TurnoverSerializer
    template = "group1/turnover/form.html"

    date_request_start = forms.fields.DateField(label="С даты создания")
    date_request_end = forms.fields.DateField(label="По дату создания")
    date_payment_start = forms.fields.DateField(label="С даты оплаты")
    date_payment_end = forms.fields.DateField(label="По дату оплаты")
    update = forms.fields.SubmitField(placeholder="Обновить", is_action=True)
    reset = forms.fields.ResetField(placeholder="Сбросить", is_action=True)

    # def validate_on_submit(self, *args, **kwargs) -> bool:
    #     is_valid = super().validate_on_submit(*args, **kwargs)
    #     if is_valid:
    #         date_start = self.serializer.date_start
    #         date_end = self.serializer.date_end
    #         if date_start and date_end and date_start > date_end:
    #             is_valid = False
    #             self.set_error(
    #                 "Начальная дата должна быть меньше или равна конечно даты."
    #             )
    #     return is_valid
