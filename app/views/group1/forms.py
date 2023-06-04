from app import forms

from . import serializers


class SegmentsForm(forms.FilterForm):
    action = "group1:segments"
    serializer_class = serializers.SegmentsSerializer
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
                    "Начальная дата должна быть меньше или равна конечной даты."
                )
        return is_valid


class TurnoverForm(forms.FilterForm):
    action = "group1:turnover"
    serializer_class = serializers.TurnoverSerializer
    template = "group1/turnover/form.html"

    tab = forms.fields.HiddenField()
    date_request_start = forms.fields.DateField(label="С даты создания")
    date_request_end = forms.fields.DateField(label="По дату создания")
    date_payment_start = forms.fields.DateField(label="С даты оплаты")
    date_payment_end = forms.fields.DateField(label="По дату оплаты")
    update = forms.fields.SubmitField(placeholder="Обновить", is_action=True)
    reset = forms.fields.ResetField(placeholder="Сбросить", is_action=True)

    def validate_on_submit(self, *args, **kwargs) -> bool:
        is_valid = super().validate_on_submit(*args, **kwargs)
        if is_valid:
            date_request_start = self.serializer.date_request_start
            date_request_end = self.serializer.date_request_end
            date_payment_start = self.serializer.date_payment_start
            date_payment_end = self.serializer.date_payment_end
            if (
                date_request_start
                and date_request_end
                and date_request_start > date_request_end
            ):
                is_valid = False
                self.set_error(
                    "Начальная дата создания должна быть меньше или равна конечной даты создания."
                )
            if (
                date_payment_start
                and date_payment_end
                and date_payment_start > date_payment_end
            ):
                is_valid = False
                self.set_error(
                    "Начальная дата оплаты должна быть меньше или равна конечной даты оплаты."
                )
        return is_valid


class ClustersForm(forms.FilterForm):
    action = "group1:clusters"
    serializer_class = serializers.ClustersSerializer
    template = "group1/clusters/form.html"

    tab = forms.fields.HiddenField()
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
                    "Начальная дата должна быть меньше или равна конечной даты."
                )
        return is_valid


class LandingsForm(forms.FilterForm):
    action = "group1:landings"
    serializer_class = serializers.LandingsSerializer
    template = "group1/landings/form.html"

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
                    "Начальная дата должна быть меньше или равна конечной даты."
                )
        return is_valid


class ChannelsForm(forms.FilterForm):
    action = "group1:channels"
    serializer_class = serializers.ChannelsSerializer
    template = "group1/channels/form.html"

    date_from = forms.fields.DateField(label="С даты")
    date_to = forms.fields.DateField(label="По дату")
    account = forms.fields.SelectField(label="Аккаунт", choices=[])
    update = forms.fields.SubmitField(placeholder="Обновить", is_action=True)
    reset = forms.fields.ResetField(placeholder="Сбросить", is_action=True)

    def validate_on_submit(self, *args, **kwargs) -> bool:
        is_valid = super().validate_on_submit(*args, **kwargs)
        if is_valid:
            date_from = self.serializer.date_from
            date_to = self.serializer.date_to
            if date_from and date_to and date_from > date_to:
                is_valid = False
                self.set_error(
                    "Начальная дата должна быть меньше или равна конечной даты."
                )
        return is_valid


class TrafficSourcesForm(forms.FilterForm):
    action = "group1:traffic_sources"
    serializer_class = serializers.TrafficSourcesSerializer
    template = "group1/traffic-sources/form.html"

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
                    "Начальная дата должна быть меньше или равна конечной даты."
                )
        return is_valid


class SegmentsStatsForm(forms.FilterForm):
    action = "group1:segments_stats"
    serializer_class = serializers.SegmentsStatsSerializer
    template = "group1/segments-stats/form.html"

    tab = forms.fields.HiddenField()
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
                    "Начальная дата должна быть меньше или равна конечной даты."
                )
        return is_valid


class LeadsTAStatsForm(forms.FilterForm):
    action = "group1:leads_ta_stats"
    serializer_class = serializers.LeadsTAStatsSerializer
    template = "group1/leads-ta-stats/form.html"

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
                    "Начальная дата должна быть меньше или равна конечной даты."
                )
        return is_valid
