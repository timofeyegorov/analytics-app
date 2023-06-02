from flask import current_app, request

from analytic.views.base import FormView
from analytic.utils import segments_calculate

from .forms import SegmentsForm, TurnoverForm


class SegmentsView(FormView):
    template_name = "group1/segments/index.html"
    title = "Сегменты"
    form_class = SegmentsForm

    def form_valid(self):
        pickle_app = current_app.ext("pickle")

        date_start = self.form.serializer.date_start
        date_end = self.form.serializer.date_end

        if date_start or date_end:
            leads = pickle_app.load("leads")
            leads["created_at"] = leads["created_at"].apply(lambda item: item.date())

            if date_start:
                leads = leads[leads["created_at"] >= date_start]

            if date_end:
                leads = leads[leads["created_at"] <= date_end]

            if not len(leads):
                self.form.set_error("Нет данных для заданного периода")
                tables = None
            else:
                tables = segments_calculate(leads)

        else:
            tables = pickle_app.load("segments")

        self.context("tables", tables)


class TurnoverView(FormView):
    template_name = "group1/turnover/index.html"
    title = "Обороты"
    form_class = TurnoverForm

    def form_valid(self):
        pickle_app = current_app.ext("pickle")

        date_request_start = self.form.serializer.date_request_start
        date_request_end = self.form.serializer.date_request_end
        date_payment_start = self.form.serializer.date_payment_start
        date_payment_end = self.form.serializer.date_payment_end

        tab = request.args.get("tab")
        if (
            date_request_start
            or date_request_end
            or date_payment_start
            or date_payment_end
        ):
            leads = pickle_app.load("leads")
            leads["created_at"] = leads["created_at"].apply(lambda item: item.date())
            leads["date_payment"] = leads["date_payment"].apply(
                lambda item: item.date()
            )
            print(leads)
        #     if date_request_start:
        #         leads = leads[
        #             leads.created_at
        #             >= datetime.strptime(date_request_start, "%Y-%m-%d")
        #         ]
        #     if date_request_end:
        #         leads = leads[
        #             leads.created_at <= datetime.strptime(date_request_end, "%Y-%m-%d")
        #         ]
        #     if date_payment_start:
        #         leads = leads[
        #             leads.date_payment
        #             >= datetime.strptime(date_payment_start, "%Y-%m-%d")
        #         ]
        #     if date_payment_end:
        #         leads = leads[
        #             leads.date_payment
        #             <= datetime.strptime(date_payment_end, "%Y-%m-%d")
        #         ]
        #     if len(leads) == 0:
        #         return render_template(
        #             "turnover.html", error="Нет данных для заданного периода"
        #         )
        #     # return render_template(
        #     #     'turnover.html',
        #     #     error='Not enough data',
        #     #     date_request_start=date_request_start,
        #     #     date_request_end=date_request_end,
        #     #     date_payment_start=date_payment_start,
        #     #     date_payment_end=date_payment_end,
        #     #     tab=tab
        #     #     )
        #     tables = calculate_turnover(leads)
        #     return render_template(
        #         "turnover.html",
        #         tables=tables,
        #         # date_request_start=date_request_start,
        #         # date_request_end=date_request_end,
        #         # date_payment_start=date_payment_start,
        #         # date_payment_end=date_payment_end,
        #         tab=tab,
        #     )
        # tables = get_turnover()
        #
        # return render_template(
        #     "turnover.html",
        #     tables=tables,
        #     # date_request_start=date_request_start,
        #     # date_request_end=date_request_end,
        #     # date_payment_start=date_payment_start,
        #     # date_payment_end=date_payment_end,
        #     tab=tab,
        # )
