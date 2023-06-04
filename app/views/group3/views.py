from flask import current_app, request

from app.views.base import TemplateView, DownloadView


class PaymentsAccumulationView(TemplateView):
    template_name = "group3/payments-accumulation/index.html"
    title = "Оплаты и ROI - накопление"

    def get(self, *args, **kwargs):
        pickle_app = current_app.app("pickle")

        self.context("tab", request.args.get("tab"))
        self.context("tables", pickle_app.load("payments_accumulation"))

        return super().get(*args, **kwargs)


class MarginalityView(TemplateView):
    template_name = "group3/marginality/index.html"
    title = "Маржинальность"

    def get(self, *args, **kwargs):
        pickle_app = current_app.app("pickle")

        self.context("tab", request.args.get("tab"))
        self.context("tables", pickle_app.load("marginality"))

        return super().get(*args, **kwargs)


class GetPlotCSVView(DownloadView):
    mimetype = "text/csv"

    def get(self, *args, **kwargs):
        pickle_app = current_app.app("pickle")

        marginality = request.args.get("value")
        if ", " in marginality:
            report, subname = marginality.split(", ")
            data = pickle_app.load(report)
            data = data[subname]
        else:
            report = marginality
            data = pickle_app.load(report)

        self.filename = f"{report}.csv"
        self.file = data.to_csv()

        return super().get(*args, **kwargs)
