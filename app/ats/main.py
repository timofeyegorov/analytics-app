from datetime import datetime
from api.load import start_import
from flask import Flask, render_template, request
from tools import table_number, table_opener, table_time_day, table_opener_number, table_opener_time, show_openers_list, \
    show_numbers_list
from preparData import filter_numbers, filter_openers, filter_delete
from flask.views import MethodView

app = Flask(__name__)


class CallsMain(MethodView):
    def get(self):
        return render_template("index.html", data_list=show_openers_list(), numbers_list=show_numbers_list())

    def post(self):
        if 'change_data' in request.form:
            start_date = (datetime.strptime(request.form["start_date"], '%Y-%m-%d').date()).strftime('%d-%m-%Y')
            end_date = (datetime.strptime(request.form["end_date"], '%Y-%m-%d').date()).strftime('%d-%m-%Y')
            # Вызываем функцию для получения данных с api
            start_import(start_date, end_date)
            message = f'Получены данные звонков с {start_date} по {end_date}'
            return render_template("index.html", data_list=show_openers_list(), numbers_list=show_numbers_list(),
                                   message=message)
        elif 'change_openers' in request.form:
            filter_openers(request.form.getlist('options'))
            return render_template("index.html", data_list=show_openers_list(), numbers_list=show_numbers_list())
        elif 'change_numbers' in request.form:
            filter_numbers(request.form.getlist('options'))
            return render_template("index.html", data_list=show_openers_list(), numbers_list=show_numbers_list())
        elif 'dell_filters' in request.form:
            filter_delete()
            return render_template("index.html", data_list=show_openers_list(), numbers_list=show_numbers_list())
        else:
            return render_template("index.html", data_list=show_openers_list(), numbers_list=show_numbers_list())


class callsNumbers(MethodView):
    def get(self):
        pivot_table = table_number()
        table_html = pivot_table.to_html(classes='table table-striped table-bordered')
        return render_template('numbers.html', table=table_html)


class callsOpeners(MethodView):
    def get(self):
        pivot_table = table_opener()
        table_html = pivot_table.to_html(classes='table table-striped table-bordered')
        return render_template('openers.html', table=table_html)


class callsHours(MethodView):
    def get(self):
        pivot_table = table_time_day()
        table_html = pivot_table.to_html(classes='table table-striped table-bordered')
        return render_template('hours.html', table=table_html)


class callsOpenerNumber(MethodView):
    def get(self):
        pivot_table = table_opener_number(2)
        table_html = pivot_table.to_html(classes='table table-striped table-bordered')
        return render_template('openernumber.html', table=table_html)

    def post(self):
        value = request.form.get('choice')
        if value == 'choice1':
            pivot_table = table_opener_number(1)
            table_html = pivot_table.to_html(classes='table table-striped table-bordered')
            return render_template('openernumber2.html', table=table_html)
        else:
            pivot_table = table_opener_number(2)
            table_html = pivot_table.to_html(classes='table table-striped table-bordered')
            return render_template('openernumber.html', table=table_html)


class callsOpnerHour(MethodView):
    def get(self):
        pivot_table = table_opener_time(2)
        table_html = pivot_table.to_html(classes='table table-striped table-bordered')
        return render_template('openerhours.html', table=table_html)

    def post(self):
        value = request.form.get('choice')
        if value == 'choice1':
            pivot_table = table_opener_time(1)
            table_html = pivot_table.to_html(classes='table table-striped table-bordered')
            return render_template('openerhours2.html', table=table_html)
        else:
            pivot_table = table_opener_time(2)
            table_html = pivot_table.to_html(classes='table table-striped table-bordered')
            return render_template('openerhours.html', table=table_html)


app.add_url_rule('/', view_func=CallsMain.as_view('calls_main'))
app.add_url_rule('/numbers', view_func=callsNumbers.as_view('calls_numbers'))
app.add_url_rule('/openers', view_func=callsOpeners.as_view('calls_openers'))
app.add_url_rule('/hours', view_func=callsHours.as_view('calls_hours'))
app.add_url_rule('/openernumber', view_func=callsOpenerNumber.as_view('calls_openernumber'))
app.add_url_rule('/openerhours', view_func=callsOpenerNumber.as_view('calls_openerhour'))

if __name__ == '__main__':
    app.run(debug=True)
