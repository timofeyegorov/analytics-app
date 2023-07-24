from datetime import datetime
from api.load import start_import
from flask import Flask, render_template, request
from tools import table_number, table_opener, table_time_day, table_opener_number, table_opener_time, show_openers_list, \
    show_numbers_list
from preparData import filter_numbers, filter_openers, filter_delete

app = Flask(__name__)



@app.route("/", methods=["GET", "POST"])
def index():
    # Получаем данные из формы
    if request.method == "POST":
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
    if request.method == "GET":
        return render_template("index.html", data_list=show_openers_list(), numbers_list=show_numbers_list())


# Ниже страницы для отчетов по звонкам
@app.route('/numbers')
def show_numbers():
    pivot_table = table_number()
    table_html = pivot_table.to_html(classes='table table-striped table-bordered')
    return render_template('numbers.html', table=table_html)


@app.route('/openers')
def show_openeres():
    pivot_table = table_opener()
    table_html = pivot_table.to_html(classes='table table-striped table-bordered')
    return render_template('openers.html', table=table_html)


@app.route('/hours')
def show_hours():
    pivot_table = table_time_day()
    table_html = pivot_table.to_html(classes='table table-striped table-bordered')
    return render_template('hours.html', table=table_html)


@app.route('/openernumber', methods=["GET", "POST"])
def show_opener_number():
    if request.method == "GET":
        pivot_table = table_opener_number(2)
        table_html = pivot_table.to_html(classes='table table-striped table-bordered')
        return render_template('openernumber.html', table=table_html)
    if request.method == "POST":
        value = request.form.get('choice')
        if value == 'choice1':
            pivot_table = table_opener_number(1)
            table_html = pivot_table.to_html(classes='table table-striped table-bordered')
            return render_template('openernumber2.html', table=table_html)
        else:
            pivot_table = table_opener_number(2)
            table_html = pivot_table.to_html(classes='table table-striped table-bordered')
            return render_template('openernumber.html', table=table_html)


@app.route('/openerhours', methods=["GET", "POST"])
def show_opener_hours():
    if request.method == "GET":
        pivot_table = table_opener_time(2)
        table_html = pivot_table.to_html(classes='table table-striped table-bordered')
        return render_template('openerhours.html', table=table_html)
    if request.method == "POST":
        value = request.form.get('choice')
        if value == 'choice1':
            pivot_table = table_opener_time(1)
            table_html = pivot_table.to_html(classes='table table-striped table-bordered')
            return render_template('openerhours2.html', table=table_html)
        else:
            pivot_table = table_opener_time(2)
            table_html = pivot_table.to_html(classes='table table-striped table-bordered')
            return render_template('openerhours.html', table=table_html)


if __name__ == '__main__':
    app.run(debug=True)
