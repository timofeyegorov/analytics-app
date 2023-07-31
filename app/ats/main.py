# from datetime import datetime
# from api.load import start_import
# from flask import Flask, render_template, request, redirect, url_for
# from tools import table_number, table_opener, table_time_day, table_opener_number, table_opener_time, show_openers_list, \
#     show_numbers_list, table_timecall
# from preparData import filter_numbers, filter_openers, filter_delete, settings_openers, settings_delete, set_datarange, \
#     show_datarange
# from flask.views import MethodView
#
# app = Flask(__name__)
#
#
# class CallsMain(MethodView):
#     def get(self):
#         return render_template("index.html", data_list=show_openers_list(), numbers_list=show_numbers_list(),
#                                datarange=show_datarange())
#
#     def post(self):
#         if 'change_data' in request.form:
#             start_date = (datetime.strptime(request.form["start_date"], '%Y-%m-%d').date()).strftime('%d-%m-%Y')
#             end_date = (datetime.strptime(request.form["end_date"], '%Y-%m-%d').date()).strftime('%d-%m-%Y')
#             data_range = f'Текущий диапазон данных: с {start_date} по {end_date}'
#             set_datarange(data_range)
#             # Вызываем функцию для получения данных с api
#             if start_import(start_date, end_date) == 200:
#                 message = f'Получены данные звонков с {start_date} по {end_date}'
#             else:
#                 message = f'Ошибка получения данных на стороне Sipuni'
#             return render_template("index.html", data_list=show_openers_list(), numbers_list=show_numbers_list(),
#                                    message=message, datarange=show_datarange())
#         elif 'change_openers' in request.form:
#             filter_openers(request.form.getlist('options'))
#             return redirect(url_for('calls_main'))
#         elif 'change_numbers' in request.form:
#             filter_numbers(request.form.getlist('options'))
#             return redirect(url_for('calls_main'))
#         elif 'dell_filters' in request.form:
#             filter_delete()
#             return redirect(url_for('calls_main'))
#         else:
#             return redirect(url_for('calls_main'))
#
#
# class callsNumbers(MethodView):
#     def get(self):
#         pivot_table = table_number()
#         table_html = pivot_table.to_html(classes='table table-striped table-bordered')
#         return render_template('numbers.html', table=table_html, data_list=show_openers_list(),
#                                numbers_list=show_numbers_list())
#
#     def post(self):
#         if 'change_openers' in request.form:
#             filter_openers(request.form.getlist('options'))
#             return redirect(url_for('calls_numbers'))
#         elif 'change_numbers' in request.form:
#             filter_numbers(request.form.getlist('options'))
#             return redirect(url_for('calls_numbers'))
#         elif 'dell_filters' in request.form:
#             filter_delete()
#             return redirect(url_for('calls_numbers'))
#         else:
#             return redirect(url_for('calls_main'))
#
#
# class callsOpeners(MethodView):
#     def get(self):
#         pivot_table = table_opener()
#         table_html = pivot_table.to_html(classes='table table-striped table-bordered')
#         return render_template('openers.html', table=table_html, data_list=show_openers_list(),
#                                numbers_list=show_numbers_list())
#
#     def post(self):
#         if 'change_openers' in request.form:
#             filter_openers(request.form.getlist('options'))
#             return redirect(url_for('calls_openers'))
#         elif 'change_numbers' in request.form:
#             filter_numbers(request.form.getlist('options'))
#             return redirect(url_for('calls_openers'))
#         elif 'dell_filters' in request.form:
#             filter_delete()
#             return redirect(url_for('calls_openers'))
#         else:
#             return redirect(url_for('calls_main'))
#
#
# class callsHours(MethodView):
#     def get(self):
#         pivot_table = table_time_day()
#         table_html = pivot_table.to_html(classes='table table-striped table-bordered')
#         return render_template('hours.html', table=table_html, data_list=show_openers_list(),
#                                numbers_list=show_numbers_list())
#
#     def post(self):
#         if 'change_openers' in request.form:
#             filter_openers(request.form.getlist('options'))
#             return redirect(url_for('calls_hours'))
#         elif 'change_numbers' in request.form:
#             filter_numbers(request.form.getlist('options'))
#             return redirect(url_for('calls_hours'))
#         elif 'dell_filters' in request.form:
#             filter_delete()
#             return redirect(url_for('calls_hours'))
#         else:
#             return redirect(url_for('calls_main'))
#
#
# class callsOpenerNumber(MethodView):
#     def get(self):
#         pivot_table = table_opener_number(2)
#         table_html = pivot_table.to_html(classes='table table-striped table-bordered')
#         return render_template('openernumber.html', table=table_html, data_list=show_openers_list(),
#                                numbers_list=show_numbers_list())
#
#     def post(self):
#         value = request.form.get('choice')
#         if value == 'choice1':
#             pivot_table = table_opener_number(1)
#             table_html = pivot_table.to_html(classes='table table-striped table-bordered')
#             return render_template('openernumber2.html', table=table_html, data_list=show_openers_list(),
#                                    numbers_list=show_numbers_list())
#         if value == 'choice2':
#             return redirect(url_for('calls_openernumber'))
#         if 'change_openers' in request.form:
#             filter_openers(request.form.getlist('options'))
#             return redirect(url_for('calls_openernumber'))
#         elif 'change_numbers' in request.form:
#             filter_numbers(request.form.getlist('options'))
#             return redirect(url_for('calls_openernumber'))
#         elif 'dell_filters' in request.form:
#             filter_delete()
#             return redirect(url_for('calls_openernumber'))
#         else:
#             return redirect(url_for('calls_main'))
#
#
# class callsOpenerHour(MethodView):
#     def get(self):
#         pivot_table = table_opener_time(2)
#         table_html = pivot_table.to_html(classes='table table-striped table-bordered')
#         return render_template('openerhours.html', table=table_html, data_list=show_openers_list(),
#                                numbers_list=show_numbers_list())
#
#     def post(self):
#         value = request.form.get('choice')
#         if value == 'choice1':
#             pivot_table = table_opener_time(1)
#             table_html = pivot_table.to_html(classes='table table-striped table-bordered')
#             return render_template('openerhours2.html', table=table_html, data_list=show_openers_list(),
#                                    numbers_list=show_numbers_list())
#         if value == 'choice2':
#             return redirect(url_for('calls_openerhour'))
#         if 'change_openers' in request.form:
#             filter_openers(request.form.getlist('options'))
#             return redirect(url_for('calls_openerhour'))
#         elif 'change_numbers' in request.form:
#             filter_numbers(request.form.getlist('options'))
#             return redirect(url_for('calls_openerhour'))
#         elif 'dell_filters' in request.form:
#             filter_delete()
#             return redirect(url_for('calls_openerhour'))
#         else:
#             return redirect(url_for('calls_main'))
#
#
# class callsMedTime(MethodView):
#     def get(self):
#         pivot_table = table_timecall()
#         table_html = pivot_table.to_html(classes='table table-striped table-bordered')
#         return render_template('medtime.html', table=table_html, data_list=show_openers_list(),
#                                numbers_list=show_numbers_list())
#
#     def post(self):
#         if 'change_openers' in request.form:
#             filter_openers(request.form.getlist('options'))
#             return redirect(url_for('calls_medtime'))
#         elif 'change_numbers' in request.form:
#             filter_numbers(request.form.getlist('options'))
#             return redirect(url_for('calls_medtime'))
#         elif 'dell_filters' in request.form:
#             filter_delete()
#             return redirect(url_for('calls_medtime'))
#         else:
#             return redirect(url_for('calls_main'))
#
#
# class callsSettings(MethodView):
#     def get(self):
#         return render_template('settings.html', openers=show_openers_list())
#
#     def post(self):
#         if 'change_settings' in request.form:
#             settings_openers(request.form.getlist('options'))
#             return render_template("settings.html", openers=show_openers_list())
#         elif 'dell_settings' in request.form:
#             settings_delete()
#             return render_template("settings.html", openers=settings_delete())
#         else:
#             return render_template("settings.html", openers=settings_delete())
#
#
# app.add_url_rule('/', view_func=CallsMain.as_view('calls_main'))
# app.add_url_rule('/numbers', view_func=callsNumbers.as_view('calls_numbers'))
# app.add_url_rule('/openers', view_func=callsOpeners.as_view('calls_openers'))
# app.add_url_rule('/hours', view_func=callsHours.as_view('calls_hours'))
# app.add_url_rule('/openernumber', view_func=callsOpenerNumber.as_view('calls_openernumber'))
# app.add_url_rule('/openerhours', view_func=callsOpenerHour.as_view('calls_openerhour'))
# app.add_url_rule('/medtime', view_func=callsMedTime.as_view('calls_medtime'))
# app.add_url_rule('/settings', view_func=callsSettings.as_view('calls_settings'))
#
# if __name__ == '__main__':
#     app.run(debug=True)
