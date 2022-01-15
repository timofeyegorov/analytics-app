"""
Данные для подключения:
- к базе данных MySQL
- к hh.ru
- к ройстату (для получения данных по расходам на каждого трафиколога
- к базе данных Redis
"""

config = {
    'database': {
        'user': 'analytic',
        'password': '-6>X%XWu$`6wJwy4',
        'host': 'localhost',
        'db': 'analytic',
        'charset': 'utf8'
    },
    'hh': {
        'client_id': 'QVOP5SPTVVCK20L3NELQ8EJ988BJAPL0ETR0T9OV5T5OF4EMG3L0JHLA83MHPH65',
        'client_secret': 'SFLASDLNB68O8S9C545SHPLL5VSHUOUEH9ICBSK42KN7O0A53ERHFVFDAJGKD3Q9',
        'employer_id': 3754394,
    },
    'roistat': {
        'api_key': '5f89e56872aff50d17343d2ec1bf6b77',
        'project_id': '150173',
    },
    'redis': {
        'host': 'localhost',
        'port': '6379',
        'db': '1',
    }
}

# DATA_FOLDER = '/data/projects/analytic/python/analytics-app/app/dags/data'
# RESULTS_FOLDER = '/data/projects/analytic/python/app2/app/dags/results'
# CREDENTIALS_FILE = '/data/projects/analytic/python/app2/analytics-322510-46607fe39c6c.json'

DATA_FOLDER = r'C:\Users\admin\PycharmProjects\analytics-app-main\app\dags\data'
RESULTS_FOLDER = r'C:\Users\admin\PycharmProjects\analytics-app-main\app\results'
CREDENTIALS_FILE = r'C:\Users\admin\PycharmProjects\analytics-app-main\analytics-322510-46607fe39c6c.json'
