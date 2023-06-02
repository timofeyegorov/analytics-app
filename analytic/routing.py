from analytic import views


"""
keys: name of route
values: tuple of
    1. url
    2. view class
"""
MAPPER = {
    # Главная страница и страница авторизации
    "index": ("/", views.IndexView),
    "login": ("/login", views.LoginView),
    # Страницы группы 1
    "segments": ("/segments", views.group1.SegmentsView),
    "turnover": ("/turnover", views.group1.TurnoverView),
    # Все остальные страницы, не вошедшие в верхние: страница не найдена
    "not_found": ("/<path:path>", views.NotFoundView),
    # "statistics": ("/statistics", views.StatisticsView),
}
