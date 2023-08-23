class JsonParseService:

    @staticmethod
    def minutes_to_hours(minute: int):
        hours = minute // 60
        minutes = minute % 60
        return f'{hours} ч. {minutes} мин.' if hours >= 1 else f'{minutes} мин.'

    def to_dict(self, tmp_json: dict) -> dict:
        data = dict()

        data['quality'] = [tmp_json.get('quality')]
        data['quality_percent'] = [tmp_json.get('quality')]
        data['typeZoom'] = [tmp_json.get('type')]
        data['ratio'] = [tmp_json.get('ratio')]
        data['expand'] = ['']
        data['question'] = [tmp_json.get('question')]
        data['push_quantity'] = [tmp_json.get('push')]
        data['profit_quantity'] = [tmp_json.get('profit')]
        data['duration'] = [
            self.minutes_to_hours(tmp_json.get('duration'))
        ]
        data['text'] = [tmp_json.get('text_type')]

        # first hierarchy (total info)
        total_reports = tmp_json.get('content')['8'].get('Отчеты')
        data['revealing'] = [total_reports.get('Выявление')]
        data['presentation'] = [total_reports.get('Презентация')]
        data['objections'] = [total_reports.get('Возражения')]
        data['materials'] = [None]
        data['general_report'] = [total_reports.get('Общий отчет')]

        # second hierarchy (revealing)
        revealing = tmp_json.get('content')['4'].get('Выявление')
        data['revealing_manager_questions'] = [revealing.get("Вопросы менеджера")]
        data['revealing_needs'] = [revealing.get("Потребности")]
        data['revealing_work'] = [revealing.get("Трудоустройство")]
        data['revealing_salary'] = [revealing.get("Зарплата")]
        data['revealing_remote'] = [revealing.get("Удаленка")]
        data['revealing_personal_up'] = [revealing.get("Развитие личностное")]
        data['revealing_it_up'] = [revealing.get("Развитие в IT")]
        data['revealing_activity_change'] = [revealing.get("Смена деятельности")]
        data['revealing_hobby'] = [revealing.get("Хобби")]
        data['revealing_career'] = [revealing.get("Карьера")]
        data['revealing_project'] = [revealing.get("Проект")]

        # second hierarchy (presentation)
        presentation = tmp_json.get('content')['5'].get('Презентация')
        data['presentation_advantages'] = [presentation.get('Презентация').get('Преимущества название МП')]
        data['presentation_demopanel'] = [presentation.get('Презентация').get('Демопанель')]
        data['presentation_platform'] = [presentation.get('Презентация').get('Платформа обучения')]
        data['presentation_stud'] = [presentation.get('Презентация').get('Примеры других студентов')]
        data['presentation_tariff'] = [presentation.get('Тарифы, Пуш, оплата').get('Тарифы')]
        data['presentation_push'] = [presentation.get('Тарифы, Пуш, оплата').get('Пуш')]
        data['presentation_payment'] = [presentation.get('Тарифы, Пуш, оплата').get('Варианты оплат')]

        # second hierarchy (objections)
        objections = tmp_json.get('content')['6'].get('Возражения')
        data['objections_customer'] = [objections.get('Возражения клиента')]
        data['objections_not_working'] = [objections.get('Не получится')]
        data['objections_training_period'] = [objections.get('Срок обучения')]
        data['objections_filling_courses'] = [objections.get('Наполнение курсов')]
        data['objections_expensive'] = [objections.get('Дорого')]
        data['objections_profession_not_suitable'] = [objections.get('Не подойдет профессия')]
        data['objections_think_advise'] = [objections.get('Подумаю/Посоветуюсь')]
        data['objections_contract'] = [objections.get('Изучить договор/материалы')]
        data['objections_not_enough_skill'] = [objections.get('Не хватит навыков и опыта')]
        data['objections_project_to_approved'] = [objections.get('Нужно согласовать проект')]
        data['objections_before_buy'] = [objections.get('Нужно попробовать перед покупкой')]
        data['objections_reviews'] = [objections.get('Отзывы')]
        data['objections_employment'] = [objections.get('Трудоустройство')]
        data['objections_developer_salary'] = [objections.get('Зарплата разработчика')]

        # second hierarchy (materials)
        materials = tmp_json.get('content')['7'].get('Материалы')
        data['materials_promised'] = [materials.get('Обещанные материалы менеджером')]
        data['materials_gpt_recommendations'] = [materials.get('Рекомендации GPT')]

        return data
