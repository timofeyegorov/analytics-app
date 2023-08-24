class JsonParseService:

    @staticmethod
    def minutes_to_hours(minute: int):
        hours = minute // 60
        minutes = minute % 60
        return f'{hours} ч. {minutes} мин.' if hours >= 1 else f'{minutes} мин.'

    @staticmethod
    def modify_text(d: dict):
        temp_dict = dict()
        temp_dict["text"] = d["text"].replace("\n", "<br />")
        temp_dict["quality"] = d["quality"]
        return temp_dict

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
        data['text'] = [tmp_json.get('text_type').replace("\n", "<br/>")]

        # first hierarchy (total info)
        total_reports = tmp_json.get('content')['8'].get('Отчеты')
        data['revealing'] = [self.modify_text(total_reports.get('Выявление'))]
        data['presentation'] = [self.modify_text(total_reports.get('Презентация'))]
        data['objections'] = [self.modify_text(total_reports.get('Возражения'))]
        data['materials'] = [None]
        data['general_report'] = [self.modify_text(total_reports.get('Общий отчет'))]

        # second hierarchy (revealing)
        revealing = tmp_json.get('content')['4'].get('Выявление')
        data['revealing_manager_questions'] = [self.modify_text(revealing.get("Вопросы менеджера"))]
        data['revealing_needs'] = [self.modify_text(revealing.get("Потребности"))]
        data['revealing_work'] = [self.modify_text(revealing.get("Трудоустройство"))]
        data['revealing_salary'] = [self.modify_text(revealing.get("Зарплата"))]
        data['revealing_remote'] = [self.modify_text(revealing.get("Удаленка"))]
        data['revealing_personal_up'] = [self.modify_text(revealing.get("Развитие личностное"))]
        data['revealing_it_up'] = [self.modify_text(revealing.get("Развитие в IT"))]
        data['revealing_activity_change'] = [self.modify_text(revealing.get("Смена деятельности"))]
        data['revealing_hobby'] = [self.modify_text(revealing.get("Хобби"))]
        data['revealing_career'] = [self.modify_text(revealing.get("Карьера"))]
        data['revealing_project'] = [self.modify_text(revealing.get("Проект"))]

        # second hierarchy (presentation)
        presentation = tmp_json.get('content')['5'].get('Презентация')
        data['presentation_advantages'] = [self.modify_text(presentation.get('Презентация').get('Преимущества название МП'))]
        data['presentation_demopanel'] = [self.modify_text(presentation.get('Презентация').get('Демопанель'))]
        data['presentation_platform'] = [self.modify_text(presentation.get('Презентация').get('Платформа обучения'))]
        data['presentation_stud'] = [self.modify_text(presentation.get('Презентация').get('Примеры других студентов'))]
        data['presentation_tariff'] = [self.modify_text(presentation.get('Тарифы, Пуш, оплата').get('Тарифы'))]
        data['presentation_push'] = [self.modify_text(presentation.get('Тарифы, Пуш, оплата').get('Пуш'))]
        data['presentation_payment'] = [self.modify_text(presentation.get('Тарифы, Пуш, оплата').get('Варианты оплат'))]

        # second hierarchy (objections)
        objections = tmp_json.get('content')['6'].get('Возражения')
        data['objections_customer'] = [self.modify_text(objections.get('Возражения клиента'))]
        data['objections_not_working'] = [self.modify_text(objections.get('Не получится'))]
        data['objections_training_period'] = [self.modify_text(objections.get('Срок обучения'))]
        data['objections_filling_courses'] = [self.modify_text(objections.get('Наполнение курсов'))]
        data['objections_expensive'] = [self.modify_text(objections.get('Дорого'))]
        data['objections_profession_not_suitable'] = [self.modify_text(objections.get('Не подойдет профессия'))]
        data['objections_think_advise'] = [self.modify_text(objections.get('Подумаю/Посоветуюсь'))]
        data['objections_contract'] = [self.modify_text(objections.get('Изучить договор/материалы'))]
        data['objections_not_enough_skill'] = [self.modify_text(objections.get('Не хватит навыков и опыта'))]
        data['objections_project_to_approved'] = [self.modify_text(objections.get('Нужно согласовать проект'))]
        data['objections_before_buy'] = [self.modify_text(objections.get('Нужно попробовать перед покупкой'))]
        data['objections_reviews'] = [self.modify_text(objections.get('Отзывы'))]
        data['objections_employment'] = [self.modify_text(objections.get('Трудоустройство'))]
        data['objections_developer_salary'] = [self.modify_text(objections.get('Зарплата разработчика'))]

        # second hierarchy (materials)
        materials = tmp_json.get('content')['7'].get('Материалы')
        data['materials_promised'] = [self.modify_text(materials.get('Обещанные материалы менеджером'))]
        data['materials_gpt_recommendations'] = [self.modify_text(materials.get('Рекомендации GPT'))]

        return data
