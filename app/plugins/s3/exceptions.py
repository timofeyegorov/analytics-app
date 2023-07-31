from enum import Enum


class S3ExceptionMessage(Enum):
    undefined = "Неизвестная ошибка"
    environment = "Не заданы или неверно заданы переменные окружения.\n\nНеобходимо правильно определить переменные в файле окружения .env:\n%s"


class S3Exception(Exception):
    message = S3ExceptionMessage.undefined

    def __init__(self, *args):
        super().__init__(self.message.value % args if args else self.message.value)


class S3EnvironmentException(S3Exception):
    message = S3ExceptionMessage.environment

    def __init__(self, *args):
        super().__init__(
            "\n".join(
                [f"    S3_{item[0].upper()}: {item[1]}" for item in args[0].items()]
            )
        )
