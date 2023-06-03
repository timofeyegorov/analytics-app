from app.forms.serializers import BaseSerializer


class LoginSerializer(BaseSerializer):
    login: str
    password: str


class LogoutSerializer(BaseSerializer):
    pass
