# Run

run:
	flask --app app:flask_app run --host 0.0.0.0 --port 8000 --debug


# Installation

pip-upgrade:
	pip install --upgrade pip

install:
	pip install --no-input --requirement ./requirements.txt

uninstall:
	pip uninstall -y -r <(pip freeze)

reinstall: uninstall install
