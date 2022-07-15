COMPOSE_FILE:=./compose/docker-compose.yml


# Installation

docker:
	IFS="="; read -a OSID_LIST <<< `cat /etc/*release | grep "^ID="`; OSID=${OSID_LIST[1]}
	IFS="="; read -a DISTRIB_CODENAME_LIST <<< `cat /etc/*release | grep "^DISTRIB_CODENAME="`; DISTRIB_CODENAME=${DISTRIB_CODENAME_LIST[1]}
	sudo apt -y install apt-transport-https ca-certificates curl software-properties-common
	curl -fsSL https://download.docker.com/linux/$OSID/gpg | sudo apt-key add -
	sudo add-apt-repository "deb [arch=`dpkg --print-architecture`] https://download.docker.com/linux/$OSID $DISTRIB_CODENAME stable"
	sudo apt update && sudo apt -y install docker-ce
	sudo curl -L https://github.com/docker/compose/releases/download/v2.5.1/docker-compose-`uname -s`-`uname -m` -o /usr/local/bin/docker-compose
	sudo chmod +x /usr/local/bin/docker-compose

upgrade:
	pip install --upgrade pip

install:
	pip install --no-input --requirement ./requirements.txt

uninstall:
	pip uninstall -y -r <(pip freeze)

reinstall: uninstall install


# Docker compose

up:
	docker-compose -f $(COMPOSE_FILE) up

build:
	docker-compose -f $(COMPOSE_FILE) build

build_nocache:
	docker-compose -f $(COMPOSE_FILE) build --no-cache

create:
	docker-compose -f $(COMPOSE_FILE) create

start:
	docker-compose -f $(COMPOSE_FILE) start

restart:
	docker-compose -f $(COMPOSE_FILE) restart

stop:
	docker-compose -f $(COMPOSE_FILE) stop

ps:
	docker-compose -f $(COMPOSE_FILE) ps

remove: stop
	docker-compose -f $(COMPOSE_FILE) rm -f


# Docker

logs:
	docker logs -f --tail=100 $(c)

exec:
	docker exec -it $(c) /bin/bash

clear_builder_cache:
	docker builder prune -f


# Run

run_debug:
	./debug.py

containers_restart: remove build create start
