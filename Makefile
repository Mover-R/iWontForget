.PHONY: check-all start-all down-all check-infra start-infra down-infra check-services start-services down-services

check-all: check-infra check-services
start-all: start-infra start-services
down-all: down-infra down-services

check-infra:
	docker-compose \
	-f ./infra/docker-compose.infra.yaml \
	--env-file ./infra/.env.local \
	config

start-infra:
	docker-compose \
	-f ./infra/docker-compose.infra.yaml \
	--env-file ./infra/.env.local \
	up --build -d

down-infra:
	docker-compose \
	-f ./infra/docker-compose.infra.yaml \
	--env-file ./infra/.env.local \
	down

check-services:
	docker-compose \
	-f ./infra/docker-compose.services.yaml \
	--env-file ./infra/.env.local \
	config

start-services:
	docker-compose \
	-f ./infra/docker-compose.services.yaml \
	--env-file ./infra/.env.local \
	up --build

down-services:
	docker-compose \
	-f ./infra/docker-compose.services.yaml \
	--env-file ./infra/.env.local \
	down