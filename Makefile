

check-infra:
	docker-compose \
	-f ./infra/docker-compose.infra.yaml \
	--env-file ./infra/.env.local \
	config

start-infra:
	docker-compose \
	-f ./infra/docker-compose.infra.yaml \
	--env-file ./infra/.env.local \
	up --build

down-infra:
	docker-compose \
	-f ./infra/docker-compose.infra.yaml \
	--env-file ./infra/.env.local \
	down