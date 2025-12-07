.PHONY: ingest up down status

ingest:
	./ingest.sh

up:
	docker compose -f dev/docker-compose.yaml up -d --build

down:
	docker compose -f dev/docker-compose.yaml down

status:
	docker compose -f dev/docker-compose.yaml ps
