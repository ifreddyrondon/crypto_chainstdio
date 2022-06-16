#!make

ifneq ("$(wildcard .env)","")
	include .env
	export
endif

# Dependencies

.PHONY: deps
deps: deps/goose

.PHONY: deps/goose
deps/goose:
ifeq (, $(shell which goose))
	brew install goose
endif

# Database
POSTGRES_URI = "postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}/${POSTGRES_DB}"
.PHONY: migrate
migrate:
	goose -dir sql/migrations/ postgres ${POSTGRES_URI} up

.PHONY: rollback
rollback:
	goose -dir sql/migrations/ postgres ${POSTGRES_URI} down

.PHONY: add-migration
add-migration:
	goose -dir "sql/migrations/" create $(name) sql

# Runners

.PHONY: docker-up
docker-up:
	docker-compose up -d

.PHONY: docker-stop
docker-stop:
	docker-compose stop


.PHONY: run
run:
	go vet ./cmd/sync/ && go run ./cmd/sync/
