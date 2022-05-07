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
	docker-compose up

.PHONY: docker-stop
docker-stop:
	docker-compose stop

.PHONY: docker-background
docker-background:
	docker-compose up -d

.PHONY: worker
worker:
	go vet ./cmd/worker/ && go run ./cmd/worker/
