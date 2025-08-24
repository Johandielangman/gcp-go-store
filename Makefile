#!make

# Load the env variables
include .env
export

run: build
	@./bin/main

build:
	@go build -o ./bin/main main.go

# The count=1 is to prevent cached tests
test:
	@go test -v -count=1 ./...