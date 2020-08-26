SHELL=bash

BUILD=build
BIN_DIR?=.

FTB_DATASET_API=ftb-dataset-api

build:
	@mkdir -p $(BUILD)/$(BIN_DIR)
	go build -o $(BUILD)/$(BIN_DIR)/$(FTB_DATASET_API) cmd/$(FTB_DATASET_API)/main.go

debug: build
	HUMAN_LOG=1 go run -race cmd/$(FTB_DATASET_API)/main.go

test:
	go test -cover -race ./...

.PHONY: build api test
