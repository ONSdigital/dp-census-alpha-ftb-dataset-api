SHELL=bash

BUILD=build
BIN_DIR?=.

MONGODB_BIND_ADDR=${mongodb_bind_addr}
FTB_DATASET_API_URL=${FTBDATASET_API_URL}
FTB_AUTH_TOKEN=${ftb_auth_token}
FTB_HOST=${ftb_host}

REMOVE_DATASETS=remove-datasets
UPLOAD_DATASETS=upload-datasets

build:
	@mkdir -p ../$(BUILD)/$(BIN_DIR)

remove-datasets: build
	go build -o ../$(BUILD)/$(BIN_DIR)/$(REMOVE_DATASETS) $(REMOVE_DATASETS)/main.go
	HUMAN_LOG=1 go run -race $(REMOVE_DATASETS)/main.go -mongodb-bind=$(MONGODB_BIND_ADDR)


upload-datasets: build
	go build -o ../$(BUILD)/$(BIN_DIR)/$(UPLOAD_DATASETS) $(UPLOAD_DATASETS)/main.go
	HUMAN_LOG=1 go run -race $(UPLOAD_DATASETS)/main.go -mongodb-bind=$(MONGODB_BIND_ADDR) -ftb-dataset-api-url=$(FTB_DATASET_API_URL) -ftb-host=$(FTB_HOST) -ftb-auth-token=$(FTB_AUTH_TOKEN)

run: remove-datasets upload-datasets

test:
	go test -cover -race ./...

.PHONY: run remove-datasets upload-datasets build test
