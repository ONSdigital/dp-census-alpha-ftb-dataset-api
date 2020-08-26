SHELL=bash

BUILD=build
BIN_DIR?=.

MONGODB_BIND_ADDR=${mongodb_bind_addr}
FTB_AUTH_TOKEN=${ftb_auth_token}
FTB_HOST=${ftb_host}

UPLOAD_DATASETS=upload-datasets

build:
	go generate ../...
	@mkdir -p ../$(BUILD)/$(BIN_DIR)

upload-datasets: build
	go build -o ../$(BUILD)/$(BIN_DIR)/$(UPLOAD_DATASETS) $(UPLOAD_DATASETS)/main.go
	HUMAN_LOG=1 go run -race $(UPLOAD_DATASETS)/main.go -mongodb-bind=$(MONGODB_BIND_ADDR) -ftb-host=$(FTB_HOST) -ftb-auth-token=$(FTB_AUTH_TOKEN)

test:
	go test -cover -race ./...

.PHONY: upload-datasets build test
