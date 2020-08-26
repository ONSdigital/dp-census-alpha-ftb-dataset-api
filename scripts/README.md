# Scripts

A list of scripts which load data into elasticsearch for use in the Search API.

## A list of scripts

- [upload ftb datasets](#upload-ftb-datasets)

### Upload FTB Datasets

This script retrieves a People FTB data blob stored in the FTB datasetore and will build out an FTB dataset for the blob and any tables defined in the code (currently one). The result is a dataset, edition, version and dimension options stored in mongo db for FTB data blob and all tables that have been defined.

You can run either of the following commands:

- Use Makefile
    - Set `mongodb_bind_addr`, `ftb_auth_token` and `ftb_host` environment variables with:
    ```
    export mongodb_bind_addr=<mongodb bind address>
    export ftb_auth_token=<ftb auth token>
    export ftb_host=<ftb host and port> e.g. `localhost:10100`
    ```
    - Run `make upload-datasets`
- Use go run command with or without flags `-mongodb-bind-addr`, `-ftb-auth-token` and `-ftb-host` being set
    - `go run retrieve-cmd-datasets/main.go -mongodb-bind-addr=<mongodb bind address> -ftb-auth-token=<ftb auth token> -ftb-host=<ftb host and port>`
    
if you do not set the flags or environment variables for mongodb bind address and ftb host , the script will use a default value set to `localhost:27017` and `localhost:10100` respectively. You must provide the auth token to gain access to the ftb service, this does not need to include the term `Bearer ` prepended to the randomly generated unique identifier. Please read the [census alpha api proxy documentation on how to obtain token](https://github.com/ONSdigital/dp-census-alpha-api-proxy).
