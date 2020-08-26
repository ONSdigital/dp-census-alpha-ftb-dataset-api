# Scripts

A list of scripts which load data into elasticsearch for use in the Search API.

## A list of scripts

- [upload ftb datasets](#upload-ftb-datasets)

### Upload FTB Datasets

This script retrieves a People FTB data blob stored in the FTB datasetore and will build out an FTB dataset for the blob and any tables defined in the code (currently one). The result is a dataset, edition, version and dimension options stored in mongo db for FTB data blob and all tables that have been defined.

You can run either of the following commands:

- Use Makefile
    - Set `mongodb_bind_addr`, `FTBDATASET_API_URL`, `ftb_auth_token` and `ftb_host` environment variables with:
    ```
    export mongodb_bind_addr=<mongodb bind address>
    export FTBDATASET_API_URL=<ftb dataset api url - host:port> // This should be the location of the ftb dataset API application e.g. "localhost:10400"
    export ftb_auth_token=<ftb auth token>
    export ftb_host=<ftb host and port> e.g. "localhost:10100"
    ```
    - Run `make upload-datasets`
- Use go run command with or without flags `-mongodb-bind-addr`, `-ftb-dataset-api-url`, `-ftb-auth-token` and `-ftb-host` being set
    - `go run retrieve-cmd-datasets/main.go -mongodb-bind-addr=<mongodb bind address> -ftb-dataset-api-url=<ftb dataset api url> -ftb-auth-token=<ftb auth token> -ftb-host=<ftb host and port>`
    
if you do not set the flags or environment variables for mongodb bind address and ftb host , the script will use a default value set to `localhost:27017` and `localhost:10100` respectively. You must provide the auth token to gain access to the ftb service, this does not need to include the term `Bearer ` prepended to the randomly generated unique identifier. Please read the [census alpha api proxy documentation on how to obtain token](https://github.com/ONSdigital/dp-census-alpha-api-proxy).

#### Update Script

One can manually update the script to generate more tables of your choice by simply adding the following piece of code after line 144 in `upload-datasets/main.go`. All string surrounded with chevrons <string> will need to be updated with the data you want to create.

```
    // Create FTB Data Table
	tableData.dimensions = map[string]bool{
		"<dimension>": true,
		"<dimension>": true,
		"<dimension>": true,
		"<dimension>": true,
	}

	tableData.name = "<name-of-table>"
	tableData.title = "2011 Census - <human friendly title of table>"
	tableData.description = "<A description of the table>"

	datasetFTBTable, editionFTBTable, versionFTBTable, err := api.createFTBDatasetTable(ctx, mongo, ftbBlob, tableData)
	if err != nil {
		os.Exit(1)
	}

	// Update FTB data blob with list of tables
	datasetFTBTables = append(datasetFTBTables, datasetFTBTable)
	editionFTBTables = append(editionFTBTables, editionFTBTable)
	versionFTBTables = append(versionFTBTables, versionFTBTable)
```

The map of `tableData.Dimensions` can hold as many dimensions as you see fit but the names must match that of the dimensions displayed in the FTB blob. To find the correct naming of a dimension, one can run this script to load into mongo, then view the dimensions listed against the FTB blob (People) version document. Make request to the API or go directly to mongodb using the likes of Robomongo or on commandline.