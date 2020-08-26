# dp-census-alpha-ftb-dataset-api

This is the census alpha ftb dataset API application for census project. The API is a duplicate of the [dp-dataset-api](https://github.com/ONSdigital/dp-dataset-api) codebase containing only the following endpoints:

```
GET /datasets
GET /datasets/{id}
GET /datasets/{id}
GET /datasets/{id}/editions
GET /datasets/{id}/editions/{edition}
GET /datasets/{id}/editions/{edition}/versions
GET /datasets/{id}/editions/{edition}/versions/{version}
GET /datasets/{id}/editions/{edition}/versions/{version}/metadata
GET /datasets/{id}/editions/{edition}/versions/{version}/dimensions
GET /datasets/{id}/editions/{edition}/versions/{version}/dimensions/{dimension}/options
```

This api also has stripped back all unecessary code, such as authentication, auditting and healthchecks, with minimal updates to the data models. These new models should be backward compatible with the existing dataset API, so any cmd datasets should also be able to sit under this API and will be returned by the relevant endpoints listed above.

### Requirements

In order to run the service locally you will need the following:

- Go
- Git
- mongodb 3.4+

### Getting started

- Clone the repo go get github.com/ONSdigital/dp-census-alpha-ftb-dataset-api
- Run mongodb instance
- Follow [setting up data](#setting-up-data)
- Run `make debug` to start ftb dataset API service

Follow swagger documentation on how to interact with local api, some examples are below:

```
curl -XGET localhost:10400/datasets -vvv
curl -XGET localhost:10400/datasets/People -vvv
curl -XGET localhost:10400/datasets/People/editions/2011/versions -vvv
curl -XGET localhost:10400/datasets/People/editions/2011/versions/1 -vvv
curl -XGET localhost:10400/datasets/People/editions/2011/versions/1/metadata -vvv
curl -XGET localhost:10400/datasets/People/editions/2011/versions/1/dimensions -vvv
curl -XGET localhost:10400/datasets/People/editions/2011/versions/1/dimensions/AGE/options -vvv
```

#### Setting up data

Once mongodb is running and you can connect to your ftb instance. Follow the instructions [here](scripts/README.md) to load in ftb data blob `People`.

### Configuration

| Environment variable        | Default                | Description
| --------------------------- | ---------------------- | -----------
| BIND_ADDR                   | :10400                 | The host and port to bind to |
| CODE_LIST_API_URL           | http://localhost:22400 | The host name for the CodeList API |
| FTBDATASET_API_URL          | http://localhost:10400 | The host name for the FTB Dataset API |
| GRACEFUL_SHUTDOWN_TIMEOUT   | 5s                     | The graceful shutdown timeout in seconds |
| WEBSITE_URL                 | http://localhost:20000 | The host name for the website |
| MONGODB_BIND_ADDR           | localhost:27017        | The MongoDB bind address |
| MONGODB_COLLECTION          | datasets               | The MongoDB collection for datasets |
| MONGODB_DATABASE            | ftb-datasets           | The MongoDB dataset database |

### Notes

One can run the unit tests with `make test`
