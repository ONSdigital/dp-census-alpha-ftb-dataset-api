# Scripts

A list of scripts which load data into elasticsearch for use in the Search API.

## A list of scripts

- [retrieve cmd datasets](#retrieve-cmd-datasets)
- [load parent docs](#load-datasets)
- [retrieve dataset taxonomy](#retrieve-dataset-taxonomy)
- [load postcodes](#load-postcode)
- [geojson](#load-data-from-geojson-files)
    - 2011 Lower Layer Super Output Areas (LSOA)
    - 2011 Middle Layer Super Output Areas (MSOA)
    - 2011 Output Areas (OA)
    - 2015 Towns and Cities (TCITY)
    - 2019 UK Countries
- [build hierarchies json](#build-hierarchies-json)

### Retrieve CMD Datasets

This script retrieves a list of datasets stored in mongodb instance and will check that the url to dataset resource on the ons website exists before storing the data in a csv file.

You can run either of the following commands:

- Use Makefile
    - Set `mongodb_bind_addr` and/or `filename` environment variable with:
    ```
    export mongodb_bind_addr=<mongodb bind address>
    export filename=<file name and loaction>
    ```
    - Run `make cmd-datasets-csv`
- Use go run command with or without flags `-mongodb-bind-addr` and/or `-filename` being set
    - `go run retrieve-cmd-datasets/main.go -mongodb-bind-addr=<mongodb bind address> -filename=<file name and location>`
    
if you do not set the flags or environment variables for mongodb bind address and filename, the script will use a default value set to `localhost:27017` and `cmd-datasets.csv` respectively.

### Load Datasets

This script reads a csv file defined by flag/environment variable or default value and stores the dataset data into elasticsearch. The csv must contain particular headers (but not in any necessary order).

One can use the Retrieve cmd datasets script to generate a new csv file or use the pre-generated one stored as `cmd-datasets.csv`.

- Use Makefile
    - Set `dataset_index`, `filename` and/or `elasticsearch_url` environment variable with:
    ```
    export dataset_index=<elasticsearch index>
    export filename=<file name and loaction>
    export elasticsearch_url=<elasticsearch bind address>
    ```
    - Optionally set `dimensions_filename` environment variable with, should end with `.json`:
    ```
    export taxonomy_filename=<filename and location>
    ```
    - Optionally set `taxonomy_filename` environment variable with, should end with `.json`:
    ```
    export taxonomy_filename=<filename and location>
    ```
    - Run `make upload-datasets`
- Use go run command with or without flags `-dataset-index`, `-filename` and/or `-elasticsearch_url` being set
    - `go run upload-datasets/main.go -dataset-index=<elasticsearch index> -filename=<file name and loaction> -dimensions-filename=<dimensions file and location> -taxonomy-filename=<taxonomy file name and location> -elasticsearch_url=<elasticsearch bin address>`

Taxonomy and Dimensions will be stored in a json file that will be read into memory in the dataset search API on start up, these file names and locations should match the environment configurations for `TAXONOMY_FILENAME` and `DIMENSIONS_FILENAME` respectively. For ease of use just run the make commands without editing flags or setting environment variables for these variables.

### Retrieve Dataset Taxonomy

This script scrapes the ons website to pull out taxonomy hierarchy by iterating through pages.

You can run either of the following commands:

- Use Makefile
    - Set `taxonomy_filename` environment variable with, should end with `.json`:
    ```
    export taxonomy_filename=<filename and location>
    ```
    - Run `make taxonomy-json`
- Use go run command with or without flags `-filename` being set
    - `go run retrieve-dataset-taxonomy/main.go -filename=<file name and loaction>`
    
if you do not set the flag or environment variable for filename, then the script will use a default value set to `../taxonomy/taxonomy.json`.

### Load Postcode

This script loads postcode data for all postcodes across the UK as of Febraury 2020 from a csv file downloaded from the geo portal [here](https://geoportal.statistics.gov.uk/datasets/national-statistics-postcode-lookup-february-2020).

Once file is downloaded (from above link), unzip file. The data layout to postcode data should look like this:
    - NSPL_FEB_2020_UK
      - Data
        - NSPL_FEB_2020_UK.csv

Upload postcode data to elasticsearch index with:
`make postcode`
This will take approximately 4 minutes and 20 seconds and documents will be stored in `test_postcode` index.

### Load data from GEOJSON files

This script loads geographical boundaries for 2011 census data. This includes lower and middle layer output areas, as well as other output areas, towns and cities across England and Wales only.

Files can be downloaded from the geoportal -> boundaries -> census boundaries -> select geography layer. This will tend to open up a search of all relevant boundaries, select the data you would like to view/import. The new screen will have a drop down list to the right of webpage titled `APIs`, click the drop down and copy the GEOJSON url. Paste the url into the browser and it will automatically download the data, be patient this may take some time; below is a list of urls used for the geojson scripts (these might break if geoportal decides to move the geojson file location):

- [UK Countries 2019 uk-bgc](https://opendata.arcgis.com/datasets/b789ba2f70fe45eb92402cee87092730_0.geojson)
- [Major Towns And Cities 2015](https://opendata.arcgis.com/datasets/58b0dfa605d5459b80bf08082999b27c_0.geojson)
- [Middle layer super output areas december 2011 ew-bgc](https://opendata.arcgis.com/datasets/29fdaa2efced40378ce8173b411aeb0e_2.geojson)
- [Lower layer super output areas december 2011 ew-bgc](https://opendata.arcgis.com/datasets/e993add3f1944437bc91ec7c76100c63_0.geojson)
- [Output areas december 2011 ew-bgc](https://opendata.arcgis.com/datasets/f79fc19485704ce68523d8d70d84a913_0.geojson)

Once the above files have downloaded, move the files to root of this repository and store under geojson folder.

Upload all the data to elasticsearch index with:
`make geojson`
This will take a long time as it it populates 150,000+ records with full polygon boundaries into elasticsearch `area_profiles` index and create a `hierarchy.json` file containing a list of hierarchies that an api user can filter an area profile data type.

There are actually five separate scripts which handle generating data for COUNTRIES, LSOA, MSOA, OA and TCITY files. These can be run separately using `make countries`, `make lsoa`, `make msoa`, `make oa`, `make tcity` respectively. Be aware that if you are running this for the first time you will need to create the `area_profiles` index, you can do this by running `make refreshgeojson`. One can rebuild the list of hierarchies using `make hierarchies`

The refresh script deletes the index and recreates it with 0 data.

### Build Hierarchies JSON

As described at the bottom of [load data from geojson files section](#load-data-from-geojson-files), one can rebuild the hierarchy json file by running `make hierarchies`, this is a list of hierarchies based on the geojson scripts that exist and if the scripts get extended to incorporate new levels of geographical hierarchies then the hardcoded list in hierarchies script will also need updating.
