package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/ONSdigital/dp-census-alpha-ftb-dataset-api/models"
	dphttp "github.com/ONSdigital/dp-net/http"
	"github.com/ONSdigital/log.go/log"
	uuid "github.com/satori/go.uuid"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const (
	database            = "ftb-datasets"
	datasetCollection   = "datasets"
	editionCollection   = "editions"
	versionCollection   = "instances"
	dimOptionCollection = "dimension.options"

	defaultBindAddr         = "localhost:27017"
	defaultFTBDatasetAPIURL = "http://localhost:10400"
	defaultFTBHost          = "localhost:8491"
	defaultFTBAuthToken     = "auth-token"

	edition          = "2011"
	license          = "Open Government Licence v3.0"
	qmiHRef          = "https://www.ons.gov.uk/census/2011census/howourcensusworks/howwetookthe2011census"
	releaseFrequency = "Decennial"
	unitOfMeasure    = "Persons"
)

var (
	bindAddr, datasetAPIURL, ftbHost, ftbAuthToken string

	nationalStatistic = true
	publisher         = models.Publisher{
		HRef: "https://www.ons.gov.uk",
		Name: "Office for National Statistics (ONS)",
		Type: "Non-ministerial department",
	}
)

// ErrorUnexpectedStatusCode represents the error message to be returned when
// the status received from elastic is not as expected
var ErrorUnexpectedStatusCode = errors.New("unexpected status code from api")

func main() {
	ctx := context.Background()
	flag.StringVar(&bindAddr, "mongodb-bind", defaultBindAddr, "the address including authorisation if needed to bind to mongo database")
	flag.StringVar(&ftbHost, "ftb-host", defaultFTBHost, "the url to the FTB database")
	flag.StringVar(&ftbAuthToken, "ftb-auth-token", defaultFTBAuthToken, "the authorisation token to access FTB API")
	flag.StringVar(&datasetAPIURL, "ftb-dataset-api-url", defaultFTBDatasetAPIURL, "the url to the FTB dataset API")
	flag.Parse()

	if bindAddr == "" {
		bindAddr = defaultBindAddr
	}

	if datasetAPIURL == "" {
		datasetAPIURL = defaultFTBDatasetAPIURL
	}

	if ftbHost == "" {
		ftbHost = defaultFTBHost
	}

	if ftbAuthToken == "" {
		ftbAuthToken = defaultFTBAuthToken
	}

	ftbAuthToken = "Bearer " + ftbAuthToken

	log.Event(ctx, "script variables", log.INFO, log.Data{"mongodb_bind_addr": bindAddr, "ftb_api_url": ftbHost, "ftb_auth_token": ftbAuthToken})

	mongo := Mongo{
		CodeListURL: "http://localhost:10500",
		Database:    database,
		URI:         bindAddr,
	}

	session, err := mongo.Init()
	if err != nil {
		log.Event(ctx, "unable to connect to mongo database", log.ERROR, log.Error(err), log.Data{"mongodb-bind-addr": bindAddr})
		os.Exit(1)
	}

	mongo.Session = session

	cli := dphttp.NewClient()
	api := NewFTBAPI(cli, ftbHost)

	// Get People Dataset
	path := "/v8/codebook/People?cats=false"
	responseBody, status, err := api.getFTBAPI(ctx, ftbAuthToken, ftbHost, path)
	if err != nil {
		log.Event(ctx, "failed to make request to FTB", log.Data{"ftb_url": ftbHost + path})
		os.Exit(1)
	}

	ftbBlob := &FTBData{}

	if err := json.Unmarshal(responseBody, ftbBlob); err != nil {
		log.Event(ctx, "unable to unmarshal json body", log.ERROR, log.Error(err))
		os.Exit(1)
	}

	log.Event(ctx, "got response", log.Data{"response": ftbBlob, "status": status})

	// Create FTB Data Blob
	// Extracts complete codebook and loads into Mongo, potentially only done once in RL?
	tableData, err := api.createFTBDatasetBlob(ctx, mongo, ftbBlob)
	if err != nil {
		os.Exit(1)
	}

	var datasetFTBTables, editionFTBTables, versionFTBTables []models.Table

	// Create FTB Data Table
	tableData.dimensions = map[string]bool{
		"AGE":   true,
		"CARER": true,
		"SEX":   true,
		"MSOA":  true,
	}

	tableData.name = "2011-census-carer-msoa-age-and-sex"
	tableData.title = "2011 Census - Unpaid Care for Middle Layer Super Output Areas across Age and Sex"
	tableData.description = "The provision of unpaid care across middle layer super output areas containing variations across age and sex from 2011 census."

	datasetFTBTable, editionFTBTable, versionFTBTable, err := api.createFTBDatasetTable(ctx, mongo, ftbBlob, tableData)
	if err != nil {
		os.Exit(1)
	}

	// Update FTB data blob with list of tables
	datasetFTBTables = append(datasetFTBTables, datasetFTBTable)
	editionFTBTables = append(editionFTBTables, editionFTBTable)
	versionFTBTables = append(versionFTBTables, versionFTBTable)

	if err = api.updateFTBDatasetBlob(ctx, mongo, tableData.ftbBlob.datasetID, tableData.ftbBlob.versionID, datasetFTBTables, editionFTBTables, versionFTBTables); err != nil {
		os.Exit(1)
	}

	log.Event(ctx, "successfully completed loading ftb datasets", log.INFO)
	// Update ftb data blob with tables
}

type FTBData struct {
	Dataset    FTBDataset  `json:"dataset"`
	Dimensions []Dimension `json:"codebook"`
}

// FTBDataset represents the dataset returned from FTB
type FTBDataset struct {
	Description string `json:"description"`
	Name        string `json:"name"`
}

type Dimension struct {
	Codes   []string `json:"codes,omitempty"`
	Label   string   `json:"label"`
	Labels  []string `json:"labels,omitempty"`
	Name    string   `json:"name"`
	MapFrom []string `json:"mapFrom"`
}

type TableData struct {
	description        string
	flexibleDimensions []models.Dimension
	ftbBlob            FTBBlob
	name               string
	title              string
	dimensions         map[string]bool
	datasetLink        string
	editionLink        string
	versionLink        string
}

type FTBBlob struct {
	datasetID string
	versionID string
}

func (api *API) createFTBDatasetBlob(ctx context.Context, mongo Mongo, ftbBlob *FTBData) (tableData TableData, err error) {
	// Create dataset version
	datasetID := ftbBlob.Dataset.Name
	versionID := uuid.NewV4().String()
	collectionID := uuid.NewV4().String()

	dimensions := []models.Dimension{}
	headers := []string{"ftb-blob"}

	dimensionOptionCounts := make(map[string]int)

	// Create Dimension Options
	for _, dim := range ftbBlob.Dimensions {
		// Get dimension options
		var ftbOptions *FTBData
		ftbOptions, err = api.retrieveDimensionOptions(ctx, dim.Name)
		if err != nil {
			log.Event(ctx, "failed to retrieve dimension options document", log.ERROR, log.Error(err))
			return
		}

		dimensionOptionCounts[dim.Name] = len(ftbOptions.Dimensions[0].Codes)

		log.Event(ctx, "got dimension", log.Data{"dimension_name": ftbOptions.Dimensions[0].Name, "dimension_label": ftbOptions.Dimensions[0].Label, "label_count": len(ftbOptions.Dimensions[0].Labels), "code_count": len(ftbOptions.Dimensions[0].Codes)})

		options := make([]interface{}, 500)
		// Add each dimension option to mongo
		for i, option := range ftbOptions.Dimensions[0].Codes {

			label := ftbOptions.Dimensions[0].Codes[i]
			if len(ftbOptions.Dimensions[0].Labels) > 0 {
				label = ftbOptions.Dimensions[0].Labels[i]
			}
			dimensionOption := &models.DimensionOption{
				InstanceID:  versionID,
				Label:       label,
				LastUpdated: time.Now().UTC(),
				Links: models.DimensionOptionLinks{
					Code: models.LinkObject{
						HRef: fmt.Sprintf("%s/code-lists/%s/codes/%s", mongo.CodeListURL, ftbOptions.Dimensions[0].Name, option),
						ID:   option,
					},
					CodeList: models.LinkObject{
						HRef: fmt.Sprintf("%s/code-lists/%s", mongo.CodeListURL, ftbOptions.Dimensions[0].Name),
						ID:   ftbOptions.Dimensions[0].Name,
					},
					Version: models.LinkObject{
						HRef: datasetAPIURL + "/datasets/" + datasetID + "/editions/" + edition + "/versions/1",
						ID:   "1",
					},
				},
				Name:   ftbOptions.Dimensions[0].Name,
				Option: option,
			}

			options = append(options, dimensionOption)

			// Do a bulk upload of 500 documents at a time to speed the process of loading data into mongo db
			if len(options) == 500 {
				if err = mongo.BulkInsertDimensionOptions(options); err != nil {
					log.Event(ctx, "failed to add dimension options in bulk request to mongo db", log.ERROR, log.Error(err))
					return
				}

				options = nil
			}
		}

		// Add leftover docs to mongo
		if len(options) != 0 {
			if err = mongo.BulkInsertDimensionOptions(options); err != nil {
				log.Event(ctx, "failed to add last set of dimension options in bulk request to mongo db", log.ERROR, log.Error(err))
				return
			}

			options = nil
		}
	}

	for _, dim := range ftbBlob.Dimensions {
		dimension := models.Dimension{
			Description:     "",
			HRef:            "http://localhost:22400/code-lists/" + dim.Name,
			ID:              dim.Name,
			Name:            dim.Label,
			Label:           dim.Label,
			NumberOfOptions: dimensionOptionCounts[dim.Name],
		}
		if len(dim.MapFrom) < 1 {
			dimension.Category = dim.Name
		} else {
			dimension.Category = dim.MapFrom[0]
		}

		dimensions = append(dimensions, dimension)
		headers = append(headers, dim.Name)
	}

	versionDoc := &models.Version{
		CollectionID:  collectionID,
		Dimensions:    dimensions,
		Downloads:     nil,
		Edition:       edition,
		FTBType:       "ftb-blob",
		Headers:       headers,
		ID:            versionID,
		LatestChanges: nil,
		Links: &models.VersionLinks{
			Dataset: &models.LinkObject{
				HRef: datasetAPIURL + "/datasets/" + datasetID,
				ID:   datasetID,
			},
			Dimensions: &models.LinkObject{
				HRef: datasetAPIURL + "/datasets/" + datasetID + "/editions/" + edition + "/versions/1/dimensions",
			},
			Edition: &models.LinkObject{
				HRef: datasetAPIURL + "/datasets/" + datasetID + "/editions/" + edition,
				ID:   edition,
			},
			Self: &models.LinkObject{
				HRef: datasetAPIURL + "/instances/" + versionID,
			},
			Spatial: nil,
			Version: &models.LinkObject{
				HRef: datasetAPIURL + "/datasets/" + datasetID + "/editions/" + edition + "/versions/1",
				ID:   "1",
			},
		},
		ReleaseDate: "22/03/2012",
		State:       "published",
		Tables:      nil,
		Temporal: &[]models.TemporalFrequency{
			{
				Frequency: "Decennial",
			},
		},
		Type:       "ftb",
		UsageNotes: nil,
		Version:    1,
	}

	// Store version doc
	if err = mongo.UpsertVersion(versionID, versionDoc); err != nil {
		log.Event(ctx, "failed to upload version document", log.ERROR, log.Error(err))
		return
	}

	// Create Dataset
	currentDatasetDoc := &models.Dataset{
		CollectionID: collectionID,
		Contacts: []models.ContactDetails{
			{
				Email:     "census-team@ons.gov.uk",
				Name:      "census-team",
				Telephone: "+44 (0)845 601 3034",
			},
		},
		Description: "2011 Census data for People",
		FTBType:     "ftb-blob",
		ID:          datasetID,
		Keywords:    []string{"census", "people"},
		License:     license,
		Links: &models.DatasetLinks{
			Editions: &models.LinkObject{
				HRef: datasetAPIURL + "/datasets/" + datasetID + "/editions",
			},
			LatestVersion: &models.LinkObject{
				HRef: datasetAPIURL + "/datasets/" + datasetID + "/editions/" + edition + "/versions/1",
				ID:   "1",
			},
			Self: &models.LinkObject{
				HRef: datasetAPIURL + "/datasets/" + datasetID,
			},
			Taxonomy: &models.LinkObject{},
		},
		Methodologies:     nil,
		NationalStatistic: &nationalStatistic,
		NextRelease:       "N/A",
		Publications:      nil,
		Publisher:         &publisher,
		QMI: &models.GeneralDetails{
			HRef: qmiHRef,
		},
		RelatedDatasets:  nil,
		ReleaseFrequency: releaseFrequency,
		State:            "published",
		Tables: &[]models.Table{
			{}, // TODO add tables
		},
		Theme:         "census",
		Title:         "Census 2011 - People",
		Type:          "ftb",
		UnitOfMeasure: unitOfMeasure,
		URI:           "",
	}

	datasetDoc := &models.DatasetUpdate{
		Current: currentDatasetDoc,
		Next:    currentDatasetDoc,
	}

	// Store dataset doc
	if err = mongo.UpsertDataset(datasetID, datasetDoc); err != nil {
		log.Event(ctx, "failed to upload dataset document", log.ERROR, log.Error(err))
		return
	}

	// Create Edition
	currentEditionDoc := &models.Edition{
		Edition: edition,
		FTBType: "ftb-blob",
		Links: &models.EditionUpdateLinks{
			Dataset: &models.LinkObject{
				HRef: datasetAPIURL + "/datasets/" + datasetID,
				ID:   datasetID,
			},
			LatestVersion: &models.LinkObject{
				HRef: datasetAPIURL + "/datasets/" + datasetID + "/editions/" + edition + "/versions/1",
				ID:   "1",
			},
			Self: &models.LinkObject{
				HRef: datasetAPIURL + "/datasets/" + datasetID + "/editions/" + edition,
			},
			Versions: &models.LinkObject{
				HRef: datasetAPIURL + "/datasets/" + datasetID + "/editions/" + edition + "/versions",
			},
		},
		State:  "published",
		Tables: nil,
		Type:   "ftb",
	}

	// Store edition doc
	editionDoc := &models.EditionUpdate{
		Current: currentEditionDoc,
		Next:    currentEditionDoc,
	}

	// Store dataset doc
	if err = mongo.UpsertEdition(datasetID, editionDoc); err != nil {
		log.Event(ctx, "failed to upload edition document", log.ERROR, log.Error(err))
		return
	}

	log.Event(ctx, "successfully completed loading ftb data blob", log.INFO)

	tableData.versionLink = versionDoc.Links.Version.HRef
	tableData.datasetLink = currentDatasetDoc.Links.Self.HRef
	tableData.editionLink = currentEditionDoc.Links.Self.HRef
	tableData.flexibleDimensions = dimensions
	tableData.ftbBlob = FTBBlob{
		datasetID: datasetID,
		versionID: versionID,
	}

	return
}

func (api *API) createFTBDatasetTable(ctx context.Context, mongo Mongo, ftbBlob *FTBData, tableData TableData) (models.Table, models.Table, models.Table, error) {
	// Create dataset version
	datasetID := tableData.name
	versionID := uuid.NewV4().String()
	collectionID := uuid.NewV4().String()

	dimensions := []models.Dimension{}
	headers := []string{"ftb-table"}
	keywords := []string{"census", "people"}

	var datasetFTBTable, editionFTBTable, versionFTBTable models.Table

	count := 0
	for _, dim := range ftbBlob.Dimensions {
		if _, ok := tableData.dimensions[dim.Name]; !ok {
			continue
		}

		count++
		dimension := models.Dimension{
			Description: "",
			HRef:        "http://localhost:22400/code-lists/" + dim.Name,
			ID:          dim.Name,
			Name:        dim.Label,
			Label:       dim.Label,
		}
		if len(dim.MapFrom) < 1 {
			dimension.Category = dim.Name
		} else {
			dimension.Category = dim.MapFrom[0]
		}

		keywords = append(keywords, strings.ToLower(dimension.Category))
		dimensions = append(dimensions, dimension)
		headers = append(headers, dim.Name)
	}

	if len(tableData.dimensions) != count {
		return datasetFTBTable, editionFTBTable, versionFTBTable, errors.New("Dimension not found to create ftb dataset table")
	}

	var dimensionsWithOptionCounts []models.Dimension

	// Create Dimension Options
	for _, dim := range dimensions {
		// Get dimension options
		// (Variable categories in FTB terms)
		ftbOptions, err := api.retrieveDimensionOptions(ctx, dim.ID)
		if err != nil {
			log.Event(ctx, "failed to retrieve dimension options document", log.ERROR, log.Error(err))
			return datasetFTBTable, editionFTBTable, versionFTBTable, err
		}

		dim.NumberOfOptions = len(ftbOptions.Dimensions[0].Codes)
		dimensionsWithOptionCounts = append(dimensionsWithOptionCounts, dim)

		options := make([]interface{}, 500)

		// Add each dimension option to mongo
		for i, option := range ftbOptions.Dimensions[0].Codes {

			label := ftbOptions.Dimensions[0].Codes[i]
			if len(ftbOptions.Dimensions[0].Labels) > 0 {
				label = ftbOptions.Dimensions[0].Labels[i]
			}
			dimensionOption := &models.DimensionOption{
				InstanceID:  versionID,
				Label:       label,
				LastUpdated: time.Now().UTC(),
				Links: models.DimensionOptionLinks{
					Code: models.LinkObject{
						HRef: fmt.Sprintf("%s/code-lists/%s/codes/%s", mongo.CodeListURL, ftbOptions.Dimensions[0].Name, option),
						ID:   option,
					},
					CodeList: models.LinkObject{
						HRef: fmt.Sprintf("%s/code-lists/%s", mongo.CodeListURL, ftbOptions.Dimensions[0].Name),
						ID:   ftbOptions.Dimensions[0].Name,
					},
					Version: models.LinkObject{
						HRef: datasetAPIURL + "/datasets/" + datasetID + "/editions/" + edition + "/versions/1",
						ID:   "1",
					},
				},
				Name:   ftbOptions.Dimensions[0].Name,
				Option: option,
			}

			options = append(options, dimensionOption)

			// Do a bulk upload of 500 documents at a time to speed the process of loading data into mongo db
			if len(options) == 500 {
				if err := mongo.BulkInsertDimensionOptions(options); err != nil {
					log.Event(ctx, "failed to add dimension options in bulk request to mongo db", log.ERROR, log.Error(err))
					return datasetFTBTable, editionFTBTable, versionFTBTable, err
				}

				options = nil
			}
		}

		// Add leftover docs to mongo
		if len(options) != 0 {
			if err := mongo.BulkInsertDimensionOptions(options); err != nil {
				log.Event(ctx, "failed to add last set of dimension options in bulk request to mongo db", log.ERROR, log.Error(err))
				return datasetFTBTable, editionFTBTable, versionFTBTable, err
			}

			options = nil
		}
	}

	versionDoc := &models.Version{
		CollectionID:   collectionID,
		Dimensions:     dimensionsWithOptionCounts,
		Downloads:      nil,
		Edition:        edition,
		FTBType:        "ftb-table",
		FlexDimensions: &tableData.flexibleDimensions,
		Headers:        headers,
		ID:             versionID,
		IsBasedOn: &[]models.IsBasedOn{
			{
				ID:   tableData.versionLink,
				Type: "DataSet",
			},
		},
		LatestChanges: nil,
		Links: &models.VersionLinks{
			Dataset: &models.LinkObject{
				HRef: datasetAPIURL + "/datasets/" + datasetID,
				ID:   datasetID,
			},
			Dimensions: &models.LinkObject{
				HRef: datasetAPIURL + "/datasets/" + datasetID + "/editions/" + edition + "/versions/1/dimensions",
			},
			Edition: &models.LinkObject{
				HRef: datasetAPIURL + "/datasets/" + datasetID + "/editions/" + edition,
				ID:   edition,
			},
			Self: &models.LinkObject{
				HRef: datasetAPIURL + "/instances/" + versionID,
			},
			Spatial: nil,
			Version: &models.LinkObject{
				HRef: datasetAPIURL + "/datasets/" + datasetID + "/editions/" + edition + "/versions/1",
				ID:   "1",
			},
		},
		ReleaseDate: "22/03/2012",
		State:       "published",
		Tables:      nil,
		Temporal: &[]models.TemporalFrequency{
			{
				Frequency: "Decennial",
			},
		},
		Type:       "ftb",
		UsageNotes: nil,
		Version:    1,
	}

	// Store version doc
	if err := mongo.UpsertVersion(versionID, versionDoc); err != nil {
		log.Event(ctx, "failed to upload version document", log.ERROR, log.Error(err))
		return datasetFTBTable, editionFTBTable, versionFTBTable, err
	}

	// Create Dataset
	currentDatasetDoc := &models.Dataset{
		CollectionID: collectionID,
		Contacts: []models.ContactDetails{
			{
				Email:     "census-team@ons.gov.uk",
				Name:      "census-team",
				Telephone: "+44 (0)845 601 3034",
			},
		},
		Description: tableData.description,
		IsBasedOn: &[]models.IsBasedOn{
			{
				ID:   tableData.datasetLink,
				Type: "DataSet",
			},
		},
		FTBType:  "ftb-table",
		ID:       datasetID,
		Keywords: keywords,
		License:  license,
		Links: &models.DatasetLinks{
			Editions: &models.LinkObject{
				HRef: datasetAPIURL + "/datasets/" + datasetID + "/editions",
			},
			LatestVersion: &models.LinkObject{
				HRef: datasetAPIURL + "/datasets/" + datasetID + "/editions/" + edition + "/versions/1",
				ID:   "1",
			},
			Self: &models.LinkObject{
				HRef: datasetAPIURL + "/datasets/" + datasetID,
			},
			Taxonomy: &models.LinkObject{},
		},
		Methodologies:     nil,
		NationalStatistic: &nationalStatistic,
		NextRelease:       "N/A",
		Publications:      nil,
		Publisher:         &publisher,
		QMI: &models.GeneralDetails{
			HRef: qmiHRef,
		},
		RelatedDatasets:  nil,
		ReleaseFrequency: releaseFrequency,
		State:            "published",
		Theme:            "census",
		Title:            tableData.title,
		Type:             "ftb",
		UnitOfMeasure:    unitOfMeasure,
		URI:              "",
	}

	datasetDoc := &models.DatasetUpdate{
		Current: currentDatasetDoc,
		Next:    currentDatasetDoc,
	}

	// Store dataset doc
	if err := mongo.UpsertDataset(datasetID, datasetDoc); err != nil {
		log.Event(ctx, "failed to upload dataset document", log.ERROR, log.Error(err))
		return datasetFTBTable, editionFTBTable, versionFTBTable, err
	}

	// Create Edition
	currentEditionDoc := &models.Edition{
		Edition: edition,
		FTBType: "ftb-table",
		IsBasedOn: &[]models.IsBasedOn{
			{
				ID:   tableData.editionLink,
				Type: "DataSet",
			},
		},
		Links: &models.EditionUpdateLinks{
			Dataset: &models.LinkObject{
				HRef: datasetAPIURL + "/datasets/" + datasetID,
				ID:   datasetID,
			},
			LatestVersion: &models.LinkObject{
				HRef: datasetAPIURL + "/datasets/" + datasetID + "/editions/" + edition + "/versions/1",
				ID:   "1",
			},
			Self: &models.LinkObject{
				HRef: datasetAPIURL + "/datasets/" + datasetID + "/editions/" + edition,
			},
			Versions: &models.LinkObject{
				HRef: datasetAPIURL + "/datasets/" + datasetID + "/editions/" + edition + "/versions",
			},
		},
		State: "published",
		Type:  "ftb",
	}

	// Store edition doc
	editionDoc := &models.EditionUpdate{
		Current: currentEditionDoc,
		Next:    currentEditionDoc,
	}

	// Store dataset doc
	if err := mongo.UpsertEdition(datasetID, editionDoc); err != nil {
		log.Event(ctx, "failed to upload edition document", log.ERROR, log.Error(err))
		return datasetFTBTable, editionFTBTable, versionFTBTable, err
	}

	log.Event(ctx, "successfully completed loading ftb data table", log.INFO)

	datasetFTBTable = models.Table{
		HRef:  datasetAPIURL + "/datasets/" + datasetID,
		Title: tableData.title,
	}

	editionFTBTable = models.Table{
		HRef:  datasetAPIURL + "/datasets/" + datasetID + "/editions/" + edition,
		Title: tableData.title,
	}

	versionFTBTable = models.Table{
		HRef:  datasetAPIURL + "/datasets/" + datasetID + "/editions/" + edition + "/versions/1",
		Title: tableData.title,
	}

	return datasetFTBTable, editionFTBTable, versionFTBTable, nil
}

func (api *API) updateFTBDatasetBlob(ctx context.Context, mongo Mongo, datasetID, instanceID string, datasetFTBTables, editionFTBTables, versionFTBTables []models.Table) error {
	// Update dataset People with dataset tables
	datasetUpdates := make(bson.M)
	datasetUpdates["next.tables"] = datasetFTBTables
	datasetUpdates["current.tables"] = datasetFTBTables

	if err := mongo.UpdateDataset(datasetID, datasetUpdates); err != nil {
		log.Event(ctx, "failed to update dataset doc with tables", log.ERROR, log.Error(err))
		return err
	}

	// Update edition People with edition tables
	editionUpdates := make(bson.M)
	editionUpdates["next.tables"] = editionFTBTables
	editionUpdates["current.tables"] = editionFTBTables

	if err := mongo.UpdateEdition(datasetID, editionUpdates); err != nil {
		log.Event(ctx, "failed to update edition doc with tables", log.ERROR, log.Error(err))
		return err
	}

	// Update version People with version tables
	versionUpdates := make(bson.M)
	versionUpdates["tables"] = versionFTBTables

	if err := mongo.UpdateVersion(instanceID, versionUpdates); err != nil {
		log.Event(ctx, "failed to update version doc with tables", log.ERROR, log.Error(err))
		return err
	}

	return nil
}

func (api *API) getFTBAPI(ctx context.Context, ftbAuthToken, ftbURL, path string) ([]byte, int, error) {
	return api.callFTBAPI(ctx, "GET", ftbAuthToken, ftbURL, path, nil)
}

// API aggregates a client and URL and other common data for accessing the API
type API struct {
	clienter dphttp.Clienter
	url      string
}

// NewFTBAPI creates an FTBAPI object
func NewFTBAPI(clienter dphttp.Clienter, FTBAPIURL string) *API {
	return &API{
		clienter: clienter,
		url:      FTBAPIURL,
	}
}

func (api *API) retrieveDimensionOptions(ctx context.Context, dim string) (*FTBData, error) {
	// Get People Dataset
	path := "/v8/codebook/People?var=" + dim

	responseBody, _, err := api.getFTBAPI(ctx, ftbAuthToken, ftbHost, path)
	if err != nil {
		log.Event(ctx, "failed to make request to FTB", log.Data{"ftb_url": ftbHost + path})
		return nil, err
	}

	ftbDimOptions := &FTBData{}

	if err := json.Unmarshal(responseBody, ftbDimOptions); err != nil {
		log.Event(ctx, "unable to unmarshal json body", log.ERROR, log.Error(err))
		return nil, err
	}

	return ftbDimOptions, nil
}

func (api *API) callFTBAPI(ctx context.Context, method, authToken, host, path string, payload interface{}) ([]byte, int, error) {
	ftbURL := host + path

	logData := log.Data{"url": ftbURL, "method": method}

	URL, err := url.Parse(ftbURL)
	if err != nil {
		log.Event(ctx, "failed to create url for ftb call", log.ERROR, log.Error(err), logData)
		return nil, 0, err
	}
	path = URL.String()
	logData["url"] = path

	var req *http.Request

	if payload != nil {
		req, err = http.NewRequest(method, path, bytes.NewReader(payload.([]byte)))
		req.Header.Add("Content-type", "application/json")
		logData["payload"] = string(payload.([]byte))
	} else {
		req, err = http.NewRequest(method, path, nil)
	}
	// check req, above, didn't error
	if err != nil {
		log.Event(ctx, "failed to create request for call to ftb", log.ERROR, log.Error(err), logData)
		return nil, 0, err
	}

	// Set auth token
	req.Header.Set("Authorization", authToken)

	resp, err := api.clienter.Do(ctx, req)
	if err != nil {
		log.Event(ctx, "failed to call ftb", log.ERROR, log.Error(err), logData)
		return nil, 0, err
	}
	defer resp.Body.Close()

	logData["http_code"] = resp.StatusCode

	jsonBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Event(ctx, "failed to read response body from call to ftb", log.ERROR, log.Error(err), logData)
		return nil, resp.StatusCode, err
	}
	logData["json_body"] = string(jsonBody)
	logData["status_code"] = resp.StatusCode

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= 300 {
		log.Event(ctx, "failed", log.ERROR, log.Error(ErrorUnexpectedStatusCode), logData)
		return nil, resp.StatusCode, ErrorUnexpectedStatusCode
	}

	return jsonBody, resp.StatusCode, nil
}

// Mongo represents a simplistic MongoDB configuration.
type Mongo struct {
	CodeListURL string
	Database    string
	Session     *mgo.Session
	URI         string
}

// Init creates a new mgo.Session with a strong consistency and a write mode of "majortiy".
func (m *Mongo) Init() (session *mgo.Session, err error) {
	if session != nil {
		return nil, errors.New("session already exists")
	}

	if session, err = mgo.Dial(m.URI); err != nil {
		return nil, err
	}

	session.EnsureSafe(&mgo.Safe{WMode: "majority"})
	session.SetMode(mgo.Strong, true)

	return session, nil
}

// UpsertVersion adds or overrides an existing version document
func (m *Mongo) UpsertVersion(id string, version *models.Version) (err error) {
	s := m.Session.Copy()
	defer s.Close()

	update := bson.M{
		"$set": version,
		"$setOnInsert": bson.M{
			"last_updated": time.Now(),
		},
	}

	_, err = s.DB(m.Database).C(versionCollection).UpsertId(id, update)
	return err
}

// UpdateVersion updates an existing version document
func (m *Mongo) UpdateVersion(id string, updates bson.M) (err error) {
	s := m.Session.Copy()
	defer s.Close()

	err = s.DB(m.Database).C("instances").Update(bson.M{"id": id}, bson.M{"$set": updates})
	return
}

// UpsertDataset adds or overides an existing dataset document
func (m *Mongo) UpsertDataset(id string, datasetDoc *models.DatasetUpdate) (err error) {
	s := m.Session.Copy()
	defer s.Close()

	update := bson.M{
		"$set": datasetDoc,
		"$setOnInsert": bson.M{
			"last_updated": time.Now(),
		},
	}

	_, err = s.DB(m.Database).C(datasetCollection).UpsertId(id, update)
	return
}

// UpdateDataset updates an existing dataset document
func (m *Mongo) UpdateDataset(id string, updates bson.M) (err error) {
	s := m.Session.Copy()
	defer s.Close()

	update := bson.M{"$set": updates}
	if err = s.DB(m.Database).C(datasetCollection).UpdateId(id, update); err != nil {
		if err == mgo.ErrNotFound {
			return errors.New("dataset not found")
		}
		return err
	}

	return nil
}

// UpsertEdition adds or overides an existing edition document
func (m *Mongo) UpsertEdition(datasetID string, editionDoc *models.EditionUpdate) (err error) {
	s := m.Session.Copy()
	defer s.Close()

	selector := bson.M{
		"next.edition":          edition,
		"next.links.dataset.id": datasetID,
	}

	editionDoc.Next.LastUpdated = time.Now()

	update := bson.M{
		"$set": editionDoc,
	}

	_, err = s.DB(m.Database).C(editionCollection).Upsert(selector, update)
	return
}

// UpdateEdition updates an existing edition document
func (m *Mongo) UpdateEdition(datasetID string, updates bson.M) (err error) {
	s := m.Session.Copy()
	defer s.Close()

	selector := bson.M{
		"next.edition":          edition,
		"next.links.dataset.id": datasetID,
	}

	update := bson.M{"$set": updates}
	if err = s.DB(m.Database).C(editionCollection).Update(selector, update); err != nil {
		if err == mgo.ErrNotFound {
			return errors.New("edition not found")
		}
		return err
	}

	return nil
}

// AddDimensionToInstance to the dimension collection
func (m *Mongo) AddDimensionToInstance(option *models.DimensionOption) error {
	s := m.Session.Copy()
	defer s.Close()

	option.LastUpdated = time.Now().UTC()
	_, err := s.DB(m.Database).C(dimOptionCollection).Upsert(bson.M{"instance_id": option.InstanceID, "name": option.Name,
		"option": option.Option}, &option)

	return err
}

// BulkInsertDimensionOptions to the dimension.options collection
func (m *Mongo) BulkInsertDimensionOptions(options []interface{}) error {
	s := m.Session.Copy()
	defer s.Close()

	s.Clone()
	bulk := s.Clone().DB(m.Database).C(dimOptionCollection).Bulk()
	bulk.Insert(options...)
	_, err := bulk.Run()

	return err
}
