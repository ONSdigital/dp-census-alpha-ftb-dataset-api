package api

import (
	"context"
	"net/http"
	"strconv"

	"github.com/ONSdigital/dp-census-alpha-ftb-dataset-api/config"
	"github.com/ONSdigital/dp-census-alpha-ftb-dataset-api/store"
	"github.com/ONSdigital/dp-census-alpha-ftb-dataset-api/url"
	"github.com/ONSdigital/go-ns/server"
	"github.com/ONSdigital/log.go/log"
	"github.com/gorilla/mux"
)

var httpServer *server.Server

const (
	hasDownloads = "has_downloads"
)

var (
	trueStringified = strconv.FormatBool(true)
)

//API provides an interface for the routes
type API interface {
	CreateFTBDatasetAPI(string, *mux.Router, store.DataStore) *FTBDatasetAPI
}

// DownloadsGenerator pre generates full file downloads for the specified dataset/edition/version
type DownloadsGenerator interface {
	Generate(ctx context.Context, datasetID, instanceID, edition, version string) error
}

// FTBDatasetAPI manages requests against a dataset
type FTBDatasetAPI struct {
	dataStore  store.DataStore
	host       string
	Router     *mux.Router
	urlBuilder *url.Builder
}

// CreateAndInitialiseFTBDatasetAPI create a new FTBDatasetAPI instance based on the configuration provided.
func CreateAndInitialiseFTBDatasetAPI(ctx context.Context, cfg config.Configuration, dataStore store.DataStore, urlBuilder *url.Builder, errorChan chan error) {
	router := mux.NewRouter()
	api := NewFTBDatasetAPI(ctx, cfg, router, dataStore, urlBuilder)

	httpServer = server.New(cfg.BindAddr, api.Router)

	// Disable this here to allow main to manage graceful shutdown of the entire app.
	httpServer.HandleOSSignals = false

	go func() {
		log.Event(ctx, "Starting ftb dataset api...", log.INFO)
		if err := httpServer.ListenAndServe(); err != nil {
			log.Event(ctx, "api http server returned error", log.ERROR, log.Error(err))
			errorChan <- err
		}
	}()
}

// NewFTBDatasetAPI create a new FTB Dataset API instance and register the API routes based on the application configuration.
func NewFTBDatasetAPI(ctx context.Context, cfg config.Configuration, router *mux.Router, dataStore store.DataStore, urlBuilder *url.Builder) *FTBDatasetAPI {
	api := &FTBDatasetAPI{
		dataStore:  dataStore,
		host:       cfg.FTBDatasetAPIURL,
		Router:     router,
		urlBuilder: urlBuilder,
	}

	log.Event(ctx, "enabling only public endpoints for dataset api", log.INFO)
	api.enablePublicEndpoints(ctx)

	return api
}

// enablePublicEndpoints register only the public GET endpoints.
func (api *FTBDatasetAPI) enablePublicEndpoints(ctx context.Context) {
	api.get("/datasets", api.getDatasets)
	api.get("/datasets/{dataset_id}", api.getDataset)
	api.get("/datasets/{dataset_id}/editions", api.getEditions)
	api.get("/datasets/{dataset_id}/editions/{edition}", api.getEdition)
	api.get("/datasets/{dataset_id}/editions/{edition}/versions", api.getVersions)
	api.get("/datasets/{dataset_id}/editions/{edition}/versions/{version}", api.getVersion)
	api.get("/datasets/{dataset_id}/editions/{edition}/versions/{version}/metadata", api.getMetadata)
	api.get("/datasets/{dataset_id}/editions/{edition}/versions/{version}/dimensions", api.getDimensions)
	api.get("/datasets/{dataset_id}/editions/{edition}/versions/{version}/dimensions/{dimension}/options", api.getDimensionOptions)
}

// get register a GET http.HandlerFunc.
func (api *FTBDatasetAPI) get(path string, handler http.HandlerFunc) {
	api.Router.HandleFunc(path, handler).Methods("GET")
}

func setJSONContentType(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
}

// Close represents the graceful shutting down of the http server
func Close(ctx context.Context) error {
	if err := httpServer.Shutdown(ctx); err != nil {
		return err
	}
	log.Event(ctx, "graceful shutdown of http server complete", log.INFO)
	return nil
}
