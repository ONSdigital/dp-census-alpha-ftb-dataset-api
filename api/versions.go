package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	errs "github.com/ONSdigital/dp-census-alpha-ftb-dataset-api/apierrors"
	"github.com/ONSdigital/dp-census-alpha-ftb-dataset-api/models"
	"github.com/ONSdigital/log.go/log"
	"github.com/gorilla/mux"
)

var (
	// errors that map to a HTTP 404 response
	notFound = map[error]bool{
		errs.ErrDatasetNotFound: true,
		errs.ErrEditionNotFound: true,
		errs.ErrVersionNotFound: true,
	}

	// errors that map to a HTTP 400 response
	badRequest = map[error]bool{
		errs.ErrUnableToParseJSON:                      true,
		models.ErrPublishedVersionCollectionIDInvalid:  true,
		models.ErrAssociatedVersionCollectionIDInvalid: true,
		models.ErrVersionStateInvalid:                  true,
	}

	// HTTP 500 responses with a specific message
	internalServerErrWithMessage = map[error]bool{
		errs.ErrResourceState: true,
	}
)

// VersionDetails contains the details that uniquely identify a version resource
type VersionDetails struct {
	datasetID string
	edition   string
	version   string
}

func (api *FTBDatasetAPI) getVersions(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	datasetID := vars["dataset_id"]
	edition := vars["edition"]
	logData := log.Data{"dataset_id": datasetID, "edition": edition}

	var state string

	if err := api.dataStore.Backend.CheckDatasetExists(datasetID, state); err != nil {
		log.Event(ctx, "failed to find dataset for list of versions", log.ERROR, log.Error(err), logData)
		handleVersionAPIErr(ctx, err, w, logData)
		return
	}

	if err := api.dataStore.Backend.CheckEditionExists(datasetID, edition, state); err != nil {
		log.Event(ctx, "failed to find edition for list of versions", log.ERROR, log.Error(err), logData)
		handleVersionAPIErr(ctx, err, w, logData)
		return
	}

	results, err := api.dataStore.Backend.GetVersions(ctx, datasetID, edition, state)
	if err != nil {
		log.Event(ctx, "failed to find any versions for dataset edition", log.ERROR, log.Error(err), logData)
		handleVersionAPIErr(ctx, err, w, logData)
		return
	}

	var hasInvalidState bool
	for _, item := range results.Items {
		if err = models.CheckState("version", item.State); err != nil {
			hasInvalidState = true
			log.Event(ctx, "unpublished version has an invalid state", log.ERROR, log.Error(err), log.Data{"state": item.State})
			break
		}
	}

	if hasInvalidState {
		handleVersionAPIErr(ctx, err, w, logData)
		return
	}

	b, err := json.Marshal(results)
	if err != nil {
		log.Event(ctx, "failed to marshal list of version resources into bytes", log.ERROR, log.Error(err), logData)
		handleVersionAPIErr(ctx, err, w, logData)
		return
	}

	setJSONContentType(w)
	_, err = w.Write(b)
	if err != nil {
		log.Event(ctx, "error writing bytes to response", log.ERROR, log.Error(err), logData)
		handleVersionAPIErr(ctx, err, w, logData)
	}
	log.Event(ctx, "getVersions endpoint: request successful", log.INFO, logData)
}

func (api *FTBDatasetAPI) getVersion(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	datasetID := vars["dataset_id"]
	edition := vars["edition"]
	version := vars["version"]
	logData := log.Data{"dataset_id": datasetID, "edition": edition, "version": version}

	var state string
	if err := api.dataStore.Backend.CheckDatasetExists(datasetID, state); err != nil {
		log.Event(ctx, "failed to find dataset", log.ERROR, log.Error(err), logData)
		handleVersionAPIErr(ctx, err, w, logData)
		return
	}

	if err := api.dataStore.Backend.CheckEditionExists(datasetID, edition, state); err != nil {
		log.Event(ctx, "failed to find edition for dataset", log.ERROR, log.Error(err), logData)
		handleVersionAPIErr(ctx, err, w, logData)
		return
	}

	results, err := api.dataStore.Backend.GetVersion(datasetID, edition, version, state)
	if err != nil {
		log.Event(ctx, "failed to find version for dataset edition", log.ERROR, log.Error(err), logData)
		handleVersionAPIErr(ctx, err, w, logData)
		return
	}

	results.Links.Self.HRef = results.Links.Version.HRef

	if err = models.CheckState("version", results.State); err != nil {
		log.Event(ctx, "unpublished version has an invalid state", log.ERROR, log.Error(err), log.Data{"state": results.State})
		handleVersionAPIErr(ctx, errs.ErrResourceState, w, logData)
		return
	}

	b, err := json.Marshal(results)
	if err != nil {
		log.Event(ctx, "failed to marshal version resource into bytes", log.ERROR, log.Error(err), logData)
		handleVersionAPIErr(ctx, err, w, logData)
		return
	}

	setJSONContentType(w)
	_, err = w.Write(b)
	if err != nil {
		log.Event(ctx, "failed writing bytes to response", log.ERROR, log.Error(err), logData)
		handleVersionAPIErr(ctx, err, w, logData)
	}
	log.Event(ctx, "getVersion endpoint: request successful", log.INFO, logData)
}

func handleVersionAPIErr(ctx context.Context, err error, w http.ResponseWriter, data log.Data) {
	var status int
	switch {
	case notFound[err]:
		status = http.StatusNotFound
	case badRequest[err]:
		status = http.StatusBadRequest
	case internalServerErrWithMessage[err]:
		status = http.StatusInternalServerError
	case strings.HasPrefix(err.Error(), "missing mandatory fields:"):
		status = http.StatusBadRequest
	case strings.HasPrefix(err.Error(), "invalid fields:"):
		status = http.StatusBadRequest
	default:
		err = errs.ErrInternalServer
		status = http.StatusInternalServerError
	}

	if data == nil {
		data = log.Data{}
	}

	log.Event(ctx, "request unsuccessful", log.ERROR, log.Error(err), data)
	http.Error(w, err.Error(), status)
}
