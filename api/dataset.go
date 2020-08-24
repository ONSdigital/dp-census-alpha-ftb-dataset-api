package api

import (
	"context"
	"encoding/json"
	"net/http"

	errs "github.com/ONSdigital/dp-census-alpha-ftb-dataset-api/apierrors"
	"github.com/ONSdigital/dp-census-alpha-ftb-dataset-api/models"
	"github.com/ONSdigital/log.go/log"
	"github.com/gorilla/mux"
)

var (
	// errors that should return a 403 status
	datasetsForbidden = map[error]bool{
		errs.ErrDeletePublishedDatasetForbidden: true,
		errs.ErrAddDatasetAlreadyExists:         true,
	}

	// errors that should return a 204 status
	datasetsNoContent = map[error]bool{
		errs.ErrDeleteDatasetNotFound: true,
	}

	// errors that should return a 400 status
	datasetsBadRequest = map[error]bool{
		errs.ErrAddUpdateDatasetBadRequest: true,
	}

	// errors that should return a 404 status
	resourcesNotFound = map[error]bool{
		errs.ErrDatasetNotFound:  true,
		errs.ErrEditionsNotFound: true,
	}
)

func (api *FTBDatasetAPI) getDatasets(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	datasets, err := api.dataStore.Backend.GetDatasets(ctx)
	if err != nil {
		log.Event(ctx, "api endpoint getDatasets datastore.GetDatasets returned an error", log.ERROR, log.Error(err))
		handleDatasetAPIErr(ctx, err, w, nil)
		return
	}
	logData := log.Data{}

	datasetsResponse := &models.DatasetUpdateResults{Items: datasets}

	b, err := json.Marshal(datasetsResponse)
	if err != nil {
		log.Event(ctx, "api endpoint getDatasets failed to marshal dataset resource into bytes", log.ERROR, log.Error(err), logData)
		handleDatasetAPIErr(ctx, err, w, nil)
		return
	}

	setJSONContentType(w)
	if _, err = w.Write(b); err != nil {
		log.Event(ctx, "api endpoint getDatasets error writing response body", log.ERROR, log.Error(err))
		handleDatasetAPIErr(ctx, err, w, nil)
		return
	}
	log.Event(ctx, "api endpoint getDatasets request successful", log.INFO)
}

func (api *FTBDatasetAPI) getDataset(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	datasetID := vars["dataset_id"]
	logData := log.Data{"dataset_id": datasetID}

	log.Event(ctx, "got this far 1")
	dataset, err := api.dataStore.Backend.GetDataset(datasetID)
	if err != nil {
		log.Event(ctx, "getDataset endpoint: dataStore.Backend.GetDataset returned an error", log.ERROR, log.Error(err), logData)
		handleDatasetAPIErr(ctx, err, w, logData)
		return
	}

	// Check dataset exists
	if dataset == nil {
		log.Event(ctx, "getDataset endpoint: published or unpublished dataset not found", log.INFO, logData)
		handleDatasetAPIErr(ctx, errs.ErrDatasetNotFound, w, logData)
		return
	}

	log.Event(ctx, "getDataset endpoint: dataset found", log.INFO, logData)

	var b []byte
	b, err = json.Marshal(dataset)
	if err != nil {
		log.Event(ctx, "getDataset endpoint: failed to marshal dataset resource into bytes", log.ERROR, log.Error(err), logData)
		handleDatasetAPIErr(ctx, err, w, logData)
		return
	}

	setJSONContentType(w)
	if _, err = w.Write(b); err != nil {
		log.Event(ctx, "getDataset endpoint: error writing bytes to response", log.ERROR, log.Error(err), logData)
		handleDatasetAPIErr(ctx, err, w, logData)
	}
	log.Event(ctx, "getDataset endpoint: request successful", log.INFO, logData)
}

func handleDatasetAPIErr(ctx context.Context, err error, w http.ResponseWriter, data log.Data) {
	if data == nil {
		data = log.Data{}
	}

	var status int
	switch {
	case datasetsForbidden[err]:
		status = http.StatusForbidden
	case datasetsNoContent[err]:
		status = http.StatusNoContent
	case datasetsBadRequest[err]:
		status = http.StatusBadRequest
	case resourcesNotFound[err]:
		status = http.StatusNotFound
	default:
		err = errs.ErrInternalServer
		status = http.StatusInternalServerError
	}

	data["responseStatus"] = status
	log.Event(ctx, "request unsuccessful", log.ERROR, log.Error(err), data)
	http.Error(w, err.Error(), status)
}
