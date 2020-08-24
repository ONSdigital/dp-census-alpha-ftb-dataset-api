package api

import (
	"encoding/json"
	"net/http"

	errs "github.com/ONSdigital/dp-census-alpha-ftb-dataset-api/apierrors"
	"github.com/ONSdigital/dp-census-alpha-ftb-dataset-api/models"
	"github.com/ONSdigital/log.go/log"
	"github.com/gorilla/mux"
)

func (api *FTBDatasetAPI) getMetadata(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	datasetID := vars["dataset_id"]
	edition := vars["edition"]
	version := vars["version"]
	logData := log.Data{"dataset_id": datasetID, "edition": edition, "version": version}

	versionDoc, err := api.dataStore.Backend.GetVersion(datasetID, edition, version, "")
	if err != nil {
		if err == errs.ErrVersionNotFound {
			log.Event(ctx, "getMetadata endpoint: failed to find version for dataset edition", log.ERROR, log.Error(err), logData)

			handleMetadataErr(w, errs.ErrMetadataVersionNotFound)
			return
		}

		log.Event(ctx, "getMetadata endpoint: get datastore.getVersion returned an error", log.ERROR, log.Error(err), logData)
		handleMetadataErr(w, err)
		return
	}

	datasetDoc, err := api.dataStore.Backend.GetDataset(datasetID)
	if err != nil {
		log.Event(ctx, "getMetadata endpoint: get datastore.getDataset returned an error", log.ERROR, log.Error(err), logData)
		handleMetadataErr(w, err)
		return
	}

	if err = api.dataStore.Backend.CheckEditionExists(datasetID, edition, ""); err != nil {
		log.Event(ctx, "getMetadata endpoint: failed to find edition for dataset", log.ERROR, log.Error(err), logData)
		handleMetadataErr(w, err)
		return
	}

	if err = models.CheckState("version", versionDoc.State); err != nil {
		logData["state"] = versionDoc.State
		log.Event(ctx, "getMetadata endpoint: unpublished version has an invalid state", log.ERROR, log.Error(err), logData)
		handleMetadataErr(w, err)
		return
	}

	var metaDataDoc *models.Metadata
	// combine version and dataset metadata
	if versionDoc.State != models.PublishedState {
		metaDataDoc = models.CreateMetaDataDoc(datasetDoc.Next, versionDoc, api.urlBuilder)
	} else {
		metaDataDoc = models.CreateMetaDataDoc(datasetDoc.Current, versionDoc, api.urlBuilder)
	}

	b, err := json.Marshal(metaDataDoc)
	if err != nil {
		log.Event(ctx, "getMetadata endpoint: failed to marshal metadata resource into bytes", log.ERROR, log.Error(err), logData)
		handleMetadataErr(w, err)
		return
	}

	setJSONContentType(w)
	if _, err = w.Write(b); err != nil {
		log.Event(ctx, "getMetadata endpoint: failed to write bytes to response", log.ERROR, log.Error(err), logData)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	log.Event(ctx, "getMetadata endpoint: get metadata request successful", log.INFO, logData)
}

func handleMetadataErr(w http.ResponseWriter, err error) {
	var responseStatus int

	switch {
	case err == errs.ErrUnauthorised:
		responseStatus = http.StatusNotFound
	case err == errs.ErrEditionNotFound:
		responseStatus = http.StatusNotFound
	case err == errs.ErrMetadataVersionNotFound:
		responseStatus = http.StatusNotFound
	case err == errs.ErrDatasetNotFound:
		responseStatus = http.StatusNotFound
	default:
		err = errs.ErrInternalServer
		responseStatus = http.StatusInternalServerError
	}

	http.Error(w, err.Error(), responseStatus)
}
