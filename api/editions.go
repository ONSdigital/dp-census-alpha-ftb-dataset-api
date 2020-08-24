package api

import (
	"encoding/json"
	"net/http"

	errs "github.com/ONSdigital/dp-census-alpha-ftb-dataset-api/apierrors"
	"github.com/ONSdigital/log.go/log"
	"github.com/gorilla/mux"
)

func (api *FTBDatasetAPI) getEditions(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	datasetID := vars["dataset_id"]
	logData := log.Data{"dataset_id": datasetID}

	var state string
	logData["state"] = state

	if err := api.dataStore.Backend.CheckDatasetExists(datasetID, state); err != nil {
		log.Event(ctx, "getEditions endpoint: unable to find dataset", log.ERROR, log.Error(err), logData)
		if err == errs.ErrDatasetNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}

		http.Error(w, errs.ErrInternalServer.Error(), http.StatusInternalServerError)
		return
	}

	results, err := api.dataStore.Backend.GetEditions(ctx, datasetID, state)
	if err != nil {
		log.Event(ctx, "getEditions endpoint: unable to find editions for dataset", log.ERROR, log.Error(err), logData)
		if err == errs.ErrEditionNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}

		http.Error(w, errs.ErrInternalServer.Error(), http.StatusInternalServerError)
		return
	}

	// User has valid authentication to get raw edition document
	b, err := json.Marshal(results)
	if err != nil {
		log.Event(ctx, "getEditions endpoint: failed to marshal a list of edition resources into bytes", log.ERROR, log.Error(err), logData)
		http.Error(w, errs.ErrInternalServer.Error(), http.StatusInternalServerError)
		return
	}

	setJSONContentType(w)
	_, err = w.Write(b)
	if err != nil {
		log.Event(ctx, "getEditions endpoint: failed writing bytes to response", log.ERROR, log.Error(err), logData)
		http.Error(w, errs.ErrInternalServer.Error(), http.StatusInternalServerError)
	}

	log.Event(ctx, "getEditions endpoint: request successful", log.INFO, logData)
}

func (api *FTBDatasetAPI) getEdition(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	datasetID := vars["dataset_id"]
	edition := vars["edition"]
	logData := log.Data{"dataset_id": datasetID, "edition": edition}

	var state string

	if err := api.dataStore.Backend.CheckDatasetExists(datasetID, state); err != nil {
		log.Event(ctx, "getEdition endpoint: unable to find dataset", log.ERROR, log.Error(err), logData)

		if err == errs.ErrDatasetNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}

		http.Error(w, errs.ErrInternalServer.Error(), http.StatusInternalServerError)
		return
	}

	editionDoc, err := api.dataStore.Backend.GetEdition(datasetID, edition, state)
	if err != nil {
		log.Event(ctx, "getEdition endpoint: unable to find edition", log.ERROR, log.Error(err), logData)

		if err == errs.ErrEditionNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}

		http.Error(w, errs.ErrInternalServer.Error(), http.StatusInternalServerError)
		return
	}

	b, err := json.Marshal(editionDoc)
	if err != nil {
		log.Event(ctx, "getEdition endpoint: failed to marshal edition resource into bytes", log.ERROR, log.Error(err), logData)

		http.Error(w, errs.ErrInternalServer.Error(), http.StatusInternalServerError)
		return
	}

	setJSONContentType(w)
	_, err = w.Write(b)
	if err != nil {
		log.Event(ctx, "getEdition endpoint: failed to write byte to response", log.ERROR, log.Error(err), logData)
		http.Error(w, errs.ErrInternalServer.Error(), http.StatusInternalServerError)
		return
	}
	log.Event(ctx, "getEdition endpoint: request successful", log.INFO, logData)
}
