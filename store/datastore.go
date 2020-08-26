package store

import (
	"context"

	"github.com/ONSdigital/dp-census-alpha-ftb-dataset-api/models"
	"github.com/globalsign/mgo/bson"
)

// DataStore provides a datastore.Storer interface used to store, retrieve, remove or update datasets
type DataStore struct {
	Backend Storer
}

//go:generate moq -out datastoretest/datastore.go -pkg storetest . Storer

// Storer represents basic data access via Get, Remove and Upsert methods.
type Storer interface {
	CheckDatasetExists(ID, state string) error
	CheckEditionExists(ID, editionID, state string) error
	GetDataset(ID string) (*models.DatasetUpdate, error)
	GetDatasets(ctx context.Context) ([]models.DatasetUpdate, error)
	GetDimensionsFromInstance(ID string) (*models.DimensionNodeResults, error)
	GetDimensions(datasetID, versionID string) ([]bson.M, error)
	GetDimensionOptions(version *models.Version, dimension string) (*models.DimensionOptionResults, error)
	GetEdition(ID, editionID, state string) (*models.EditionUpdate, error)
	GetEditions(ctx context.Context, ID, state string) (*models.EditionUpdateResults, error)
	GetInstances(ctx context.Context, states []string, datasets []string) (*models.InstanceResults, error)
	GetInstance(ID string) (*models.Instance, error)
	GetNextVersion(datasetID, editionID string) (int, error)
	GetUniqueDimensionAndOptions(ID, dimension string) (*models.DimensionValues, error)
	GetVersion(datasetID, editionID, version, state string) (*models.Version, error)
	GetVersions(ctx context.Context, datasetID, editionID, state string) (*models.VersionResults, error)
}
