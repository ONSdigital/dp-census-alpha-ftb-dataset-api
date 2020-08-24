package mongo

import (
	"context"

	errs "github.com/ONSdigital/dp-census-alpha-ftb-dataset-api/apierrors"
	"github.com/ONSdigital/dp-census-alpha-ftb-dataset-api/models"
	"github.com/ONSdigital/log.go/log"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
)

const instanceCollection = "instances"

// GetInstances from a mongo collection
func (m *Mongo) GetInstances(ctx context.Context, states []string, datasets []string) (*models.InstanceResults, error) {
	s := m.Session.Copy()
	defer s.Close()

	filter := bson.M{}
	if len(states) > 0 {
		filter["state"] = bson.M{"$in": states}
	}

	if len(datasets) > 0 {
		filter["links.dataset.id"] = bson.M{"$in": datasets}
	}

	iter := s.DB(m.Database).C(instanceCollection).Find(filter).Sort("-$natural").Iter()
	defer func() {
		err := iter.Close()
		if err != nil {
			log.Event(ctx, "error closing iterator", log.ERROR, log.Error(err), log.Data{"state_query": states, "dataset_query": datasets})
		}
	}()

	results := []models.Instance{}
	if err := iter.All(&results); err != nil {
		if err == mgo.ErrNotFound {
			return nil, errs.ErrDatasetNotFound
		}
		return nil, err
	}

	return &models.InstanceResults{Items: results}, nil
}

// GetInstance returns a single instance from an ID
func (m *Mongo) GetInstance(ID string) (*models.Instance, error) {
	s := m.Session.Copy()
	defer s.Close()

	var instance models.Instance
	err := s.DB(m.Database).C(instanceCollection).Find(bson.M{"id": ID}).One(&instance)

	if err == mgo.ErrNotFound {
		return nil, errs.ErrInstanceNotFound
	}

	return &instance, err
}
