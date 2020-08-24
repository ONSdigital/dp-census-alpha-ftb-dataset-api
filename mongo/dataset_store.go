package mongo

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"

	errs "github.com/ONSdigital/dp-census-alpha-ftb-dataset-api/apierrors"
	"github.com/ONSdigital/dp-census-alpha-ftb-dataset-api/models"
	"github.com/ONSdigital/log.go/log"
)

// Mongo represents a simplistic MongoDB configuration.
type Mongo struct {
	CodeListURL    string
	Collection     string
	Database       string
	DatasetURL     string
	Session        *mgo.Session
	URI            string
	lastPingTime   time.Time
	lastPingResult error
}

const (
	editionsCollection = "editions"
)

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

// GetDatasets retrieves all dataset documents
func (m *Mongo) GetDatasets(ctx context.Context) ([]models.DatasetUpdate, error) {
	s := m.Session.Copy()
	defer s.Close()

	iter := s.DB(m.Database).C("datasets").Find(nil).Iter()
	defer func() {
		err := iter.Close()
		if err != nil {
			log.Event(ctx, "error closing iterator", log.ERROR, log.Error(err))
		}
	}()

	results := []models.DatasetUpdate{}
	if err := iter.All(&results); err != nil {
		if err == mgo.ErrNotFound {
			return nil, errs.ErrDatasetNotFound
		}
		return nil, err
	}

	return results, nil
}

// GetDataset retrieves a dataset document
func (m *Mongo) GetDataset(id string) (*models.DatasetUpdate, error) {
	s := m.Session.Copy()
	defer s.Close()
	var dataset models.DatasetUpdate
	err := s.DB(m.Database).C("datasets").Find(bson.M{"_id": id}).One(&dataset)
	if err != nil {
		if err == mgo.ErrNotFound {
			return nil, errs.ErrDatasetNotFound
		}
		return nil, err
	}

	return &dataset, nil
}

// GetEditions retrieves all edition documents for a dataset
func (m *Mongo) GetEditions(ctx context.Context, id, state string) (*models.EditionUpdateResults, error) {
	s := m.Session.Copy()
	defer s.Close()

	selector := buildEditionsQuery(id, state)

	iter := s.DB(m.Database).C(editionsCollection).Find(selector).Iter()
	defer func() {
		err := iter.Close()
		if err != nil {
			log.Event(ctx, "error closing edition iterator", log.ERROR, log.Error(err), log.Data{"selector": selector})
		}
	}()

	var results []*models.EditionUpdate
	if err := iter.All(&results); err != nil {
		if err == mgo.ErrNotFound {
			return nil, errs.ErrEditionNotFound
		}
		return nil, err
	}

	if len(results) < 1 {
		return nil, errs.ErrEditionNotFound
	}
	return &models.EditionUpdateResults{Items: results}, nil
}

func buildEditionsQuery(id, state string) bson.M {
	var selector bson.M
	if state != "" {
		selector = bson.M{
			"current.links.dataset.id": id,
			"current.state":            state,
		}
	} else {
		selector = bson.M{
			"next.links.dataset.id": id,
		}
	}

	return selector
}

// GetEdition retrieves an edition document for a dataset
func (m *Mongo) GetEdition(id, editionID, state string) (*models.EditionUpdate, error) {
	s := m.Session.Copy()
	defer s.Close()

	selector := buildEditionQuery(id, editionID, state)

	var edition models.EditionUpdate
	err := s.DB(m.Database).C(editionsCollection).Find(selector).One(&edition)
	if err != nil {
		if err == mgo.ErrNotFound {
			return nil, errs.ErrEditionNotFound
		}
		return nil, err
	}
	return &edition, nil
}

func buildEditionQuery(id, editionID, state string) bson.M {
	var selector bson.M
	if state != "" {
		selector = bson.M{
			"current.links.dataset.id": id,
			"current.edition":          editionID,
			"current.state":            state,
		}
	} else {
		selector = bson.M{
			"next.links.dataset.id": id,
			"next.edition":          editionID,
		}
	}

	return selector
}

// GetNextVersion retrieves the latest version for an edition of a dataset
func (m *Mongo) GetNextVersion(datasetID, edition string) (int, error) {
	s := m.Session.Copy()
	defer s.Close()
	var version models.Version
	var nextVersion int

	selector := bson.M{
		"links.dataset.id": datasetID,
		"edition":          edition,
	}

	// Results are sorted in reverse order to get latest version
	err := s.DB(m.Database).C("instances").Find(selector).Sort("-version").One(&version)
	if err != nil {
		if err == mgo.ErrNotFound {
			return 1, nil
		}
		return nextVersion, err
	}

	nextVersion = version.Version + 1

	return nextVersion, nil
}

// GetVersions retrieves all version documents for a dataset edition
func (m *Mongo) GetVersions(ctx context.Context, id, editionID, state string) (*models.VersionResults, error) {
	s := m.Session.Copy()
	defer s.Close()

	selector := buildVersionsQuery(id, editionID, state)

	iter := s.DB(m.Database).C("instances").Find(selector).Iter()
	defer func() {
		err := iter.Close()
		if err != nil {
			log.Event(ctx, "error closing instance iterator", log.ERROR, log.Error(err), log.Data{"selector": selector})
		}
	}()

	var results []models.Version
	if err := iter.All(&results); err != nil {
		if err == mgo.ErrNotFound {
			return nil, errs.ErrVersionNotFound
		}
		return nil, err
	}

	if len(results) < 1 {
		return nil, errs.ErrVersionNotFound
	}

	for i := 0; i < len(results); i++ {

		results[i].Links.Self.HRef = results[i].Links.Version.HRef
	}

	return &models.VersionResults{Items: results}, nil
}

func buildVersionsQuery(id, editionID, state string) bson.M {
	var selector bson.M
	if state == "" {
		selector = bson.M{
			"links.dataset.id": id,
			"edition":          editionID,
			"$or": []interface{}{
				bson.M{"state": models.EditionConfirmedState},
				bson.M{"state": models.AssociatedState},
				bson.M{"state": models.PublishedState},
			},
		}
	} else {
		selector = bson.M{
			"links.dataset.id": id,
			"edition":          editionID,
			"state":            state,
		}
	}

	return selector
}

// GetVersion retrieves a version document for a dataset edition
func (m *Mongo) GetVersion(id, editionID, versionID, state string) (*models.Version, error) {
	s := m.Session.Copy()
	defer s.Close()

	versionNumber, err := strconv.Atoi(versionID)
	if err != nil {
		return nil, err
	}
	selector := buildVersionQuery(id, editionID, state, versionNumber)

	var version models.Version
	err = s.DB(m.Database).C("instances").Find(selector).One(&version)
	if err != nil {
		if err == mgo.ErrNotFound {
			return nil, errs.ErrVersionNotFound
		}
		return nil, err
	}
	return &version, nil
}

func buildVersionQuery(id, editionID, state string, versionID int) bson.M {
	var selector bson.M
	if state != models.PublishedState {
		selector = bson.M{
			"links.dataset.id": id,
			"version":          versionID,
			"edition":          editionID,
		}
	} else {
		selector = bson.M{
			"links.dataset.id": id,
			"edition":          editionID,
			"version":          versionID,
			"state":            state,
		}
	}

	return selector
}

// CheckDatasetExists checks that the dataset exists
func (m *Mongo) CheckDatasetExists(id, state string) error {
	s := m.Session.Copy()
	defer s.Close()

	var query bson.M
	if state == "" {
		query = bson.M{
			"_id": id,
		}
	} else {
		query = bson.M{
			"_id":           id,
			"current.state": state,
		}
	}

	count, err := s.DB(m.Database).C("datasets").Find(query).Count()
	if err != nil {
		return err
	}

	if count == 0 {
		return errs.ErrDatasetNotFound
	}

	return nil
}

// CheckEditionExists checks that the edition of a dataset exists
func (m *Mongo) CheckEditionExists(id, editionID, state string) error {
	s := m.Session.Copy()
	defer s.Close()

	var query bson.M
	if state == "" {
		query = bson.M{
			"next.links.dataset.id": id,
			"next.edition":          editionID,
		}
	} else {
		query = bson.M{
			"current.links.dataset.id": id,
			"current.edition":          editionID,
			"current.state":            state,
		}
	}

	count, err := s.DB(m.Database).C(editionsCollection).Find(query).Count()
	if err != nil {
		return err
	}

	if count == 0 {
		return errs.ErrEditionNotFound
	}

	return nil
}

// Ping the mongodb database
func (m *Mongo) Ping(ctx context.Context) (time.Time, error) {
	if time.Since(m.lastPingTime) < 1*time.Second {
		return m.lastPingTime, m.lastPingResult
	}

	s := m.Session.Copy()
	defer s.Close()

	m.lastPingTime = time.Now()
	pingDoneChan := make(chan error)
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		log.Event(ctx, "db ping", log.INFO)
		err := s.Ping()
		if err != nil {
			log.Event(ctx, "Ping mongo", log.ERROR, log.Error(err))
		}
		pingDoneChan <- err
		wg.Done()
	}()

	go func() {
		wg.Wait()
		close(pingDoneChan)
	}()

	select {
	case err := <-pingDoneChan:
		m.lastPingResult = err
	case <-ctx.Done():
		m.lastPingResult = ctx.Err()
	}
	return m.lastPingTime, m.lastPingResult
}
