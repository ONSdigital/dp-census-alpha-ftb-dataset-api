package main

import (
	"context"
	"errors"
	"flag"
	"os"

	"github.com/ONSdigital/log.go/log"
	"gopkg.in/mgo.v2"
)

const (
	database            = "ftb-datasets"
	datasetCollection   = "datasets"
	editionCollection   = "editions"
	versionCollection   = "instances"
	dimOptionCollection = "dimension.options"

	defaultBindAddr = "localhost:27017"
)

var bindAddr string

// Mongo represents a simplistic MongoDB configuration.
type Mongo struct {
	Database string
	Session  *mgo.Session
	URI      string
}

func main() {

	flag.StringVar(&bindAddr, "mongodb-bind", defaultBindAddr, "the address including authorisation if needed to bind to mongo database")
	flag.Parse()

	ctx := context.Background()

	if bindAddr == "" {
		bindAddr = defaultBindAddr
	}

	log.Event(ctx, "script variables", log.INFO, log.Data{"mongodb_bind_addr": bindAddr})

	mongo := Mongo{
		Database: database,
		URI:      bindAddr,
	}

	session, err := mongo.Init()
	if err != nil {
		log.Event(ctx, "unable to connect to mongo database", log.ERROR, log.Error(err), log.Data{"mongodb-bind-addr": bindAddr})
		os.Exit(1)
	}

	mongo.Session = session

	var hasFailed bool
	// Delete all dimension options
	if err := mongo.RemoveAllDocuments(dimOptionCollection); err != nil {
		log.Event(ctx, "failed to remove all documents", log.WARN, log.Error(err), log.Data{"collection": dimOptionCollection})
		hasFailed = true
	}

	// Delete all versions
	if err := mongo.RemoveAllDocuments(versionCollection); err != nil {
		log.Event(ctx, "failed to remove all documents", log.WARN, log.Error(err), log.Data{"collection": versionCollection})
		hasFailed = true
	}

	// Delete all editions
	if err := mongo.RemoveAllDocuments(editionCollection); err != nil {
		log.Event(ctx, "failed to remove all documents", log.WARN, log.Error(err), log.Data{"collection": editionCollection})
		hasFailed = true
	}

	// Delete all datasets
	if err := mongo.RemoveAllDocuments(datasetCollection); err != nil {
		log.Event(ctx, "failed to remove all documents", log.WARN, log.Error(err), log.Data{"collection": datasetCollection})
		hasFailed = true
	}

	if hasFailed {
		log.Event(ctx, "Documents has failed to be removed from one or more collections - see previous warning messages",
			log.ERROR, log.Error(errors.New("script failed to remove all doccuments from one or more collections")))
		os.Exit(1)
	}

	log.Event(ctx, "Successfully removed all documents from ftb collections", log.INFO)
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

// RemoveAllDocuments deletes all documents from a collection
func (m *Mongo) RemoveAllDocuments(collection string) (err error) {
	s := m.Session.Copy()
	defer s.Close()

	_, err = s.DB(m.Database).C(collection).RemoveAll(nil)
	return
}
