package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/ONSdigital/dp-census-alpha-ftb-dataset-api/api"
	"github.com/ONSdigital/dp-census-alpha-ftb-dataset-api/config"
	"github.com/ONSdigital/dp-census-alpha-ftb-dataset-api/mongo"
	"github.com/ONSdigital/dp-census-alpha-ftb-dataset-api/store"
	"github.com/ONSdigital/dp-census-alpha-ftb-dataset-api/url"
	mongolib "github.com/ONSdigital/dp-mongodb"
	"github.com/ONSdigital/log.go/log"
)

// check that DatsetAPIStore satifies the the store.Storer interface
var _ store.Storer = (*DatsetAPIStore)(nil)

//DatsetAPIStore is a wrapper which embeds Neo4j Mongo structs which between them satisfy the store.Storer interface.
type DatsetAPIStore struct {
	*mongo.Mongo
}

func main() {
	log.Namespace = "dp-ftb-dataset-api"
	ctx := context.Background()

	if err := run(ctx); err != nil {
		log.Event(ctx, "application unexpectedly failed", log.ERROR, log.Error(err))
		os.Exit(1)
	}

	os.Exit(0)
}

func run(ctx context.Context) error {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	cfg, err := config.Get()
	if err != nil {
		log.Event(ctx, "failed to retrieve configuration", log.FATAL, log.Error(err))
		return err
	}

	log.Event(ctx, "config on startup", log.INFO, log.Data{"config": cfg})

	mongodb := &mongo.Mongo{
		CodeListURL: cfg.CodeListAPIURL,
		Collection:  cfg.MongoConfig.Collection,
		Database:    cfg.MongoConfig.Database,
		DatasetURL:  cfg.FTBDatasetAPIURL,
		URI:         cfg.MongoConfig.BindAddr,
	}

	session, err := mongodb.Init()
	if err != nil {
		log.Event(ctx, "failed to initialise mongo", log.ERROR, log.Error(err))
		return err
	} else {
		mongodb.Session = session
		log.Event(ctx, "listening to mongo db session", log.INFO, log.Data{
			"bind_address": cfg.BindAddr,
		})
	}

	store := store.DataStore{Backend: DatsetAPIStore{mongodb}}

	apiErrors := make(chan error, 1)

	urlBuilder := url.NewBuilder(cfg.WebsiteURL)

	api.CreateAndInitialiseFTBDatasetAPI(ctx, *cfg, store, urlBuilder, apiErrors)

	// block until a fatal error occurs
	select {
	case err := <-apiErrors:
		log.Event(ctx, "api error received", log.ERROR, log.Error(err))
	case <-signals:
		log.Event(ctx, "os signal received", log.INFO)
	}

	log.Event(ctx, fmt.Sprintf("shutdown with timeout: %s", cfg.GracefulShutdownTimeout), log.INFO)
	shutdownContext, cancel := context.WithTimeout(context.Background(), cfg.GracefulShutdownTimeout)

	// track shutdown gracefully closes app
	var gracefulShutdown bool

	// Gracefully shutdown the application closing any open resources.
	go func() {
		defer cancel()
		var hasShutdownError bool

		// stop any incoming requests before closing any outbound connections
		api.Close(shutdownContext)

		if err = mongolib.Close(ctx, mongodb.Session); err != nil {
			log.Event(shutdownContext, "failed to close mongo db session", log.ERROR, log.Error(err))
			hasShutdownError = true
		}

		if !hasShutdownError {
			gracefulShutdown = true
		}
	}()

	// wait for shutdown success (via cancel) or failure (timeout)
	<-shutdownContext.Done()

	if !gracefulShutdown {
		err = errors.New("failed to shutdown gracefully")
		log.Event(shutdownContext, "failed to shutdown gracefully ", log.ERROR, log.Error(err))
		return err
	}

	log.Event(shutdownContext, "graceful shutdown was successful", log.INFO)

	return nil
}
