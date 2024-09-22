package app

import (
	"context"
	"errors"
	"fmt"

	"github.com/ramisoul84/kfc-order/internal/config"
	"github.com/ramisoul84/kfc-order/internal/repository"
	"github.com/ramisoul84/kfc-order/internal/service"
	grpcTransport "github.com/ramisoul84/kfc-order/internal/transport/grpc"
	httpTransport "github.com/ramisoul84/kfc-order/internal/transport/http"
	"github.com/ramisoul84/kfc-order/migrations"
	"github.com/ramisoul84/kfc-order/pkg/database"
	"github.com/ramisoul84/kfc-order/pkg/kafka"
	"github.com/ramisoul84/kfc-order/pkg/logger"
	"github.com/ramisoul84/kfc-order/pkg/migrate"
	"github.com/ramisoul84/kfc-order/pkg/outbox"
)

// App wires together all application components.
type App struct {
	config        *config.Config
	logger        *logger.Logger
	db            *database.Postgres
	grpcServer    *grpcTransport.Transport
	healthServer  *httpTransport.HealthServer
	kafkaProducer *kafka.Producer
	relay         *outbox.Relay
}

// New creates and wires the application.
func New(cfg *config.Config) (*App, error) {
	log := logger.New(&logger.Config{
		Level:    cfg.Logger.Level,
		Format:   cfg.Logger.Format,
		Output:   cfg.Logger.Output,
		FilePath: cfg.Logger.FilePath,
		Service:  cfg.Logger.Service,
	})

	log.Info("starting application",
		"name", cfg.App.Name,
		"version", cfg.App.Version,
		"environment", cfg.App.Environment,
		"timezone", cfg.App.Timezone,
	)

	// Postgres
	db, err := database.NewPostgres(&cfg.DB, cfg.DSN())
	if err != nil {
		return nil, fmt.Errorf("database: %w", err)
	}
	log.Info("postgres connected", "database", cfg.DB.Name)

	if cfg.DB.AutoMigrate {
		applied, err := migrate.Up(context.Background(), db.DB, migrations.FS)
		if err != nil {
			db.Close()
			return nil, fmt.Errorf("migrate: %w", err)
		}
		log.Info("migrations up to date", "applied", applied)
	}

	// Kafka via the transactional outbox: order events are written with
	// the order change and relayed from there.
	var kafkaProducer *kafka.Producer
	var relay *outbox.Relay
	notify := func() {}
	if len(cfg.Kafka.Brokers) > 0 {
		kafkaProducer = kafka.NewProducer(cfg.Kafka.Brokers, log)
		relay = outbox.NewRelay(db.DB, kafkaProducer, outboxLockID, log)
		notify = relay.Notify
		log.Info("outbox relay initialized",
			"brokers", cfg.Kafka.Brokers,
			"topic", cfg.Kafka.OrdersTopic,
		)
	} else {
		log.Warn("kafka disabled; order events accumulate in the outbox until it is enabled")
	}

	// Layers
	orderRepo := repository.NewOrderRepository(db.DB, cfg.Kafka.OrdersTopic, notify)
	orderService := service.NewOrderService(orderRepo, cfg.Location(), log)
	orderServer := grpcTransport.NewOrderServer(orderService, log)

	return &App{
		config:        cfg,
		logger:        log,
		db:            db,
		grpcServer:    grpcTransport.NewTransport(cfg, log, orderServer),
		healthServer:  httpTransport.NewHealthServer(cfg.HTTP.Port, db.DB),
		kafkaProducer: kafkaProducer,
		relay:         relay,
	}, nil
}

// outboxLockID elects one relay among this service's replicas.
const outboxLockID = 7_310_455_201

// Start serves gRPC and the health endpoint until ctx is cancelled or a
// transport fails.
func (a *App) Start(ctx context.Context) error {
	errCh := make(chan error, 2)

	go func() {
		if err := a.grpcServer.Start(); err != nil {
			errCh <- fmt.Errorf("grpc: %w", err)
		}
	}()
	go func() {
		a.logger.Info("health server listening", "addr", ":"+a.config.HTTP.Port)
		if err := a.healthServer.Start(); err != nil {
			errCh <- fmt.Errorf("http: %w", err)
		}
	}()

	if a.relay != nil {
		go a.relay.Run(ctx)
	}

	select {
	case <-ctx.Done():
		a.logger.Info("shutdown signal received")
		return nil
	case err := <-errCh:
		a.logger.Error("transport failed", "error", err)
		return err
	}
}

// Shutdown gracefully stops all transports and releases resources.
func (a *App) Shutdown(ctx context.Context) error {
	a.logger.Info("shutting down application")

	var errs []error
	if err := a.grpcServer.Shutdown(ctx); err != nil {
		errs = append(errs, fmt.Errorf("grpc shutdown: %w", err))
	}
	if err := a.healthServer.Shutdown(ctx); err != nil {
		errs = append(errs, fmt.Errorf("http shutdown: %w", err))
	}
	if a.kafkaProducer != nil {
		if err := a.kafkaProducer.Close(); err != nil {
			errs = append(errs, fmt.Errorf("kafka producer: %w", err))
		}
	}
	if a.db != nil {
		_ = a.db.Close()
	}

	a.logger.Info("shutdown complete")
	return errors.Join(errs...)
}
