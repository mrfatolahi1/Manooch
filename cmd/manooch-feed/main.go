// Command manooch-feed is the venue daemon: it connects to one exchange,
// normalizes what it sees, and publishes to Redis.
//
// It supervises rather than exits. A dropped socket redials with jittered
// backoff behind a circuit breaker, a failed stream relaunches on its own, and
// an expired key is served over REST until the socket comes back. The only
// thing that stops the process is a signal.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/you/manooch/internal/adapter"
	"github.com/you/manooch/internal/config"
	"github.com/you/manooch/internal/observability"
	"github.com/you/manooch/internal/publish"
	"gopkg.in/yaml.v3"
)

// shutdownDeadline bounds graceful shutdown; past it we report what is still
// running rather than hanging a container restart.
const shutdownDeadline = 10 * time.Second

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "manooch-feed: %v\n", err)
		os.Exit(1)
	}
}

// flags is the parsed command line.
type flags struct {
	exchange  string
	configDir string
	validate  bool
}

func parseFlags() (flags, error) {
	var flags flags
	flag.StringVar(&flags.exchange, "exchange", "", "venue to run, upper case (required)")
	flag.StringVar(&flags.configDir, "config", "./config", "directory holding defaults.yaml and venues/")
	flag.BoolVar(&flags.validate, "validate", false, "load and validate config, print the resolved config, exit")
	flag.Parse()

	if flags.exchange == "" {
		return flags, errors.New("--exchange is required")
	}
	if flags.exchange != strings.ToUpper(flags.exchange) {
		return flags, fmt.Errorf("--exchange must be upper case, got %q", flags.exchange)
	}
	return flags, nil
}

func run() error {
	flags, err := parseFlags()
	if err != nil {
		return err
	}

	configuration, err := config.Load(flags.configDir, flags.exchange)
	if err != nil {
		return err
	}

	// Opens nothing — no Redis connection, no listener — so it is safe to run
	// against production config from anywhere.
	if flags.validate {
		return printResolved(os.Stdout, configuration, flags.configDir)
	}

	logger, err := observability.NewLogger(os.Stdout, configuration.Service.LogLevel, configuration.Venue)
	if err != nil {
		return err
	}
	metrics := observability.NewMetrics()

	// Once per process. publish_seq restarts at zero on every start, so without
	// this a consumer cannot tell a restart from messages dropped on the bus.
	instanceID := uuid.NewString()
	started := time.Now()

	logger.Info("starting",
		"instance_id", instanceID,
		"config_dir", flags.configDir,
		"enabled", configuration.Enabled,
		"streams", len(configuration.Streams()))

	// Resolved before anything is opened, so an unknown venue or an unservable
	// stream fails with no Redis connection and no bound port behind it.
	producerSet, err := planProducers(configuration, logger)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	publisher, err := dialRedis(ctx, configuration, instanceID, metrics, logger)
	if err != nil {
		return err
	}
	logger.Info("redis connected", "addr", configuration.Redis.Addr, "db", configuration.Redis.Database)

	server, err := serveAdmin(configuration, metrics, instanceID, started, logger)
	if err != nil {
		publisher.Close()
		return err
	}

	runningProducers, err := producerSet.start(ctx, configuration, publisher, metrics, logger)
	if err != nil {
		shutdown(server, publisher, &sync.WaitGroup{}, configuration, metrics, logger)
		return err
	}

	<-ctx.Done()
	logger.Info("shutting down")
	shutdown(server, publisher, runningProducers, configuration, metrics, logger)
	logger.Info("stopped")
	return nil
}

// dialRedis connects the publisher, bounded by the configured dial timeout.
// Redis is not optional: a feed that cannot publish has nothing to do.
func dialRedis(ctx context.Context, configuration *config.Config, instanceID string, metrics *observability.Metrics, logger *slog.Logger) (*publish.RedisPublisher, error) {
	dialCtx, cancel := context.WithTimeout(ctx, configuration.Redis.DialTimeout.Standard())
	defer cancel()

	return publish.NewRedis(dialCtx, publish.Options{
		Addr:          configuration.Redis.Addr,
		Database:      configuration.Redis.Database,
		DialTimeout:   configuration.Redis.DialTimeout.Standard(),
		ReadTimeout:   configuration.Redis.ReadTimeout.Standard(),
		PoolSize:      configuration.Redis.PoolSize,
		Venue:         configuration.Venue,
		InstanceID:    instanceID,
		SchemaVersion: configuration.Publish.SchemaVersion,
		Metrics:       metrics,
		Logger:        logger,
	})
}

// serveAdmin binds the admin surface and serves it in the background, returning
// a nil server when HTTP is disabled.
//
// The listener is opened synchronously: otherwise a port clash is a log line in
// a process that keeps running with no metrics and no /healthz.
func serveAdmin(configuration *config.Config, metrics *observability.Metrics, instanceID string, started time.Time, logger *slog.Logger) (*http.Server, error) {
	if !configuration.Service.HTTP.Enabled {
		return nil, nil
	}

	listener, err := net.Listen("tcp", configuration.Service.HTTP.Listen)
	if err != nil {
		return nil, fmt.Errorf("http listen: %w", err)
	}
	server := &http.Server{
		Handler:           newMux(metrics, configuration.Venue, instanceID, started),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server stopped", "error", err.Error())
		}
	}()
	logger.Info("http listening", "addr", listener.Addr().String())
	return server, nil
}

// shutdown stops everything under one deadline. Redis closes last, after the
// producers have drained, so their in-flight publishes do not fail on the way
// out and log an error that means nothing.
func shutdown(server *http.Server, publisher *publish.RedisPublisher, producers *sync.WaitGroup, configuration *config.Config, metrics *observability.Metrics, logger *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), shutdownDeadline)
	defer cancel()

	if server != nil {
		if err := server.Shutdown(ctx); err != nil {
			logger.Error("http shutdown", "error", err.Error())
		}
	}

	done := make(chan struct{})
	go func() { producers.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		// A goroutine past the deadline is holding something the next start
		// will contend with. Counted rather than set: the health tracker owns
		// this gauge and may already have leaks of its own on it.
		logger.Error("shutdown deadline exceeded, goroutines still running")
		metrics.LeakedGoroutines.WithLabelValues(configuration.Venue).Inc()
	}

	if err := publisher.Close(); err != nil {
		logger.Error("redis close", "error", err.Error())
	}
}

// printResolved writes the merged config and the exact set of Redis keys it
// implies. The key list is where a wrong symbol or a channel on the wrong
// market type becomes obvious before anything runs.
//
// The venue symbols come from the adapter rather than from a rule in the config
// package. Only the adapter knows them — BTC_USDT is BTCUSDT on Binance and
// XBTUSDTM on KuCoin — and a printout that guessed would be an operator
// checking their config against a symbol nothing will ever subscribe to.
// Building the adapter opens nothing, so this stays safe to run anywhere.
func printResolved(file *os.File, configuration *config.Config, dir string) error {
	out, err := yaml.Marshal(configuration)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(file, "# resolved configuration for %s from %s\n\n%s", configuration.Venue, dir, out); err != nil {
		return err
	}

	venueAdapter, err := adapter.New(configuration, adapter.Dependencies{})
	if err != nil {
		return err
	}
	specifications, err := adapter.Specifications(configuration)
	if err != nil {
		return err
	}

	fmt.Fprintf(file, "\n# %d streams\n", len(specifications))
	for _, specification := range specifications {
		venueSymbol, err := venueAdapter.VenueSymbol(specification.Instrument)
		if err != nil {
			return err
		}
		fmt.Fprintf(file, "# %-52s venue_symbol=%s\n",
			publish.Key(configuration.Venue, specification.Instrument.MarketType, specification.Instrument.Canonical(), specification.Channel),
			venueSymbol)
	}
	return nil
}
