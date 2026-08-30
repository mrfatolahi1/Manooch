// Command manooch-feed is the venue daemon: it connects to one exchange,
// normalizes what it sees, and publishes to Redis.
//
// There are no adapters yet. Without --synthetic the process starts, proves it
// can reach Redis, serves /healthz and idles, and says so on startup.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	pb "github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/config"
	"github.com/you/manooch/internal/obs"
	"github.com/you/manooch/internal/publish"
	"github.com/you/manooch/internal/synth"
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

func run() error {
	var (
		exchange  = flag.String("exchange", "", "venue to run, upper case (required)")
		configDir = flag.String("config", "./config", "directory holding defaults.yaml and venues/")
		validate  = flag.Bool("validate", false, "load and validate config, print the resolved config, exit")
		synthetic = flag.Bool("synthetic", false, "dev only: publish generated data instead of connecting to a venue")
	)
	flag.Parse()

	if *exchange == "" {
		return errors.New("--exchange is required")
	}
	if *exchange != strings.ToUpper(*exchange) {
		return fmt.Errorf("--exchange must be upper case, got %q", *exchange)
	}

	cfg, err := config.Load(*configDir, *exchange)
	if err != nil {
		return err
	}

	// Opens nothing — no Redis connection, no listener — so it is safe to run
	// against production config from anywhere.
	if *validate {
		return printResolved(os.Stdout, cfg, *configDir)
	}

	logger, err := obs.NewLogger(os.Stdout, cfg.Service.LogLevel, cfg.Venue)
	if err != nil {
		return err
	}
	metrics := obs.NewMetrics()

	// Once per process. publish_seq restarts at zero on every start, so without
	// this a consumer cannot tell a restart from messages dropped on the bus.
	instanceID := uuid.NewString()
	started := time.Now()

	logger.Info("starting",
		"instance_id", instanceID,
		"config_dir", *configDir,
		"enabled", cfg.Enabled,
		"synthetic", *synthetic,
		"streams", len(cfg.Streams()))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dialCtx, cancelDial := context.WithTimeout(ctx, cfg.Redis.DialTimeout.Std())
	pub, err := publish.NewRedis(dialCtx, publish.Options{
		Addr:          cfg.Redis.Addr,
		DB:            cfg.Redis.DB,
		DialTimeout:   cfg.Redis.DialTimeout.Std(),
		ReadTimeout:   cfg.Redis.ReadTimeout.Std(),
		PoolSize:      cfg.Redis.PoolSize,
		Venue:         cfg.Venue,
		InstanceID:    instanceID,
		SchemaVersion: cfg.Publish.SchemaVersion,
		Metrics:       metrics,
		Logger:        logger,
	})
	cancelDial()
	if err != nil {
		return err
	}
	defer pub.Close()
	logger.Info("redis connected", "addr", cfg.Redis.Addr, "db", cfg.Redis.DB)

	var srv *http.Server
	if cfg.Service.HTTP.Enabled {
		// Bind synchronously: otherwise a port clash is a log line in a process
		// that keeps running with no metrics and no /healthz.
		ln, err := net.Listen("tcp", cfg.Service.HTTP.Listen)
		if err != nil {
			return fmt.Errorf("http listen: %w", err)
		}
		srv = &http.Server{
			Handler:           newMux(metrics, cfg.Venue, instanceID, started),
			ReadHeaderTimeout: 5 * time.Second,
		}
		go func() {
			if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Error("http server stopped", "error", err.Error())
			}
		}()
		logger.Info("http listening", "addr", ln.Addr().String())
	}

	var wg sync.WaitGroup
	if *synthetic {
		logger.Warn("synthetic mode: publishing generated data, not venue data")
		gen := synth.New(cfg, pub, logger)
		wg.Add(1)
		go func() {
			defer wg.Done()
			gen.Run(ctx)
		}()
	} else {
		logger.Info("no venue adapter in this build: serving /healthz and idling until M1")
	}

	<-ctx.Done()
	logger.Info("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownDeadline)
	defer cancel()

	if srv != nil {
		if err := srv.Shutdown(shutdownCtx); err != nil {
			logger.Error("http shutdown", "error", err.Error())
		}
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-shutdownCtx.Done():
		// A goroutine past the deadline is holding something the next start
		// will contend with.
		logger.Error("shutdown deadline exceeded, goroutines still running")
		metrics.LeakedGoroutines.WithLabelValues(cfg.Venue).Set(1)
	}

	if err := pub.Close(); err != nil {
		logger.Error("redis close", "error", err.Error())
	}
	logger.Info("stopped")
	return nil
}

// printResolved writes the merged config and the exact set of Redis keys it
// implies. The key list is where a wrong symbol or a channel on the wrong
// market type becomes obvious before anything runs.
func printResolved(w *os.File, cfg *config.Config, dir string) error {
	out, err := yaml.Marshal(cfg)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "# resolved configuration for %s from %s\n\n%s", cfg.Venue, dir, out); err != nil {
		return err
	}

	streams := cfg.Streams()
	fmt.Fprintf(w, "\n# %d streams\n", len(streams))
	for _, s := range streams {
		fmt.Fprintf(w, "# %-52s venue_symbol=%s",
			publish.Key(cfg.Venue, s.MarketType, s.Symbol, s.Channel), s.VenueSymbol)
		if s.Channel == pb.Channel_CHANNEL_ORDERBOOK {
			fmt.Fprintf(w, " depth=%d", s.BookDepth)
		}
		fmt.Fprintln(w)
	}
	return nil
}
