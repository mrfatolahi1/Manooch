package main

import (
	"encoding/json"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/you/manooch/internal/observability"
)

// newMux builds the admin surface.
//
// There is no market-data endpoint and will not be one: consumers read Redis,
// and a second path to the same data would be a second contract that disagrees
// with the first the moment either changes.
func newMux(metrics *observability.Metrics, venue, instanceID string, started time.Time) *http.ServeMux {
	router := http.NewServeMux()

	// Liveness, not readiness: it answers ok whenever the process is serving,
	// including while the venue is unreachable and nothing is being published.
	// The architecture does not restart a process because a venue is down, so a
	// probe that went red would only invite churn. Whether a venue is actually
	// publishing is what the health keys say.
	router.HandleFunc("GET /healthz", func(response http.ResponseWriter, request *http.Request) {
		body := struct {
			Status        string  `json:"status"`
			Venue         string  `json:"venue"`
			InstanceID    string  `json:"instance_id"`
			UptimeSeconds float64 `json:"uptime_seconds"`
			Uptime        string  `json:"uptime"`
		}{
			Status:        "ok",
			Venue:         venue,
			InstanceID:    instanceID,
			UptimeSeconds: time.Since(started).Seconds(),
			Uptime:        time.Since(started).Truncate(time.Second).String(),
		}
		response.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(response).Encode(body)
	})

	router.Handle("GET /metrics", metrics.Handler())

	router.HandleFunc("/debug/pprof/", pprof.Index)
	router.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	router.HandleFunc("/debug/pprof/profile", pprof.Profile)
	router.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	router.HandleFunc("/debug/pprof/trace", pprof.Trace)

	return router
}
