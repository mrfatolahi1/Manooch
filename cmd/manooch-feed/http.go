package main

import (
	"encoding/json"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/you/manooch/internal/obs"
)

// newMux builds the admin surface.
//
// There is no market-data endpoint and will not be one: consumers read Redis,
// and a second path to the same data would be a second contract that disagrees
// with the first the moment either changes.
func newMux(m *obs.Metrics, venue, instanceID string, started time.Time) *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
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
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(body)
	})

	mux.Handle("GET /metrics", m.Handler())

	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	return mux
}
