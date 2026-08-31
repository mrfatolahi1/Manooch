package config_test

import (
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	pb "github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/config"
)

var update = flag.Bool("update", false, "rewrite the error.golden files")

// assemble builds a config directory from testdata/valid, overlaying one
// invalid case. Each case carries only the file it breaks.
func assemble(t *testing.T, invalidCase string) string {
	t.Helper()
	dir := t.TempDir()

	copyTree(t, filepath.Join("testdata", "valid"), dir)
	if invalidCase != "" {
		copyTree(t, filepath.Join("testdata", "invalid", invalidCase), dir)
	}
	return dir
}

func copyTree(t *testing.T, src, dst string) {
	t.Helper()
	err := filepath.WalkDir(src, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		if filepath.Ext(path) != ".yaml" {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, b, 0o644)
	})
	if err != nil {
		t.Fatalf("copy %s: %v", src, err)
	}
}

func TestLoadValid(t *testing.T) {
	cfg, err := config.Load(assemble(t, ""), "BINANCE")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.Venue != "BINANCE" || !cfg.Enabled {
		t.Errorf("venue = %q enabled = %v", cfg.Venue, cfg.Enabled)
	}
	if got := cfg.Service.HTTP.Listen; got != "127.0.0.1:9101" {
		t.Errorf("listen = %q", got)
	}
	if got := cfg.Redis.DialTimeout.Std().String(); got != "2s" {
		t.Errorf("dial_timeout = %q", got)
	}
	// cadence 1s * ttl_multiplier 3.
	if got := cfg.TTL(pb.Channel_CHANNEL_FUNDING); got.String() != "3s" {
		t.Errorf("TTL(funding) = %v, want 3s", got)
	}
	if got := cfg.TTLs(); len(got) != 3 {
		t.Errorf("TTLs() = %v, want one entry per configured channel", got)
	}

	// symbol_overrides wins; everything else strips the separator.
	if got := cfg.VenueSymbol("BTC_USDT"); got != "BTCUSDT" {
		t.Errorf("VenueSymbol(BTC_USDT) = %q", got)
	}
	if got := cfg.VenueSymbol("SOL_USDT"); got != "SOLUSDT" {
		t.Errorf("VenueSymbol(SOL_USDT) = %q", got)
	}

	if len(cfg.Instruments) != 1 {
		t.Fatalf("instruments = %d, want 1", len(cfg.Instruments))
	}
	perp := cfg.Instruments[0]
	if perp.MT != pb.MarketType_MARKET_TYPE_PERP_LINEAR {
		t.Errorf("instruments[0].MT = %v", perp.MT)
	}
	wantChans := []pb.Channel{
		pb.Channel_CHANNEL_MARK_PRICE, pb.Channel_CHANNEL_INDEX_PRICE,
		pb.Channel_CHANNEL_FUNDING,
	}
	if len(perp.Chans) != len(wantChans) {
		t.Fatalf("instruments[0].Chans = %v", perp.Chans)
	}
	for i, ch := range wantChans {
		if perp.Chans[i] != ch {
			t.Errorf("instruments[0].Chans[%d] = %v, want %v", i, perp.Chans[i], ch)
		}
	}
}

// TestLoadInvalid walks testdata/invalid; each subdirectory is one broken
// config and error.golden is the exact message an operator sees. Those messages
// are checked in because they are the interface to whoever fixes the config.
func TestLoadInvalid(t *testing.T) {
	entries, err := os.ReadDir(filepath.Join("testdata", "invalid"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) == 0 {
		t.Fatal("no invalid cases found")
	}

	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		t.Run(e.Name(), func(t *testing.T) {
			dir := assemble(t, e.Name())
			cfg, err := config.Load(dir, "BINANCE")
			if err == nil {
				t.Fatalf("Load succeeded, want an error (got venue %q)", cfg.Venue)
			}

			got := strings.ReplaceAll(err.Error(), dir, "<config>")
			goldenPath := filepath.Join("testdata", "invalid", e.Name(), "error.golden")
			if *update {
				if err := os.WriteFile(goldenPath, []byte(got+"\n"), 0o644); err != nil {
					t.Fatal(err)
				}
				return
			}

			want, err := os.ReadFile(goldenPath)
			if err != nil {
				t.Fatalf("%v (run: go test ./internal/config -update)", err)
			}
			if got != strings.TrimRight(string(want), "\n") {
				t.Errorf("error mismatch\n got: %s\nwant: %s", got, want)
			}

			// Whatever the rule, every line must name a config file.
			for _, line := range strings.Split(got, "\n") {
				if strings.HasPrefix(line, "  ") { // continuation of a yaml error
					continue
				}
				if !strings.HasPrefix(line, "<config>/") {
					t.Errorf("error line does not name a config file: %q", line)
				}
			}
		})
	}
}

func TestLoadMissingVenueFile(t *testing.T) {
	_, err := config.Load(assemble(t, ""), "KRAKEN")
	if err == nil {
		t.Fatal("Load of an unknown venue succeeded")
	}
	if !strings.Contains(err.Error(), filepath.Join("venues", "kraken.yaml")) {
		t.Errorf("error does not name the missing file: %v", err)
	}
}
