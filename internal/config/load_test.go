package config_test

import (
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/you/manooch/gen/manoochv1"
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

func copyTree(t *testing.T, source, destination string) {
	t.Helper()
	err := filepath.WalkDir(source, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		target := filepath.Join(destination, relative)
		if entry.IsDir() {
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
		t.Fatalf("copy %s: %v", source, err)
	}
}

func TestLoadValid(t *testing.T) {
	configuration, err := config.Load(assemble(t, ""), "BINANCE")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if configuration.Venue != "BINANCE" || !configuration.Enabled {
		t.Errorf("venue = %q enabled = %v", configuration.Venue, configuration.Enabled)
	}
	if got := configuration.Service.HTTP.Listen; got != "127.0.0.1:9101" {
		t.Errorf("listen = %q", got)
	}
	if got := configuration.Redis.DialTimeout.Standard().String(); got != "2s" {
		t.Errorf("dial_timeout = %q", got)
	}
	// cadence 1s * ttl_multiplier 3.
	if got := configuration.TimeToLive(manoochv1.Channel_CHANNEL_FUNDING); got.String() != "3s" {
		t.Errorf("TTL(funding) = %v, want 3s", got)
	}
	if got := configuration.TimeToLiveByChannel(); len(got) != 3 {
		t.Errorf("TTLs() = %v, want one entry per configured channel", got)
	}

	// symbol_overrides is carried through untouched. What a venue calls an
	// instrument is the adapter's answer, not this package's: the rule differs
	// per venue and a fallback here would be right for at most one of them.
	if got := configuration.SymbolOverrides["BTC_USDT"]; got != "BTCUSDT" {
		t.Errorf("symbol_overrides[BTC_USDT] = %q", got)
	}

	if len(configuration.Instruments) != 1 {
		t.Fatalf("instruments = %d, want 1", len(configuration.Instruments))
	}
	perp := configuration.Instruments[0]
	if perp.ResolvedMarketType != manoochv1.MarketType_MARKET_TYPE_PERP_LINEAR {
		t.Errorf("instruments[0].ResolvedMarketType = %v", perp.ResolvedMarketType)
	}
	wantChannels := []manoochv1.Channel{
		manoochv1.Channel_CHANNEL_MARK_PRICE, manoochv1.Channel_CHANNEL_INDEX_PRICE,
		manoochv1.Channel_CHANNEL_FUNDING,
	}
	if len(perp.ResolvedChannels) != len(wantChannels) {
		t.Fatalf("instruments[0].ResolvedChannels = %v", perp.ResolvedChannels)
	}
	for i, channel := range wantChannels {
		if perp.ResolvedChannels[i] != channel {
			t.Errorf("instruments[0].ResolvedChannels[%d] = %v, want %v", i, perp.ResolvedChannels[i], channel)
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

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		t.Run(entry.Name(), func(t *testing.T) {
			dir := assemble(t, entry.Name())
			configuration, err := config.Load(dir, "BINANCE")
			if err == nil {
				t.Fatalf("Load succeeded, want an error (got venue %q)", configuration.Venue)
			}

			got := strings.ReplaceAll(err.Error(), dir, "<config>")
			goldenPath := filepath.Join("testdata", "invalid", entry.Name(), "error.golden")
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
