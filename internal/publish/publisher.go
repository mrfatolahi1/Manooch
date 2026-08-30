package publish

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	pb "github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/core"
	"github.com/you/manooch/internal/obs"
	"google.golang.org/protobuf/proto"
)

// A Publisher writes one message to one topic. ttl == 0 means the key never
// expires, for event-driven channels such as trades where silence is normal and
// liveness comes from the socket instead.
type Publisher interface {
	Publish(ctx context.Context, key string, msg proto.Message, ttl time.Duration) error
	Close() error
}

// enveloped is satisfied by every payload in the schema: each carries its
// Envelope in field 1.
type enveloped interface {
	proto.Message
	GetEnv() *pb.Envelope
}

// errLogInterval caps the write-error log rate: Redis being down is one fact,
// not one fact per message.
const errLogInterval = time.Second

// Options configures a RedisPublisher.
type Options struct {
	Addr        string
	DB          int
	DialTimeout time.Duration
	ReadTimeout time.Duration
	PoolSize    int

	Venue         string
	InstanceID    string
	SchemaVersion uint32

	Metrics *obs.Metrics
	Logger  *slog.Logger

	// Now is swappable for tests. Defaults to time.Now.
	Now func() time.Time
}

// RedisPublisher writes to Redis as a last-value cache plus a Pub/Sub fan-out.
type RedisPublisher struct {
	rdb  *redis.Client
	opts Options
	now  func() time.Time

	mu         sync.Mutex
	seq        map[string]uint64
	lastErrLog time.Time
}

var _ Publisher = (*RedisPublisher)(nil)

// NewRedis dials Redis and fails if it is not there. A feed that cannot publish
// has nothing to do, and starting anyway leaves consumers on a stale cache with
// no indication why.
func NewRedis(ctx context.Context, opts Options) (*RedisPublisher, error) {
	if opts.Logger == nil {
		return nil, fmt.Errorf("publish: no logger")
	}
	if opts.InstanceID == "" {
		return nil, fmt.Errorf("publish: no instance id")
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:         opts.Addr,
		DB:           opts.DB,
		DialTimeout:  opts.DialTimeout,
		ReadTimeout:  opts.ReadTimeout,
		WriteTimeout: opts.ReadTimeout,
		PoolSize:     opts.PoolSize,
	})
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("publish: redis %s db %d: %w", opts.Addr, opts.DB, err)
	}

	return &RedisPublisher{
		rdb:  rdb,
		opts: opts,
		now:  opts.Now,
		seq:  make(map[string]uint64),
	}, nil
}

// Publish stamps the envelope fields the publisher owns and writes the message
// in one pipelined round trip:
//
//	SET     <key> <bytes> PX <ttl_ms>
//	PUBLISH <key> <bytes>
//
// The SET lets a cold consumer read current state instead of waiting for the
// next tick, and makes freshness a property of the data: key present means
// fresh, key absent means stale, with no second timestamp to drift out of sync.
func (p *RedisPublisher) Publish(ctx context.Context, key string, msg proto.Message, ttl time.Duration) error {
	m, ok := msg.(enveloped)
	if !ok {
		return fmt.Errorf("publish %s: %T carries no envelope", key, msg)
	}
	env := m.GetEnv()
	if env == nil {
		return fmt.Errorf("publish %s: %T has a nil envelope", key, msg)
	}
	// Never publish without a status: a consumer that cannot tell healthy from
	// stale is worse off than one with no data.
	if env.Status == pb.Status_STATUS_UNSPECIFIED {
		return fmt.Errorf("publish %s: envelope status is unspecified", key)
	}

	p.mu.Lock()
	p.seq[key]++
	env.PublishSeq = p.seq[key]
	env.InstanceId = p.opts.InstanceID
	env.SchemaVersion = p.opts.SchemaVersion
	if env.Venue == "" {
		env.Venue = p.opts.Venue
	}
	// Set here and nowhere else: any earlier measures when we decided to
	// publish rather than when we did, which is the gap being looked for.
	publishTime := p.now()
	env.PublishTimeNs = publishTime.UnixNano()

	// Under the lock: env is a pointer into the caller's message, so a second
	// publish of the same message would race with the stamping above.
	b, marshalErr := proto.Marshal(msg)
	p.mu.Unlock()

	if marshalErr != nil {
		return fmt.Errorf("publish %s: marshal: %w", key, marshalErr)
	}

	pipe := p.rdb.Pipeline()
	if ttl > 0 {
		ms := ttl.Milliseconds()
		if ms == 0 {
			ms = 1 // a sub-millisecond TTL would round to PX 0, which Redis rejects
		}
		pipe.Do(ctx, "SET", key, b, "PX", ms)
	} else {
		// No expiry: liveness for this channel comes from elsewhere.
		pipe.Do(ctx, "SET", key, b)
	}
	pipe.Do(ctx, "PUBLISH", key, b)

	if _, err := pipe.Exec(ctx); err != nil {
		p.onWriteError(key, err)
		return fmt.Errorf("publish %s: %w", key, err)
	}

	p.observe(env, publishTime)
	return nil
}

// observe records metrics for a successful publish. Labels come from the
// envelope rather than from re-parsing the key on the hot path.
func (p *RedisPublisher) observe(env *pb.Envelope, publishTime time.Time) {
	if p.opts.Metrics == nil {
		return
	}
	venue := env.Venue
	channel := core.ChannelName(env.Channel)

	marketType, symbol := VenueScope, ""
	if env.Instrument != nil {
		marketType = core.MarketTypeName(env.Instrument.MarketType)
		symbol = env.Instrument.Canonical
	}

	p.opts.Metrics.MessagesPublished.
		WithLabelValues(venue, marketType, symbol, channel, core.SourceName(env.Source)).Inc()

	// A negative latency means our clock is behind the venue's: a skew signal,
	// not a measurement, and averaging it in would hide both.
	if env.ExchangeTimeNs > 0 {
		if d := publishTime.UnixNano() - env.ExchangeTimeNs; d >= 0 {
			p.opts.Metrics.PublishLatency.WithLabelValues(venue, channel).Observe(float64(d) / float64(time.Second))
		}
	}
	if env.RecvTimeNs > 0 {
		if d := publishTime.UnixNano() - env.RecvTimeNs; d >= 0 {
			p.opts.Metrics.InternalLatency.WithLabelValues(venue, channel).Observe(float64(d) / float64(time.Second))
		}
	}
}

// onWriteError counts and, at most once a second, logs a failed write. It never
// blocks and never panics: a Redis outage must degrade the feed, not stop it.
func (p *RedisPublisher) onWriteError(key string, err error) {
	if p.opts.Metrics != nil {
		p.opts.Metrics.RedisPublishErrors.WithLabelValues(p.opts.Venue).Inc()
	}

	now := p.now()
	p.mu.Lock()
	log := now.Sub(p.lastErrLog) >= errLogInterval
	if log {
		p.lastErrLog = now
	}
	p.mu.Unlock()

	if log {
		p.opts.Logger.Error("redis publish failed", "key", key, "error", err.Error())
	}
}

// Close releases the connection pool.
func (p *RedisPublisher) Close() error { return p.rdb.Close() }
