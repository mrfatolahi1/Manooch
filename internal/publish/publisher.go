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

// A Publisher writes one message to one topic.
//
// ttl == 0 means the key never expires. That is for event-driven channels such
// as trades, where silence is normal and liveness comes from the socket rather
// than from the key still being there.
type Publisher interface {
	Publish(ctx context.Context, key string, msg proto.Message, ttl time.Duration) error
	Close() error
}

// enveloped is satisfied by every payload message in the schema: each one
// carries its Envelope in field 1.
type enveloped interface {
	proto.Message
	GetEnv() *pb.Envelope
}

// errLogInterval caps how often a failing Redis write is logged. Redis being
// down is one fact, not one fact per message, and a feed that logs every
// dropped publish takes the disk down with it.
const errLogInterval = time.Second

// Options configures a RedisPublisher.
type Options struct {
	Addr        string
	DB          int
	DialTimeout time.Duration
	ReadTimeout time.Duration
	PoolSize    int

	Venue      string
	InstanceID string
	// SchemaVersion is stamped on every envelope so a consumer can tell which
	// version of the contract produced the message it is holding.
	SchemaVersion uint32

	Metrics *obs.Metrics
	Logger  *slog.Logger

	// Now is swappable for tests. Defaults to time.Now.
	Now func() time.Time
}

// RedisPublisher publishes to Redis as a last-value cache plus a Pub/Sub fan-out.
type RedisPublisher struct {
	rdb  *redis.Client
	opts Options
	now  func() time.Time

	mu         sync.Mutex
	seq        map[string]uint64
	lastErrLog time.Time
}

var _ Publisher = (*RedisPublisher)(nil)

// NewRedis dials Redis and fails if it is not there. Redis is not optional:
// a feed that cannot publish has nothing to do, and starting anyway would
// leave consumers reading a stale cache with no indication why.
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
// to Redis as one pipelined round trip:
//
//	SET     <key> <bytes> PX <ttl_ms>
//	PUBLISH <key> <bytes>
//
// The SET buys two things. A consumer starting cold reads current state
// immediately instead of waiting for the next tick. And freshness becomes a
// property of the data rather than bookkeeping alongside it: key present means
// fresh, key absent means stale, with no second timestamp to drift out of sync
// with the first.
//
// The same string is the key and the Pub/Sub channel. Redis keeps those in
// separate namespaces, so there is no collision and a consumer holding one
// holds the other.
func (p *RedisPublisher) Publish(ctx context.Context, key string, msg proto.Message, ttl time.Duration) error {
	m, ok := msg.(enveloped)
	if !ok {
		return fmt.Errorf("publish %s: %T carries no envelope", key, msg)
	}
	env := m.GetEnv()
	if env == nil {
		return fmt.Errorf("publish %s: %T has a nil envelope", key, msg)
	}
	// Invariant: never publish data without a status. A consumer that cannot
	// tell healthy from stale is worse off than one with no data at all.
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
	// publish_time_ns is set here and nowhere else: any earlier and it would
	// measure the time we decided to publish rather than the time we did,
	// which is precisely the gap an operator is trying to see.
	publishTime := p.now()
	env.PublishTimeNs = publishTime.UnixNano()

	// Marshalled under the lock because env is a pointer into the caller's
	// message and a second publish of the same message would race with it.
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
		// No expiry: a last-value cache whose liveness is someone else's job.
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

// observe records the metrics for a successful publish. Labels come from the
// envelope rather than from re-parsing the key: the envelope is structured and
// already in hand.
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

	// Negative latencies mean our clock is behind the venue's. They are a
	// clock-skew signal, not a latency measurement, and averaging them into
	// the histogram would hide both.
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

// onWriteError counts and, at most once a second, logs a failed write. It
// never blocks the caller and never panics: a Redis outage must degrade the
// feed, not stop the process that will have to reconnect.
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
