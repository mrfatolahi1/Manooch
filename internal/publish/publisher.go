package publish

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/core"
	"github.com/you/manooch/internal/observability"
	"google.golang.org/protobuf/proto"
)

// A Publisher writes one message to one topic. timeToLive == 0 means the key
// never expires, for channels with no cadence of their own, where an expiring
// key would report a working stream as dead.
type Publisher interface {
	Publish(ctx context.Context, key string, message proto.Message, timeToLive time.Duration) error
	Close() error
}

// enveloped is satisfied by every payload in the schema: each carries its
// Envelope in field 1.
type enveloped interface {
	proto.Message
	GetEnv() *manoochv1.Envelope
}

// errorLogInterval caps the write-error log rate: Redis being down is one fact,
// not one fact per message.
const errorLogInterval = time.Second

// Options configures a RedisPublisher.
type Options struct {
	Addr        string
	Database    int
	DialTimeout time.Duration
	ReadTimeout time.Duration
	PoolSize    int

	Venue         string
	InstanceID    string
	SchemaVersion uint32

	Metrics *observability.Metrics
	Logger  *slog.Logger

	// Now is swappable for tests. Defaults to time.Now.
	Now func() time.Time
}

// RedisPublisher writes to Redis as a last-value cache plus a Pub/Sub fan-out.
type RedisPublisher struct {
	redisClient *redis.Client
	options     Options
	now         func() time.Time

	mutex        sync.Mutex
	sequence     map[string]uint64
	lastErrorLog time.Time
}

var _ Publisher = (*RedisPublisher)(nil)

// NewRedis dials Redis and fails if it is not there. A feed that cannot publish
// has nothing to do, and starting anyway leaves consumers on a stale cache with
// no indication why.
func NewRedis(ctx context.Context, options Options) (*RedisPublisher, error) {
	if options.Logger == nil {
		return nil, fmt.Errorf("publish: no logger")
	}
	if options.InstanceID == "" {
		return nil, fmt.Errorf("publish: no instance id")
	}
	if options.Now == nil {
		options.Now = time.Now
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr:         options.Addr,
		DB:           options.Database,
		DialTimeout:  options.DialTimeout,
		ReadTimeout:  options.ReadTimeout,
		WriteTimeout: options.ReadTimeout,
		PoolSize:     options.PoolSize,
	})
	if err := redisClient.Ping(ctx).Err(); err != nil {
		_ = redisClient.Close()
		return nil, fmt.Errorf("publish: redis %s db %d: %w", options.Addr, options.Database, err)
	}

	return &RedisPublisher{
		redisClient: redisClient,
		options:     options,
		now:         options.Now,
		sequence:    make(map[string]uint64),
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
func (publisher *RedisPublisher) Publish(ctx context.Context, key string, message proto.Message, timeToLive time.Duration) error {
	carrier, ok := message.(enveloped)
	if !ok {
		return fmt.Errorf("publish %s: %T carries no envelope", key, message)
	}
	envelope := carrier.GetEnv()
	if envelope == nil {
		return fmt.Errorf("publish %s: %T has a nil envelope", key, message)
	}
	// Never publish without a status: a consumer that cannot tell healthy from
	// stale is worse off than one with no data.
	if envelope.Status == manoochv1.Status_STATUS_UNSPECIFIED {
		return fmt.Errorf("publish %s: envelope status is unspecified", key)
	}

	publisher.mutex.Lock()
	publisher.sequence[key]++
	envelope.PublishSeq = publisher.sequence[key]
	envelope.InstanceId = publisher.options.InstanceID
	envelope.SchemaVersion = publisher.options.SchemaVersion
	if envelope.Venue == "" {
		envelope.Venue = publisher.options.Venue
	}
	// Set here and nowhere else: any earlier measures when we decided to
	// publish rather than when we did, which is the gap being looked for.
	publishTime := publisher.now()
	envelope.PublishTimeNs = publishTime.UnixNano()

	// Under the lock: envelope is a pointer into the caller's message, so a second
	// publish of the same message would race with the stamping above.
	b, marshalErr := proto.Marshal(message)
	publisher.mutex.Unlock()

	if marshalErr != nil {
		return fmt.Errorf("publish %s: marshal: %w", key, marshalErr)
	}

	pipe := publisher.redisClient.Pipeline()
	if timeToLive > 0 {
		milliseconds := timeToLive.Milliseconds()
		if milliseconds == 0 {
			milliseconds = 1 // a sub-millisecond TTL would round to PX 0, which Redis rejects
		}
		pipe.Do(ctx, "SET", key, b, "PX", milliseconds)
	} else {
		// No expiry: liveness for this channel comes from elsewhere.
		pipe.Do(ctx, "SET", key, b)
	}
	pipe.Do(ctx, "PUBLISH", key, b)

	if _, err := pipe.Exec(ctx); err != nil {
		publisher.onWriteError(key, err)
		return fmt.Errorf("publish %s: %w", key, err)
	}

	publisher.observe(envelope, publishTime)
	return nil
}

// observe records metrics for a successful publish. Labels come from the
// envelope rather than from re-parsing the key on the hot path.
func (publisher *RedisPublisher) observe(envelope *manoochv1.Envelope, publishTime time.Time) {
	if publisher.options.Metrics == nil {
		return
	}
	venue := envelope.Venue
	channel := core.ChannelName(envelope.Channel)

	marketType, symbol := VenueScope, ""
	if envelope.Instrument != nil {
		marketType = core.MarketTypeName(envelope.Instrument.MarketType)
		symbol = envelope.Instrument.Canonical
	}

	publisher.options.Metrics.MessagesPublished.
		WithLabelValues(venue, marketType, symbol, channel, core.SourceName(envelope.Source)).Inc()

	// A negative latency means our clock is behind the venue's: a skew signal,
	// not a measurement, and averaging it in would hide both.
	//
	// An exchange time that is an event time rather than a send time is not a
	// latency at all: a funding rate carries the instant it settled, so the
	// difference is how old the value is, and folding that into the histogram
	// would put one venue's funding channel permanently in the last bucket.
	if envelope.ExchangeTimeIsSendTime && envelope.ExchangeTimeNs > 0 {
		if d := publishTime.UnixNano() - envelope.ExchangeTimeNs; d >= 0 {
			publisher.options.Metrics.PublishLatency.WithLabelValues(venue, channel).Observe(float64(d) / float64(time.Second))
		}
	}
	if envelope.RecvTimeNs > 0 {
		if d := publishTime.UnixNano() - envelope.RecvTimeNs; d >= 0 {
			publisher.options.Metrics.InternalLatency.WithLabelValues(venue, channel).Observe(float64(d) / float64(time.Second))
		}
	}
}

// onWriteError counts and, at most once a second, logs a failed write. It never
// blocks and never panics: a Redis outage must degrade the feed, not stop it.
func (publisher *RedisPublisher) onWriteError(key string, err error) {
	if publisher.options.Metrics != nil {
		publisher.options.Metrics.RedisPublishErrors.WithLabelValues(publisher.options.Venue).Inc()
	}

	now := publisher.now()
	publisher.mutex.Lock()
	log := now.Sub(publisher.lastErrorLog) >= errorLogInterval
	if log {
		publisher.lastErrorLog = now
	}
	publisher.mutex.Unlock()

	if log {
		publisher.options.Logger.Error("redis publish failed", "key", key, "error", err.Error())
	}
}

// Redis exposes the underlying client for the reads the write path does not
// do: the fallback watcher's keyspace subscription and its pipelined EXISTS
// sweep.
//
// It is deliberately not a way to write. Everything published goes through
// Publish, which is the only place the envelope is stamped and the only place
// publish_time_ns is set.
func (publisher *RedisPublisher) Redis() *redis.Client { return publisher.redisClient }

// Close releases the connection pool.
func (publisher *RedisPublisher) Close() error { return publisher.redisClient.Close() }
