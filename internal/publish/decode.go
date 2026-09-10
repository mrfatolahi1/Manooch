package publish

import (
	"fmt"

	"github.com/you/manooch/gen/manoochv1"
	"google.golang.org/protobuf/proto"
)

// NewMessage returns an empty message of the type a channel carries.
//
// The mapping lives beside the key scheme because a key's channel component is
// the only thing that says what the bytes are. It exists for manooch-tap and
// manooch-status, which are handed arbitrary keys by Redis; a data consumer
// subscribes to channels it chose and knows the type already.
func NewMessage(channel manoochv1.Channel) (proto.Message, error) {
	switch channel {
	case manoochv1.Channel_CHANNEL_MARK_PRICE:
		return &manoochv1.MarkPrice{}, nil
	case manoochv1.Channel_CHANNEL_INDEX_PRICE:
		return &manoochv1.IndexPrice{}, nil
	case manoochv1.Channel_CHANNEL_FUNDING:
		return &manoochv1.Funding{}, nil
	case manoochv1.Channel_CHANNEL_METADATA:
		return &manoochv1.InstrumentMeta{}, nil
	case manoochv1.Channel_CHANNEL_HEALTH:
		return &manoochv1.Health{}, nil
	case manoochv1.Channel_CHANNEL_RATELIMIT:
		return &manoochv1.RateLimit{}, nil
	default:
		return nil, fmt.Errorf("no message type for channel %v", channel)
	}
}

// Decode unmarshals a payload and returns it along with its envelope.
func Decode(channel manoochv1.Channel, b []byte) (proto.Message, *manoochv1.Envelope, error) {
	message, err := NewMessage(channel)
	if err != nil {
		return nil, nil, err
	}
	if err := proto.Unmarshal(b, message); err != nil {
		return nil, nil, fmt.Errorf("unmarshal %v: %w", channel, err)
	}
	envelope := message.(enveloped).GetEnv()
	if envelope == nil {
		return nil, nil, fmt.Errorf("%v message has no envelope", channel)
	}
	return message, envelope, nil
}
