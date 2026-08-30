package publish

import (
	"fmt"

	pb "github.com/you/manooch/gen/manoochv1"
	"google.golang.org/protobuf/proto"
)

// NewMessage returns an empty message of the type a channel carries.
//
// The mapping lives beside the key scheme because a key's channel component is
// the only thing that says what the bytes are. It exists for manooch-tap and
// manooch-status, which are handed arbitrary keys by Redis; a data consumer
// subscribes to channels it chose and knows the type already.
func NewMessage(ch pb.Channel) (proto.Message, error) {
	switch ch {
	case pb.Channel_CHANNEL_ORDERBOOK:
		return &pb.OrderBook{}, nil
	case pb.Channel_CHANNEL_TRADES:
		return &pb.Trades{}, nil
	case pb.Channel_CHANNEL_MARK_PRICE:
		return &pb.MarkPrice{}, nil
	case pb.Channel_CHANNEL_INDEX_PRICE:
		return &pb.IndexPrice{}, nil
	case pb.Channel_CHANNEL_FUNDING:
		return &pb.Funding{}, nil
	case pb.Channel_CHANNEL_METADATA:
		return &pb.InstrumentMeta{}, nil
	case pb.Channel_CHANNEL_HEALTH:
		return &pb.Health{}, nil
	default:
		return nil, fmt.Errorf("no message type for channel %v", ch)
	}
}

// Decode unmarshals a payload and returns it along with its envelope.
func Decode(ch pb.Channel, b []byte) (proto.Message, *pb.Envelope, error) {
	msg, err := NewMessage(ch)
	if err != nil {
		return nil, nil, err
	}
	if err := proto.Unmarshal(b, msg); err != nil {
		return nil, nil, fmt.Errorf("unmarshal %v: %w", ch, err)
	}
	env := msg.(enveloped).GetEnv()
	if env == nil {
		return nil, nil, fmt.Errorf("%v message has no envelope", ch)
	}
	return msg, env, nil
}
