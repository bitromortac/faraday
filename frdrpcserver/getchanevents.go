package frdrpcserver

import (
	"context"
	"fmt"
	"time"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/lightninglabs/faraday/chanevents"
	"github.com/lightninglabs/faraday/frdrpc"
)

// GetChannelEvents implements the frdrpc.FaradayServerServer interface.
func (s *RPCServer) GetChannelEvents(ctx context.Context,
	req *frdrpc.ChannelEventsRequest) (*frdrpc.ChannelEventsResponse, error) {

	// Sanity check request.
	if req.ChanPoint == "" {
		return nil, fmt.Errorf("channel point required")
	}

	channel, err := s.cfg.ChanEvents.GetChannel(ctx, req.ChanPoint)
	if err != nil {
		return nil, fmt.Errorf("unable to find channel: %v", err)
	}

	// If no start time is specified, we'll use a zero start time to query
	// for all events.
	var startTime time.Time
	if req.StartTime != 0 {
		startTime = time.Unix(int64(req.StartTime), 0)
	}

	// If no end time is specified, we'll use a zero end time which will be
	// interpreted as the present.
	endTime := time.Now()
	if req.EndTime != 0 {
		endTime = time.Unix(int64(req.EndTime), 0)
	}

	events, err := s.cfg.ChanEvents.GetChannelEvents(
		ctx, channel.ID, startTime, endTime,
	)
	if err != nil {
		return nil, err
	}

	// Marshal the events into the RPC response.
	rpcEvents, err := marshalRPCChannelEvents(events)
	if err != nil {
		return nil, err
	}

	return &frdrpc.ChannelEventsResponse{
		Events: rpcEvents,
	}, nil
}

// marshalRPCChannelEvents converts a slice of chanevents.ChannelEvent into a
// slice of frdrpc.ChannelEvent.
func marshalRPCChannelEvents(events []*chanevents.ChannelEvent) (
	[]*frdrpc.ChannelEvent, error) {

	rpcEvents := make([]*frdrpc.ChannelEvent, len(events))

	for i, event := range events {
		rpcEvent := &frdrpc.ChannelEvent{
			Timestamp: uint64(event.Timestamp.Unix()),
			EventType: frdrpc.ChannelEventType(event.EventType),
		}

		event.LocalBalance.WhenSome(func(b btcutil.Amount) {
			rpcEvent.LocalBalance = uint64(b)
		})
		event.RemoteBalance.WhenSome(func(b btcutil.Amount) {
			rpcEvent.RemoteBalance = uint64(b)
		})

		rpcEvents[i] = rpcEvent
	}

	return rpcEvents, nil
}
