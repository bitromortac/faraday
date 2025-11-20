package frdrpcserver

import (
	"context"
	"time"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/lightninglabs/faraday/chanevents"
	"github.com/lightninglabs/faraday/frdrpc"
)

// ForwardingAbility implements the frdrpc.FaradayServerServer interface.
func (s *RPCServer) ForwardingAbility(ctx context.Context,
	req *frdrpc.ForwardingAbilityRequest) (
	*frdrpc.ForwardingAbilityResponse, error) {

	log.Infof("Received ForwardingAbility request: %v", req)

	// If no start time is specified, we'll use a zero start time to query
	// for all events.
	var startTime time.Time
	if req.StartTime != 0 {
		startTime = time.Unix(int64(req.StartTime), 0)
	}

	// If no end time is specified, we'll use the current time.
	endTime := time.Now()
	if req.EndTime != 0 {
		endTime = time.Unix(int64(req.EndTime), 0)
	}

	forwardingAnalyzer := chanevents.NewForwardingAnalyzer(
		s.cfg.ChanEvents, s.cfg.Lnd,
	)

	threshold := btcutil.Amount(req.ThresholdAmtSat)
	pairs, err := forwardingAnalyzer.EffectiveUptime(
		ctx, startTime, endTime, float64(req.ForwardPercentile),
		threshold,
	)
	if err != nil {
		return nil, err
	}

	var rpcPairs []*frdrpc.ForwardingAbilityPair
	for peerIn, outMap := range pairs {
		for peerOut, ability := range outMap {
			rpcPairs = append(rpcPairs, &frdrpc.ForwardingAbilityPair{
				PeerIn:  peerIn,
				PeerOut: peerOut,
				Ability: &frdrpc.ForwardingAbility{
					Velocity:       ability.Velocity,
					UptimeFraction: ability.UptimeFraction,
				},
			})
		}
	}

	return &frdrpc.ForwardingAbilityResponse{
		Pairs: rpcPairs,
	}, nil
}
