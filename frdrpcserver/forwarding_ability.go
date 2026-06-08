package frdrpcserver

import (
	"context"
	"log/slog"
	"time"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/lightninglabs/faraday/frdrpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// defaultLiquidityFloorSat is the liquidity floor applied when the request
// leaves liquidity_floor_sat unset. It approximates the smallest amount a
// rebalancer would still move, below which a pair is not economically
// forwardable.
const defaultLiquidityFloorSat = 50_000

// ForwardingAbility evaluates and returns the raw effective-uptime and
// forwarded-volume facts between peer pairs over a given timeframe. It validates
// parameters to prevent overflow and invokes the forwarding analyzer.
func (s *RPCServer) ForwardingAbility(ctx context.Context,
	req *frdrpc.ForwardingAbilityRequest) (*frdrpc.ForwardingAbilityResponse, error) {

	log.DebugS(ctx, "Handling ForwardingAbility request",
		slog.Uint64("start_time", req.StartTime),
		slog.Uint64("end_time", req.EndTime),
		slog.Uint64("liquidity_floor_sat", req.LiquidityFloorSat),
	)

	if req.StartTime > 1<<63-1 {
		return nil, status.Error(
			codes.InvalidArgument,
			"start_time exceeds maximum allowed value",
		)
	}
	if req.EndTime > 1<<63-1 {
		return nil, status.Error(
			codes.InvalidArgument,
			"end_time exceeds maximum allowed value",
		)
	}

	startTime := time.Unix(int64(req.StartTime), 0)
	endTime := time.Now()
	if req.EndTime != 0 {
		endTime = time.Unix(int64(req.EndTime), 0)
	}

	if startTime.After(endTime) {
		return nil, status.Error(
			codes.InvalidArgument,
			"start_time must be less than or equal to end_time",
		)
	}

	if s.cfg.ForwardingAnalyzer == nil {
		return nil, status.Error(
			codes.Unavailable,
			"forwarding analyzer is not configured",
		)
	}

	liquidityFloor := req.LiquidityFloorSat
	if liquidityFloor == 0 {
		liquidityFloor = defaultLiquidityFloorSat
	}

	abilities, err := s.cfg.ForwardingAnalyzer.EffectiveUptime(
		ctx, startTime, endTime, btcutil.Amount(liquidityFloor),
	)
	if err != nil {
		log.ErrorS(ctx, "EffectiveUptime failed", err,
			slog.Time("start_time", startTime),
			slog.Time("end_time", endTime),
			slog.Uint64("liquidity_floor_sat", liquidityFloor),
		)
		return nil, status.Errorf(
			codes.Internal,
			"failed to calculate effective uptime: %v", err,
		)
	}

	// Convert the flat map to the nested map required by the codec, carrying
	// the raw facts through unchanged. EffectiveUptime is truncated to whole
	// seconds here, matching the second-granularity wire field; a pair with
	// only sub-second qualifying uptime therefore reports zero uptime while
	// still carrying its forwarded volume.
	nested := make(map[string]map[string]frdrpc.ForwardingAbility)
	for pair, ability := range abilities {
		if _, ok := nested[pair.PeerIn]; !ok {
			nested[pair.PeerIn] =
				make(map[string]frdrpc.ForwardingAbility)
		}
		nested[pair.PeerIn][pair.PeerOut] = frdrpc.ForwardingAbility{
			EffectiveUptimeS: int64(ability.EffectiveUptime.Seconds()),
			ForwardedSat:     int64(ability.ForwardedAmount),
		}
	}

	resp, err := frdrpc.EncodeForwardingAbility(
		nested, startTime.Unix(), endTime.Unix(),
	)
	if err != nil {
		log.ErrorS(ctx, "EncodeForwardingAbility failed", err,
			slog.Int("pairs", len(abilities)),
		)
		return nil, status.Errorf(
			codes.Internal,
			"failed to encode forwarding ability: %v", err,
		)
	}

	return resp, nil
}
