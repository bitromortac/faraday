package frdrpcserver

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/lightninglabs/faraday/chanevents"
	"github.com/lightninglabs/faraday/frdrpc"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type mockForwardingAnalyzer struct {
	effectiveUptimeFunc func(ctx context.Context, startTime, endTime time.Time,
		liquidityFloor btcutil.Amount) (
		map[chanevents.PeerPair]chanevents.ForwardingAbility, error)
}

func (m *mockForwardingAnalyzer) EffectiveUptime(ctx context.Context, startTime,
	endTime time.Time, liquidityFloor btcutil.Amount) (
	map[chanevents.PeerPair]chanevents.ForwardingAbility, error) {

	return m.effectiveUptimeFunc(ctx, startTime, endTime, liquidityFloor)
}

func TestForwardingAbilityValidation(t *testing.T) {
	server := NewRPCServer(&Config{})

	ctx := context.Background()

	// Test startTime > endTime.
	_, err := server.ForwardingAbility(ctx, &frdrpc.ForwardingAbilityRequest{
		StartTime: 200,
		EndTime:   100,
	})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.InvalidArgument, st.Code())
}

func TestForwardingAbilitySuccess(t *testing.T) {
	mockAlz := &mockForwardingAnalyzer{
		effectiveUptimeFunc: func(ctx context.Context, startTime,
			endTime time.Time, liquidityFloor btcutil.Amount) (
			map[chanevents.PeerPair]chanevents.ForwardingAbility, error) {

			require.Equal(t, int64(100), startTime.Unix())
			require.Equal(t, int64(200), endTime.Unix())

			// The explicit floor is passed straight through.
			require.Equal(t, btcutil.Amount(1000), liquidityFloor)

			return map[chanevents.PeerPair]chanevents.ForwardingAbility{
				{PeerIn: "02aaaabbbbcccc0000000000000000000000000000000000000000000000000001", PeerOut: "02aaaabbbbcccc0000000000000000000000000000000000000000000000000002"}: {
					EffectiveUptime: 90 * time.Second,
					ForwardedAmount: 550,
				},
			}, nil
		},
	}

	server := NewRPCServer(&Config{
		ForwardingAnalyzer: mockAlz,
	})

	ctx := context.Background()
	resp, err := server.ForwardingAbility(ctx, &frdrpc.ForwardingAbilityRequest{
		StartTime:         100,
		EndTime:           200,
		LiquidityFloorSat: 1000,
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Len(t, resp.Peers, 2)
	require.Len(t, resp.Entries, 1)
	require.Equal(t, int64(100), resp.StartTime)
	require.Equal(t, int64(200), resp.EndTime)
	require.Equal(t, int64(90), resp.Entries[0].EffectiveUptimeS)
	require.Equal(t, int64(550), resp.Entries[0].ForwardedSat)
}

func TestForwardingAbilityDefaultFloor(t *testing.T) {
	mockAlz := &mockForwardingAnalyzer{
		effectiveUptimeFunc: func(ctx context.Context, startTime,
			endTime time.Time, liquidityFloor btcutil.Amount) (
			map[chanevents.PeerPair]chanevents.ForwardingAbility, error) {

			// An unset floor resolves to the server default.
			require.Equal(
				t, btcutil.Amount(defaultLiquidityFloorSat),
				liquidityFloor,
			)

			return nil, nil
		},
	}

	server := NewRPCServer(&Config{
		ForwardingAnalyzer: mockAlz,
	})

	ctx := context.Background()
	_, err := server.ForwardingAbility(ctx, &frdrpc.ForwardingAbilityRequest{
		StartTime: 100,
		EndTime:   200,
	})
	require.NoError(t, err)
}

func TestForwardingAbilityNoAnalyzer(t *testing.T) {
	server := NewRPCServer(&Config{})

	ctx := context.Background()
	_, err := server.ForwardingAbility(ctx, &frdrpc.ForwardingAbilityRequest{
		StartTime:         100,
		EndTime:           200,
		LiquidityFloorSat: 1000,
	})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Unavailable, st.Code())
}

func TestForwardingAbilityInternalError(t *testing.T) {
	mockAlz := &mockForwardingAnalyzer{
		effectiveUptimeFunc: func(ctx context.Context, startTime,
			endTime time.Time, liquidityFloor btcutil.Amount) (
			map[chanevents.PeerPair]chanevents.ForwardingAbility, error) {

			return nil, errors.New("db lookup failed")
		},
	}

	server := NewRPCServer(&Config{
		ForwardingAnalyzer: mockAlz,
	})

	ctx := context.Background()
	_, err := server.ForwardingAbility(ctx, &frdrpc.ForwardingAbilityRequest{
		StartTime:         100,
		EndTime:           200,
		LiquidityFloorSat: 1000,
	})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Internal, st.Code())
}
