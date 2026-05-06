package frdrpcserver_test

import (
	"context"
	"encoding/hex"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/lightninglabs/faraday/chanevents"
	"github.com/lightninglabs/faraday/frdrpc"
	"github.com/lightninglabs/faraday/frdrpcserver"
	"github.com/lightninglabs/faraday/frdrpcserver/mock"
	"github.com/lightningnetwork/lnd/clock"
	tmock "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var errAnalyzerFailed = errors.New("analyzer failed")

// pk constructs a 33-byte compressed-pubkey hex string for fixtures.
func pk(b byte) string {
	return "02" + strings.Repeat(hex.EncodeToString([]byte{b}), 32)
}

// TestForwardingAbilityValidation pins the input-validation branches of the
// ForwardingAbility handler to their expected gRPC status code and message.
// Every branch short-circuits before the analyzer is invoked, so the test
// needs only the cfg to reach the validator under test, not a working lnd or a
// populated chanevents store.
func TestForwardingAbilityValidation(t *testing.T) {
	t.Parallel()

	// An empty real store lets every case past the first guard reach the
	// validator under test. The nil-store case exercises that guard itself.
	clk := clock.NewTestClock(time.Unix(1, 0))
	store := chanevents.NewTestDB(t, clk)

	tests := []struct {
		name     string
		store    *chanevents.Store
		req      *frdrpc.ForwardingAbilityRequest
		wantCode codes.Code
		wantMsg  string
	}{
		{
			name:  "store not configured",
			store: nil,
			req: &frdrpc.ForwardingAbilityRequest{
				StartTime: 1, EndTime: 2,
			},
			wantCode: codes.FailedPrecondition,
			wantMsg:  "channel events store not configured",
		},
		{
			name:  "start_time over MaxInt64",
			store: store,
			req: &frdrpc.ForwardingAbilityRequest{
				StartTime: math.MaxInt64 + 1,
				EndTime:   math.MaxInt64,
			},
			wantCode: codes.InvalidArgument,
			wantMsg:  "start_time and end_time must be <= MaxInt64",
		},
		{
			name:  "end_time over MaxInt64",
			store: store,
			req: &frdrpc.ForwardingAbilityRequest{
				StartTime: 1,
				EndTime:   math.MaxInt64 + 1,
			},
			wantCode: codes.InvalidArgument,
			wantMsg:  "start_time and end_time must be <= MaxInt64",
		},
		{
			name:  "percentile below zero",
			store: store,
			req: &frdrpc.ForwardingAbilityRequest{
				StartTime: 1, EndTime: 2,
				ForwardPercentile: -0.1,
			},
			wantCode: codes.InvalidArgument,
			wantMsg:  "forward_percentile must be in [0, 100]",
		},
		{
			name:  "percentile above hundred",
			store: store,
			req: &frdrpc.ForwardingAbilityRequest{
				StartTime: 1, EndTime: 2,
				ForwardPercentile: 100.1,
			},
			wantCode: codes.InvalidArgument,
			wantMsg:  "forward_percentile must be in [0, 100]",
		},
	}

	for _, tc := range tests {
		t.Run(
			tc.name,
			func(t *testing.T) {
				t.Parallel()

				s := frdrpcserver.NewRPCServer(
					&frdrpcserver.Config{
						ChanEvents: tc.store,
					},
				)

				resp, err := s.ForwardingAbility(
					context.Background(), tc.req,
				)
				require.Nil(t, resp)
				require.Error(t, err)

				st, ok := status.FromError(err)
				require.True(
					t, ok, "expected gRPC status error",
				)
				require.Equal(t, tc.wantCode, st.Code())
				require.Contains(t, st.Message(), tc.wantMsg)
			},
		)
	}
}

// TestForwardingAbilityHandlerRoundTrip exercises the gRPC handler
// end-to-end against a fake analyzer. It is the canonical "how do I
// integrate this RPC?" demo: a client builds a ForwardingAbilityRequest,
// the handler calls the analyzer, encodes the matrix, and the test
// decodes the response with frdrpc.DecodeForwardingAbility — exactly the
// flow real consumers (CLI, external Go clients) follow. Equivalence is
// asserted within uint16 quantisation tolerance.
func TestForwardingAbilityHandlerRoundTrip(t *testing.T) {
	t.Parallel()

	// Three peers; only one pair has a non-zero velocity, the rest carry
	// uniform high uptime — the "common case" the encoding is tuned for.
	pkA, pkB, pkC := pk(0x01), pk(0x02), pk(0x03)
	matrix := map[chanevents.PeerPair]chanevents.ForwardingAbility{
		{PeerIn: pkA, PeerOut: pkA}: {UptimeFraction: 0.99},
		{PeerIn: pkA, PeerOut: pkB}: {UptimeFraction: 0.95},
		{PeerIn: pkA, PeerOut: pkC}: {UptimeFraction: 0.90},
		{PeerIn: pkB, PeerOut: pkA}: {UptimeFraction: 0.95},
		{PeerIn: pkB, PeerOut: pkB}: {UptimeFraction: 0.99},
		{PeerIn: pkB, PeerOut: pkC}: {
			UptimeFraction: 0.80,
			Velocity:       42.5,
		},
		{PeerIn: pkC, PeerOut: pkA}: {UptimeFraction: 0.90},
		{PeerIn: pkC, PeerOut: pkB}: {UptimeFraction: 0.80},
		{PeerIn: pkC, PeerOut: pkC}: {UptimeFraction: 0.99},
	}

	analyzer := &mock.ForwardingAnalyzer{}
	analyzer.On(
		"EffectiveUptime",
		tmock.Anything, tmock.Anything, tmock.Anything,
		tmock.Anything, tmock.Anything,
	).Return(matrix, nil).Once()

	clk := clock.NewTestClock(time.Unix(1, 0))
	server := frdrpcserver.NewRPCServer(&frdrpcserver.Config{
		ChanEvents:         chanevents.NewTestDB(t, clk),
		ForwardingAnalyzer: analyzer,
	})

	resp, err := server.ForwardingAbility(
		context.Background(), &frdrpc.ForwardingAbilityRequest{
			StartTime:         100,
			EndTime:           200,
			ForwardPercentile: 50,
			ThresholdAmtSat:   1,
		},
	)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Len(t, resp.Peers, 3)
	require.Len(t, resp.Velocities, 1)

	got, err := frdrpc.DecodeForwardingAbility(resp)
	require.NoError(t, err)

	for pair, want := range matrix {
		gotAbility := got[pair.PeerIn][pair.PeerOut]
		require.InDelta(
			t, want.UptimeFraction,
			gotAbility.UptimeFraction, 1.0/65535,
			"uptime mismatch for %s->%s",
			pair.PeerIn, pair.PeerOut,
		)
		require.InDelta(
			t, want.Velocity, gotAbility.Velocity, 1e-3,
			"velocity mismatch for %s->%s",
			pair.PeerIn, pair.PeerOut,
		)
	}

	analyzer.AssertExpectations(t)
}

// TestForwardingAbilityHandlerPropagatesAnalyzerError verifies that an
// analyzer error short-circuits the handler without producing a partial
// response — guarding against a class of bug where the encoding step
// would silently swallow the upstream failure.
func TestForwardingAbilityHandlerPropagatesAnalyzerError(t *testing.T) {
	t.Parallel()

	analyzer := &mock.ForwardingAnalyzer{}
	analyzer.On(
		"EffectiveUptime",
		tmock.Anything, tmock.Anything, tmock.Anything,
		tmock.Anything, tmock.Anything,
	).Return(nil, errAnalyzerFailed).Once()

	clk := clock.NewTestClock(time.Unix(1, 0))
	server := frdrpcserver.NewRPCServer(&frdrpcserver.Config{
		ChanEvents:         chanevents.NewTestDB(t, clk),
		ForwardingAnalyzer: analyzer,
	})

	resp, err := server.ForwardingAbility(
		context.Background(), &frdrpc.ForwardingAbilityRequest{},
	)
	require.ErrorIs(t, err, errAnalyzerFailed)
	require.Nil(t, resp)

	analyzer.AssertExpectations(t)
}
