package chanevents

import (
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/lightningnetwork/lnd/fn/v2"
	"github.com/stretchr/testify/require"
)

// newEvent is a helper to create a ChannelEvent for tests.
func newEvent(chanID int64, ts int64, eventType EventType,
	local, remote btcutil.Amount) *ChannelEvent {

	return &ChannelEvent{
		ChannelID:     chanID,
		Timestamp:     time.Unix(ts, 0),
		EventType:     eventType,
		LocalBalance:  fn.Some(local),
		RemoteBalance: fn.Some(remote),
	}
}

// newStatusEvent is a helper to create a ChannelEvent for online/offline
// events where balances are not relevant.
func newStatusEvent(chanID int64, ts int64, eventType EventType) *ChannelEvent {
	return &ChannelEvent{
		ChannelID:     chanID,
		Timestamp:     time.Unix(ts, 0),
		EventType:     eventType,
		LocalBalance:  fn.None[btcutil.Amount](),
		RemoteBalance: fn.None[btcutil.Amount](),
	}
}

// TestCalculatePairEffectiveUptime tests the calculatePairEffectiveUptime method.
func TestCalculatePairEffectiveUptime(t *testing.T) {
	var (
		chanInID  int64 = 1
		chanOutID int64 = 2
		startTime       = time.Unix(100, 0)
		endTime         = time.Unix(200, 0)
	)

	testCases := []struct {
		name string

		inStates  map[int64]*channelState
		outStates map[int64]*channelState
		inEvents  []*ChannelEvent
		outEvents []*ChannelEvent

		successAmts       []btcutil.Amount
		thresholdAmount   btcutil.Amount
		forwardPercentile float64

		expected    *ForwardingAbility
		expectedErr string
	}{
		{
			name: "Basic case always online",
			inStates: map[int64]*channelState{
				chanInID: {
					online:        true,
					remoteBalance: 1000,
				},
			},
			outStates: map[int64]*channelState{
				chanOutID: {
					online:       true,
					localBalance: 800,
				},
			},
			successAmts: []btcutil.Amount{100},
			expected: &ForwardingAbility{
				Velocity:       1, // 100 sats / 100s
				UptimeFraction: 1.0,
			},
		},
		{
			name: "Channel goes offline",
			inStates: map[int64]*channelState{
				chanInID: {
					online:        true,
					remoteBalance: 1000,
				},
			},
			outStates: map[int64]*channelState{
				chanOutID: {
					online:       true,
					localBalance: 800,
				},
			},
			inEvents: []*ChannelEvent{
				newStatusEvent(chanInID, 150, EventTypeOffline),
			},
			successAmts:     []btcutil.Amount{100},
			thresholdAmount: 1,
			expected: &ForwardingAbility{
				Velocity:       2, // 100 sats / 50s
				UptimeFraction: 0.5,
			},
		},
		{
			name: "Balance change",
			inStates: map[int64]*channelState{
				chanInID: {
					online:        true,
					remoteBalance: 1000,
				},
			},
			outStates: map[int64]*channelState{
				chanOutID: {
					online:       true,
					localBalance: 800,
				},
			},
			outEvents: []*ChannelEvent{
				newEvent(chanOutID, 150, EventTypeUpdate, 1200, 0),
			},
			successAmts:     []btcutil.Amount{100},
			thresholdAmount: 1,
			// Balance changes at t=150, so for the first 50s the
			// liquidity is 800, then it's 1000 for the next 50s.
			// The total effective uptime is 100s.
			expected: &ForwardingAbility{
				Velocity:       1, // 100 sats / 100s
				UptimeFraction: 1.0,
			},
		},
		{
			name: "Duplicate event timestamps",
			inStates: map[int64]*channelState{
				chanInID: {
					online:        true,
					remoteBalance: 1000,
				},
			},
			outStates: map[int64]*channelState{
				chanOutID: {
					online:       true,
					localBalance: 800,
				},
			},
			inEvents: []*ChannelEvent{
				newStatusEvent(chanInID, 150, EventTypeOffline),
			},
			outEvents: []*ChannelEvent{
				newEvent(chanOutID, 150, EventTypeUpdate, 1200, 0),
			},
			successAmts:     []btcutil.Amount{100},
			thresholdAmount: 1,
			// At t=150, two events happen.
			// From t=100 to t=150 (50s), liquidity is
			// min(1000, 800) = 800. After t=150, chanIn is
			// offline, so liquidity is 0 for the remaining 50s.
			expected: &ForwardingAbility{
				Velocity:       2, // 100 sats / 50s
				UptimeFraction: 0.5,
			},
		},
		{
			name: "No initial state",
			inStates: map[int64]*channelState{
				chanInID: {online: false},
			},
			outStates: map[int64]*channelState{
				chanOutID: {online: false},
			},
			inEvents: []*ChannelEvent{
				newEvent(chanInID, 120, EventTypeUpdate, 0, 1000),
			},
			outEvents: []*ChannelEvent{
				newEvent(chanOutID, 140, EventTypeUpdate, 800, 0),
			},
			successAmts:     []btcutil.Amount{100},
			thresholdAmount: 1,
			// We don't have initial state, so we can't determine
			// liquidity until we see an event on both channels.
			// At t=140 we know the liquidity is 800, and it's
			// online for the remaining 60s of the 100s total.
			// So uptime fraction is 0.6 for 800.
			expected: &ForwardingAbility{
				Velocity:       1.6666666666666667, // 100 sats / 60s
				UptimeFraction: 0.6,
			},
		},
		{
			name: "Multiple channels for out peer",
			inStates: map[int64]*channelState{
				chanInID: {
					online:        true,
					remoteBalance: 1000,
				},
			},
			outStates: map[int64]*channelState{
				chanOutID: {
					online:       true,
					localBalance: 800,
				},
				3: {
					online:       true,
					localBalance: 500,
				},
			},
			outEvents: []*ChannelEvent{
				newEvent(chanOutID, 150, EventTypeUpdate, 1200, 0),
			},
			successAmts:     []btcutil.Amount{100},
			thresholdAmount: 1,
			// We expect the liquidity to be the sum of the
			// available balances of the out channels.
			// t=100-150: min(1000, 800 + 500) = 1000
			// t=150-200: min(1000, 1200 + 500) = 1000
			expected: &ForwardingAbility{
				Velocity:       1, // 100 sats / 100s
				UptimeFraction: 1.0,
			},
		},
		{
			name: "Circular payment ability",
			inStates: map[int64]*channelState{
				chanInID: {
					online:       true,
					localBalance: 1000,
				},
			},
			outStates: map[int64]*channelState{
				chanInID: {
					online:       true,
					localBalance: 1000,
				},
			},
			inEvents: []*ChannelEvent{
				newEvent(chanInID, 150, EventTypeUpdate, 500, 500),
			},
			outEvents: []*ChannelEvent{
				newEvent(chanInID, 150, EventTypeUpdate, 500, 500),
			},
			successAmts:     []btcutil.Amount{100},
			thresholdAmount: 1,
			// For the first 50s, liquidity is min(1000, 0) = 0.
			// For the next 50s, liquidity is min(500, 500) = 500.
			expected: &ForwardingAbility{
				Velocity:       2, // 100 sats / 50s
				UptimeFraction: 0.5,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := calculatePairEffectiveUptime(
				startTime, endTime, tc.forwardPercentile,
				tc.thresholdAmount, tc.successAmts,
				tc.inStates, tc.outStates,
				tc.inEvents, tc.outEvents,
			)

			if tc.expectedErr != "" {
				require.ErrorContains(t, err, tc.expectedErr)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.expected, result)
		})
	}
}
