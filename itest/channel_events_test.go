package itest

import (
	"context"
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/lightninglabs/faraday/frdrpc"
	"github.com/lightninglabs/lndclient"
	"github.com/lightningnetwork/lnd/lnrpc"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/stretchr/testify/require"
)

// TestGetChannelEvents tests the GetChannelEvents rpc endpoint.
func TestGetChannelEvents(t *testing.T) {
	c := newTestContext(t)
	defer c.stop()

	ctx := context.Background()

	// We will start by opening a channel from alice to bob.
	var aliceChannelAmt = btcutil.Amount(500000)

	err := c.aliceClient.Client.Connect(
		ctx, c.bobPubkey, "localhost:10012", true,
	)
	require.NoError(c.t, err, "could not connect nodes")

	aliceChannel, _ := c.openChannel(
		c.aliceClient.Client, c.bobPubkey, aliceChannelAmt,
	)

	// TODO: fix the timing issue.
	time.Sleep(3 * time.Second)

	// Now we'll send a payment from alice to bob to generate a balance
	// update event.
	var paymentAmount lnwire.MilliSatoshi = 20000000

	hash, payreq := c.addInvoice(c.bobClient.Client, paymentAmount)
	c.makePayment(
		c.aliceClient.LndServices, c.bobClient.LndServices,
		lndclient.SendPaymentRequest{
			Invoice:     payreq,
			PaymentHash: &hash,
			Timeout:     paymentTimeout,
		}, lnrpc.Payment_SUCCEEDED,
	)

	// We now close the channel to generate an offline event.
	c.closeChannel(c.aliceClient.Client, aliceChannel, true)

	// We'll query for all events and then check that we have at least
	// these three. It's possible that there are more events due to lnd's
	// internal workings, so we won't assert the exact count.
	events, err := c.faradayClient.GetChannelEvents(
		ctx, &frdrpc.ChannelEventsRequest{
			ChanPoint: aliceChannel.String(),
		},
	)
	require.NoError(c.t, err, "could not get channel events")

	// Check that we have the expected event types.
	var (
		onlineEvents  int
		updateEvents  int
		offlineEvents int
	)

	for _, event := range events.Events {
		switch event.EventType {
		case frdrpc.ChannelEventType_CHAN_EVENT_ONLINE:
			onlineEvents++

		case frdrpc.ChannelEventType_CHAN_EVENT_UPDATE:
			updateEvents++

		case frdrpc.ChannelEventType_CHAN_EVENT_OFFLINE:
			offlineEvents++
		}
	}

	// We expect to see these events for this channel:
	// 1. Channel Open: online, update (initial balance)
	// 2. Channel Active: online
	// 3. Payment sent: two updates (update_add, update_fulfill)
	// 4. Channel Offline: offline
	// 5. Channel Close: offline
	//
	require.Len(t, events.Events, 7)
	require.Equal(t, 2, onlineEvents)
	require.Equal(t, 3, updateEvents)
	require.Equal(t, 2, offlineEvents)
}

// TestForwardingAbility tests the ForwardingAbility rpc endpoint.
func TestForwardingAbility(t *testing.T) {
	c := newTestContext(t)
	defer c.stop()

	ctx := context.Background()

	// We will start by opening a channel from alice to bob.
	var aliceChannelAmt = btcutil.Amount(500000)

	err := c.aliceClient.Client.Connect(
		ctx, c.bobPubkey, "localhost:10012", true,
	)
	require.NoError(c.t, err, "could not connect nodes")

	// Alice opens a channel to Bob. Faraday will recognize this channel
	// open event and will add online and update events for it.
	aliceChannel, _ := c.openChannel(
		c.aliceClient.Client, c.bobPubkey, aliceChannelAmt,
	)

	// TODO: wait for the channel open to be ready with lntest.
	time.Sleep(3 * time.Second)

	// Wait for channel events to be processed.
	// TODO: replace with sync.Wait once we have lntest.
	assertEvents := func(expected int) {
		var events *frdrpc.ChannelEventsResponse
		var err error
		for range 10 {
			events, err = c.faradayClient.GetChannelEvents(
				ctx, &frdrpc.ChannelEventsRequest{
					ChanPoint: aliceChannel.String(),
				},
			)
			require.NoError(c.t, err, "could not get channel events")

			if len(events.Events) == expected {
				t.Logf("Found events %v", events.Events)
				return
			}
			time.Sleep(500 * time.Microsecond)
		}
		require.Failf(
			c.t, "fail", "expected exactly %d events for channel %s, have %d",
			expected, aliceChannel.String(), len(events.Events),
		)
	}

	// We process the events. We expect two events: online, update and online.
	assertEvents(3)

	// The intest of the forwarding ability interval is the time after
	// the channel open and before we make a payment.
	startTime := time.Now()

	// Initially we don't have any forwarding ability since there is no
	// liquidity. Only when we do the payment we'll shift the balance and
	// evaluate to have forwarding ability. After the balance shift we wait
	// the same amount of time such that we have a ~50% uptime fraction.
	time.Sleep(4 * time.Second)

	// Now, let's make a payment from Alice to Bob to establish some
	// remote balance for the channel.
	var paymentAmtMsat lnwire.MilliSatoshi = 250000 * 1000
	hash, payreq := c.addInvoice(c.bobClient.Client, paymentAmtMsat)
	c.makePayment(
		c.aliceClient.LndServices, c.bobClient.LndServices,
		lndclient.SendPaymentRequest{
			Invoice:     payreq,
			PaymentHash: &hash,
			Timeout:     paymentTimeout,
		}, lnrpc.Payment_SUCCEEDED,
	)

	// The payment will generate two update events, one for the add and one
	// for the settle.
	assertEvents(5)

	// We wait the same amount of time to have a ~50% uptime fraction.
	time.Sleep(4 * time.Second)

	endTime := time.Now()

	// Now we'll query for the forwarding ability.
	abilities, err := c.faradayClient.ForwardingAbility(
		ctx, &frdrpc.ForwardingAbilityRequest{
			StartTime:       uint64(startTime.Unix()),
			EndTime:         uint64(endTime.Unix()),
			ThresholdAmtSat: 1,
		},
	)
	require.NoError(c.t, err, "could not get forwarding ability")

	// We should only have a single pair: Bob -> Bob (circular).
	require.Len(t, abilities.Pairs, 1)

	ability := abilities.Pairs[0]
	require.Equal(t, c.bobPubkey.String(), ability.PeerIn)
	require.Equal(t, c.bobPubkey.String(), ability.PeerOut)

	// We expect a zero velocity since no forwards occurred for this pair,
	// but a non-zero uptime fraction since there was a period where
	// circular forwarding was possible.
	require.Equal(t, 0.0, ability.Ability.Velocity)
	require.InDelta(
		t, 0.5, ability.Ability.UptimeFraction, 0.1,
	)
}
