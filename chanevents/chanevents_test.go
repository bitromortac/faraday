package chanevents

import (
	"context"
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/lightningnetwork/lnd/clock"
	"github.com/lightningnetwork/lnd/fn/v2"
	"github.com/stretchr/testify/require"
)

var (
	testPubKey    = "028d4c6347426f2e3f5e2b8e4a1c3b9f1c4e5d6f7a8b9c0d1e2f3a4b5c6d7e8f9"
	testChanPoint = "test_txid:0"
	testTime      = time.Unix(1, 0)
)

// TestChanEvents tests the chanevents store.
func TestChanEvents(t *testing.T) {
	t.Parallel()

	// First, create a new test database.
	clock := clock.NewTestClock(testTime)
	store := NewTestDB(t, clock)
	ctx := context.Background()

	// Add a peer.
	peer := &Peer{
		PubKey: testPubKey,
		Alias:  fn.Some("test_peer"),
	}
	peerID, err := store.AddPeer(ctx, peer)
	require.NoError(t, err)
	require.NotZero(t, peerID)

	// Get the peer and assert it is the same.
	dbPeer, err := store.GetPeer(ctx, "non_existent_pubkey")
	require.ErrorIs(t, err, errUnknownPeer)
	require.Nil(t, dbPeer)

	dbPeer, err = store.GetPeer(ctx, peer.PubKey)
	require.NoError(t, err)
	require.Equal(t, peer.PubKey, dbPeer.PubKey)
	require.Equal(t, peer.Alias, dbPeer.Alias)

	// Now, add the same peer again but with a different alias.
	// This should update the alias and return the same peer ID.
	peer.Alias = fn.Some("new_alias")
	newPeerID, err := store.AddPeer(ctx, peer)
	require.NoError(t, err)
	require.Equal(t, peerID, newPeerID)

	// Get the peer again and assert the alias has been updated.
	dbPeer, err = store.GetPeer(ctx, peer.PubKey)
	require.NoError(t, err)
	require.Equal(t, peer.Alias, dbPeer.Alias)

	// Add a channel for an unknown peer and assert an error is returned.
	channel := &Channel{
		ChannelPoint:   testChanPoint,
		ShortChannelID: fn.None[int64](),
		PeerID:         9999, // Non-existent peer ID.
	}
	channelID, err := store.AddChannel(ctx, channel)
	require.Error(t, err)
	require.Zero(t, channelID)

	// Add a channel for the peer.
	var scid int64 = 123
	channel = &Channel{
		ChannelPoint:   testChanPoint,
		ShortChannelID: fn.Some(scid),
		PeerID:         peerID,
	}
	channelID, err = store.AddChannel(ctx, channel)
	require.NoError(t, err)
	require.NotZero(t, channelID)

	// Get a non-existent channel and assert an error is returned.
	dbChannel, err := store.GetChannel(ctx, "non-existent-chan-point")
	require.ErrorIs(t, err, errUnknownChannel)
	require.Nil(t, dbChannel)

	// Get the channel and assert it is the same.
	dbChannel, err = store.GetChannel(ctx, channel.ChannelPoint)
	require.NoError(t, err)
	require.Equal(t, channel.ChannelPoint, dbChannel.ChannelPoint)
	require.Equal(t, channel.ShortChannelID, dbChannel.ShortChannelID)
	require.Equal(t, channel.PeerID, dbChannel.PeerID)

	// Add an online event for the channel.
	onlineEvent := &ChannelEvent{
		ChannelID: channelID,
		EventType: EventTypeOnline,
	}
	err = store.AddChannelEvent(ctx, onlineEvent)
	require.NoError(t, err)

	// Advance the clock for the next event.
	clock.SetTime(testTime.Add(time.Second))

	// Add an update event for the channel.
	localBalance := btcutil.Amount(1000)
	remoteBalance := btcutil.Amount(2000)
	updateEvent := &ChannelEvent{
		ChannelID:     channelID,
		EventType:     EventTypeUpdate,
		LocalBalance:  fn.Some(localBalance),
		RemoteBalance: fn.Some(remoteBalance),
	}
	err = store.AddChannelEvent(ctx, updateEvent)
	require.NoError(t, err)

	// Get the channel events and assert they are correct.
	events, err := store.GetChannelEvents(
		ctx, channelID, time.Unix(0, 0), time.Unix(3, 0),
	)
	require.NoError(t, err)
	require.Len(t, events, 2)

	require.Equal(t, onlineEvent.EventType, events[0].EventType)
	require.Equal(t, testTime.Unix(), events[0].Timestamp.Unix())
	require.True(t, events[0].LocalBalance.IsNone())
	require.True(t, events[0].RemoteBalance.IsNone())

	require.Equal(t, updateEvent.EventType, events[1].EventType)
	require.Equal(t, testTime.Add(time.Second).Unix(), events[1].Timestamp.Unix())
	require.Equal(t, updateEvent.LocalBalance, events[1].LocalBalance)
	require.Equal(t, updateEvent.RemoteBalance, events[1].RemoteBalance)
}
