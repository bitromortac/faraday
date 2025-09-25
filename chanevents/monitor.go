package chanevents

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/lightninglabs/lndclient"
	"github.com/lightningnetwork/lnd/fn/v2"
)

var (
	// errMonitorAlreadyStarted is returned when the monitor is already
	// started.
	errMonitorAlreadyStarted = errors.New("monitor already started")

	// errMonitorNotStarted is returned when the monitor is not started.
	errMonitorNotStarted = errors.New("monitor not started")
)

// Monitor is an active component that listens to LND channel events and records
// them in the database.
type Monitor struct {
	started atomic.Bool

	// lnd is the lnd client that the monitor will use to subscribe to
	// channel events.
	lnd lndclient.LightningClient

	// store is the channel events store that the monitor will use to record
	// channel events.
	store *Store

	wg   sync.WaitGroup
	quit chan struct{}
}

// NewMonitor creates a new channel events monitor.
func NewMonitor(lnd lndclient.LightningClient, store *Store) *Monitor {
	return &Monitor{
		lnd:   lnd,
		store: store,
	}
}

// Start starts the channel events monitor.
func (m *Monitor) Start(ctx context.Context) error {
	if !m.started.CompareAndSwap(false, true) {
		return errMonitorAlreadyStarted
	}

	log.Info("Starting channel events monitor")

	m.quit = make(chan struct{})

	m.wg.Add(1)
	go m.monitorLoop(ctx)

	return nil
}

// Stop stops the channel events monitor.
func (m *Monitor) Stop() error {
	if !m.started.Load() {
		return errMonitorNotStarted
	}

	log.Info("Stopping channel events monitor")

	close(m.quit)
	m.wg.Wait()

	return nil
}

// monitorLoop is the main loop of the channel events monitor.
func (m *Monitor) monitorLoop(ctx context.Context) {
	defer m.wg.Done()

	log.Info("Channel events monitor started")

	// TODO: First, wait for lnd to be fully synced.

	// Initial state sync.
	if err := m.initialSync(ctx); err != nil {
		log.Errorf("error during initial sync: %v", err)
		// We'll continue anyway, maybe the subscription will work.
	}

	// Subscribe to channel events.
	eventChan, errChan, err := m.lnd.SubscribeChannelEvents(ctx)
	if err != nil {
		log.Errorf("error subscribing to channel events: %v", err)
		return
	}

	for {
		select {
		case event, ok := <-eventChan:
			if !ok {
				log.Info("Channel event stream closed")
				return
			}
			if err := m.handleChannelEvent(ctx, event); err != nil {
				log.Errorf("error handling channel event: %v", err)
			}

		case err, ok := <-errChan:
			if !ok {
				log.Info("Channel event error stream closed")
				return
			}
			log.Errorf("error from channel event subscription: %v", err)
			return

		case <-m.quit:
			log.Info("Channel events monitor stopping")
			return

		case <-ctx.Done():
			log.Info("Channel events monitor stopping")
			return
		}
	}
}

// initialSync performs an initial sync of the channel state.
func (m *Monitor) initialSync(ctx context.Context) error {
	log.Info("Performing initial sync of channel state")

	channels, err := m.lnd.ListChannels(ctx, false, false)
	if err != nil {
		return fmt.Errorf("error listing channels: %w", err)
	}

	for _, channel := range channels {
		if err := m.addChannel(ctx, &channel); err != nil {
			log.Errorf("error adding channel %s: %v",
				channel.ChannelPoint, err)
			continue
		}

		dbChan, err := m.store.GetChannel(ctx, channel.ChannelPoint)
		if err != nil {
			log.Errorf("error getting channel %s from db: %v",
				channel.ChannelPoint, err)
			continue
		}

		eventType := EventTypeOffline
		if channel.Active {
			eventType = EventTypeOnline
		}

		if err := m.store.AddChannelEvent(ctx, &ChannelEvent{
			ChannelID: dbChan.ID,
			EventType: eventType,
		}); err != nil {
			log.Errorf("error adding event for channel %s: %v",
				channel.ChannelPoint, err)
		}

		if err := m.store.AddChannelEvent(ctx, &ChannelEvent{
			ChannelID:     dbChan.ID,
			EventType:     EventTypeUpdate,
			LocalBalance:  fn.Some(channel.LocalBalance),
			RemoteBalance: fn.Some(channel.RemoteBalance),
		}); err != nil {
			log.Errorf("error adding event for channel %s: %v",
				channel.ChannelPoint, err)
		}
	}

	return nil
}

// addChannel adds a channel and its peer to the store.
func (m *Monitor) addChannel(ctx context.Context,
	channel *lndclient.ChannelInfo) error {

	var aliasOpt fn.Option[string]
	if channel.PeerAlias != "" {
		aliasOpt = fn.Some(channel.PeerAlias)
	}

	peerID, err := m.store.AddPeer(ctx, &Peer{
		PubKey: channel.PubKeyBytes.String(),
		Alias:  aliasOpt,
	})
	if err != nil {
		return fmt.Errorf("error adding peer %s: %w",
			channel.PubKeyBytes, err)
	}

	var shortChanID fn.Option[int64]
	if channel.ChannelID != 0 {
		shortChanID = fn.Some(int64(channel.ChannelID))
	}

	_, err = m.store.AddChannel(ctx, &Channel{
		ChannelPoint:   channel.ChannelPoint,
		ShortChannelID: shortChanID,
		PeerID:         peerID,
	})
	if err != nil {
		// If the channel already exists, we can ignore the error.
		dbChan, getErr := m.store.GetChannel(ctx, channel.ChannelPoint)
		if getErr == nil {
			// If the short channel ID is not set, update it.
			if !dbChan.ShortChannelID.IsSome() && channel.ChannelID != 0 {
				log.Infof("Updating short channel ID for %s",
					channel.ChannelPoint)
				return m.store.UpdateChannel(
					ctx, dbChan.ID, int64(channel.ChannelID),
				)
			}
			return nil
		}
		return fmt.Errorf("error adding channel %s: %w",
			channel.ChannelPoint, err)
	}

	log.Infof("Added channel %s to db", channel.ChannelPoint)

	return nil
}

// handleChannelEvent handles a single channel event.
func (m *Monitor) handleChannelEvent(ctx context.Context,
	event *lndclient.ChannelEventUpdate) error {

	switch event.UpdateType {
	case lndclient.OpenChannelUpdate:
		openChannel := event.OpenedChannelInfo
		if openChannel == nil {
			return fmt.Errorf("open_channel event is nil")
		}

		log.Infof("Handling OPEN_CHANNEL event for %s",
			openChannel.ChannelPoint)

		// The lnrpc.Channel does not have the alias, so we need to
		// get it separately.
		if err := m.addChannel(ctx, openChannel); err != nil {
			return err
		}

		// Now add the online and update events.
		dbChan, err := m.store.GetChannel(ctx, openChannel.ChannelPoint)
		if err != nil {
			return err
		}

		if err := m.store.AddChannelEvent(ctx, &ChannelEvent{
			ChannelID: dbChan.ID,
			EventType: EventTypeOnline,
		}); err != nil {
			return err
		}

		return m.store.AddChannelEvent(ctx, &ChannelEvent{
			ChannelID:     dbChan.ID,
			EventType:     EventTypeUpdate,
			LocalBalance:  fn.Some(openChannel.LocalBalance),
			RemoteBalance: fn.Some(openChannel.RemoteBalance),
		})

	case lndclient.ClosedChannelUpdate:
		// Not implemented yet.
		log.Infof("Ignoring event CLOSED_CHANNEL")
		return nil

	case lndclient.ActiveChannelUpdate:
		return m.addOnlineEvent(ctx, event.ChannelPoint.String())

	case lndclient.InactiveChannelUpdate:
		return m.addOfflineEvent(ctx, event.ChannelPoint.String())

	case lndclient.PendingOpenChannelUpdate:
		// Not implemented yet.
		log.Infof("Ignoring event PENDING_OPEN_CHANNEL")
		return nil
	}

	return nil
}

// addOnlineEvent adds an online event for a channel.
func (m *Monitor) addOnlineEvent(ctx context.Context,
	channelPoint string) error {

	channel, err := m.store.GetChannel(ctx, channelPoint)
	if err != nil {
		return fmt.Errorf("error getting channel %s: %w", channelPoint,
			err)
	}

	log.Infof("Adding online event for channel %s", channelPoint)

	return m.store.AddChannelEvent(ctx, &ChannelEvent{
		ChannelID: channel.ID,
		EventType: EventTypeOnline,
	})
}

// addOfflineEvent adds an offline event for a channel.
func (m *Monitor) addOfflineEvent(ctx context.Context,
	channelPoint string) error {

	channel, err := m.store.GetChannel(ctx, channelPoint)
	if err != nil {
		return fmt.Errorf("error getting channel %s: %w", channelPoint,
			err)
	}

	log.Infof("Adding offline event for channel %s", channelPoint)

	return m.store.AddChannelEvent(ctx, &ChannelEvent{
		ChannelID: channel.ID,
		EventType: EventTypeOffline,
	})
}
