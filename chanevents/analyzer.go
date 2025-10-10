package chanevents

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/lightninglabs/lndclient"
)

// EventsSource is an interface that provides access to channel events.
type EventsSource interface {
	// GetLatestChannelUpdateBefore returns the latest channel event before a
	// given time. If no event is found, nil is returned.
	GetLatestChannelUpdateBefore(ctx context.Context, channelID int64,
		before time.Time) (*ChannelEvent, error)

	// GetChannelEvents returns all events for a channel within a given
	// time range.
	GetChannelEvents(ctx context.Context, channelID int64,
		startTime, endTime time.Time) ([]*ChannelEvent, error)

	GetChannelByShortChanID(ctx context.Context, shortChannelID uint64) (*Channel, error)

	// ScidToPeerMap returns a map from short channel ID to peer public key.
	ScidToPeerMap(ctx context.Context) (map[uint64]string, error)
}

// channelState represents the state of a channel at a given point in time.
type channelState struct {
	online        bool
	localBalance  btcutil.Amount
	remoteBalance btcutil.Amount
}

// ForwardingAbility defines the summary of the forwarding ability of a peer pair.
type ForwardingAbility struct {
	// Velocity is the forwarding velocity in sat/s during effective uptime.
	Velocity float64

	// UptimeFraction is the fraction of time the channel was considered
	// effective.
	UptimeFraction float64
}

// ForwardingAnalyzer is a struct that can be used to analyze the forwarding
// ability of a pair of channels.
type ForwardingAnalyzer struct {
	store EventsSource
	lnd   lndclient.LndServices
}

// NewForwardingAnalyzer creates a new ForwardingAnalyzer.
func NewForwardingAnalyzer(store EventsSource,
	lnd lndclient.LndServices) *ForwardingAnalyzer {

	return &ForwardingAnalyzer{
		store: store,
		lnd:   lnd,
	}
}

// peerPair is a struct that represents a pair of peers.
type peerPair struct {
	peerIn  string
	peerOut string
}

// EffectiveUptime calculates the effective uptime and forwarding velocity for
// all peer pairs.
func (a *ForwardingAnalyzer) EffectiveUptime(ctx context.Context,
	startTime, endTime time.Time, fwdPct float64,
	threshold btcutil.Amount) (
	map[string]map[string]ForwardingAbility, error) {

	log.Debugf("Calculating effective uptime from %v to %v with "+
		"forward percentile %.2f and threshold %d sat",
		startTime, endTime, fwdPct, threshold)

	// Fetch a map of historical channels to peers.
	scidToPeer, err := a.store.ScidToPeerMap(ctx)
	if err != nil {
		return nil, err
	}
	log.Debugf("Found %d historical channels", len(scidToPeer))

	// Get all forwarding events in the time range and process them.
	successfulForwards, channelPeersConsidered, err := a.getForwardingData(
		ctx, startTime, endTime, scidToPeer,
	)
	if err != nil {
		return nil, err
	}
	log.Debugf("Found %d peer pairs with successful forwards",
		len(successfulForwards))

	// Add currently open channels to the set of channels to consider.
	err = a.addOpenChannels(ctx, channelPeersConsidered)
	if err != nil {
		return nil, err
	}

	// For each channel considered, fetch its events and initial states,
	// and group them by peer.
	peerEvents, initialStates, err := a.getPeerChannelData(
		ctx, startTime, endTime, channelPeersConsidered,
	)
	if err != nil {
		return nil, err
	}
	log.Debugf("Fetched balance events for %d peers", len(peerEvents))

	// Calculate forwarding ability for all peer pairs.
	return calculateAllPairsUptime(
		startTime, endTime, fwdPct, threshold, successfulForwards,
		initialStates, peerEvents,
	)
}

// getForwardingData fetches and processes forwarding history to identify
// successful forwards and channels involved.
func (a *ForwardingAnalyzer) getForwardingData(ctx context.Context,
	startTime, endTime time.Time, scidToPeer map[uint64]string) (
	map[peerPair][]btcutil.Amount, map[uint64]string, error) {

	// Get all forwarding events in the time range.
	fwds, err := a.lnd.Client.ForwardingHistory(ctx,
		lndclient.ForwardingHistoryRequest{
			StartTime: startTime,
			EndTime:   endTime,
		})
	if err != nil {
		return nil, nil, err
	}
	log.Debugf("Found %d forwarding events", len(fwds.Events))

	// We only consider channels that either are open or that appeared in
	// the forwarding history during the time range.
	channelPeersConsidered := make(map[uint64]string)

	// Process forwards to build a map of successful forward amounts.
	successfulForwards := make(map[peerPair][]btcutil.Amount)
	for _, fwd := range fwds.Events {
		inPeer, ok := scidToPeer[fwd.ChannelIn]
		if !ok {
			log.Warnf("Could not find peer for incoming channel %d",
				fwd.ChannelIn)
			continue
		}

		outPeer, ok := scidToPeer[fwd.ChannelOut]
		if !ok {
			log.Warnf("Could not find peer for outgoing channel %d",
				fwd.ChannelOut)
			continue
		}

		channelPeersConsidered[fwd.ChannelIn] = inPeer
		channelPeersConsidered[fwd.ChannelOut] = outPeer

		pair := peerPair{
			peerIn:  inPeer,
			peerOut: outPeer,
		}

		amt := fwd.AmountMsatOut.ToSatoshis()
		successfulForwards[pair] = append(successfulForwards[pair], amt)
	}

	return successfulForwards, channelPeersConsidered, nil
}

// addOpenChannels adds currently open channels to the set of channels to
// consider for uptime analysis.
func (a *ForwardingAnalyzer) addOpenChannels(ctx context.Context,
	channelPeersConsidered map[uint64]string) error {

	// We are further interested in peers with currently open channels,
	// which is why we fetch the open channels from lnd.
	openChannels, err := a.lnd.Client.ListChannels(ctx, false, false)
	if err != nil {
		return err
	}

	// We add the open channels to the considered channels set.
	for _, channel := range openChannels {
		channelPeersConsidered[channel.ChannelID] =
			channel.PubKeyBytes.String()
	}

	return nil
}

// getPeerChannelData fetches channel events and initial states for all
// considered peers.
func (a *ForwardingAnalyzer) getPeerChannelData(ctx context.Context,
	startTime, endTime time.Time, channelPeersConsidered map[uint64]string) (
	map[string][]*ChannelEvent, map[string]map[int64]*channelState, error) {

	// For each channel considered, fetch its events in the time range and
	// group them by peer.
	peerEvents := make(map[string][]*ChannelEvent)
	initialStates := make(map[string]map[int64]*channelState)
	for scid, peerPubKey := range channelPeersConsidered {
		channel, err := a.store.GetChannelByShortChanID(ctx, scid)
		if err != nil {
			return nil, nil, err
		}

		// We keep track of the initial channel states for each peer's
		// channels.
		state, err := a.getInitialChannelState(
			ctx,
			channel.ID,
			startTime,
		)
		if err != nil {
			return nil, nil, err
		}
		if _, ok := initialStates[peerPubKey]; !ok {
			initialStates[peerPubKey] = make(map[int64]*channelState)
		}
		initialStates[peerPubKey][channel.ID] = state

		// We keep track of all events for each peer's channels.
		events, ok := peerEvents[peerPubKey]
		if !ok {
			events = []*ChannelEvent{}
		}

		chanEvents, err := a.store.GetChannelEvents(
			ctx, channel.ID, startTime, endTime,
		)
		if err != nil {
			return nil, nil, err
		}

		events = append(events, chanEvents...)
		peerEvents[peerPubKey] = events
	}

	return peerEvents, initialStates, nil
}

// calculateAllPairsUptime calculates the forwarding ability for all pairs of
// peers.
func calculateAllPairsUptime(startTime,
	endTime time.Time, fwdPct float64, threshold btcutil.Amount,
	successfulForwards map[peerPair][]btcutil.Amount,
	initialStates map[string]map[int64]*channelState,
	peerEvents map[string][]*ChannelEvent) (
	map[string]map[string]ForwardingAbility, error) {

	results := make(map[string]map[string]ForwardingAbility)

	for peerIn, inStates := range initialStates {
		for peerOut, outStates := range initialStates {
			pair := peerPair{
				peerIn:  peerIn,
				peerOut: peerOut,
			}

			// Get the successful forward amounts for this pair. It will
			// be nil if there were no forwards.
			successAmts := successfulForwards[pair]

			ability, err := calculatePairEffectiveUptime(
				startTime, endTime, fwdPct, threshold,
				successAmts, inStates, outStates,
				peerEvents[pair.peerIn], peerEvents[pair.peerOut],
			)
			if err != nil {
				return nil, err
			}

			if _, ok := results[pair.peerIn]; !ok {
				results[pair.peerIn] = make(map[string]ForwardingAbility)
			}
			results[pair.peerIn][pair.peerOut] = *ability
		}
	}

	return results, nil
}

// getInitialChannelState returns the state of a channel at a given start time
// by looking at the last event before that time.
func (a *ForwardingAnalyzer) getInitialChannelState(ctx context.Context,
	channelID int64, startTime time.Time) (*channelState, error) {

	// First, get the latest channel update event before the start time.
	// This event determines the initial balances of the channel.
	lastUpdate, err := a.store.GetLatestChannelUpdateBefore(
		ctx, channelID, startTime,
	)
	if err != nil {
		return nil, err
	}

	// If there are no update events before the start time, we assume the
	// channel was offline with zero balance.
	if lastUpdate == nil {
		log.Tracef("No update event for channel %d before %v",
			channelID, startTime)
		return &channelState{online: false}, nil
	}

	// The initial state is based on this last update event. An update event
	// always implies the channel is online.
	state := &channelState{
		online: true,
	}
	lastUpdate.LocalBalance.WhenSome(func(amt btcutil.Amount) {
		state.localBalance = amt
	})
	lastUpdate.RemoteBalance.WhenSome(func(amt btcutil.Amount) {
		state.remoteBalance = amt
	})

	// Now, we need to check if there were any other events between the last
	// update and the start time. These could be online/offline events that
	// change the online status of the channel.
	otherEvents, err := a.store.GetChannelEvents(
		ctx, channelID, lastUpdate.Timestamp, startTime,
	)
	if err != nil {
		return nil, err
	}

	// The query above is inclusive of the last update event, so we need to
	// filter it out.
	var filteredEvents []*ChannelEvent
	for _, event := range otherEvents {
		if event.ID != lastUpdate.ID {
			filteredEvents = append(filteredEvents, event)
		}
	}
	otherEvents = filteredEvents

	// The events from the store are already sorted by timestamp. We can now
	// just replay the events on top of the initial state.
	for _, event := range otherEvents {
		switch event.EventType {
		case EventTypeOffline:
			state.online = false

		case EventTypeOnline:
			state.online = true

		// We should not see any update events here, as we already got
		// the latest one before the start time.
		case EventTypeUpdate:
			return nil, errors.New("found unexpected update " +
				"event between last update and start time")

		default:
			return nil, errors.New("found unknown event type")
		}
	}

	return state, nil
}

func calculatePairEffectiveUptime(
	startTime, endTime time.Time, forwardPercentile float64,
	thresholdAmount btcutil.Amount, successAmts []btcutil.Amount, inStates,
	outStates map[int64]*channelState, inEvents,
	outEvents []*ChannelEvent) (*ForwardingAbility, error) {

	log.Trace("Calculating effective uptime with initial inStates")
	for chanID, state := range inStates {
		log.Tracef(" In chanID=%d: online=%v, local=%d, remote=%d",
			chanID, state.online, state.localBalance,
			state.remoteBalance)
	}
	log.Trace("Calculating effective uptime with initial outStates")

	// Create copies of the channel states to avoid mutating the originals.
	inStatesCopy := make(map[int64]*channelState, len(inStates))
	for chanID, state := range inStates {
		inStatesCopy[chanID] = &channelState{
			online:        state.online,
			localBalance:  state.localBalance,
			remoteBalance: state.remoteBalance,
		}
	}
	inStates = inStatesCopy

	outStatesCopy := make(map[int64]*channelState, len(outStates))
	for chanID, state := range outStates {
		outStatesCopy[chanID] = &channelState{
			online:        state.online,
			localBalance:  state.localBalance,
			remoteBalance: state.remoteBalance,
		}
	}
	outStates = outStatesCopy

	// Determine the threshold amount. If we have successful forwards, we'll
	// use the larger of the user-specified threshold and a percentile of
	// the successful forward amounts.
	var finalThreshold btcutil.Amount
	if len(successAmts) > 0 {
		p, err := percentile(successAmts, forwardPercentile)
		if err != nil {
			return nil, err
		}
		finalThreshold = max(p, thresholdAmount)
	} else {
		finalThreshold = thresholdAmount
	}
	log.Tracef("Using final forwarding liquidity threshold: %d sat",
		finalThreshold)

	// Merge and sort all events chronologically to process them in order.
	// We must allocate a new slice to avoid mutating the backing array of
	// inEvents, which is reused across iterations.
	mergedEvents := make([]*ChannelEvent, 0, len(inEvents)+len(outEvents))
	mergedEvents = append(mergedEvents, inEvents...)
	mergedEvents = append(mergedEvents, outEvents...)

	sort.SliceStable(mergedEvents, func(i, j int) bool {
		return mergedEvents[i].Timestamp.Before(
			mergedEvents[j].Timestamp,
		)
	})

	var totalEffectiveUptime time.Duration
	lastTimestamp := startTime

	// Iterate through the sorted events to calculate total effective uptime.
	for _, event := range mergedEvents {
		log.Tracef("Processing event: chanID=%d, type=%v, time=%v",
			event.ChannelID, event.EventType, event.Timestamp)

		intervalDuration := event.Timestamp.Sub(lastTimestamp)
		log.Tracef(" Interval duration: %v", intervalDuration)

		// Before processing the event, calculate the forwarding
		// liquidity for the interval. If it exceeds our threshold,
		// we add the interval's duration to the total effective
		// uptime.
		fwdLiquidity := calculateFwdLiquidity(inStates, outStates)
		log.Tracef(" Forwarding liquidity: %d, threshold: %d",
			fwdLiquidity, finalThreshold)
		if intervalDuration > 0 && fwdLiquidity > finalThreshold {
			totalEffectiveUptime += intervalDuration
		}

		// Update the appropriate channel state based on the event.
		var modifiedState *channelState
		if state, ok := inStates[event.ChannelID]; ok {
			modifiedState = state
		} else {
			modifiedState = outStates[event.ChannelID]
		}

		switch event.EventType {
		case EventTypeOffline:
			modifiedState.online = false
		case EventTypeOnline:
			modifiedState.online = true
		case EventTypeUpdate:
			modifiedState.online = true
			event.LocalBalance.WhenSome(func(amt btcutil.Amount) {
				modifiedState.localBalance = amt
			})
			event.RemoteBalance.WhenSome(func(amt btcutil.Amount) {
				modifiedState.remoteBalance = amt
			})
		}

		lastTimestamp = event.Timestamp
	}

	// After the loop, account for the final interval until the end time.
	lastIntervalDuration := endTime.Sub(lastTimestamp)
	log.Tracef("Final interval duration: %v", lastIntervalDuration)
	if lastIntervalDuration > 0 {
		fwdLiquidity := calculateFwdLiquidity(inStates, outStates)
		log.Tracef(" Final forwarding liquidity: %d, threshold: %d",
			fwdLiquidity, finalThreshold)
		if fwdLiquidity > finalThreshold {
			totalEffectiveUptime += lastIntervalDuration
		}
	}

	// Calculate the total amount from successful forwards.
	var totalSuccessfulAmount btcutil.Amount
	for _, amt := range successAmts {
		totalSuccessfulAmount += amt
	}

	// If there was no effective uptime, we can't calculate velocity.
	if totalEffectiveUptime == 0 {
		return &ForwardingAbility{
			// Fallback to total observed successful amount if we can't
			// determine a velocity.
			Velocity:       float64(totalSuccessfulAmount),
			UptimeFraction: 0,
		}, nil
	}

	log.Tracef("Total effective uptime: %v", totalEffectiveUptime)
	log.Tracef("Total successful forwarded amount: %d sat", totalSuccessfulAmount)
	log.Tracef("Total duration: %v", endTime.Sub(startTime))
	log.Tracef("Uptime fraction: %.4f",
		float64(totalEffectiveUptime)/float64(endTime.Sub(startTime)))

	// Calculate velocity and uptime fraction.
	velocity := float64(totalSuccessfulAmount) / totalEffectiveUptime.Seconds()
	totalDuration := endTime.Sub(startTime)
	uptimeFraction := float64(totalEffectiveUptime) / float64(totalDuration)

	return &ForwardingAbility{
		Velocity:       velocity,
		UptimeFraction: uptimeFraction,
	}, nil
}

// calculateFwdLiquidity determines the forwarding liquidity based on the
// aggregated state of the channels for the incoming and outgoing peers.
func calculateFwdLiquidity(inStates, outStates map[int64]*channelState) btcutil.Amount {
	var totalInRemoteBalance btcutil.Amount
	for _, state := range inStates {
		if state.online {
			totalInRemoteBalance += state.remoteBalance
		}
	}

	var totalOutLocalBalance btcutil.Amount
	for _, state := range outStates {
		if state.online {
			totalOutLocalBalance += state.localBalance
		}
	}

	return min(totalInRemoteBalance, totalOutLocalBalance)
}

// percentile calculates the value at a given percentile in a slice of amounts.
func percentile(amts []btcutil.Amount, p float64) (btcutil.Amount, error) {
	if len(amts) == 0 {
		return 0, nil
	}
	if p < 0 || p > 100 {
		return 0, errors.New("percentile must be between 0 and 100")
	}

	// Create a copy to avoid modifying the original slice.
	sortedAmts := make([]btcutil.Amount, len(amts))
	copy(sortedAmts, amts)

	sort.Slice(sortedAmts, func(i, j int) bool {
		return sortedAmts[i] < sortedAmts[j]
	})

	index := (p / 100) * float64(len(sortedAmts)-1)
	return sortedAmts[int(index)], nil
}
