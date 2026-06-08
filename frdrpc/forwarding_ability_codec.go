package frdrpc

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
)

// maxPackedPeers is the largest peer set a response can address. packed_idx
// splits a uint32 into two 16-bit indices (in << 16 | out), so each direction
// can reference at most 65535 distinct peers.
const maxPackedPeers = 1<<16 - 1

// ForwardingAbility is a client-facing mirror of the raw forwarding facts for
// one direction of a peer pair over the analysis window. Derived rates and
// categories are left to the consumer.
type ForwardingAbility struct {
	// EffectiveUptimeS is the seconds the pair held at least the requested
	// liquidity floor of directional forwardable liquidity over the window.
	// The value is whole seconds: sub-second uptime floors to zero, so a
	// pair that forwarded volume over a fleeting qualifying window can
	// report a zero uptime alongside a non-zero ForwardedSat.
	EffectiveUptimeS int64

	// ForwardedSat is the total successfully forwarded amount over the
	// window, in satoshis.
	ForwardedSat int64
}

// hasSignal reports whether the ability carries a non-zero forwarding fact and
// therefore warrants an entry on the wire. Pairs without a signal are dropped
// to keep the response sparse.
func (a ForwardingAbility) hasSignal() bool {
	return a.EffectiveUptimeS > 0 || a.ForwardedSat > 0
}

// EncodeForwardingAbility serializes a nested map of peer forwarding abilities
// into a memory-efficient sparse gRPC response over [startTime, endTime]. To
// optimize payload size, it deduplicates public keys and packs peer pairs into
// 32-bit indices, emitting only pairs with non-zero effective uptime or
// forwarded volume.
func EncodeForwardingAbility(
	abilities map[string]map[string]ForwardingAbility,
	startTime, endTime int64) (*ForwardingAbilityResponse, error) {

	// First, find all unique peers involved in entries that carry a signal.
	// Keys are normalized to lower-case hex so a peer that appears in mixed
	// case across entries collapses to a single index rather than being
	// silently dropped at lookup time.
	peerSet := make(map[string]struct{})
	for inPeer, outMap := range abilities {
		for outPeer, ability := range outMap {
			if !ability.hasSignal() {
				continue
			}

			peerSet[strings.ToLower(inPeer)] = struct{}{}
			peerSet[strings.ToLower(outPeer)] = struct{}{}
		}
	}

	// Decode to raw bytes and sort.
	var rawPeers [][]byte
	for peerHex := range peerSet {
		b, err := hex.DecodeString(peerHex)
		if err != nil {
			return nil, err
		}
		rawPeers = append(rawPeers, b)
	}

	sort.Slice(
		rawPeers,
		func(i, j int) bool {
			return bytes.Compare(rawPeers[i], rawPeers[j]) < 0
		},
	)

	// Peer indices occupy 16 bits each in packed_idx, so the set must stay
	// within maxPackedPeers. Beyond it, an index would overflow its field
	// and silently decode to the wrong peer pair, so fail loudly instead.
	if len(rawPeers) > maxPackedPeers {
		return nil, fmt.Errorf("peer set of %d exceeds the %d "+
			"addressable by packed_idx", len(rawPeers),
			maxPackedPeers)
	}

	// Create map for index lookup using normalized lowercase hex strings.
	peerIndex := make(map[string]uint32)
	for idx, b := range rawPeers {
		peerIndex[hex.EncodeToString(b)] = uint32(idx)
	}

	// Build the entries.
	var entries []*ForwardingAbilityEntry
	for inPeer, outMap := range abilities {
		inIdx, okIn := peerIndex[strings.ToLower(inPeer)]
		if !okIn {
			continue
		}

		for outPeer, ability := range outMap {
			if !ability.hasSignal() {
				continue
			}

			outIdx, okOut := peerIndex[strings.ToLower(outPeer)]
			if !okOut {
				continue
			}

			packed := (inIdx << 16) | outIdx
			entries = append(entries, &ForwardingAbilityEntry{
				PackedIdx:        packed,
				EffectiveUptimeS: ability.EffectiveUptimeS,
				ForwardedSat:     ability.ForwardedSat,
			})
		}
	}

	// Sort entries by packed_idx for deterministic output and testability.
	sort.Slice(
		entries,
		func(i, j int) bool {
			return entries[i].PackedIdx < entries[j].PackedIdx
		},
	)

	return &ForwardingAbilityResponse{
		Peers:     rawPeers,
		Entries:   entries,
		StartTime: startTime,
		EndTime:   endTime,
	}, nil
}

// DecodeForwardingAbility reconstructs the nested map of peer forwarding
// abilities from a sparse packed gRPC response. It validates packed indices
// against the decoded peer list to prevent out-of-bounds errors.
func DecodeForwardingAbility(resp *ForwardingAbilityResponse) (
	map[string]map[string]ForwardingAbility, error) {

	result := make(map[string]map[string]ForwardingAbility)
	if resp == nil {
		return result, nil
	}

	numPeers := uint32(len(resp.Peers))

	for _, entry := range resp.Entries {
		inIdx := entry.PackedIdx >> 16
		outIdx := entry.PackedIdx & 0xffff

		if inIdx >= numPeers || outIdx >= numPeers {
			return nil, errors.New("decoded peer index out of " +
				"bounds")
		}

		inPeer := hex.EncodeToString(resp.Peers[inIdx])
		outPeer := hex.EncodeToString(resp.Peers[outIdx])

		if _, ok := result[inPeer]; !ok {
			result[inPeer] = make(map[string]ForwardingAbility)
		}

		result[inPeer][outPeer] = ForwardingAbility{
			EffectiveUptimeS: entry.EffectiveUptimeS,
			ForwardedSat:     entry.ForwardedSat,
		}
	}

	return result, nil
}
