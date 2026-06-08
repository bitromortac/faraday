package frdrpc

import (
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// fwdKey returns a distinct 33-byte compressed-pubkey hex string for n. Keys
// sort ascending in n, matching the byte ordering the encoder applies.
func fwdKey(n int) string {
	return fmt.Sprintf("02%064x", n)
}

// pair is one expected decoded entry, flattened from the nested result map for
// easy comparison.
type pair struct {
	in      string
	out     string
	ability ForwardingAbility
}

// TestForwardingAbilityCodecRoundTrip verifies that encoding a matrix of peer
// abilities and decoding it back preserves the raw facts, emits peers in sorted
// order, and drops pairs that carry no signal.
func TestForwardingAbilityCodecRoundTrip(t *testing.T) {
	const startTime, endTime int64 = 1000, 4600

	tests := []struct {
		name      string
		abilities map[string]map[string]ForwardingAbility
		wantPeers []string
		wantPairs []pair
	}{
		{
			// A zero-uptime pair that still moved volume must
			// survive so the consumer keeps the demand signal.
			name: "retains zero-uptime volume",
			abilities: map[string]map[string]ForwardingAbility{
				fwdKey(1): {
					fwdKey(2): {
						EffectiveUptimeS: 2880,
						ForwardedSat:     1500,
					},
				},
				fwdKey(2): {
					fwdKey(1): {
						EffectiveUptimeS: 0,
						ForwardedSat:     2500,
					},
				},
			},
			wantPeers: []string{fwdKey(1), fwdKey(2)},
			wantPairs: []pair{
				{fwdKey(1), fwdKey(2), ForwardingAbility{2880, 1500}},
				{fwdKey(2), fwdKey(1), ForwardingAbility{0, 2500}},
			},
		},
		{
			name: "sorts peers ascending",
			abilities: map[string]map[string]ForwardingAbility{
				fwdKey(3): {
					fwdKey(1): {EffectiveUptimeS: 5},
				},
				fwdKey(1): {
					fwdKey(2): {ForwardedSat: 7},
				},
			},
			wantPeers: []string{fwdKey(1), fwdKey(2), fwdKey(3)},
			wantPairs: []pair{
				{fwdKey(3), fwdKey(1), ForwardingAbility{5, 0}},
				{fwdKey(1), fwdKey(2), ForwardingAbility{0, 7}},
			},
		},
		{
			name: "drops zero-signal pairs",
			abilities: map[string]map[string]ForwardingAbility{
				fwdKey(1): {
					fwdKey(2): {
						EffectiveUptimeS: 0,
						ForwardedSat:     0,
					},
				},
			},
			wantPeers: []string{},
			wantPairs: []pair{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := EncodeForwardingAbility(
				tc.abilities, startTime, endTime,
			)
			require.NoError(t, err)
			require.Equal(t, startTime, resp.StartTime)
			require.Equal(t, endTime, resp.EndTime)

			gotPeers := make([]string, len(resp.Peers))
			for i, p := range resp.Peers {
				gotPeers[i] = hex.EncodeToString(p)
			}
			require.Equal(t, tc.wantPeers, gotPeers)

			decoded, err := DecodeForwardingAbility(resp)
			require.NoError(t, err)

			got := make(map[string]ForwardingAbility)
			for in, outMap := range decoded {
				for out, ability := range outMap {
					got[in+"->"+out] = ability
				}
			}
			require.Len(t, got, len(tc.wantPairs))
			for _, wp := range tc.wantPairs {
				require.Equal(
					t, wp.ability, got[wp.in+"->"+wp.out],
				)
			}
		})
	}
}

// TestForwardingAbilityDecodeBadIndex verifies that a packed index referencing
// a peer beyond the decoded peer list is rejected rather than silently mapped.
func TestForwardingAbilityDecodeBadIndex(t *testing.T) {
	resp := &ForwardingAbilityResponse{
		Peers: [][]byte{{1, 2, 3}},
		Entries: []*ForwardingAbilityEntry{
			{
				// Out index 1 is out of bounds for a single peer.
				PackedIdx:        (0 << 16) | 1,
				EffectiveUptimeS: 3600,
				ForwardedSat:     1000,
			},
		},
	}

	_, err := DecodeForwardingAbility(resp)
	require.ErrorContains(t, err, "peer index out of bounds")
}

// TestForwardingAbilityEncodePeerCap verifies that a peer set too large to
// address with packed_idx is rejected loudly instead of overflowing an index
// into the wrong peer pair.
func TestForwardingAbilityEncodePeerCap(t *testing.T) {
	outMap := make(map[string]ForwardingAbility)
	for i := 1; i <= maxPackedPeers+1; i++ {
		outMap[fwdKey(i)] = ForwardingAbility{EffectiveUptimeS: 1}
	}
	abilities := map[string]map[string]ForwardingAbility{
		fwdKey(0): outMap,
	}

	_, err := EncodeForwardingAbility(abilities, 0, 1)
	require.ErrorContains(t, err, "exceeds")
}

// TestForwardingAbilityEncodeNormalizesCase verifies that a peer appearing in
// mixed hex case collapses to a single index rather than producing a duplicate
// peer entry.
func TestForwardingAbilityEncodeNormalizesCase(t *testing.T) {
	// Use a key with hex letters so its upper- and lower-case forms are
	// genuinely distinct map keys.
	peer := fwdKey(0xabcdef)

	abilities := map[string]map[string]ForwardingAbility{
		strings.ToUpper(peer): {
			fwdKey(2): {ForwardedSat: 20},
		},
		peer: {
			fwdKey(3): {ForwardedSat: 40},
		},
	}

	resp, err := EncodeForwardingAbility(abilities, 0, 1)
	require.NoError(t, err)

	// The upper- and lower-case forms of the shared peer must dedup to one
	// index, leaving exactly three distinct peers.
	require.Len(t, resp.Peers, 3)

	decoded, err := DecodeForwardingAbility(resp)
	require.NoError(t, err)
	require.Equal(
		t, ForwardingAbility{ForwardedSat: 20},
		decoded[peer][fwdKey(2)],
	)
	require.Equal(
		t, ForwardingAbility{ForwardedSat: 40},
		decoded[peer][fwdKey(3)],
	)
}
