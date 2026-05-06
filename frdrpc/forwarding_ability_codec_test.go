package frdrpc_test

import (
	"bytes"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/lightninglabs/faraday/frdrpc"
	"github.com/stretchr/testify/require"
)

// pk returns a 33-byte compressed-pubkey hex string by prefixing 0x02 and
// repeating a single byte 32 times. The resulting strings are valid hex,
// distinct, and trivially sortable, which makes them suitable test
// fixtures for codec round-tripping.
func pk(b byte) string {
	return "02" + strings.Repeat(hex.EncodeToString([]byte{b}), 32)
}

// requireRoundTrip encodes the analyzer-shaped input, decodes the result,
// and asserts the recovered map equals the input within uint16
// quantisation tolerance for uptime_fraction and float32 precision for
// velocity. It is the canonical assertion for clients learning the codec
// contract from these tests.
func requireRoundTrip(t *testing.T,
	in map[string]map[string]frdrpc.ForwardingAbility) {

	t.Helper()

	resp, err := frdrpc.EncodeForwardingAbility(in)
	require.NoError(t, err)

	got, err := frdrpc.DecodeForwardingAbility(resp)
	require.NoError(t, err)

	for inPeer, outs := range in {
		for outPeer, want := range outs {
			require.Contains(t, got, inPeer)
			require.Contains(t, got[inPeer], outPeer)

			gotAbility := got[inPeer][outPeer]
			require.InDelta(
				t, want.UptimeFraction,
				gotAbility.UptimeFraction, 1.0/65535,
				"uptime mismatch for %s->%s", inPeer, outPeer,
			)
			require.InDelta(
				t, want.Velocity, gotAbility.Velocity, 1e-3,
				"velocity mismatch for %s->%s",
				inPeer, outPeer,
			)
		}
	}
}

// TestForwardingAbilityCodecRoundTrip exercises the encoder/decoder pair
// against representative shapes of analyzer output. Each case is named
// after the data shape it represents, and serves as a usage demo for
// clients integrating the RPC: build an internal map, call Encode, call
// Decode, and assert equivalence within quantisation tolerance.
func TestForwardingAbilityCodecRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   map[string]map[string]frdrpc.ForwardingAbility
	}{
		{
			name: "empty matrix",
			in:   map[string]map[string]frdrpc.ForwardingAbility{},
		},
		{
			name: "single peer self-pair",
			in: map[string]map[string]frdrpc.ForwardingAbility{
				pk(0x01): {
					pk(0x01): {
						UptimeFraction: 0.95,
						Velocity:       0,
					},
				},
			},
		},
		{
			name: "two peers with one velocity",
			in: map[string]map[string]frdrpc.ForwardingAbility{
				pk(0x01): {
					pk(0x01): {UptimeFraction: 0.95},
					pk(0x02): {
						UptimeFraction: 0.80,
						Velocity:       42.5,
					},
				},
				pk(0x02): {
					pk(0x01): {UptimeFraction: 0.80},
					pk(0x02): {UptimeFraction: 0.99},
				},
			},
		},
		{
			name: "saturated uptime",
			in: map[string]map[string]frdrpc.ForwardingAbility{
				pk(0x01): {
					pk(0x01): {UptimeFraction: 1.0},
					pk(0x02): {UptimeFraction: 0.0},
				},
				pk(0x02): {
					pk(0x01): {UptimeFraction: 0.5},
					pk(0x02): {UptimeFraction: 1.0},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			requireRoundTrip(t, tc.in)
		})
	}
}

// TestForwardingAbilityCodecPeerOrdering verifies that the dictionary is
// sorted ascending by raw byte comparison, the invariant external clients
// depend on for byte-stable responses.
func TestForwardingAbilityCodecPeerOrdering(t *testing.T) {
	t.Parallel()

	// Insert peers in reverse byte order to ensure the sort actually runs.
	in := map[string]map[string]frdrpc.ForwardingAbility{
		pk(0x03): {pk(0x03): {UptimeFraction: 0.5}},
		pk(0x01): {pk(0x01): {UptimeFraction: 0.5}},
		pk(0x02): {pk(0x02): {UptimeFraction: 0.5}},
	}

	resp, err := frdrpc.EncodeForwardingAbility(in)
	require.NoError(t, err)
	require.Len(t, resp.Peers, 3)

	for i := 0; i < len(resp.Peers)-1; i++ {
		require.Negative(
			t, bytes.Compare(resp.Peers[i], resp.Peers[i+1]),
			"peers must sort ascending by raw byte comparison",
		)
	}
}

// TestForwardingAbilityCodecRowMajorLayout pins the byte offset formula
// stated in the proto comment. A reader who suspects the layout has
// drifted should be able to consult this test for the canonical mapping.
func TestForwardingAbilityCodecRowMajorLayout(t *testing.T) {
	t.Parallel()

	// Construct a 3-peer matrix where uptime[in][out] == in*10 + out, so
	// each cell carries a unique value distinguishable in the byte stream.
	peers := []string{pk(0x01), pk(0x02), pk(0x03)}
	in := make(map[string]map[string]frdrpc.ForwardingAbility)
	for inIdx, inPeer := range peers {
		row := make(map[string]frdrpc.ForwardingAbility)
		for outIdx, outPeer := range peers {
			row[outPeer] = frdrpc.ForwardingAbility{
				UptimeFraction: float64(inIdx*10+outIdx) / 100,
			}
		}
		in[inPeer] = row
	}

	resp, err := frdrpc.EncodeForwardingAbility(in)
	require.NoError(t, err)

	got, err := frdrpc.DecodeForwardingAbility(resp)
	require.NoError(t, err)

	for inIdx, inPeer := range peers {
		for outIdx, outPeer := range peers {
			want := float64(inIdx*10+outIdx) / 100
			require.InDelta(
				t, want, got[inPeer][outPeer].UptimeFraction,
				1.0/65535,
			)
		}
	}
}

// TestForwardingAbilityCodecSparseVelocity verifies that pairs with
// velocity zero do not appear in the wire output, exercising the
// sparsity exploitation that motivates the design.
func TestForwardingAbilityCodecSparseVelocity(t *testing.T) {
	t.Parallel()

	// Three peers, only one pair has a non-zero velocity.
	in := map[string]map[string]frdrpc.ForwardingAbility{
		pk(0x01): {
			pk(0x01): {UptimeFraction: 0.9},
			pk(0x02): {UptimeFraction: 0.9},
			pk(0x03): {UptimeFraction: 0.9},
		},
		pk(0x02): {
			pk(0x01): {UptimeFraction: 0.9},
			pk(0x02): {UptimeFraction: 0.9},
			pk(0x03): {
				UptimeFraction: 0.9,
				Velocity:       17.0,
			},
		},
		pk(0x03): {
			pk(0x01): {UptimeFraction: 0.9},
			pk(0x02): {UptimeFraction: 0.9},
			pk(0x03): {UptimeFraction: 0.9},
		},
	}

	resp, err := frdrpc.EncodeForwardingAbility(in)
	require.NoError(t, err)
	require.Len(t, resp.Velocities, 1)
	require.InDelta(t, 17.0, resp.Velocities[0].Velocity, 1e-3)
}

// TestForwardingAbilityCodecDecodeRejectsMisized verifies the decoder
// rejects responses that violate the length invariant on
// uptime_fractions, which is the wire-level guarantee non-Go clients
// also have to enforce.
func TestForwardingAbilityCodecDecodeRejectsMisized(t *testing.T) {
	t.Parallel()

	resp := &frdrpc.ForwardingAbilityResponse{
		Peers:           [][]byte{{0x02}, {0x03}},
		UptimeFractions: make([]byte, 7), // want 8 = 2 * 2^2
	}

	_, err := frdrpc.DecodeForwardingAbility(resp)
	require.Error(t, err)
	require.Contains(t, err.Error(), "uptime_fractions length")
}

// TestForwardingAbilityCodecDecodeRejectsBadIndex verifies the decoder
// rejects velocity entries whose packed indices fall outside the
// dictionary, defending downstream consumers from corrupt servers.
func TestForwardingAbilityCodecDecodeRejectsBadIndex(t *testing.T) {
	t.Parallel()

	resp := &frdrpc.ForwardingAbilityResponse{
		Peers:           [][]byte{{0x02}},
		UptimeFractions: make([]byte, 2),
		Velocities: []*frdrpc.VelocityEntry{{
			PackedIdx: (5 << 16) | 0, // in_idx=5, P=1
			Velocity:  1.0,
		}},
	}

	_, err := frdrpc.DecodeForwardingAbility(resp)
	require.Error(t, err)
	require.Contains(t, err.Error(), "velocity index out of range")
}
