package main

import (
	"encoding/hex"
	"strings"
	"testing"

	"github.com/lightninglabs/faraday/frdrpc"
	"github.com/stretchr/testify/require"
)

// pk constructs a 33-byte compressed-pubkey hex string for fixtures.
func pk(b byte) string {
	return "02" + strings.Repeat(hex.EncodeToString([]byte{b}), 32)
}

// TestRenderForwardingAbilityOmitsZeroPairs verifies the CLI renderer
// suppresses pairs whose uptime and velocity are both zero, since on a
// node with hundreds of peers the all-zero majority would otherwise
// drown out the interesting cells.
func TestRenderForwardingAbilityOmitsZeroPairs(t *testing.T) {
	t.Parallel()

	in := map[string]map[string]frdrpc.ForwardingAbility{
		pk(0x01): {
			pk(0x01): {UptimeFraction: 0.95},
			pk(0x02): {},
		},
		pk(0x02): {
			pk(0x01): {},
			pk(0x02): {Velocity: 12.5, UptimeFraction: 0.5},
		},
	}

	resp, err := frdrpc.EncodeForwardingAbility(in)
	require.NoError(t, err)

	rendered, err := renderForwardingAbility(resp)
	require.NoError(t, err)

	require.Contains(t, rendered, pk(0x01))
	require.Contains(t, rendered, pk(0x02))

	// The pk(0x01)→pk(0x02) and pk(0x02)→pk(0x01) cells are zero/zero
	// and must not appear as their own JSON entries. Counting the
	// number of "peer_in" keys should equal the number of
	// non-zero pairs.
	require.Equal(
		t, 2, strings.Count(rendered, `"peer_in"`),
		"expected exactly two non-zero pairs in the rendered output",
	)
}

// TestRenderForwardingAbilityStableOrdering verifies the output is
// sorted by (peer_in, peer_out) so repeated runs produce diff-friendly
// output.
func TestRenderForwardingAbilityStableOrdering(t *testing.T) {
	t.Parallel()

	in := map[string]map[string]frdrpc.ForwardingAbility{
		pk(0x03): {pk(0x01): {UptimeFraction: 0.5}},
		pk(0x01): {pk(0x03): {UptimeFraction: 0.5}},
		pk(0x02): {pk(0x02): {UptimeFraction: 0.5}},
	}

	resp, err := frdrpc.EncodeForwardingAbility(in)
	require.NoError(t, err)

	rendered, err := renderForwardingAbility(resp)
	require.NoError(t, err)

	// The first non-zero peer_in occurrence must be pk(0x01) since pk
	// values are sorted by raw byte value and we hex-encoded them
	// starting with bytes 0x01, 0x02, 0x03.
	first := strings.Index(rendered, `"peer_in"`)
	require.GreaterOrEqual(t, first, 0)

	// Snip the first 200 chars after the first peer_in key to find the
	// pubkey value attached to it.
	require.Contains(t, rendered[first:first+200], pk(0x01))
}
