package frdrpc_test

import (
	"fmt"

	"github.com/lightninglabs/faraday/frdrpc"
)

// ExampleEncodeForwardingAbility demonstrates the canonical encode →
// wire → decode flow for the ForwardingAbility RPC. It is the godoc
// counterpart to the table-driven round-trip tests, kept short enough
// to skim while reading the package documentation.
func ExampleEncodeForwardingAbility() {
	// Two peers; the (peer_a, peer_b) pair forwarded at 12 sat/s.
	peerA := "020101010101010101010101010101010101010101010101010101010101010101"
	peerB := "020202020202020202020202020202020202020202020202020202020202020202"

	in := map[string]map[string]frdrpc.ForwardingAbility{
		peerA: {
			peerA: {UptimeFraction: 1.0},
			peerB: {UptimeFraction: 0.95, Velocity: 12.0},
		},
		peerB: {
			peerA: {UptimeFraction: 0.95},
			peerB: {UptimeFraction: 1.0},
		},
	}

	resp, err := frdrpc.EncodeForwardingAbility(in)
	if err != nil {
		panic(err)
	}

	// The dictionary holds two 33-byte pubkeys and the dense matrix is
	// 2 * 2^2 = 8 bytes; one pair carries a non-zero velocity.
	fmt.Println("peers:", len(resp.Peers))
	fmt.Println("uptime bytes:", len(resp.UptimeFractions))
	fmt.Println("velocities:", len(resp.Velocities))

	got, err := frdrpc.DecodeForwardingAbility(resp)
	if err != nil {
		panic(err)
	}

	fmt.Printf("a->b velocity: %.1f\n", got[peerA][peerB].Velocity)

	// Output:
	// peers: 2
	// uptime bytes: 8
	// velocities: 1
	// a->b velocity: 12.0
}
