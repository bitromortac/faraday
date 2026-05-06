// Package frdrpc additionally exports a codec for the ForwardingAbility RPC.
//
// The codec converts between a peer-keyed nested map of pair abilities
// and the ForwardingAbilityResponse wire format described in faraday.proto.
// It is shared by the server, the CLI, and any external Go consumer of
// this RPC, so the wire contract is implemented exactly once.
//
// The codec lives in the frdrpc Go module (alongside the generated proto
// types) so external consumers can decode responses without pulling in
// any internal faraday packages.
//
// Example round trip:
//
//	resp, err := frdrpc.EncodeForwardingAbility(in)
//	if err != nil { return err }
//	got, err := frdrpc.DecodeForwardingAbility(resp)
//	if err != nil { return err }
//	// got equals in within uint16 quantisation tolerance (~1.5e-5).
package frdrpc

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"sort"
)

// ForwardingAbility is the codec's value type for a single (peer_in,
// peer_out) pair. It mirrors the analyzer's internal representation but
// lives in the proto module so the codec stays standalone — external
// consumers can use it without importing internal faraday packages.
type ForwardingAbility struct {
	// Velocity is the forwarding velocity in sat/s during effective uptime.
	Velocity float64

	// UptimeFraction is the fraction of time, in [0, 1], that the pair
	// had threshold-exceeding forwarding liquidity available.
	UptimeFraction float64
}

// uptimeQuantum is the resolution of the uint16 uptime encoding. Values
// further apart than 1/65535 round to distinct cells; values closer than
// that collapse, which clients must accept when comparing decoded output
// against pre-encoded input.
const uptimeQuantum = 1.0 / 65535

// EncodeForwardingAbility serialises a peer-keyed pair ability map as the
// wire form documented on ForwardingAbilityResponse: a sorted peer
// dictionary, a dense uint16 uptime matrix in row-major order, and a
// sparse velocity list. Cells absent from the input map encode as zero.
//
// Invariant: the returned dictionary is sorted by raw-byte comparison so
// identical inputs produce byte-identical responses.
//
// Invariant: len(resp.UptimeFractions) == 2 * len(resp.Peers)^2.
//
// Invariant: a pair (in, out) appears in resp.Velocities only when its
// velocity is non-zero.
func EncodeForwardingAbility(
	abilities map[string]map[string]ForwardingAbility) (
	*ForwardingAbilityResponse, error) {

	rawPeers, indexOf, err := buildPeerDictionary(abilities)
	if err != nil {
		return nil, err
	}

	p := len(rawPeers)
	uptime := make([]byte, 2*p*p)
	var velocities []*VelocityEntry

	for inHex, outs := range abilities {
		in := indexOf[inHex]
		for outHex, ability := range outs {
			out := indexOf[outHex]

			offset := 2 * (in*p + out)
			q := uint16(math.Round(
				clamp01(ability.UptimeFraction) * 65535,
			))
			binary.LittleEndian.PutUint16(uptime[offset:], q)

			if ability.Velocity != 0 {
				velocities = append(
					velocities, &VelocityEntry{
						PackedIdx: uint32(in)<<16 |
							uint32(out),
						Velocity: float32(
							ability.Velocity,
						),
					},
				)
			}
		}
	}

	return &ForwardingAbilityResponse{
		Peers:           rawPeers,
		UptimeFractions: uptime,
		Velocities:      velocities,
	}, nil
}

// DecodeForwardingAbility is the inverse of EncodeForwardingAbility. It
// validates the length invariant on UptimeFractions, rejects velocity
// entries whose indices fall outside the dictionary, and returns a
// fully-populated P×P map keyed by hex-encoded pubkey.
func DecodeForwardingAbility(resp *ForwardingAbilityResponse) (
	map[string]map[string]ForwardingAbility, error) {

	p := len(resp.Peers)
	if want := 2 * p * p; len(resp.UptimeFractions) != want {
		return nil, fmt.Errorf("uptime_fractions length %d, "+
			"want %d for %d peers",
			len(resp.UptimeFractions), want, p)
	}

	hexPeers := make([]string, p)
	for i, raw := range resp.Peers {
		hexPeers[i] = hex.EncodeToString(raw)
	}

	out := make(map[string]map[string]ForwardingAbility, p)
	for in := 0; in < p; in++ {
		row := make(map[string]ForwardingAbility, p)
		for outIdx := 0; outIdx < p; outIdx++ {
			offset := 2 * (in*p + outIdx)
			q := binary.LittleEndian.Uint16(
				resp.UptimeFractions[offset:],
			)
			row[hexPeers[outIdx]] = ForwardingAbility{
				UptimeFraction: float64(q) / 65535,
			}
		}
		out[hexPeers[in]] = row
	}

	for _, ve := range resp.Velocities {
		in := int(ve.PackedIdx >> 16)
		outIdx := int(ve.PackedIdx & 0xFFFF)
		if in >= p || outIdx >= p {
			return nil, fmt.Errorf("velocity index out of "+
				"range: in=%d out=%d P=%d", in, outIdx, p)
		}

		ability := out[hexPeers[in]][hexPeers[outIdx]]
		ability.Velocity = float64(ve.Velocity)
		out[hexPeers[in]][hexPeers[outIdx]] = ability
	}

	return out, nil
}

// buildPeerDictionary collects every distinct pubkey that appears as
// either an incoming or outgoing peer in the input matrix, hex-decodes
// it to raw bytes, sorts the result by raw-byte comparison, and returns
// both the sorted dictionary and a reverse lookup from hex pubkey to
// dictionary index.
func buildPeerDictionary(
	abilities map[string]map[string]ForwardingAbility) (
	[][]byte, map[string]int, error) {

	peerSet := make(map[string]struct{})
	for in, outs := range abilities {
		peerSet[in] = struct{}{}
		for out := range outs {
			peerSet[out] = struct{}{}
		}
	}

	type indexedPeer struct {
		hex string
		raw []byte
	}
	peers := make([]indexedPeer, 0, len(peerSet))
	for hexKey := range peerSet {
		raw, err := hex.DecodeString(hexKey)
		if err != nil {
			return nil, nil, fmt.Errorf("decode peer %q: %w",
				hexKey, err)
		}
		peers = append(peers, indexedPeer{hex: hexKey, raw: raw})
	}

	sort.Slice(peers, func(i, j int) bool {
		return bytes.Compare(peers[i].raw, peers[j].raw) < 0
	})

	dictionary := make([][]byte, len(peers))
	indexOf := make(map[string]int, len(peers))
	for i, p := range peers {
		dictionary[i] = p.raw
		indexOf[p.hex] = i
	}

	return dictionary, indexOf, nil
}

// clamp01 saturates a float into [0, 1] before quantisation. Out-of-range
// inputs would otherwise round to bogus uint16 values; this preserves the
// monotonicity of the wire encoding for malformed analyzer output.
func clamp01(f float64) float64 {
	switch {
	case f < 0:
		return 0
	case f > 1:
		return 1
	default:
		return f
	}
}
