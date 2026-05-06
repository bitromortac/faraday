package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/lightninglabs/faraday/frdrpc"
	"github.com/urfave/cli"
)

// forwardingAbilityCommand is the CLI command for querying the forwarding
// ability of peer pairs over a specified time window.
var forwardingAbilityCommand = cli.Command{
	Name:     "forwardingability",
	Category: "channels",
	Usage:    "Get the forwarding ability of a peer over a given time period.",
	Flags: []cli.Flag{
		cli.Uint64Flag{
			Name:  "start_time",
			Usage: "start time for the report (unix timestamp)",
		},
		cli.Uint64Flag{
			Name:  "end_time",
			Usage: "end time for the report (unix timestamp)",
		},
		cli.Float64Flag{
			Name:  "forward_percentile",
			Usage: "the percentile of successful forward amounts to use as a threshold",
			Value: 50.0,
		},
		cli.Uint64Flag{
			Name:  "threshold_amt_sat",
			Usage: "the threshold amount in satoshis to use",
		},
	},
	Action: queryForwardingAbility,
}

// queryForwardingAbility issues a ForwardingAbility RPC request with parameters
// from the CLI context and prints the response as JSON.
func queryForwardingAbility(ctx *cli.Context) error {
	client, cleanup := getClient(ctx)
	defer cleanup()

	req := &frdrpc.ForwardingAbilityRequest{}

	if ctx.IsSet("start_time") {
		req.StartTime = ctx.Uint64("start_time")
	} else {
		req.StartTime = uint64(
			time.Now().Add(-30 * 24 * time.Hour).Unix(),
		)
	}

	if ctx.IsSet("end_time") {
		req.EndTime = ctx.Uint64("end_time")
	} else {
		req.EndTime = uint64(time.Now().Unix())
	}

	req.ForwardPercentile = float32(ctx.Float64("forward_percentile"))
	req.ThresholdAmtSat = ctx.Uint64("threshold_amt_sat")

	rpcCtx := context.Background()
	resp, err := client.ForwardingAbility(rpcCtx, req)
	if err != nil {
		return err
	}

	rendered, err := renderForwardingAbility(resp)
	if err != nil {
		return err
	}

	fmt.Println(rendered)

	return nil
}

// pairView is the human-facing projection of a single (peer_in, peer_out)
// row of the forwarding-ability matrix.
type pairView struct {
	PeerIn         string  `json:"peer_in"`
	PeerOut        string  `json:"peer_out"`
	UptimeFraction float64 `json:"uptime_fraction"`
	Velocity       float64 `json:"velocity"`
}

// renderForwardingAbility decodes a wire-form ForwardingAbilityResponse
// into the analyzer-shaped map and projects it as a JSON array of pair
// views, suppressing pairs whose uptime AND velocity are both zero so
// the output stays scannable on large nodes.
func renderForwardingAbility(resp *frdrpc.ForwardingAbilityResponse) (
	string, error) {

	abilities, err := frdrpc.DecodeForwardingAbility(resp)
	if err != nil {
		return "", err
	}

	views := make([]pairView, 0, len(abilities))
	for inPeer, row := range abilities {
		for outPeer, ability := range row {
			if ability.UptimeFraction == 0 && ability.Velocity == 0 {
				continue
			}
			views = append(views, pairView{
				PeerIn:         inPeer,
				PeerOut:        outPeer,
				UptimeFraction: ability.UptimeFraction,
				Velocity:       ability.Velocity,
			})
		}
	}

	// Stable order: sort lexicographically on (peer_in, peer_out) so
	// repeated runs against the same data produce identical CLI output.
	sort.Slice(views, func(i, j int) bool {
		if views[i].PeerIn != views[j].PeerIn {
			return views[i].PeerIn < views[j].PeerIn
		}

		return views[i].PeerOut < views[j].PeerOut
	})

	out, err := json.MarshalIndent(views, "", "    ")
	if err != nil {
		return "", err
	}

	return string(out), nil
}
