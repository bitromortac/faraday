package main

import (
	"context"
	"sort"

	"github.com/lightninglabs/faraday/frdrpc"
	"github.com/urfave/cli"
)

var forwardingAbilityCommand = cli.Command{
	Name:     "forwardingability",
	Category: "insights",
	Usage:    "Get forwarding ability analysis of peer pairs.",
	Flags: []cli.Flag{
		cli.Int64Flag{
			Name:  "start_time",
			Usage: "start time of the query range as a unix timestamp",
		},
		cli.Int64Flag{
			Name: "end_time",
			Usage: "end time of the query range as a unix " +
				"timestamp; zero defaults to the server's " +
				"current time",
		},
		cli.Uint64Flag{
			Name: "liquidity_floor_sat",
			Usage: "the minimum directional liquidity in satoshis " +
				"for a pair to count as economically " +
				"forwardable; zero uses the server default",
		},
	},
	Action: queryForwardingAbility,
}

type pairView struct {
	PeerIn           string  `json:"peer_in"`
	PeerOut          string  `json:"peer_out"`
	EffectiveUptimeS int64   `json:"effective_uptime_s"`
	ForwardedSat     int64   `json:"forwarded_sat"`
	UptimeFraction   float64 `json:"uptime_fraction"`
	Velocity         float64 `json:"velocity"`
}

func queryForwardingAbility(ctx *cli.Context) error {
	client, cleanup := getClient(ctx)
	defer cleanup()

	req := &frdrpc.ForwardingAbilityRequest{
		StartTime:         uint64(ctx.Int64("start_time")),
		EndTime:           uint64(ctx.Int64("end_time")),
		LiquidityFloorSat: ctx.Uint64("liquidity_floor_sat"),
	}

	rpcCtx := context.Background()
	resp, err := client.ForwardingAbility(rpcCtx, req)
	if err != nil {
		return err
	}

	abilities, err := frdrpc.DecodeForwardingAbility(resp)
	if err != nil {
		return err
	}

	// The metrics are raw, so derive uptime fraction and velocity here from
	// the window the server reported.
	windowSeconds := resp.EndTime - resp.StartTime

	var views []pairView
	for inPeer, outMap := range abilities {
		for outPeer, ability := range outMap {
			var uptimeFraction, velocity float64
			if windowSeconds > 0 {
				uptimeFraction = float64(ability.EffectiveUptimeS) /
					float64(windowSeconds)
			}
			if ability.EffectiveUptimeS > 0 {
				velocity = float64(ability.ForwardedSat) /
					float64(ability.EffectiveUptimeS)
			}

			views = append(views, pairView{
				PeerIn:           inPeer,
				PeerOut:          outPeer,
				EffectiveUptimeS: ability.EffectiveUptimeS,
				ForwardedSat:     ability.ForwardedSat,
				UptimeFraction:   uptimeFraction,
				Velocity:         velocity,
			})
		}
	}

	// Stable sort by PeerIn, then PeerOut.
	sort.SliceStable(views, func(i, j int) bool {
		if views[i].PeerIn != views[j].PeerIn {
			return views[i].PeerIn < views[j].PeerIn
		}
		return views[i].PeerOut < views[j].PeerOut
	})

	printJSON(views)
	return nil
}
