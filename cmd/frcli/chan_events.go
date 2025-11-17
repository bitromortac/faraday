package main

import (
	"context"

	"github.com/lightninglabs/faraday/frdrpc"
	"github.com/urfave/cli"
)

var chanEventsCommand = cli.Command{
	Name:      "chanevents",
	Category:  "reporting",
	Usage:     "Get a report of channel events.",
	Description: `
	Get a report for a channel which provides a detailed 
	account of its lifecycle events.`,
	ArgsUsage: "funding_txid [output_index]",
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:  "funding_txid",
			Usage: "the txid of the channel's funding transaction",
		},
		cli.IntFlag{
			Name: "output_index",
			Usage: "the output index for the funding output of " +
				"the funding transaction",
		},
		cli.Int64Flag{
			Name:  "start_time",
			Usage: "start time of the query range as a unix timestamp",
		},
		cli.Int64Flag{
			Name:  "end_time",
			Usage: "end time of the query range as a unix timestamp",
		},
	},
	Action: queryChanEvents,
}

func queryChanEvents(ctx *cli.Context) error {
	client, cleanup := getClient(ctx)
	defer cleanup()

	// Show command help if the channel point was not provided.
	if ctx.NArg() == 0 && ctx.String("funding_txid") == "" {
		return cli.ShowCommandHelp(ctx, "chanevents")
	}

	outpoint, err := parseChannelPoint(ctx)
	if err != nil {
		return err
	}

	req := &frdrpc.ChannelEventsRequest{
		ChanPoint: outpoint.String(),
		StartTime: uint64(ctx.Int64("start_time")),
		EndTime:   uint64(ctx.Int64("end_time")),
	}

	rpcCtx := context.Background()
	report, err := client.GetChannelEvents(rpcCtx, req)
	if err != nil {
		return err
	}

	printRespJSON(report)
	return nil
}
