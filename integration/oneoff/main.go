package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	_ "github.com/joho/godotenv/autoload"
	"github.com/pkg/errors"
	"github.com/urfave/cli/v2"
	"github.com/ybbus/jsonrpc/v3"

	"storagestats/integration/filplus/util"
	"storagestats/pkg/model"
	"storagestats/pkg/model/rpc"
	"storagestats/pkg/resolver"
	"storagestats/pkg/task"
	"storagestats/worker/bitswap"
	"storagestats/worker/graphsync"
	"storagestats/worker/http"
)

//nolint:forbidigo,forcetypeassert,exhaustive
func main() {
	app := &cli.App{
		Name:      "oneoff",
		Usage:     "make a simple oneoff task that works with filplus tests",
		ArgsUsage: "providerID dealID",
		Action: func(cctx *cli.Context) error {
			ctx := cctx.Context
			providerID := cctx.Args().Get(0)
			dealIDStr := cctx.Args().Get(1)
			dealID, err := strconv.ParseUint(dealIDStr, 10, 32)
			if err != nil {
				return errors.Wrap(err, "failed to parse dealID")
			}

			providerResolver, err := resolver.NewProviderResolver(
				"https://api.node.glif.io/rpc/v0",
				"",
				time.Minute,
			)
			if err != nil {
				return errors.Wrap(err, "failed to create provider resolver")
			}

			providerInfo, err := providerResolver.ResolveProvider(ctx, providerID)
			if err != nil {
				return errors.Wrap(err, "failed to resolve provider")
			}

			locationResolver := resolver.NewLocationResolver("", time.Minute)
			_, err = locationResolver.ResolveMultiaddrsBytes(ctx, providerInfo.Multiaddrs)
			if err != nil {
				return errors.Wrap(err, "failed to resolve location")
			}

			ipInfo, err := resolver.GetPublicIPInfo(ctx, "", "")
			if err != nil {
				return errors.Wrap(err, "failed to get public ip info")
			}

			// 拉取 MarketDeal
			lotusClient := jsonrpc.NewClient("https://api.node.glif.io")
			var deal rpc.Deal
			if err := lotusClient.CallFor(ctx, &deal, "Filecoin.StateMarketStorageDeal", dealID, nil); err != nil {
				return errors.Wrap(err, "failed to get deal")
			}

			// 将 MarketDeal 映射为 DBClaim（最小必要字段，适配 util.AddTasks）
			termStart := int64(deal.Proposal.StartEpoch)
			termEnd := int64(deal.Proposal.EndEpoch)
			termMax := termEnd - termStart
			if termMax < 0 {
				termMax = 0
			}

			claims := []model.DBClaim{
				{
					// ClaimID/ProviderID/ClientID/Sector 在本场景无法从 MarketDeal 直接给出，置 0
					// 任务分发只会用到 MinerAddr/ClientAddr/DataCID/Size
					ClientAddr: deal.Proposal.Client,           // f1/f3...
					MinerAddr:  deal.Proposal.Provider,         // f0...（provider 对应矿工ID地址）
					DataCID:    deal.Proposal.PieceCID.Root,    // 作为 piece 检索用 CID
					Size:       int64(deal.Proposal.PieceSize), // padded piece size (bytes)
					TermStart:  termStart,                      // from deal
					TermMax:    termMax,                        // End - Start
					TermMin:    0,                              // 不可得，置 0
					UpdatedAt:  time.Now().UTC(),
					Meta: map[string]any{
						"deal_id":      dealID, // 作为参考信息保存在 meta
						"verified":     deal.Proposal.VerifiedDeal,
						"label":        deal.Proposal.Label,
						"sector_start": deal.State.SectorStartEpoch,
						"slash_epoch":  deal.State.SlashEpoch,
						"last_updated": deal.State.LastUpdatedEpoch,
					},
				},
			}

			// 生成任务（util.AddTasks 现已支持 []model.DBClaim）
			tasks, results := util.AddTasks(ctx, "oneoff", ipInfo, claims, locationResolver, *providerResolver)

			if len(results) > 0 {
				fmt.Println("Errors encountered when creating tasks:")
				for _, result := range results {
					r := result.(task.Result)
					fmt.Println(r)
				}
			}
			if len(tasks) > 0 {
				fmt.Println("Retrieval Test Results:")
				for _, tsk := range tasks {
					t := tsk.(task.Task)
					var result *task.RetrievalResult
					fmt.Printf(" -- Test %s --\n", t.Module)
					switch t.Module {
					case "graphsync":
						result, err = graphsync.Worker{}.DoWork(t)
					case "http":
						result, err = http.Worker{}.DoWork(t)
					case "bitswap":
						result, err = bitswap.Worker{}.DoWork(t)
					}
					if err != nil {
						fmt.Printf("Error: %s\n", err)
					} else {
						fmt.Printf("Success: %v\n", result)
					}
				}
			}
			return nil
		},
	}

	if err := app.Run(os.Args); err != nil {
		panic(err)
	}
}
