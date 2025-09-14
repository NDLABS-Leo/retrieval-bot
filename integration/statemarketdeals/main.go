// main.go
package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/builtin"
	lotusapi "github.com/filecoin-project/lotus/api"
	lotusclient "github.com/filecoin-project/lotus/api/client"
	"github.com/filecoin-project/lotus/api/v1api"
	"github.com/filecoin-project/lotus/chain/types"
	logging "github.com/ipfs/go-log/v2"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

/********** 日志 **********/
var log = logging.Logger("claims-crawler")

/********** 配置 **********/
type cfg struct {
	LotusURL  string
	LotusJWT  string
	MongoURI  string
	MongoDB   string
	MongoColl string
}

func mustEnv(key, def string) string {
	v := os.Getenv(key)
	if v == "" {
		if def != "" {
			return def
		}
		log.Fatalf("missing env %s", key)
	}
	return v
}

func loadCfg() cfg {
	return cfg{
		LotusURL:  mustEnv("FULLNODE_API_URL", ""),
		LotusJWT:  os.Getenv("FULLNODE_API_TOKEN"),
		MongoURI:  mustEnv("MONGO_URI", ""),
		MongoDB:   mustEnv("MONGO_DB", "filstats"),
		MongoColl: mustEnv("MONGO_CLAIMS_COLL", "claims"),
	}
}

/********** Mongo 文档结构 **********/
type DBClaim struct {
	ClaimID    int64          `bson:"claim_id"`    // verifreg.ClaimId
	ProviderID int64          `bson:"provider_id"` // abi.ActorID
	ClientID   int64          `bson:"client_id"`   // abi.ActorID
	ClientAddr string         `bson:"client_addr"` // f1/f3... address
	DataCID    string         `bson:"data_cid"`    // cid string
	Size       int64          `bson:"size"`        // padded piece size
	TermMin    int64          `bson:"term_min"`    // epochs
	TermMax    int64          `bson:"term_max"`    // epochs
	TermStart  int64          `bson:"term_start"`  // epoch
	Sector     uint64         `bson:"sector"`      // sector number
	MinerAddr  string         `bson:"miner_addr"`  // f0... ID 地址
	UpdatedAt  time.Time      `bson:"updated_at"`  // upsert 时间
	Meta       map[string]any `bson:"meta,omitempty"`
}

/********** Lotus 连接 **********/
func connectLotus(ctx context.Context, url, jwt string) (v1api.FullNode, func(), error) {
	hdr := http.Header{}
	if jwt != "" {
		hdr.Set("Authorization", "Bearer "+jwt)
	}
	full, closer, err := lotusclient.NewFullNodeRPCV1(ctx, url, hdr)
	if err != nil {
		return nil, func() {}, fmt.Errorf("connect lotus: %w", err)
	}
	return full, func() { closer() }, nil
}

/********** Mongo 连接与索引 **********/
func connectMongo(ctx context.Context, uri, db, coll string) (*mongo.Client, *mongo.Collection, error) {
	mc, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, nil, err
	}
	c := mc.Database(db).Collection(coll)

	// 唯一索引：(provider_id, claim_id)
	idx := mongo.IndexModel{
		Keys:    bson.D{{Key: "provider_id", Value: 1}, {Key: "claim_id", Value: 1}},
		Options: options.Index().SetUnique(true).SetName("uniq_provider_claim"),
	}
	if _, err := c.Indexes().CreateOne(ctx, idx); err != nil {
		return nil, nil, fmt.Errorf("create index: %w", err)
	}
	// 辅助索引（检索/排序）
	_, _ = c.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "client_addr", Value: 1}}},
		{Keys: bson.D{{Key: "miner_addr", Value: 1}}},
		{Keys: bson.D{{Key: "updated_at", Value: -1}}},
	})

	return mc, c, nil
}

/********** 是否有算力 **********/
func hasNonZeroPower(p *lotusapi.MinerPower) bool {
	if p == nil {
		return false
	}
	return p.MinerPower.RawBytePower.GreaterThan(types.NewInt(0)) ||
		p.MinerPower.QualityAdjPower.GreaterThan(types.NewInt(0))
}

/********** 全量：逐矿工拉 StateGetClaims 并 upsert **********/
func crawlOnce(ctx context.Context, api v1api.FullNode, coll *mongo.Collection) error {
	head := types.EmptyTSK

	// 1) 所有矿工
	miners, err := api.StateListMiners(ctx, head)
	if err != nil {
		return fmt.Errorf("StateListMiners: %w", err)
	}
	log.Infof("miners listed: %d", len(miners))

	// 2) 过滤有算力
	var active []address.Address
	for _, m := range miners {
		mp, err := api.StateMinerPower(ctx, m, head)
		if err != nil {
			log.Warnw("StateMinerPower error", "miner", m.String(), "err", err)
			continue
		}
		if hasNonZeroPower(mp) {
			active = append(active, m)
		}
	}
	log.Infof("active miners (non-zero power): %d", len(active))

	// 3) upsert
	upserts := 0
	for _, m := range active {
		// provider ID
		idAddr, err := api.StateLookupID(ctx, m, head)
		if err != nil {
			log.Warnw("StateLookupID", "miner", m.String(), "err", err)
			continue
		}
		id, err := address.IDFromAddress(idAddr)
		if err != nil {
			log.Warnw("IDFromAddress", "idAddr", idAddr.String(), "err", err)
			continue
		}
		providerID := abi.ActorID(id)

		// claims
		mclaims, err := api.StateGetClaims(ctx, idAddr, head)
		if err != nil {
			log.Warnw("StateGetClaims", "provider", idAddr.String(), "err", err)
			continue
		}
		if len(mclaims) == 0 {
			continue
		}

		for cidNum, c := range mclaims {
			// client 显示友好地址
			idClientAddr, err := address.NewIDAddress(uint64(c.Client))
			if err != nil {
				log.Warnw("NewIDAddress", "client_id", c.Client, "err", err)
				continue
			}
			keyAddr, err := api.StateAccountKey(ctx, idClientAddr, head)
			clientAddrStr := idClientAddr.String()
			if err == nil {
				clientAddrStr = keyAddr.String()
			}

			doc := DBClaim{
				ClaimID:    int64(cidNum),
				ProviderID: int64(providerID),
				ClientID:   int64(c.Client),
				ClientAddr: clientAddrStr,
				DataCID:    c.Data.String(),
				Size:       int64(c.Size),
				TermMin:    int64(c.TermMin),
				TermMax:    int64(c.TermMax),
				TermStart:  int64(c.TermStart),
				Sector:     uint64(c.Sector),
				MinerAddr:  idAddr.String(),
				UpdatedAt:  time.Now(),
			}
			filter := bson.M{"provider_id": doc.ProviderID, "claim_id": doc.ClaimID}
			update := bson.M{"$set": doc}
			_, err = coll.UpdateOne(ctx, filter, update, options.Update().SetUpsert(true))
			if err != nil {
				log.Warnw("upsert claim failed", "provider_id", doc.ProviderID, "claim_id", doc.ClaimID, "err", err)
				continue
			}
			upserts++
		}
	}
	log.Infow("full crawl finished", "upserts", upserts)
	return nil
}

/********** 元数据：last_height & full_import_done **********/
const metaCollName = "claims_meta"
const metaKeyLastHeight = "last_height"
const metaKeyFullDone = "full_import_done"

type metaDoc struct {
	ID     string `bson:"_id"`
	Height int64  `bson:"height,omitempty"`
	Done   bool   `bson:"done,omitempty"`
}

func getLastHeight(ctx context.Context, db *mongo.Database) (int64, error) {
	var md metaDoc
	err := db.Collection(metaCollName).FindOne(ctx, bson.M{"_id": metaKeyLastHeight}).Decode(&md)
	if err == mongo.ErrNoDocuments {
		return 0, nil
	}
	return md.Height, err
}

func setLastHeight(ctx context.Context, db *mongo.Database, h int64) error {
	_, err := db.Collection(metaCollName).UpdateOne(
		ctx,
		bson.M{"_id": metaKeyLastHeight},
		bson.M{"$set": bson.M{"height": h}},
		options.Update().SetUpsert(true),
	)
	return err
}

func getFullImportDone(ctx context.Context, db *mongo.Database) (bool, error) {
	var md metaDoc
	err := db.Collection(metaCollName).FindOne(ctx, bson.M{"_id": metaKeyFullDone}).Decode(&md)
	if err == mongo.ErrNoDocuments {
		return false, nil
	}
	return md.Done, err
}

func setFullImportDone(ctx context.Context, db *mongo.Database, done bool) error {
	_, err := db.Collection(metaCollName).UpdateOne(
		ctx,
		bson.M{"_id": metaKeyFullDone},
		bson.M{"$set": bson.M{"done": done}},
		options.Update().SetUpsert(true),
	)
	return err
}

/********** 增量配置 **********/
type incCfg struct {
	PollEvery       time.Duration // 轮询间隔（<30s，含抖动）
	SafeDelay       int64         // 与链头保持的安全延迟（避免重组）
	BackfillStart   int64         // 没有 last_height 时，向后回补的高度窗口
	MaxHeightsBatch int64         // 单轮最多处理的高度数量（防止落后太多时一次扫太多）
}

/********** 增量主循环 **********/
func runIncrementalLoop(
	ctx context.Context,
	api v1api.FullNode,
	db *mongo.Database,
	claimsColl *mongo.Collection,
	cfg incCfg,
) {
	log.Infow("incremental loop start",
		"poll", cfg.PollEvery.String(),
		"safeDelay", cfg.SafeDelay,
		"backfill", cfg.BackfillStart,
		"batch", cfg.MaxHeightsBatch,
	)

	tick := time.NewTicker(cfg.PollEvery)
	defer tick.Stop()

	jitter := func() time.Duration {
		// -30% ~ +30% 抖动
		r := (rand.Float64()*0.6 - 0.3)
		return time.Duration(float64(cfg.PollEvery) * r)
	}

	for {
		select {
		case <-ctx.Done():
			log.Info("incremental loop stop")
			return
		case <-tick.C:
			// 轻微抖动，避免与30s出块整齐重叠
			time.Sleep(jitter())

			// 获取 head & 目标高度（安全延迟）
			tip, err := api.ChainHead(ctx)
			if err != nil {
				log.Warnw("ChainHead failed", "err", err)
				continue
			}
			curH := int64(tip.Height())
			target := curH - cfg.SafeDelay
			if target < 0 {
				continue
			}

			// 取 last_height
			last, err := getLastHeight(ctx, db)
			if err != nil {
				log.Warnw("getLastHeight failed", "err", err)
				continue
			}
			var start int64
			if last == 0 {
				// 第一次（或未初始化），从 target - BackfillStart 开始
				start = target - cfg.BackfillStart
				if start < 0 {
					start = 0
				}
			} else {
				start = last + 1
			}
			if start > target {
				// 已经追平
				continue
			}

			// 限制单轮处理高度数，避免落后太多一次扫太多
			end := target
			if cfg.MaxHeightsBatch > 0 && end-start+1 > cfg.MaxHeightsBatch {
				end = start + cfg.MaxHeightsBatch - 1
			}

			processedTo := last
			for h := start; h <= end; h++ {
				// 本高度 TipSet
				ts, err := api.ChainGetTipSetByHeight(ctx, abi.ChainEpoch(h), tip.Key())
				if err != nil {
					log.Warnw("ChainGetTipSetByHeight failed", "height", h, "err", err)
					break
				}
				if ts == nil {
					continue
				}

				// 采集 To=VerifiedRegistry 的消息，把 From 作为候选 Provider
				candidates := make(map[string]struct{}, 64)
				for _, bh := range ts.Blocks() {
					// 取某个区块的消息
					bm, err := api.ChainGetBlockMessages(ctx, bh.Cid())
					if err != nil {
						log.Warnw("ChainGetBlockMessages failed", "height", h, "block", bh.Cid().String(), "err", err)
						continue
					}

					// BLS 消息
					for _, m := range bm.BlsMessages {
						if m.To == builtin.VerifiedRegistryActorAddr {
							candidates[m.From.String()] = struct{}{}
						}
					}
					// Secp256k1 签名消息
					for _, sm := range bm.SecpkMessages {
						if sm.Message.To == builtin.VerifiedRegistryActorAddr {
							candidates[sm.Message.From.String()] = struct{}{}
						}
					}
				}

				if len(candidates) == 0 {
					processedTo = h
					continue
				}

				// 对每个候选 Provider（From 地址），拉取 claims 并 upsert
				upserts := 0
				for fromStr := range candidates {
					fromAddr, err := address.NewFromString(fromStr)
					if err != nil {
						continue
					}
					// 归一为 ID 地址
					idAddr, err := api.StateLookupID(ctx, fromAddr, ts.Key())
					if err != nil {
						// 不是矿工/或查不到，跳过
						continue
					}
					id, err := address.IDFromAddress(idAddr)
					if err != nil {
						continue
					}
					providerID := abi.ActorID(id)

					// 拉 claims（以该高度的 parent tipset 状态去读更稳定）
					mclaims, err := api.StateGetClaims(ctx, idAddr, ts.Key())
					if err != nil || len(mclaims) == 0 {
						continue
					}

					for cidNum, c := range mclaims {
						idClientAddr, err := address.NewIDAddress(uint64(c.Client))
						if err != nil {
							continue
						}
						keyAddr, err := api.StateAccountKey(ctx, idClientAddr, ts.Key())
						clientAddrStr := idClientAddr.String()
						if err == nil {
							clientAddrStr = keyAddr.String()
						}

						doc := DBClaim{
							ClaimID:    int64(cidNum),
							ProviderID: int64(providerID),
							ClientID:   int64(c.Client),
							ClientAddr: clientAddrStr,
							DataCID:    c.Data.String(),
							Size:       int64(c.Size),
							TermMin:    int64(c.TermMin),
							TermMax:    int64(c.TermMax),
							TermStart:  int64(c.TermStart),
							Sector:     uint64(c.Sector),
							MinerAddr:  idAddr.String(),
							UpdatedAt:  time.Now(),
						}
						filter := bson.M{"provider_id": doc.ProviderID, "claim_id": doc.ClaimID}
						update := bson.M{"$set": doc}
						if _, err := claimsColl.UpdateOne(ctx, filter, update, options.Update().SetUpsert(true)); err == nil {
							upserts++
						}
					}
				}

				log.Infow("incremental upsert", "height", h, "providers", len(candidates), "upserts", upserts)
				processedTo = h
			}

			// 更新 last_height
			if processedTo >= start {
				if err := setLastHeight(ctx, db, processedTo); err != nil {
					log.Warnw("setLastHeight failed", "height", processedTo, "err", err)
				} else {
					log.Infow("advance last_height", "to", processedTo)
				}
			}
		}
	}
}

/********** 启动引导：只在需要时全量，否则直接增量 **********/
func bootstrapAndStart(
	ctx context.Context,
	api v1api.FullNode,
	db *mongo.Database,
	claimsColl *mongo.Collection,
	fullOnce func(context.Context, v1api.FullNode, *mongo.Collection) error,
	inc incCfg,
) {
	done, _ := getFullImportDone(ctx, db)

	// 基于当前 head 初始化 last_height
	tip, err := api.ChainHead(ctx)
	if err != nil {
		log.Fatalw("ChainHead failed", "err", err)
	}
	curH := int64(tip.Height())
	initLast := curH - inc.SafeDelay - inc.BackfillStart
	if initLast < 0 {
		initLast = 0
	}

	if done {
		log.Info("full import marked as done, start incremental only")
		go runIncrementalLoop(ctx, api, db, claimsColl, inc)
		return
	}

	// 检查 claims 是否为空
	cnt, err := claimsColl.EstimatedDocumentCount(ctx)
	if err != nil {
		log.Fatalw("count claims failed", "err", err)
	}

	if cnt == 0 {
		// 真·首次全量
		log.Info("claims empty, running initial full import...")
		if err := fullOnce(ctx, api, claimsColl); err != nil {
			log.Fatalw("initial full import failed", "err", err)
		}
		if err := setFullImportDone(ctx, db, true); err != nil {
			log.Warnw("set full_import_done failed", "err", err)
		}
		if err := setLastHeight(ctx, db, initLast); err != nil {
			log.Warnw("init last_height failed", "height", initLast, "err", err)
		}
		log.Infow("initial full import done", "init_last_height", initLast)
	} else {
		// 已有大量数据：视作全量完成，跳过全量
		log.Infow("claims already populated, skipping full import",
			"estimated_count", cnt, "init_last_height", initLast)
		if err := setFullImportDone(ctx, db, true); err != nil {
			log.Warnw("set full_import_done failed", "err", err)
		}
		if lh, _ := getLastHeight(ctx, db); lh == 0 {
			if err := setLastHeight(ctx, db, initLast); err != nil {
				log.Warnw("init last_height failed", "height", initLast, "err", err)
			}
		}
	}

	// 启动增量
	go runIncrementalLoop(ctx, api, db, claimsColl, inc)
}

/********** main **********/
func main() {
	_ = logging.SetLogLevel("*", "info")

	cfg := loadCfg()
	log.Infow("boot",
		"lotus", maskURLToken(cfg.LotusURL),
		"mongo", cfg.MongoURI,
		"db", cfg.MongoDB, "coll", cfg.MongoColl,
	)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	// lotus
	full, closeLotus, err := connectLotus(ctx, cfg.LotusURL, cfg.LotusJWT)
	if err != nil {
		log.Fatal(err)
	}
	defer closeLotus()

	// mongo
	mc, claimsColl, err := connectMongo(ctx, cfg.MongoURI, cfg.MongoDB, cfg.MongoColl)
	if err != nil {
		log.Fatal(err)
	}
	defer mc.Disconnect(ctx)

	// 启动：按需全量，随后增量
	bootstrapAndStart(ctx, full, mc.Database(cfg.MongoDB), claimsColl, crawlOnce, incCfg{
		PollEvery:       15 * time.Second, // < 30s，留余裕
		SafeDelay:       1,                // 与头部保持1个 Epoch 安全延迟
		BackfillStart:   120,              // 无 last_height 时回补约 1h
		MaxHeightsBatch: 180,              // 单轮最多处理 180 个高度（~90 分钟）
	})

	<-ctx.Done()
	log.Info("shutting down")
}

/********** 小工具 **********/
func maskURLToken(u string) string {
	// 仅用于日志把 token 简单打码（如果放在 URL 查询参数里）
	// 不强壮，只是避免误打完整 token
	if len(u) < 8 {
		return u
	}
	return u[:8] + "...(" + fmt.Sprintf("%d bytes)", len(u))
}

// debug helper: Hex fmt (unused; keep for local print needs)
func hexStr(b []byte) string { return hex.EncodeToString(b) }
