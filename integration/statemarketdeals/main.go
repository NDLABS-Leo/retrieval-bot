// main.go
package main

import (
	"context"
	"encoding/json"
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

	"github.com/ipfs/go-cid"
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

	// 增量配置
	PollEvery       time.Duration // 轮询间隔（建议 <30s）
	SafeDelay       int64         // 与链头保持的安全延迟（Epoch）
	BackfillStart   int64         // 无 last_height 时回补窗口（Epoch 数）
	MaxHeightsBatch int64         // 单轮最多处理的高度数量
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

func parseInt64(s string) (int64, error) {
	var v int64
	_, err := fmt.Sscan(s, &v)
	return v, err
}

func loadCfg() cfg {
	poll := 15 * time.Second
	if s := os.Getenv("POLL_EVERY"); s != "" {
		if d, err := time.ParseDuration(s); err == nil {
			poll = d
		}
	}
	safeDelay := int64(1)
	if s := os.Getenv("SAFE_DELAY"); s != "" {
		if v, err := parseInt64(s); err == nil {
			safeDelay = v
		}
	}
	backfill := int64(120)
	if s := os.Getenv("BACKFILL_START"); s != "" {
		if v, err := parseInt64(s); err == nil {
			backfill = v
		}
	}
	maxBatch := int64(180)
	if s := os.Getenv("MAX_HEIGHTS_BATCH"); s != "" {
		if v, err := parseInt64(s); err == nil {
			maxBatch = v
		}
	}

	return cfg{
		LotusURL:        mustEnv("FULLNODE_API_URL", ""),
		LotusJWT:        os.Getenv("FULLNODE_API_TOKEN"),
		MongoURI:        mustEnv("MONGO_URI", ""),
		MongoDB:         mustEnv("MONGO_DB", "filstats"),
		MongoColl:       mustEnv("MONGO_CLAIMS_COLL", "claims"),
		PollEvery:       poll,
		SafeDelay:       safeDelay,
		BackfillStart:   backfill,
		MaxHeightsBatch: maxBatch,
	}
}

/********** Mongo 文档结构 **********/
type DBClaim struct {
	// 唯一键三元组（建立联合唯一索引）
	ProviderID int64  `bson:"provider_id"`
	ClientID   int64  `bson:"client_id"`
	DataCID    string `bson:"data_cid"`

	Size      int64     `bson:"size"`
	TermMin   int64     `bson:"term_min"`
	TermMax   int64     `bson:"term_max"`
	TermStart int64     `bson:"term_start"`
	Sector    uint64    `bson:"sector"`
	UpdatedAt time.Time `bson:"updated_at"`

	// 仅在全量时可补充（增量不一定有）
	ClientAddr string `bson:"client_addr,omitempty"`
	MinerAddr  string `bson:"miner_addr,omitempty"`
	ClaimID    int64  `bson:"claim_id,omitempty"` // 全量时可带入（不参与唯一约束）
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

/********** Mongo 连接 + 索引 **********/
func connectMongo(ctx context.Context, uri, db, coll string) (*mongo.Client, *mongo.Collection, error) {
	mc, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, nil, err
	}
	c := mc.Database(db).Collection(coll)

	// 唯一索引：provider_id + client_id + data_cid（幂等去重）
	_, _ = c.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "provider_id", Value: 1},
			{Key: "client_id", Value: 1},
			{Key: "data_cid", Value: 1},
		},
		Options: options.Index().SetUnique(true).SetName("uniq_provider_client_data"),
	})
	// 辅助查询
	_, _ = c.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "updated_at", Value: -1}}},
	})

	return mc, c, nil
}

/********** meta：存 last_height 与 full_import_done **********/
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

/********** 全量：StateGetClaims 扫描并入库 **********/
func hasNonZeroPower(p *lotusapi.MinerPower) bool {
	if p == nil {
		return false
	}
	return p.MinerPower.RawBytePower.GreaterThan(types.NewInt(0)) ||
		p.MinerPower.QualityAdjPower.GreaterThan(types.NewInt(0))
}

func fullImport(ctx context.Context, api v1api.FullNode, coll *mongo.Collection) error {
	head := types.EmptyTSK

	// 1) 所有矿工
	miners, err := api.StateListMiners(ctx, head)
	if err != nil {
		return fmt.Errorf("StateListMiners: %w", err)
	}
	log.Infof("full-import: miners listed=%d", len(miners))

	// 2) 过滤有算力
	var active []address.Address
	for _, m := range miners {
		mp, err := api.StateMinerPower(ctx, m, head)
		if err != nil {
			log.Warnw("StateMinerPower", "miner", m.String(), "err", err)
			continue
		}
		if hasNonZeroPower(mp) {
			active = append(active, m)
		}
	}
	log.Infof("full-import: active miners (non-zero power)=%d", len(active))

	// 3) 逐个 Provider 拉 claims 并 upsert
	var upserts int64
	for _, m := range active {
		// 归一到 ID 地址与 ActorID
		idAddr, err := api.StateLookupID(ctx, m, head)
		if err != nil {
			log.Warnw("StateLookupID", "miner", m.String(), "err", err)
			continue
		}
		aid, err := address.IDFromAddress(idAddr)
		if err != nil {
			log.Warnw("IDFromAddress", "minerID", idAddr.String(), "err", err)
			continue
		}
		providerID := int64(abi.ActorID(aid))

		mclaims, err := api.StateGetClaims(ctx, idAddr, head)
		if err != nil || len(mclaims) == 0 {
			continue
		}

		for claimID, c := range mclaims {
			// 可读 client 地址（兜底用 ID 地址）
			idClientAddr, _ := address.NewIDAddress(uint64(c.Client))
			keyAddr, err := api.StateAccountKey(ctx, idClientAddr, head)
			clientAddrStr := idClientAddr.String()
			if err == nil {
				clientAddrStr = keyAddr.String()
			}

			doc := DBClaim{
				ProviderID: providerID,
				ClientID:   int64(c.Client),
				DataCID:    c.Data.String(),
				Size:       int64(c.Size),
				TermMin:    int64(c.TermMin),
				TermMax:    int64(c.TermMax),
				TermStart:  int64(c.TermStart),
				Sector:     uint64(c.Sector),
				UpdatedAt:  time.Now(),
				ClientAddr: clientAddrStr,
				MinerAddr:  idAddr.String(),
				ClaimID:    int64(claimID),
			}
			n, err := upsertClaims(ctx, coll, []DBClaim{doc})
			if err != nil {
				log.Warnw("full-import upsert", "provider", providerID, "client", doc.ClientID, "data", doc.DataCID, "err", err)
				continue
			}
			upserts += n
		}
	}

	log.Infow("full-import done", "inserted_new", upserts)
	return nil
}

const claimAllocationsMethodNum = abi.MethodNum(9)

/********** 判断是否 ClaimAllocations 调用（兼容 v9~v12） **********/
// isClaimAllocationsCall 判断消息是否为 VerifiedRegistry.ClaimAllocations 调用
func isClaimAllocationsCall(to address.Address, method abi.MethodNum) bool {
	if to != builtin.VerifiedRegistryActorAddr {
		return false
	}
	// VerifiedRegistry.ClaimAllocations 方法号固定为 9
	return method == claimAllocationsMethodNum
}

/********** 解析 ClaimAllocations 参数（JSON 解码） **********/
type claimLite struct {
	Provider  uint64 `json:"Provider"`
	Client    uint64 `json:"Client"`
	Data      string `json:"Data"`
	Size      uint64 `json:"Size"`
	TermMin   int64  `json:"TermMin"`
	TermMax   int64  `json:"TermMax"`
	TermStart int64  `json:"TermStart"`
	Sector    uint64 `json:"Sector"`
}
type claimParamsJSON struct {
	Claims []claimLite `json:"Claims"`
}

func parseClaimAllocationsParams(decJSON string) ([]DBClaim, error) {
	var p claimParamsJSON
	if err := json.Unmarshal([]byte(decJSON), &p); err != nil {
		return nil, fmt.Errorf("unmarshal ClaimAllocations params: %w", err)
	}
	out := make([]DBClaim, 0, len(p.Claims))
	for _, c := range p.Claims {
		if _, err := cid.Parse(c.Data); err != nil {
			continue
		}
		out = append(out, DBClaim{
			ProviderID: int64(c.Provider),
			ClientID:   int64(c.Client),
			DataCID:    c.Data,
			Size:       int64(c.Size),
			TermMin:    c.TermMin,
			TermMax:    c.TermMax,
			TermStart:  c.TermStart,
			Sector:     c.Sector,
			UpdatedAt:  time.Now(),
		})
	}
	return out, nil
}

/********** upsert（幂等） **********/
func upsertClaims(ctx context.Context, coll *mongo.Collection, claims []DBClaim) (int64, error) {
	var inserted int64
	for _, c := range claims {
		filter := bson.M{
			"provider_id": c.ProviderID,
			"client_id":   c.ClientID,
			"data_cid":    c.DataCID,
		}
		update := bson.M{
			"$setOnInsert": bson.M{
				"provider_id": c.ProviderID,
				"client_id":   c.ClientID,
				"data_cid":    c.DataCID,
				"size":        c.Size,
				"term_min":    c.TermMin,
				"term_max":    c.TermMax,
				"term_start":  c.TermStart,
				"sector":      c.Sector,
				"client_addr": c.ClientAddr,
				"miner_addr":  c.MinerAddr,
				"claim_id":    c.ClaimID,
				"updated_at":  c.UpdatedAt,
			},
			"$set": bson.M{"updated_at": time.Now()},
		}
		res, err := coll.UpdateOne(ctx, filter, update, options.Update().SetUpsert(true))
		if err != nil {
			log.Warnw("upsert failed", "provider_id", c.ProviderID, "client_id", c.ClientID, "data_cid", c.DataCID, "err", err)
			continue
		}
		if res.UpsertedCount > 0 {
			inserted++
		}
	}
	return inserted, nil
}

/********** 增量主循环 **********/
func runIncrementalLoop(
	ctx context.Context,
	api v1api.FullNode,
	db *mongo.Database,
	claimsColl *mongo.Collection,
	c cfg,
) {
	log.Infow("incremental loop start",
		"poll", c.PollEvery.String(),
		"safeDelay", c.SafeDelay,
		"backfill", c.BackfillStart,
		"batch", c.MaxHeightsBatch,
	)

	tick := time.NewTicker(c.PollEvery)
	defer tick.Stop()

	jitter := func() time.Duration {
		// -30% ~ +30% 抖动
		r := (rand.Float64()*0.6 - 0.3)
		return time.Duration(float64(c.PollEvery) * r)
	}

	for {
		select {
		case <-ctx.Done():
			log.Info("incremental loop stop")
			return
		case <-tick.C:
			time.Sleep(jitter())

			head, err := api.ChainHead(ctx)
			if err != nil {
				log.Warnw("ChainHead", "err", err)
				continue
			}
			curH := int64(head.Height())
			target := curH - c.SafeDelay
			if target < 0 {
				continue
			}

			last, err := getLastHeight(ctx, db)
			if err != nil {
				log.Warnw("getLastHeight failed", "err", err)
				continue
			}

			start := last + 1
			if last == 0 {
				// 首次无 last_height，从 target - BackfillStart 开始
				start = target - c.BackfillStart
				if start < 0 {
					start = 0
				}
			}
			if start > target {
				continue
			}

			end := target
			if c.MaxHeightsBatch > 0 && end-start+1 > c.MaxHeightsBatch {
				end = start + c.MaxHeightsBatch - 1
			}

			var processedTo, insertedTotal int64 = last, 0

			for h := start; h <= end; h++ {
				ts, err := api.ChainGetTipSetByHeight(ctx, abi.ChainEpoch(h), head.Key())
				if err != nil || ts == nil {
					log.Warnw("ChainGetTipSetByHeight", "height", h, "err", err)
					break
				}

				var insertedThisHeight int64

				for _, bh := range ts.Blocks() {
					bm, err := api.ChainGetBlockMessages(ctx, bh.Cid())
					if err != nil {
						log.Warnw("ChainGetBlockMessages", "height", h, "block", bh.Cid().String(), "err", err)
						continue
					}

					// BLS
					for _, m := range bm.BlsMessages {
						if !isClaimAllocationsCall(m.To, m.Method) {
							continue
						}
						raw, err := api.StateDecodeParams(ctx, m.To, m.Method, m.Params, ts.Key())
						if err != nil {
							log.Warnw("decode params (BLS)", "height", h, "err", err)
							continue
						}

						// 转成 JSON string，方便 parseClaimAllocationsParams
						decBz, err := json.Marshal(raw)
						if err != nil {
							log.Warnw("marshal decode params", "height", h, "err", err)
							continue
						}

						claims, err := parseClaimAllocationsParams(string(decBz))
						if err != nil {
							log.Warnw("parse ClaimAllocations params", "height", h, "err", err)
							continue
						}
						n, err := upsertClaims(ctx, claimsColl, claims)
						if err != nil {
							log.Warnw("upsert (BLS)", "height", h, "err", err)
							continue
						}
						insertedThisHeight += n
					}

					// SECP
					for _, sm := range bm.SecpkMessages {
						m := sm.Message
						if !isClaimAllocationsCall(m.To, m.Method) {
							continue
						}
						raw, err := api.StateDecodeParams(ctx, m.To, m.Method, m.Params, ts.Key())
						if err != nil {
							log.Warnw("decode params (BLS)", "height", h, "err", err)
							continue
						}

						// 转成 JSON string，方便 parseClaimAllocationsParams
						decBz, err := json.Marshal(raw)
						if err != nil {
							log.Warnw("marshal decode params", "height", h, "err", err)
							continue
						}

						claims, err := parseClaimAllocationsParams(string(decBz))
						if err != nil {
							log.Warnw("parse ClaimAllocations params", "height", h, "err", err)
							continue
						}
						n, err := upsertClaims(ctx, claimsColl, claims)
						if err != nil {
							log.Warnw("upsert (SECP)", "height", h, "err", err)
							continue
						}
						insertedThisHeight += n
					}
				}

				log.Infow("height processed", "height", h, "inserted", insertedThisHeight)
				insertedTotal += insertedThisHeight
				processedTo = h
			}

			if processedTo >= start {
				if err := setLastHeight(ctx, db, processedTo); err != nil {
					log.Warnw("setLastHeight failed", "height", processedTo, "err", err)
				} else {
					log.Infow("advance last_height", "to", processedTo, "deltaInserted", insertedTotal)
				}
			}
		}
	}
}

/********** 启动引导：按需全量，然后增量 **********/
func bootstrapAndStart(
	ctx context.Context,
	api v1api.FullNode,
	db *mongo.Database,
	claimsColl *mongo.Collection,
	c cfg,
) {
	done, _ := getFullImportDone(ctx, db)

	// 基于当前 head 初始化 last_height（若未设置）
	head, err := api.ChainHead(ctx)
	if err != nil {
		log.Fatal(err)
	}
	curH := int64(head.Height())
	initLast := curH - c.SafeDelay - c.BackfillStart
	if initLast < 0 {
		initLast = 0
	}

	if !done {
		// 判断集合是否为空；空则做全量
		cnt, err := claimsColl.EstimatedDocumentCount(ctx)
		if err != nil {
			log.Fatal(err)
		}
		if cnt == 0 {
			log.Info("claims empty -> running full import once ...")
			if err := fullImport(ctx, api, claimsColl); err != nil {
				log.Fatal(err)
			}
		} else {
			log.Infow("claims already populated, skip full import", "estimated_count", cnt)
		}
		if err := setFullImportDone(ctx, db, true); err != nil {
			log.Warnw("set full_import_done", "err", err)
		}
		// 初始化 last_height（如果还没设置）
		if lh, _ := getLastHeight(ctx, db); lh == 0 {
			if err := setLastHeight(ctx, db, initLast); err != nil {
				log.Warnw("init last_height", "height", initLast, "err", err)
			} else {
				log.Infow("init last_height", "to", initLast)
			}
		}
	} else {
		// 已标记全量完成：确保 last_height 有值
		if lh, _ := getLastHeight(ctx, db); lh == 0 {
			if err := setLastHeight(ctx, db, initLast); err != nil {
				log.Warnw("init last_height", "height", initLast, "err", err)
			} else {
				log.Infow("init last_height", "to", initLast)
			}
		}
	}

	// 启动增量采集
	go runIncrementalLoop(ctx, api, db, claimsColl, c)
}

/********** main **********/
func main() {
	_ = logging.SetLogLevel("*", "info")

	c := loadCfg()
	log.Infow("boot",
		"lotus", maskURLToken(c.LotusURL),
		"mongo", c.MongoURI,
		"db", c.MongoDB, "coll", c.MongoColl,
		"poll", c.PollEvery.String(),
		"safeDelay", c.SafeDelay,
		"backfill", c.BackfillStart,
		"batch", c.MaxHeightsBatch,
	)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	// lotus
	full, closeLotus, err := connectLotus(ctx, c.LotusURL, c.LotusJWT)
	if err != nil {
		log.Fatal(err)
	}
	defer closeLotus()

	// mongo
	mc, claimsColl, err := connectMongo(ctx, c.MongoURI, c.MongoDB, c.MongoColl)
	if err != nil {
		log.Fatal(err)
	}
	defer mc.Disconnect(ctx)

	// 全量 + 增量
	bootstrapAndStart(ctx, full, mc.Database(c.MongoDB), claimsColl, c)

	<-ctx.Done()
	log.Info("shutting down")
}

/********** 小工具 **********/
func maskURLToken(u string) string {
	if len(u) < 8 {
		return u
	}
	return u[:8] + "...(" + fmt.Sprintf("%d bytes)", len(u))
}
