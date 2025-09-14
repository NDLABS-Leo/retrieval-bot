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
	ClaimID    *int64         `bson:"claim_id,omitempty"` // 这里增量不从消息中取 id；保留为可空
	ProviderID int64          `bson:"provider_id"`
	ClientID   int64          `bson:"client_id"`
	ClientAddr string         `bson:"client_addr"` // 全量阶段可填；增量阶段不反推，置空
	DataCID    string         `bson:"data_cid"`
	Size       int64          `bson:"size"`
	TermMin    int64          `bson:"term_min"`
	TermMax    int64          `bson:"term_max"`
	TermStart  int64          `bson:"term_start"`
	Sector     uint64         `bson:"sector"`
	MinerAddr  string         `bson:"miner_addr"` // f0... 可选
	UpdatedAt  time.Time      `bson:"updated_at"`
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

	// 唯一索引：provider_id + client_id + data_cid + sector
	_, _ = c.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "provider_id", Value: 1}, {Key: "client_id", Value: 1}, {Key: "data_cid", Value: 1}, {Key: "sector", Value: 1}},
		Options: options.Index().SetUnique(true).SetName("uniq_provider_client_data_sector"),
	})
	// 辅助索引
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

/********** 全量：逐矿工拉 StateGetClaims 并 upsert（保留） **********/
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
				ClaimID:    ptrI64(int64(cidNum)),
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
			filter := bson.M{
				"provider_id": doc.ProviderID,
				"client_id":   doc.ClientID,
				"data_cid":    doc.DataCID,
				"sector":      doc.Sector,
			}
			update := bson.M{"$set": doc}
			_, err = coll.UpdateOne(ctx, filter, update, options.Update().SetUpsert(true))
			if err != nil {
				log.Warnw("upsert claim failed", "filter", filter, "err", err)
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
	MaxHeightsBatch int64         // 单轮最多处理的高度数量
}

/********** 仅 ClaimAllocations 的增量主循环 **********/
func runIncrementalLoop(
	ctx context.Context,
	api v1api.FullNode,
	db *mongo.Database,
	claimsColl *mongo.Collection,
	cfg incCfg,
) {
	log.Infow("incremental loop start (ClaimAllocations only)",
		"poll", cfg.PollEvery.String(),
		"safeDelay", cfg.SafeDelay,
		"backfill", cfg.BackfillStart,
		"batch", cfg.MaxHeightsBatch,
	)

	tick := time.NewTicker(cfg.PollEvery)
	defer tick.Stop()

	jitter := func() time.Duration {
		r := (rand.Float64()*0.6 - 0.3) // -30%~+30%
		return time.Duration(float64(cfg.PollEvery) * r)
	}

	for {
		select {
		case <-ctx.Done():
			log.Info("incremental loop stop")
			return
		case <-tick.C:
			time.Sleep(jitter())

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

			last, err := getLastHeight(ctx, db)
			if err != nil {
				log.Warnw("getLastHeight failed", "err", err)
				continue
			}
			var start int64
			if last == 0 {
				start = target - cfg.BackfillStart
				if start < 0 {
					start = 0
				}
			} else {
				start = last + 1
			}
			if start > target {
				continue
			}

			end := target
			if cfg.MaxHeightsBatch > 0 && end-start+1 > cfg.MaxHeightsBatch {
				end = start + cfg.MaxHeightsBatch - 1
			}

			processedTo := last
			totalInserted := 0

			for h := start; h <= end; h++ {
				ts, err := api.ChainGetTipSetByHeight(ctx, abi.ChainEpoch(h), tip.Key())
				if err != nil || ts == nil {
					if err != nil {
						log.Warnw("ChainGetTipSetByHeight failed", "height", h, "err", err)
					}
					continue
				}

				insertedThisHeight := 0
				for _, bh := range ts.Blocks() {
					bm, err := api.ChainGetBlockMessages(ctx, bh.Cid())
					if err != nil {
						log.Warnw("ChainGetBlockMessages failed", "height", h, "block", bh.Cid().String(), "err", err)
						continue
					}

					// BLS
					for _, m := range bm.BlsMessages {
						if m.To != builtin.VerifiedRegistryActorAddr {
							continue
						}
						n, err := processClaimAllocationsMsg(ctx, api, claimsColl, m, ts.Key())
						if err != nil {
							log.Warnw("process ClaimAllocations (BLS) failed", "height", h, "msg", m.Cid().String(), "err", err)
							continue
						}
						insertedThisHeight += n
					}
					// SECP
					for _, sm := range bm.SecpkMessages {
						m := &sm.Message // 这里取地址，变成 *types.Message
						if m.To != builtin.VerifiedRegistryActorAddr {
							continue
						}
						n, err := processClaimAllocationsMsg(ctx, api, claimsColl, m, ts.Key())
						if err != nil {
							log.Warnw("process ClaimAllocations (SECP) failed",
								"height", h,
								"msg", sm.Cid().String(),
								"err", err,
							)
							continue
						}
						insertedThisHeight += n
					}
				}

				if insertedThisHeight > 0 {
					log.Infow("claims inserted by ClaimAllocations", "height", h, "count", insertedThisHeight)
				}
				totalInserted += insertedThisHeight
				processedTo = h
			}

			if processedTo >= start {
				if err := setLastHeight(ctx, db, processedTo); err != nil {
					log.Warnw("setLastHeight failed", "height", processedTo, "err", err)
				} else {
					log.Infow("advance last_height", "to", processedTo, "deltaInserted", totalInserted)
				}
			}
		}
	}
}

/********** 只解析 ClaimAllocations 的消息 **********/
func processClaimAllocationsMsg(
	ctx context.Context,
	api v1api.FullNode,
	claimsColl *mongo.Collection,
	m *types.Message,
	tsk types.TipSetKey,
) (int, error) {
	// 只处理方法名为 ClaimAllocations
	decAny, err := api.StateDecodeParams(ctx, m.To, m.Method, m.Params, tsk)
	if err != nil {
		return 0, fmt.Errorf("StateDecodeParams: %w", err)
	}

	// 通常返回的是 string
	decStr, ok := decAny.(string)
	if !ok {
		return 0, fmt.Errorf("unexpected type from StateDecodeParams: %T", decAny)
	}

	methodName, paramsMap, err := parseDecodeJSON(decStr)
	if err != nil {
		return 0, fmt.Errorf("parseDecodeJSON: %w", err)
	}
	if methodName != "" && methodName != "ClaimAllocations" {
		return 0, nil
	}
	// 某些版本没有 Method 字段：继续尝试从参数结构识别
	claims := extractClaimsFromAllocParams(paramsMap)
	if len(claims) == 0 {
		return 0, nil
	}
	return upsertClaims(ctx, claimsColl, claims)
}

/********** 解析辅助（宽松字段名适配） **********/
type DecodedClaim struct {
	ProviderID int64
	ClientID   int64
	DataCID    string
	Size       int64
	TermMin    int64
	TermMax    int64
	TermStart  int64
	Sector     uint64
	MinerIDStr string
}

func parseDecodeJSON(s string) (method string, params map[string]any, err error) {
	var top map[string]any
	if err = bson.UnmarshalExtJSON([]byte(s), false, &top); err != nil {
		return "", nil, err
	}
	if v, ok := top["Method"].(string); ok && v != "" {
		method = v
	}
	if p, ok := top["Params"].(map[string]any); ok {
		params = p
	} else {
		params = top
	}
	return
}

func extractClaimsFromAllocParams(p map[string]any) []DecodedClaim {
	var out []DecodedClaim
	cands := []string{"Claims", "Allocations", "Requests", "Entries", "Sectors"}
	var arr []any
	for _, k := range cands {
		if v, ok := p[k]; ok {
			if tmp, ok := v.([]any); ok {
				arr = tmp
				break
			}
		}
	}
	if len(arr) == 0 {
		return nil
	}
	for _, it := range arr {
		m, ok := it.(map[string]any)
		if !ok {
			continue
		}
		dc := DecodedClaim{
			ProviderID: toInt64(m, "Provider", "provider", "provider_id"),
			ClientID:   toInt64(m, "Client", "client", "client_id"),
			DataCID:    toCIDStr(m, "Data", "data", "piece_cid"),
			Size:       toInt64(m, "Size", "size", "padded_size"),
			TermMin:    toInt64(m, "TermMin", "term_min", "min_term"),
			TermMax:    toInt64(m, "TermMax", "term_max", "max_term"),
			TermStart:  toInt64(m, "TermStart", "term_start", "start_epoch"),
			Sector:     uint64(toInt64(m, "Sector", "sector", "sector_number")),
		}
		if s := toString(m, "Miner", "miner", "provider_id_str"); s != "" {
			dc.MinerIDStr = s
		}
		if dc.ProviderID == 0 || dc.ClientID == 0 || dc.DataCID == "" || dc.Sector == 0 {
			continue
		}
		out = append(out, dc)
	}
	return out
}

func toString(m map[string]any, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			if s, ok := v.(string); ok {
				return s
			}
		}
	}
	return ""
}
func toInt64(m map[string]any, keys ...string) int64 {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			switch t := v.(type) {
			case int32:
				return int64(t)
			case int64:
				return t
			case float64:
				return int64(t)
			case map[string]any:
				if s, ok := t["Int"].(string); ok {
					if iv, err := parseI64(s); err == nil {
						return iv
					}
				}
			case string:
				if iv, err := parseI64(t); err == nil {
					return iv
				}
			}
		}
	}
	return 0
}
func toCIDStr(m map[string]any, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			switch t := v.(type) {
			case string:
				return t
			case map[string]any:
				if s, ok := t["/"].(string); ok && s != "" {
					return s
				}
			}
		}
	}
	return ""
}
func parseI64(s string) (int64, error) {
	var x int64
	_, err := fmt.Sscan(s, &x)
	return x, err
}

func upsertClaims(ctx context.Context, coll *mongo.Collection, claims []DecodedClaim) (int, error) {
	if len(claims) == 0 {
		return 0, nil
	}
	newCount := 0
	for _, c := range claims {
		filter := bson.M{
			"provider_id": c.ProviderID,
			"client_id":   c.ClientID,
			"data_cid":    c.DataCID,
			"sector":      c.Sector,
		}
		update := bson.M{
			"$setOnInsert": bson.M{
				"provider_id": c.ProviderID,
				"client_id":   c.ClientID,
				"client_addr": "",
				"data_cid":    c.DataCID,
				"size":        c.Size,
				"term_min":    c.TermMin,
				"term_max":    c.TermMax,
				"term_start":  c.TermStart,
				"sector":      c.Sector,
				"miner_addr":  c.MinerIDStr,
				"updated_at":  time.Now(),
			},
			"$set": bson.M{ // 即便已存在，也更新更新时间与可能变化的字段（按需）
				"size":       c.Size,
				"term_min":   c.TermMin,
				"term_max":   c.TermMax,
				"term_start": c.TermStart,
				"updated_at": time.Now(),
			},
		}
		opts := options.Update().SetUpsert(true)
		res, err := coll.UpdateOne(ctx, filter, update, opts)
		if err != nil {
			log.Warnw("upsert claim failed", "filter", filter, "err", err)
			continue
		}
		if res.UpsertedCount > 0 {
			newCount++
		}
	}
	return newCount, nil
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
		log.Info("full import marked as done, start incremental (ClaimAllocations) only")
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

	// 启动增量（仅 ClaimAllocations）
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

	// 启动：按需全量，随后增量（仅 ClaimAllocations）
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
func ptrI64(v int64) *int64 { return &v }

func maskURLToken(u string) string {
	if len(u) < 8 {
		return u
	}
	return u[:8] + "...(" + fmt.Sprintf("%d bytes)", len(u))
}

func hexStr(b []byte) string { return hex.EncodeToString(b) }
