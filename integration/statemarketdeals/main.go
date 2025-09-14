// main.go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/ipfs/go-cid"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strings"
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

/********** Mongo 文档结构（入库结构）**********/
type DBClaim struct {
	ClaimID    int64          `bson:"claim_id"`              // 可为空（增量时未直接给出），全量时可填入链上 claimId
	ProviderID int64          `bson:"provider_id"`           // abi.ActorID
	ClientID   int64          `bson:"client_id,omitempty"`   // 可选
	ClientAddr string         `bson:"client_addr,omitempty"` // 可选（解析账户钥匙地址可另行补全）
	DataCID    string         `bson:"data_cid"`              // cid string
	Size       int64          `bson:"size"`
	TermMin    int64          `bson:"term_min"`
	TermMax    int64          `bson:"term_max"`
	TermStart  int64          `bson:"term_start"`
	Sector     uint64         `bson:"sector"`
	MinerAddr  string         `bson:"miner_addr,omitempty"` // 例如 f0<providerID>；或基于 ID 地址
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

	// 业务唯一键：避免重复（增量）
	// 唯一索引：(provider_id, data_cid, sector, term_start)
	_, _ = c.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "provider_id", Value: 1}, {Key: "data_cid", Value: 1}, {Key: "sector", Value: 1}, {Key: "term_start", Value: 1}},
		Options: options.Index().SetUnique(true).SetName("uniq_claim_tuple"),
	})
	// 兼容旧逻辑（若你仍需要通过全量 claim_id 做去重，可保留此唯一索引）
	_, _ = c.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "provider_id", Value: 1}, {Key: "claim_id", Value: 1}},
		Options: options.Index().SetUnique(true).SetSparse(true).SetName("uniq_provider_claimid"),
	})
	// 辅助检索
	_, _ = c.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "client_addr", Value: 1}}},
		{Keys: bson.D{{Key: "miner_addr", Value: 1}}},
		{Keys: bson.D{{Key: "updated_at", Value: -1}}},
	})

	return mc, c, nil
}

/********** 工具：是否有算力 **********/
func hasNonZeroPower(p *lotusapi.MinerPower) bool {
	if p == nil {
		return false
	}
	return p.MinerPower.RawBytePower.GreaterThan(types.NewInt(0)) ||
		p.MinerPower.QualityAdjPower.GreaterThan(types.NewInt(0))
}

/********** 全量：逐矿工 StateGetClaims 并 upsert **********/
func crawlOnce(ctx context.Context, api v1api.FullNode, coll *mongo.Collection) error {
	head := types.EmptyTSK

	miners, err := api.StateListMiners(ctx, head)
	if err != nil {
		return fmt.Errorf("StateListMiners: %w", err)
	}
	log.Infof("miners listed: %d", len(miners))

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

	upserts := 0
	for _, m := range active {
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

		mclaims, err := api.StateGetClaims(ctx, idAddr, head)
		if err != nil {
			log.Warnw("StateGetClaims", "provider", idAddr.String(), "err", err)
			continue
		}
		if len(mclaims) == 0 {
			continue
		}

		for claimID, c := range mclaims {
			doc := DBClaim{
				ClaimID:    int64(claimID),
				ProviderID: int64(providerID),
				ClientID:   int64(c.Client),
				ClientAddr: "", // 可选：如需可再补 StateAccountKey
				DataCID:    c.Data.String(),
				Size:       int64(c.Size),
				TermMin:    int64(c.TermMin),
				TermMax:    int64(c.TermMax),
				TermStart:  int64(c.TermStart),
				Sector:     uint64(c.Sector),
				MinerAddr:  idAddr.String(),
				UpdatedAt:  time.Now(),
			}
			// 兼容：优先业务唯一键防重；若 claim_id 唯一键存在也会命中
			filter := bson.M{
				"provider_id": doc.ProviderID,
				"data_cid":    doc.DataCID,
				"sector":      doc.Sector,
				"term_start":  doc.TermStart,
			}
			update := bson.M{"$setOnInsert": doc, "$set": bson.M{"updated_at": time.Now()}}
			res, err := coll.UpdateOne(ctx, filter, update, options.Update().SetUpsert(true))
			if err != nil {
				if !mongo.IsDuplicateKeyError(err) {
					log.Warnw("upsert claim failed", "provider_id", doc.ProviderID, "claim_id", doc.ClaimID, "err", err)
				}
				continue
			}
			if res.UpsertedCount > 0 {
				upserts++
			}
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

/********** 增量：通过 StateReplay 扫 ExecutionTrace 的内部调用 **********/

// VerifiedRegistry.ClaimAllocations 的方法号（在 v9+ 一直是 9）
const claimAllocationsMethodNum = abi.MethodNum(9)

// 从 StateDecodeParams 的 JSON 中尽量判断方法名
func methodNameFromDecodeJSON(decStr string) string {
	s := strings.ToLower(decStr)
	if strings.Contains(s, "claimallocations") { // 常见
		return "ClaimAllocations"
	}
	if strings.Contains(s, "claim") && strings.Contains(s, "alloc") {
		return "ClaimAllocations"
	}
	return ""
}

func decodeParamsToString(v interface{}) string {
	switch val := v.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	default:
		// 兜底转 JSON 字符串
		bz, err := json.Marshal(val)
		if err != nil {
			return fmt.Sprintf("%v", val)
		}
		return string(bz)
	}
}

// 判断是否是 ClaimAllocations：先试图从 decode 名称判断，失败则回退方法号=9
func isClaimAllocations(ctx context.Context, api v1api.FullNode,
	to address.Address, method abi.MethodNum, params []byte, tsk types.TipSetKey) (bool, string) {

	if to != builtin.VerifiedRegistryActorAddr {
		return false, ""
	}
	decStr, err := api.StateDecodeParams(ctx, to, method, params, tsk)
	if err == nil && decStr != "" {
		desStr := decodeParamsToString(decStr)

		if methodNameFromDecodeJSON(desStr) == "ClaimAllocations" {
			return true, "name"
		}
	}
	if method == claimAllocationsMethodNum {
		return true, "mnum"
	}
	return false, ""
}

// 宽松地从 decode JSON 里提取 claims 数组（不同版本 JSON 结构不一致）
type claimJSON struct {
	Provider  uint64 `json:"Provider"`
	Client    uint64 `json:"Client"`
	Data      string `json:"Data"`
	Size      int64  `json:"Size"`
	TermMin   int64  `json:"TermMin"`
	TermMax   int64  `json:"TermMax"`
	TermStart int64  `json:"TermStart"`
	Sector    uint64 `json:"Sector"`
}

func parseClaimAllocationsParamsFromDecode(
	ctx context.Context,
	api v1api.FullNode,
	to address.Address,
	method abi.MethodNum,
	params []byte,
	tsk types.TipSetKey,
) ([]DBClaim, error) {
	decStr, err := api.StateDecodeParams(ctx, to, method, params, tsk)
	if err != nil {
		return nil, fmt.Errorf("StateDecodeParams: %w", err)
	}
	var raw map[string]any
	desStr := decodeParamsToString(decStr)
	if err := json.Unmarshal([]byte(desStr), &raw); err != nil {
		return nil, fmt.Errorf("decode json: %w", err)
	}

	// 寻找 claims 数组字段：claims / Claims / params.claims / params.Claims
	var arr any
	keys := []string{"claims", "Claims"}
	for _, k := range keys {
		if v, ok := raw[k]; ok {
			arr = v
			break
		}
	}
	if arr == nil {
		if p, ok := raw["params"].(map[string]any); ok {
			for _, k := range keys {
				if v, ok2 := p[k]; ok2 {
					arr = v
					break
				}
			}
		}
	}
	if arr == nil {
		// 没有 claims 字段，视为没有可解析的 Claim
		return nil, nil
	}

	bz, _ := json.Marshal(arr)
	var list []claimJSON
	if err := json.Unmarshal(bz, &list); err != nil {
		return nil, fmt.Errorf("claims array json: %w\nraw=%s", err, decStr)
	}

	out := make([]DBClaim, 0, len(list))
	now := time.Now()
	for _, c := range list {
		out = append(out, DBClaim{
			ProviderID: int64(c.Provider),
			ClientID:   int64(c.Client),
			DataCID:    c.Data,
			Size:       c.Size,
			TermMin:    c.TermMin,
			TermMax:    c.TermMax,
			TermStart:  c.TermStart,
			Sector:     c.Sector,
			MinerAddr:  fmt.Sprintf("f0%d", c.Provider),
			UpdatedAt:  now,
		})
	}
	return out, nil
}

// 使用业务唯一键做 upsert 去重
func upsertClaims(ctx context.Context, coll *mongo.Collection, claims []DBClaim) (int64, error) {
	if len(claims) == 0 {
		return 0, nil
	}
	var inserted int64
	for _, c := range claims {
		filter := bson.M{
			"provider_id": c.ProviderID,
			"data_cid":    c.DataCID,
			"sector":      c.Sector,
			"term_start":  c.TermStart,
		}
		update := bson.M{"$setOnInsert": c, "$set": bson.M{"updated_at": time.Now()}}
		res, err := coll.UpdateOne(ctx, filter, update, options.Update().SetUpsert(true))
		if err != nil {
			if !mongo.IsDuplicateKeyError(err) {
				return inserted, err
			}
			continue
		}
		if res.UpsertedCount > 0 {
			inserted++
		}
	}
	return inserted, nil
}

// 递归遍历 ExecutionTrace，查找内部调用
func walkExecTraceForClaims(
	ctx context.Context,
	api v1api.FullNode,
	coll *mongo.Collection,
	tsk types.TipSetKey,
	t types.ExecutionTrace,
) (int64, error) {
	var total int64

	// 当前节点
	if t.Msg.To == builtin.VerifiedRegistryActorAddr {
		ok, _ := isClaimAllocations(ctx, api, t.Msg.To, t.Msg.Method, t.Msg.Params, tsk)
		if ok {
			claims, err := parseClaimAllocationsParamsFromDecode(ctx, api, t.Msg.To, t.Msg.Method, t.Msg.Params, tsk)
			if err != nil {
				return total, err
			}
			n, err := upsertClaims(ctx, coll, claims)
			if err != nil {
				return total, err
			}
			total += n
			if n > 0 {
				log.Infow("ClaimAllocations(inner) inserted", "count", n)
			}
		}
	}

	// 子调用递归
	for _, child := range t.Subcalls {
		n, err := walkExecTraceForClaims(ctx, api, coll, tsk, child)
		if err != nil {
			return total, err
		}
		total += n
	}
	return total, nil
}

// 扫描一个消息（BLS 或 SECP），通过 StateReplay 获取 ExecutionTrace 并递归查找 ClaimAllocations
func scanOneMessageForClaims(
	ctx context.Context,
	api v1api.FullNode,
	coll *mongo.Collection,
	tsk types.TipSetKey,
	msgCid cid.Cid,
) (int64, error) {
	tr, err := api.StateReplay(ctx, tsk, msgCid) // v1.23 签名：Replay(ctx, tsk, mcid)
	if err != nil {
		return 0, fmt.Errorf("StateReplay: %w", err)
	}
	if tr == nil {
		return 0, nil
	}
	return walkExecTraceForClaims(ctx, api, coll, tsk, tr.ExecutionTrace)
}

/********** 增量主循环（按高度） **********/
type incCfg struct {
	PollEvery       time.Duration // 轮询间隔（<30s，含抖动）
	SafeDelay       int64         // 与链头保持的安全延迟（避免重组）
	BackfillStart   int64         // 没有 last_height 时，向后回补的高度窗口
	MaxHeightsBatch int64         // 单轮最多处理的高度数量
}

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

			var processedTo = last
			var deltaInserted int64

			for h := start; h <= end; h++ {
				ts, err := api.ChainGetTipSetByHeight(ctx, abi.ChainEpoch(h), tip.Key())
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

					// Replay 需要消息 CID。api.BlockMessages.Cids 顺序通常是 BLS 在前、SECP 在后。
					// 保险起见可按顺序对应扫描：
					idx := 0
					// BLS
					for range bm.BlsMessages {
						if idx >= len(bm.Cids) {
							break
						}
						cid := bm.Cids[idx]
						n, err := scanOneMessageForClaims(ctx, api, claimsColl, ts.Key(), cid)
						if err != nil {
							log.Warnw("replay(BLS) failed", "height", h, "msg", cid.String(), "err", err)
						}
						insertedThisHeight += n
						idx++
					}
					// SECP
					for range bm.SecpkMessages {
						if idx >= len(bm.Cids) {
							break
						}
						cid := bm.Cids[idx]
						n, err := scanOneMessageForClaims(ctx, api, claimsColl, ts.Key(), cid)
						if err != nil {
							log.Warnw("replay(SECP) failed", "height", h, "msg", cid.String(), "err", err)
						}
						insertedThisHeight += n
						idx++
					}
				}

				deltaInserted += insertedThisHeight
				processedTo = h
			}

			if processedTo >= start {
				if err := setLastHeight(ctx, db, processedTo); err != nil {
					log.Warnw("setLastHeight failed", "height", processedTo, "err", err)
				} else {
					log.Infow("advance last_height", "to", processedTo, "deltaInserted", deltaInserted)
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

	cnt, err := claimsColl.EstimatedDocumentCount(ctx)
	if err != nil {
		log.Fatalw("count claims failed", "err", err)
	}

	if cnt == 0 {
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

	bootstrapAndStart(ctx, full, mc.Database(cfg.MongoDB), claimsColl, crawlOnce, incCfg{
		PollEvery:       15 * time.Second, // < 30s，带抖动
		SafeDelay:       1,                // 与头部保持 1 个 Epoch 安全延迟
		BackfillStart:   120,              // 无 last_height 时回补约 1h
		MaxHeightsBatch: 180,              // 单轮最多处理 180 个高度
	})

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
