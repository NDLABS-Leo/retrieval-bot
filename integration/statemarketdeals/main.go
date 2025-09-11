package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	lotusapi "github.com/filecoin-project/lotus/api"
	lotusclient "github.com/filecoin-project/lotus/api/client"
	"github.com/filecoin-project/lotus/api/v1api"
	"github.com/filecoin-project/lotus/chain/types"
	logging "github.com/ipfs/go-log/v2"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var log = logging.Logger("claims-crawler")

// --- 配置 ---
type cfg struct {
	LotusURL  string
	LotusJWT  string
	MongoURI  string
	MongoDB   string
	MongoColl string
	Interval  time.Duration
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
		Interval:  12 * time.Hour,
	}
}

// --- Mongo 文档结构（入库结构）---
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
	MinerAddr  string         `bson:"miner_addr"`  // f0/f0.. ID 地址（provider 对应矿工ID地址）
	UpdatedAt  time.Time      `bson:"updated_at"`  // upsert 时间
	Meta       map[string]any `bson:"meta,omitempty"`
}

// --- 连接 Lotus ---
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

// --- 连接 Mongo + 索引 ---
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
	return mc, c, nil
}

// --- 判断矿工是否“有算力” ---
func hasNonZeroPower(p *lotusapi.MinerPower) bool {
	if p == nil {
		return false
	}
	// Raw 或 QAP 只要有一个 > 0 即视作有算力
	return p.MinerPower.RawBytePower.GreaterThan(types.NewInt(0)) ||
		p.MinerPower.QualityAdjPower.GreaterThan(types.NewInt(0))
}

// --- 主抓取逻辑：列出矿工 -> 拉取 claims -> upsert ---
func crawlOnce(ctx context.Context, api v1api.FullNode, coll *mongo.Collection) error {
	head := types.EmptyTSK // 用当前链头

	// 1) 所有“声明过算力”的矿工地址
	miners, err := api.StateListMiners(ctx, head)
	if err != nil {
		return fmt.Errorf("StateListMiners: %w", err)
	}
	log.Infof("miners listed: %d", len(miners)) // StateListMiners 定义见官方 API。 [oai_citation:1‡Go Packages](https://pkg.go.dev/github.com/filecoin-project/lotus/api?utm_source=chatgpt.com)

	// 2) 过滤出当前算力 > 0 的矿工
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

	// 3) 对每个矿工查询 verifreg Claims
	upserts := 0
	for _, m := range active {
		// 3.1 将矿工转为 ID 地址并取 numeric ActorID
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

		// 3.2 拉取 Claims（provider 维度）
		mclaims, err := api.StateGetClaims(ctx, idAddr, head)
		if err != nil {
			log.Warnw("StateGetClaims", "provider", idAddr.String(), "err", err)
			continue
		}
		if len(mclaims) == 0 {
			continue
		}

		// 3.3 Upsert 到 Mongo（provider_id + claim_id 唯一）
		for cidNum, c := range mclaims {
			// 先把 abi.ActorID -> f0（ID 地址）
			idClientAddr, err := address.NewIDAddress(uint64(c.Client))
			if err != nil {
				log.Warnw("NewIDAddress", "client_id", c.Client, "err", err)
				continue
			}

			// 再把 f0 -> f1/f3/f4 等“公钥/委托”地址（更可读）
			keyAddr, err := api.StateAccountKey(ctx, idClientAddr, head)
			clientAddrStr := idClientAddr.String() // 兜底用 ID 地址
			if err != nil {
				log.Warnw("StateAccountKey", "client_id", c.Client, "err", err)
			} else {
				clientAddrStr = keyAddr.String()
			}
			doc := DBClaim{
				ClaimID:    int64(cidNum), // verifreg.ClaimId 是别名整型
				ProviderID: int64(providerID),
				ClientID:   int64(c.Client),
				ClientAddr: clientAddrStr, // ← 新增
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
				// 若违反唯一键，说明已有，忽略其余错误则记录
				log.Warnw("upsert claim failed", "provider_id", doc.ProviderID, "claim_id", doc.ClaimID, "err", err)
				continue
			}
			upserts++
		}
	}
	log.Infow("crawl finished", "upserts", upserts)
	return nil
}

func main() {
	_ = logging.SetLogLevel("*", "info")

	cfg := loadCfg()
	log.Infow("boot",
		"lotus", cfg.LotusURL,
		"mongo", cfg.MongoURI,
		"db", cfg.MongoDB, "coll", cfg.MongoColl,
		"interval", cfg.Interval.String(),
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
	mc, coll, err := connectMongo(ctx, cfg.MongoURI, cfg.MongoDB, cfg.MongoColl)
	if err != nil {
		log.Fatal(err)
	}
	defer mc.Disconnect(ctx)

	// 先跑一次
	if err := crawlOnce(ctx, full, coll); err != nil {
		log.Error(err)
	}

	// 每 12 小时跑一次
	t := time.NewTicker(cfg.Interval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Info("shutting down")
			return
		case <-t.C:
			if err := crawlOnce(ctx, full, coll); err != nil {
				log.Error(err)
			}
		}
	}
}
