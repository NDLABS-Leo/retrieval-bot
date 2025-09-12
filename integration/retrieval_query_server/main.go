// main.go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Config struct {
	MongoURI  string
	MongoDB   string
	RedisAddr string
	RedisDB   int
	BindAddr  string
}

var (
	cfg       Config
	mgo       *mongo.Client
	db        *mongo.Database
	colResult *mongo.Collection // claims_task_result
	rds       *redis.Client
)

const (
	redisTTL        = 24 * time.Hour
	statsPeriod     = 24 * time.Hour
	defaultBind     = ":8787"
	zsetMinerHTTP   = "idx:miners:http" // score = http success rate
	keyMinerPrefix  = "stats:miner:"    // stats:miner:<miner_id>
	keyClientPrefix = "stats:client:"   // stats:client:<client_addr> (value = JSON array of items)
	defaultPageSize = 15
	maxPageSize     = 200
)

type RateDoc struct {
	SuccessRateHTTP      float64 `json:"success_rate_http"`
	SuccessRateGraphsync float64 `json:"success_rate_graphsync"`
	SuccessRateBitswap   float64 `json:"success_rate_bitswap"`
}

// client 统计项（一个 client 下的每个 miner 一条）
type ClientMinerItem struct {
	ClientAddr           string  `json:"client_addr"`
	MinerAddr            string  `json:"miner_addr"`
	SuccessRateHTTP      float64 `json:"success_rate_http"`
	SuccessRateGraphsync float64 `json:"success_rate_graphsync"`
	SuccessRateBitswap   float64 `json:"success_rate_bitswap"`
}

type aggOut3Keys struct {
	ID struct {
		Client string `bson:"client"`
		Miner  string `bson:"miner"`
		Module string `bson:"module"`
	} `bson:"_id"`
	Total int64 `bson:"total"`
	OK    int64 `bson:"ok"`
}

type aggOut2Keys struct {
	ID struct {
		Miner  string `bson:"miner"`
		Module string `bson:"module"`
	} `bson:"_id"`
	Total int64 `bson:"total"`
	OK    int64 `bson:"ok"`
}

func mustInit() {
	cfg = Config{
		MongoURI:  getenv("MONGO_URI", "mongodb://127.0.0.1:27017"),
		MongoDB:   getenv("MONGO_DB", "fil"),
		RedisAddr: getenv("REDIS_ADDR", "127.0.0.1:6379"),
		RedisDB:   mustAtoi(getenv("REDIS_DB", "0")),
		BindAddr:  getenv("BIND_ADDR", defaultBind),
	}

	var err error
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mgo, err = mongo.Connect(ctx, options.Client().ApplyURI(cfg.MongoURI))
	if err != nil {
		log.Fatalf("mongo connect: %v", err)
	}
	if err := mgo.Ping(ctx, nil); err != nil {
		log.Fatalf("mongo ping: %v", err)
	}
	db = mgo.Database(cfg.MongoDB)
	colResult = db.Collection("claims_task_result")

	rds = redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, DB: cfg.RedisDB})
	if err := rds.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("redis ping: %v", err)
	}
	log.Printf("init ok. mongo=%s db=%s redis=%s bind=%s", cfg.MongoURI, cfg.MongoDB, cfg.RedisAddr, cfg.BindAddr)
}

func startCron() {
	go func() {
		runOnce()
		ticker := time.NewTicker(statsPeriod)
		defer ticker.Stop()
		for range ticker.C {
			runOnce()
		}
	}()
}

func runOnce() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// 1) client_addr + miner_addr（分别统计 http/graphsync/bitswap）
	if err := computeAndStoreClientMiner(ctx); err != nil {
		log.Printf("[cron] client+miner agg error: %v", err)
	} else {
		log.Println("[cron] client+miner agg ok")
	}

	// 2) miner_addr（分别统计 http/graphsync/bitswap；ZSET 仍用 http 排序）
	if err := computeAndStoreMiner(ctx); err != nil {
		log.Printf("[cron] miner agg error: %v", err)
	} else {
		log.Println("[cron] miner agg ok")
	}
}

// ============= 聚合 =============

// client_addr + miner_addr + module
func computeAndStoreClientMiner(ctx context.Context) error {
	// 成功率 = 每个 module 下 success(true)/total；分别统计 http/graphsync/bitswap
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.M{
			// 可按需加时间窗，例如：
			// "created_at": bson.M{"$gte": time.Now().Add(-24 * time.Hour)},
			"task.module": bson.M{"$in": bson.A{"http", "graphsync", "bitswap"}},
		}}},
		{{Key: "$group", Value: bson.M{
			"_id": bson.M{
				"client": "$task.metadata.client",
				"miner":  "$task.provider.id",
				"module": "$task.module",
			},
			"total": bson.M{"$sum": 1},
			"ok":    bson.M{"$sum": bson.M{"$cond": []any{"$result.success", 1, 0}}},
		}}},
	}

	cur, err := colResult.Aggregate(ctx, pipeline, options.Aggregate().SetAllowDiskUse(true))
	if err != nil {
		return err
	}
	defer cur.Close(ctx)

	// 临时累加器：client -> miner -> module -> (ok,total)
	type pair struct{ ok, total int64 }
	acc := make(map[string]map[string]map[string]pair, 4096)

	for cur.Next(ctx) {
		var a aggOut3Keys
		if err := cur.Decode(&a); err != nil {
			return err
		}
		if a.ID.Client == "" || a.ID.Miner == "" || a.ID.Module == "" || a.Total == 0 {
			continue
		}
		if _, ok := acc[a.ID.Client]; !ok {
			acc[a.ID.Client] = make(map[string]map[string]pair)
		}
		if _, ok := acc[a.ID.Client][a.ID.Miner]; !ok {
			acc[a.ID.Client][a.ID.Miner] = make(map[string]pair, 3)
		}
		acc[a.ID.Client][a.ID.Miner][a.ID.Module] = pair{ok: a.OK, total: a.Total}
	}
	if err := cur.Err(); err != nil {
		return err
	}

	// 汇总为列表，并写入 Redis：stats:client:<client_addr>（值是 JSON 数组）
	pipe := rds.Pipeline()
	for client, miners := range acc {
		list := make([]ClientMinerItem, 0, len(miners))
		for miner, byModule := range miners {
			var httpRate, gsRate, bsRate float64

			if v, ok := byModule["http"]; ok && v.total > 0 {
				httpRate = float64(v.ok) / float64(v.total)
			}
			if v, ok := byModule["graphsync"]; ok && v.total > 0 {
				gsRate = float64(v.ok) / float64(v.total)
			}
			if v, ok := byModule["bitswap"]; ok && v.total > 0 {
				bsRate = float64(v.ok) / float64(v.total)
			}

			list = append(list, ClientMinerItem{
				ClientAddr:           client,
				MinerAddr:            miner,
				SuccessRateHTTP:      httpRate,
				SuccessRateGraphsync: gsRate,
				SuccessRateBitswap:   bsRate,
			})
		}
		// 为方便列表页展示，先按 http 降序存储
		sort.Slice(list, func(i, j int) bool { return list[i].SuccessRateHTTP > list[j].SuccessRateHTTP })
		bz, _ := json.Marshal(list)
		pipe.Set(ctx, keyClientPrefix+client, string(bz), redisTTL)
	}
	_, err = pipe.Exec(ctx)
	return err
}

// miner_addr + module
func computeAndStoreMiner(ctx context.Context) error {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.M{
			// "created_at": bson.M{"$gte": time.Now().Add(-24 * time.Hour)},
			"task.module": bson.M{"$in": bson.A{"http", "graphsync", "bitswap"}},
		}}},
		{{Key: "$group", Value: bson.M{
			"_id": bson.M{
				"miner":  "$task.provider.id",
				"module": "$task.module",
			},
			"total": bson.M{"$sum": 1},
			"ok":    bson.M{"$sum": bson.M{"$cond": []any{"$result.success", 1, 0}}},
		}}},
	}

	cur, err := colResult.Aggregate(ctx, pipeline, options.Aggregate().SetAllowDiskUse(true))
	if err != nil {
		return err
	}
	defer cur.Close(ctx)

	// 累加：miner -> module -> (ok,total)
	type pair struct{ ok, total int64 }
	acc := make(map[string]map[string]pair, 4096)

	for cur.Next(ctx) {
		var a aggOut2Keys
		if err := cur.Decode(&a); err != nil {
			return err
		}
		if a.ID.Miner == "" || a.ID.Module == "" || a.Total == 0 {
			continue
		}
		if _, ok := acc[a.ID.Miner]; !ok {
			acc[a.ID.Miner] = make(map[string]pair, 3)
		}
		acc[a.ID.Miner][a.ID.Module] = pair{ok: a.OK, total: a.Total}
	}
	if err := cur.Err(); err != nil {
		return err
	}

	pipe := rds.Pipeline()
	pipe.Del(ctx, zsetMinerHTTP) // 重建 http 排序索引

	for miner, byModule := range acc {
		var httpRate, gsRate, bsRate float64
		if v, ok := byModule["http"]; ok && v.total > 0 {
			httpRate = float64(v.ok) / float64(v.total)
		}
		if v, ok := byModule["graphsync"]; ok && v.total > 0 {
			gsRate = float64(v.ok) / float64(v.total)
		}
		if v, ok := byModule["bitswap"]; ok && v.total > 0 {
			bsRate = float64(v.ok) / float64(v.total)
		}

		doc := RateDoc{SuccessRateHTTP: httpRate, SuccessRateGraphsync: gsRate, SuccessRateBitswap: bsRate}
		bz, _ := json.Marshal(doc)
		pipe.Set(ctx, keyMinerPrefix+miner, string(bz), redisTTL)
		pipe.ZAdd(ctx, zsetMinerHTTP, redis.Z{Member: miner, Score: httpRate})
	}

	_, err = pipe.Exec(ctx)
	return err
}

// ============= HTTP =============

// /miners?miner_addr=&page=&page_size=
// - 若传 miner_addr：仅返回该 miner 的条目（不分页）
// - 否则从 ZSET 按成功率 http 降序分页
func handleMiners(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()
	miner := q.Get("miner_addr")

	if miner != "" {
		val, err := rds.Get(ctx, keyMinerPrefix+miner).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				writeJSON(w, map[string]any{"count": 0, "items": []any{}})
				return
			}
			http.Error(w, "redis error: "+err.Error(), http.StatusInternalServerError)
			return
		}
		var rd RateDoc
		_ = json.Unmarshal([]byte(val), &rd)
		item := map[string]string{
			"miner_id":               miner,
			"success_rate_http":      pct(rd.SuccessRateHTTP),
			"success_rate_graphsync": pct(rd.SuccessRateGraphsync),
			"success_rate_bitswap":   pct(rd.SuccessRateBitswap),
		}
		writeJSON(w, map[string]any{"count": 1, "items": []any{item}})
		return
	}

	page, pageSize := parsePage(q.Get("page"), q.Get("page_size"))
	start := int64((page - 1) * pageSize)
	end := start + int64(pageSize) - 1

	ids, err := rds.ZRevRange(ctx, zsetMinerHTTP, start, end).Result()
	if err != nil {
		http.Error(w, "redis zset error: "+err.Error(), http.StatusInternalServerError)
		return
	}
	items := make([]map[string]string, 0, len(ids))
	for _, id := range ids {
		val, err := rds.Get(ctx, keyMinerPrefix+id).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue
			}
			http.Error(w, "redis get error: "+err.Error(), http.StatusInternalServerError)
			return
		}
		var rd RateDoc
		_ = json.Unmarshal([]byte(val), &rd)
		items = append(items, map[string]string{
			"miner_id":               id,
			"success_rate_http":      pct(rd.SuccessRateHTTP),
			"success_rate_graphsync": pct(rd.SuccessRateGraphsync),
			"success_rate_bitswap":   pct(rd.SuccessRateBitswap),
		})
	}

	// 总数（用于分页）
	total, _ := rds.ZCard(ctx, zsetMinerHTTP).Result()
	writeJSON(w, map[string]any{
		"page":      page,
		"page_size": pageSize,
		"total":     total,
		"items":     items,
	})
}

// /clients?client_addr=&page=&page_size=
// - 必须传 client_addr
// - 从 Redis key stats:client:<client_addr> 读取 JSON 数组
// - 按 http 降序后分页返回（写入时已排序，这里再保护性排序）
func handleClients(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()
	client := q.Get("client_addr")
	if client == "" {
		http.Error(w, "client_addr is required", http.StatusBadRequest)
		return
	}

	val, err := rds.Get(ctx, keyClientPrefix+client).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			writeJSON(w, map[string]any{"count": 0, "items": []any{}})
			return
		}
		http.Error(w, "redis error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	var list []ClientMinerItem
	if err := json.Unmarshal([]byte(val), &list); err != nil {
		http.Error(w, "decode error: "+err.Error(), http.StatusInternalServerError)
		return
	}
	sort.Slice(list, func(i, j int) bool { return list[i].SuccessRateHTTP > list[j].SuccessRateHTTP })

	page, pageSize := parsePage(q.Get("page"), q.Get("page_size"))
	start := (page - 1) * pageSize
	if start >= len(list) {
		writeJSON(w, map[string]any{
			"page":      page,
			"page_size": pageSize,
			"total":     len(list),
			"items":     []any{},
		})
		return
	}
	end := start + pageSize
	if end > len(list) {
		end = len(list)
	}
	sub := list[start:end]

	items := make([]map[string]string, 0, len(sub))
	for _, it := range sub {
		items = append(items, map[string]string{
			"client_id":              it.ClientAddr,
			"miner_id":               it.MinerAddr,
			"success_rate_http":      pct(it.SuccessRateHTTP),
			"success_rate_graphsync": pct(it.SuccessRateGraphsync),
			"success_rate_bitswap":   pct(it.SuccessRateBitswap),
		})
	}

	writeJSON(w, map[string]any{
		"page":      page,
		"page_size": pageSize,
		"total":     len(list),
		"items":     items,
	})
}

// /details?miner_addr=...|client_addr=...&status=0|1&retrieval_method=http|graphsync|bitswap&page=&page_size=
func handleDetails(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()

	// 支持 http / graphsync / bitswap（默认 http）
	method := q.Get("retrieval_method")
	if method == "" {
		method = "http"
	}
	switch method {
	case "http", "graphsync", "bitswap":
	default:
		http.Error(w, "retrieval_method must be one of: http | graphsync | bitswap", http.StatusBadRequest)
		return
	}

	filter := bson.M{"task.module": method}
	if miner := q.Get("miner_addr"); miner != "" {
		filter["task.provider.id"] = miner
	}
	if client := q.Get("client_addr"); client != "" {
		filter["task.metadata.client"] = client
	}
	if status := q.Get("status"); status != "" {
		switch status {
		case "0":
			filter["result.success"] = true
		case "1":
			filter["result.success"] = false
		default:
			http.Error(w, "status must be 0 or 1", http.StatusBadRequest)
			return
		}
	}

	page, pageSize := parsePage(q.Get("page"), q.Get("page_size"))
	skip := int64((page - 1) * pageSize)
	limit := int64(pageSize)

	opts := options.Find().
		SetSort(bson.D{{Key: "created_at", Value: -1}}).
		SetSkip(skip).
		SetLimit(limit)

	cur, err := colResult.Find(ctx, filter, opts)
	if err != nil {
		http.Error(w, "mongo find error: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer cur.Close(ctx)

	type Row struct {
		MinerID         string      `json:"miner_id"`
		CID             string      `json:"cid"`
		Status          bool        `json:"status"`
		ReturnCode      string      `json:"return_code"`
		ResponseMessage string      `json:"response_message"`
		CreationTime    interface{} `json:"creation_time"`
	}

	var items []Row
	for cur.Next(ctx) {
		var m bson.M
		if err := cur.Decode(&m); err != nil {
			http.Error(w, "decode error: "+err.Error(), http.StatusInternalServerError)
			return
		}
		items = append(items, Row{
			MinerID:         getString(m, "task", "provider", "id"),
			CID:             getString(m, "task", "content", "cid"),
			Status:          getBool(m, "result", "success"),
			ReturnCode:      getString(m, "result", "error_code"),
			ResponseMessage: getString(m, "result", "error_message"),
			CreationTime:    m["created_at"],
		})
	}
	if err := cur.Err(); err != nil {
		http.Error(w, "cursor error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, map[string]any{
		"page":      page,
		"page_size": pageSize,
		"count":     len(items),
		"items":     items,
	})
}

// ============= utils =============

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
func mustAtoi(s string) int {
	n, err := strconv.Atoi(s)
	if err != nil {
		log.Fatalf("atoi %q: %v", s, err)
	}
	return n
}
func pct(f float64) string { return fmt.Sprintf("%.2f%%", f*100) }

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
}

func parsePage(pStr, psStr string) (int, int) {
	page := 1
	if v, err := strconv.Atoi(pStr); err == nil && v > 0 {
		page = v
	}
	ps := defaultPageSize
	if v, err := strconv.Atoi(psStr); err == nil && v > 0 && v <= maxPageSize {
		ps = v
	}
	return page, ps
}

func getString(m bson.M, path ...string) string {
	var cur any = m
	for _, p := range path {
		mm, ok := cur.(bson.M)
		if !ok {
			return ""
		}
		cur = mm[p]
	}
	if s, ok := cur.(string); ok {
		return s
	}
	return ""
}
func getBool(m bson.M, path ...string) bool {
	var cur any = m
	for _, p := range path {
		mm, ok := cur.(bson.M)
		if !ok {
			return false
		}
		cur = mm[p]
	}
	if b, ok := cur.(bool); ok {
		return b
	}
	return false
}

func main() {
	mustInit()
	startCron()

	http.HandleFunc("/miners", handleMiners)   // 列表/检索 miner（按 http 成功率降序）
	http.HandleFunc("/clients", handleClients) // 某 client 下的 miners（按 http 降序）
	http.HandleFunc("/details", handleDetails) // 明细（仅 http）

	log.Printf("listening on %s", cfg.BindAddr)
	log.Fatal(http.ListenAndServe(cfg.BindAddr, nil))
}
