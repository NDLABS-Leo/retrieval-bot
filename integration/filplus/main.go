package main

import (
	"context"
	"math"
	"math/rand"
	"sort"
	"time"

	logging "github.com/ipfs/go-log/v2"
	_ "github.com/joho/godotenv/autoload"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"storagestats/integration/filplus/util"
	"storagestats/pkg/env"
	"storagestats/pkg/model"
	"storagestats/pkg/resolver"
	"storagestats/pkg/task"
)

var logger = logging.Logger("filplus-integration")

// ---- 日志初始化 ----
func init() {
	logging.SetupLogging(logging.Config{
		Format: logging.PlaintextOutput, // 或 logging.ColorizedOutput / logging.JSONOutput
		Level:  logging.LevelInfo,       // 默认级别
		Stdout: true,                    // 输出到 stdout
		Stderr: false,                   // 不要 stderr，避免 nohup 里乱码
		File:   "",                      // 不直接写文件
		Labels: map[string]string{"app": "filplus-integration"},
	})

	logger.Info("logger initialized: output=stdout")
}

func main() {
	startBoot := time.Now()
	logger.Info("starting FilPlusIntegration bootstrap...")

	filplus := NewFilPlusIntegration()

	logger.With(
		"queueDB", env.GetRequiredString(env.QueueMongoDatabase),
		"marketDB", env.GetRequiredString(env.StatemarketdealsMongoDatabase),
		"resultDB", env.GetRequiredString(env.ResultMongoDatabase),
		"batchSize", filplus.batchSize,
	).Info("FilPlusIntegration initialized")

	logger.With("elapsed", time.Since(startBoot)).Info("bootstrap finished")

	for {
		loopStart := time.Now()

		// Step 1: 已在函数内完成“按 client_addr+miner_addr 分组，并且每组只保留前 30%”
		logger.Info("aggregating claims into client+provider groups (each group keep top 30% by claim_id)...")
		dealsGrouped, err := getDealsGroupedByClientProvider(filplus.marketDealsCollection)
		if err != nil {
			logger.With("err", err).Error("grouping claims failed")
			time.Sleep(5 * time.Second)
			continue
		}
		logger.With("elapsed", time.Since(loopStart)).Info("aggregation done")
		logger.Infof("grouped clients: %d", len(dealsGrouped))

		// 统计一下总条数（截取30%后）
		var totalAfterPick int
		var totalGroups int
		for _, providerDeals := range dealsGrouped {
			for _, deals := range providerDeals {
				totalGroups++
				totalAfterPick += len(deals)
			}
		}
		logger.With(
			"groups", totalGroups,
			"claims_after_top30", totalAfterPick,
		).Info("grouping summary")

		// Step 2: 每组里做随机抽样（最多 100 条）
		logger.Info("sampling up to 100 items per group and enqueue tasks...")
		enqueueStart := time.Now()
		enqueuedTotal := 0

		for client, providerDeals := range dealsGrouped {
			for provider, deals := range providerDeals {
				if len(deals) == 0 {
					continue
				}

				sampleCount := 100
				if len(deals) < sampleCount {
					sampleCount = len(deals)
				}

				// 就地打乱，然后取前 sampleCount
				shuffled := make([]model.DBClaim, len(deals))
				copy(shuffled, deals)
				rand.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
				sampledDeals := shuffled[:sampleCount]

				if err := filplus.RunOnce(context.TODO(), sampledDeals); err != nil {
					logger.With(
						"client", client,
						"provider", provider,
						"count", len(sampledDeals),
						"err", err,
					).Error("RunOnce failed")
				} else {
					enqueuedTotal += len(sampledDeals)
				}

				logger.With(
					"client", client,
					"provider", provider,
					"sampled", len(sampledDeals),
				).Info("group sampled & enqueued")

				for _, deal := range sampledDeals {
					logger.With(
						"client", client,
						"provider", provider,
						"cid", deal.DataCID,
						"size", deal.Size,
						"sector", deal.Sector,
						"claim_id", deal.ClaimID,
					).Debug("sampled deal")
				}
			}
		}

		logger.With(
			"enqueued_total", enqueuedTotal,
			"elapsed", time.Since(enqueueStart),
		).Info("sampling+enqueue finished")

		logger.With("loop_elapsed", time.Since(loopStart)).Info("loop finished")
		// 可以适当节流
		// time.Sleep(time.Minute)
	}
}

type TotalPerClient struct {
	Client string `bson:"_id"`
	Total  int64  `bson:"total"`
}

type FilPlusIntegration struct {
	taskCollection        *mongo.Collection
	marketDealsCollection *mongo.Collection
	resultCollection      *mongo.Collection
	batchSize             int
	requester             string
	locationResolver      resolver.LocationResolver
	providerResolver      resolver.ProviderResolver
	ipInfo                resolver.IPInfo
	randConst             float64
}

func GetTotalPerClient(ctx context.Context, marketDealsCollection *mongo.Collection) (map[string]int64, error) {
	nowEpoch := model.TimeToEpoch64(time.Now().UTC())

	var result []TotalPerClient
	agg, err := marketDealsCollection.Aggregate(ctx, []bson.M{
		{
			"$match": bson.M{
				"term_start": bson.M{"$gt": 0},
				"$expr": bson.M{
					"$gt": bson.A{
						bson.M{"$add": bson.A{"$term_start", "$term_max"}},
						nowEpoch,
					},
				},
			},
		},
		{
			"$group": bson.M{
				"_id": "$client_addr",
				"total": bson.M{
					"$sum": "$size",
				},
			},
		},
	})
	if err != nil {
		return nil, errors.Wrap(err, "aggregate market claims")
	}

	err = agg.All(ctx, &result)
	if err != nil {
		return nil, errors.Wrap(err, "decode market claims")
	}

	totalPerClient := make(map[string]int64, len(result))
	for _, r := range result {
		totalPerClient[r.Client] = r.Total
	}

	logger.With("clients", len(totalPerClient)).Info("GetTotalPerClient done")
	return totalPerClient, nil
}

func NewFilPlusIntegration() *FilPlusIntegration {
	ctx := context.Background()

	queueURI := env.GetRequiredString(env.QueueMongoURI)
	queueDB := env.GetRequiredString(env.QueueMongoDatabase)
	taskClient, err := mongo.Connect(ctx, options.Client().ApplyURI(queueURI))
	if err != nil {
		logger.With("err", err, "uri", queueURI).Fatal("mongo connect (queue) failed")
	}
	taskCollection := taskClient.Database(queueDB).Collection("claims_task_queue")
	logger.With("uri", queueURI, "db", queueDB).Info("connected to queue mongo")

	stateURI := env.GetRequiredString(env.StatemarketdealsMongoURI)
	stateDB := env.GetRequiredString(env.StatemarketdealsMongoDatabase)
	stateMarketDealsClient, err := mongo.Connect(ctx, options.Client().ApplyURI(stateURI))
	if err != nil {
		logger.With("err", err, "uri", stateURI).Fatal("mongo connect (market) failed")
	}
	marketDealsCollection := stateMarketDealsClient.Database(stateDB).Collection("claims")
	logger.With("uri", stateURI, "db", stateDB).Info("connected to market mongo")

	resultURI := env.GetRequiredString(env.ResultMongoURI)
	resultDB := env.GetRequiredString(env.ResultMongoDatabase)
	resultClient, err := mongo.Connect(ctx, options.Client().ApplyURI(resultURI))
	if err != nil {
		logger.With("err", err, "uri", resultURI).Fatal("mongo connect (result) failed")
	}
	resultCollection := resultClient.Database(resultDB).Collection("claims_task_result")
	logger.With("uri", resultURI, "db", resultDB).Info("connected to result mongo")

	batchSize := env.GetInt(env.FilplusIntegrationBatchSize, 100)
	providerCacheTTL := env.GetDuration(env.ProviderCacheTTL, 24*time.Hour)
	locationCacheTTL := env.GetDuration(env.LocationCacheTTL, 24*time.Hour)
	locationResolver := resolver.NewLocationResolver(env.GetRequiredString(env.IPInfoToken), locationCacheTTL)

	lotusURL := env.GetString(env.LotusAPIUrl, "https://api.node.glif.io/rpc/v0")
	lotusToken := env.GetString(env.LotusAPIToken, "")
	providerResolver, err := resolver.NewProviderResolver(lotusURL, lotusToken, providerCacheTTL)
	if err != nil {
		logger.With("err", err, "lotusURL", lotusURL).Fatal("NewProviderResolver failed")
	}

	ipInfo, err := resolver.GetPublicIPInfo(ctx, "", "")
	if err != nil {
		logger.With("err", err).Fatal("GetPublicIPInfo failed")
	}
	logger.With("ip", ipInfo.IP, "country", ipInfo.Country, "asn", ipInfo.ASN, "isp", ipInfo.ISP).Info("Public IP info retrieved")

	return &FilPlusIntegration{
		taskCollection:        taskCollection,
		marketDealsCollection: marketDealsCollection,
		batchSize:             batchSize,
		requester:             "filplus",
		locationResolver:      locationResolver,
		providerResolver:      *providerResolver,
		resultCollection:      resultCollection,
		ipInfo:                ipInfo,
		randConst:             env.GetFloat64(env.FilplusIntegrationRandConst, 4.0),
	}
}

// 先分组后根据 claim_id 倒序排序；每组仅保留前 30%
func getDealsGroupedByClientProvider(collection *mongo.Collection) (map[string]map[string][]model.DBClaim, error) {
	ctx := context.Background()

	stageStart := time.Now()
	cur, err := collection.Aggregate(ctx, mongo.Pipeline{
		{{Key: "$project", Value: bson.D{
			{Key: "client_addr", Value: 1},
			{Key: "miner_addr", Value: 1},
			{Key: "claim_id", Value: 1},
			{Key: "data_cid", Value: 1},
			{Key: "size", Value: 1},
			{Key: "sector", Value: 1},
			{Key: "term_start", Value: 1},
			{Key: "_id", Value: 0},
		}}},
	}, options.Aggregate().SetAllowDiskUse(true))
	if err != nil {
		logger.With("err", err).Error("aggregate failed")
		return nil, err
	}
	defer cur.Close(ctx)

	var deals []model.DBClaim
	if err := cur.All(ctx, &deals); err != nil {
		logger.With("err", err).Error("cursor decode failed")
		return nil, err
	}
	logger.With("scanned", len(deals), "elapsed", time.Since(stageStart)).Info("claims scanned")

	groupStart := time.Now()
	grouped := make(map[string]map[string][]model.DBClaim, 20000)
	for _, d := range deals {
		if d.ClientAddr == "" || d.MinerAddr == "" {
			continue
		}
		if _, ok := grouped[d.ClientAddr]; !ok {
			grouped[d.ClientAddr] = make(map[string][]model.DBClaim, 8)
		}
		grouped[d.ClientAddr][d.MinerAddr] = append(grouped[d.ClientAddr][d.MinerAddr], d)
	}
	logger.With("clients", len(grouped), "elapsed", time.Since(groupStart)).Info("grouped by client->miner")

	trimStart := time.Now()
	var kept int
	for c, miners := range grouped {
		for m, arr := range miners {
			if len(arr) == 0 {
				continue
			}
			sort.Slice(arr, func(i, j int) bool { return arr[i].ClaimID > arr[j].ClaimID })
			n := int(math.Ceil(float64(len(arr)) * 0.30))
			if n <= 0 {
				grouped[c][m] = nil
				continue
			}
			if n > len(arr) {
				n = len(arr)
			}
			grouped[c][m] = arr[:n]
			kept += n
		}
	}
	logger.With("kept", kept, "elapsed", time.Since(trimStart)).Info("top30% per group trimmed")

	return grouped, nil
}

func (f *FilPlusIntegration) RunOnce(ctx context.Context, documentsOne []model.DBClaim) error {
	logger.With("docs", len(documentsOne)).Info("RunOnce begin")

	waitStart := time.Now()
	for {
		count, err := f.taskCollection.CountDocuments(ctx, bson.M{"requester": f.requester})
		if err != nil {
			logger.With("err", err).Error("count tasks failed")
			return errors.Wrap(err, "failed to count tasks")
		}

		if count > int64(f.batchSize) {
			logger.With("current", count, "batchSize", f.batchSize).Warn("queue still busy, wait 10s...")
			time.Sleep(10 * time.Second)
			continue
		}
		break
	}
	logger.With("waited", time.Since(waitStart)).Info("queue capacity ok")

	tasks, results := util.AddTasks(ctx, f.requester, f.ipInfo, documentsOne, f.locationResolver, f.providerResolver)
	logger.With("tasks", len(tasks), "results", len(results)).Info("AddTasks generated")

	if len(tasks) > 0 {
		if _, err := f.taskCollection.InsertMany(ctx, tasks); err != nil {
			logger.With("err", err, "tasks", len(tasks)).Error("insert tasks failed")
			return errors.Wrap(err, "failed to insert tasks")
		}
		logger.With("inserted", len(tasks)).Info("tasks inserted")
	} else {
		logger.Info("no tasks generated")
	}

	countPerCountry := make(map[string]int)
	countPerContinent := make(map[string]int)
	countPerModule := make(map[task.ModuleName]int)
	for _, t := range tasks {
		tsk := t.(task.Task)
		countPerCountry[tsk.Provider.Country]++
		countPerContinent[tsk.Provider.Continent]++
		countPerModule[tsk.Module]++
	}
	for k, v := range countPerCountry {
		logger.With("country", k, "count", v).Info("tasks per country")
	}
	for k, v := range countPerContinent {
		logger.With("continent", k, "count", v).Info("tasks per continent")
	}
	for k, v := range countPerModule {
		logger.With("module", k, "count", v).Info("tasks per module")
	}

	if len(results) > 0 {
		if _, err := f.resultCollection.InsertMany(ctx, results); err != nil {
			logger.With("err", err, "results", len(results)).Error("insert results failed")
			return errors.Wrap(err, "failed to insert results")
		}
		logger.With("inserted", len(results)).Info("results inserted")
	}

	logger.With("docs", len(documentsOne)).Info("RunOnce done")
	return nil
}
