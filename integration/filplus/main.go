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

func main() {
	filplus := NewFilPlusIntegration()

	// 随机种子：放到 for 外，只播一次即可
	rand.Seed(time.Now().UnixNano())

	for {
		// Step 1: 已在函数内完成“按 client_addr+miner_addr 分组，并且每组只保留前 30%”
		dealsGrouped, err := getDealsGroupedByClientProvider(filplus.marketDealsCollection)
		if err != nil {
			logger.Error(err)
			continue
		}

		start := time.Now()

		// Step 2: 直接在每组里做随机抽样（最多 100 条），不再截取 30%
		for client, providerDeals := range dealsGrouped {
			for provider, deals := range providerDeals {
				if len(deals) == 0 {
					continue
				}

				// 随机抽样至多 100 条
				sampleCount := 100
				if len(deals) < sampleCount {
					sampleCount = len(deals)
				}

				// 就地打乱，然后取前 sampleCount
				// 为避免修改原切片内容，如需保留原顺序可先拷贝一份
				shuffled := make([]model.DBClaim, len(deals))
				copy(shuffled, deals)
				rand.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })

				sampledDeals := shuffled[:sampleCount]

				if err := filplus.RunOnce(context.TODO(), sampledDeals); err != nil {
					logger.Error(err)
				}

				// 日志
				logger.Infof("Client: %s, Provider: %s, Sampled Claims: %d", client, provider, len(sampledDeals))
				for _, deal := range sampledDeals {
					logger.Infof("DataCID: %s, Size: %d, Sector: %d", deal.DataCID, deal.Size, deal.Sector)
				}
			}
		}

		elapsed := time.Since(start)
		logger.Infof("Processing dealsGrouped took: %s", elapsed)

		// 如需节流，打开 sleep
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
	// 以 DBClaim 结构计算：按 client_addr 分组，合计 size。
	// 过滤逻辑参考：term_start > 0 且 (term_start + term_max) > nowEpoch（仍在有效期内）
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
		return nil, errors.Wrap(err, "failed to aggregate market claims")
	}

	err = agg.All(ctx, &result)
	if err != nil {
		return nil, errors.Wrap(err, "failed to decode market claims")
	}

	totalPerClient := make(map[string]int64)
	for _, r := range result {
		totalPerClient[r.Client] = r.Total
	}

	return totalPerClient, nil
}

func NewFilPlusIntegration() *FilPlusIntegration {
	ctx := context.Background()
	taskClient, err := mongo.
		Connect(ctx, options.Client().ApplyURI(env.GetRequiredString(env.QueueMongoURI)))
	if err != nil {
		panic(err)
	}
	taskCollection := taskClient.
		Database(env.GetRequiredString(env.QueueMongoDatabase)).Collection("claims_task_queue")

	stateMarketDealsClient, err := mongo.
		Connect(ctx, options.Client().ApplyURI(env.GetRequiredString(env.StatemarketdealsMongoURI)))
	if err != nil {
		panic(err)
	}
	marketDealsCollection := stateMarketDealsClient.
		Database(env.GetRequiredString(env.StatemarketdealsMongoDatabase)).
		Collection("claims")

	resultClient, err := mongo.Connect(ctx, options.Client().ApplyURI(env.GetRequiredString(env.ResultMongoURI)))
	if err != nil {
		panic(err)
	}
	resultCollection := resultClient.
		Database(env.GetRequiredString(env.ResultMongoDatabase)).
		Collection("claims_task_result")

	batchSize := env.GetInt(env.FilplusIntegrationBatchSize, 100)
	providerCacheTTL := env.GetDuration(env.ProviderCacheTTL, 24*time.Hour)
	locationCacheTTL := env.GetDuration(env.LocationCacheTTL, 24*time.Hour)
	locationResolver := resolver.NewLocationResolver(env.GetRequiredString(env.IPInfoToken), locationCacheTTL)
	providerResolver, err := resolver.NewProviderResolver(
		env.GetString(env.LotusAPIUrl, "https://api.node.glif.io/rpc/v0"),
		env.GetString(env.LotusAPIToken, ""),
		providerCacheTTL)
	if err != nil {
		panic(err)
	}

	// Check public IP address
	ipInfo, err := resolver.GetPublicIPInfo(ctx, "", "")
	if err != nil {
		panic(err)
	}

	logger.With("ipinfo", ipInfo).Infof("Public IP info retrieved")

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

// getDealsGroupedByClientProvider groups claims by client_addr and miner_addr, sorted by term_start desc
// 先分组后根据 claim_id 倒序排序；每组仅保留前 30%
func getDealsGroupedByClientProvider(collection *mongo.Collection) (map[string]map[string][]model.DBClaim, error) {
	ctx := context.Background()

	// 这里不强依赖 Mongo 端排序；直接把需要的字段全取回在内存里分组+排序
	// 如果数据非常大，可以考虑增加时间窗/条件，或换用 Mongo 的 $setWindowFields 做组内排名（见下方可选实现思路）
	cur, err := collection.Aggregate(ctx, mongo.Pipeline{
		// 如果需要可以加筛选条件再拉数据
		// {{"$match", bson.D{{"term_start", bson.D{{"$gt", 0}}}}}},
		// 也可以按需投影减少传输体积（示例仅保留用到的字段）
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
	})
	if err != nil {
		logger.Errorf("Failed to aggregate data: %v", err)
		return nil, err
	}
	defer cur.Close(ctx)

	var deals []model.DBClaim
	if err := cur.All(ctx, &deals); err != nil {
		logger.Errorf("Failed to decode data: %v", err)
		return nil, err
	}

	// 分组：client_addr -> miner_addr -> []DBClaim
	grouped := make(map[string]map[string][]model.DBClaim, 20000)
	for _, d := range deals {
		c := d.ClientAddr
		m := d.MinerAddr
		if c == "" || m == "" {
			continue
		}
		if _, ok := grouped[c]; !ok {
			grouped[c] = make(map[string][]model.DBClaim, 8)
		}
		grouped[c][m] = append(grouped[c][m], d)
	}

	// 组内排序（按 claim_id 倒序）并取前 30%
	for c, miners := range grouped {
		for m, arr := range miners {
			if len(arr) == 0 {
				continue
			}
			// 组内倒序：claim_id 大在前
			sort.Slice(arr, func(i, j int) bool {
				return arr[i].ClaimID > arr[j].ClaimID
			})
			// 前 30%
			n := int(math.Ceil(float64(len(arr)) * 0.30))
			if n <= 0 {
				// 没必要保留空组，直接置空或跳过
				grouped[c][m] = nil
				continue
			}
			if n > len(arr) {
				n = len(arr)
			}
			grouped[c][m] = arr[:n]
		}
	}

	return grouped, nil
}

// randomSampleFromGroup randomly samples up to sampleSize claims
func randomSampleFromGroup(deals []model.DBClaim, sampleSize int) []model.DBClaim {
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(deals), func(i, j int) {
		deals[i], deals[j] = deals[j], deals[i]
	})

	if len(deals) > sampleSize {
		deals = deals[:sampleSize]
	}
	return deals
}

func (f *FilPlusIntegration) RunOnce(ctx context.Context, documentsOne []model.DBClaim) error {
	logger.Info("start running filplus integration")

	for {
		// Check the number of tasks in the queue
		count, err := f.taskCollection.CountDocuments(ctx, bson.M{"requester": f.requester})
		if err != nil {
			return errors.Wrap(err, "failed to count tasks")
		}

		logger.With("count", count).Info("Current number of tasks in the queue")

		// If task count exceeds batch size, block and wait
		if count > int64(f.batchSize) {
			logger.Infof("Task queue still has %d tasks, waiting...", count)
			time.Sleep(10 * time.Second)
			continue
		}
		break
	}

	// 构造任务（util.AddTasks 版本已适配 DBClaim，并使用 DataCID）
	tasks, results := util.AddTasks(ctx, f.requester, f.ipInfo, documentsOne, f.locationResolver, f.providerResolver)

	if len(tasks) > 0 {
		_, err := f.taskCollection.InsertMany(ctx, tasks)
		if err != nil {
			return errors.Wrap(err, "failed to insert tasks")
		}
	}

	logger.With("count", len(tasks)).Info("inserted tasks")

	countPerCountry := make(map[string]int)
	countPerContinent := make(map[string]int)
	countPerModule := make(map[task.ModuleName]int)
	for _, t := range tasks {
		//nolint:forcetypeassert
		tsk := t.(task.Task)
		country := tsk.Provider.Country
		continent := tsk.Provider.Continent
		module := tsk.Module
		countPerCountry[country]++
		countPerContinent[continent]++
		countPerModule[module]++
	}

	for country, count := range countPerCountry {
		logger.With("country", country, "count", count).Info("tasks per country")
	}

	for continent, count := range countPerContinent {
		logger.With("continent", continent, "count", count).Info("tasks per continent")
	}

	for module, count := range countPerModule {
		logger.With("module", module, "count", count).Info("tasks per module")
	}

	if len(results) > 0 {
		_, err := f.resultCollection.InsertMany(ctx, results)
		if err != nil {
			return errors.Wrap(err, "failed to insert results")
		}
	}

	logger.With("count", len(results)).Info("inserted results")

	return nil
}
