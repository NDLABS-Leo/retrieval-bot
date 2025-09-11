package util

import (
	"context"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/pkg/errors"

	"storagestats/pkg/convert"
	"storagestats/pkg/env"
	"storagestats/pkg/model"
	"storagestats/pkg/requesterror"
	"storagestats/pkg/resolver"
	"storagestats/pkg/task"
)

var logger = logging.Logger("addTasks")

//nolint:nonamedreturns
func AddTasks(
	ctx context.Context,
	requester string,
	ipInfo resolver.IPInfo,
	documents []model.DBClaim,
	locationResolver resolver.LocationResolver,
	providerResolver resolver.ProviderResolver,
) (tasks []interface{}, results []interface{}) {
	for _, document := range documents {
		// 解析 provider（使用 DBClaim.MinerAddr：f0... 矿工ID地址）
		providerInfo, err := providerResolver.ResolveProvider(ctx, document.MinerAddr)
		if err != nil {
			logger.With("provider", document.MinerAddr).
				Error("failed to resolve provider")
			continue
		}

		// 定位 multiaddrs
		location, err := locationResolver.ResolveMultiaddrsBytes(ctx, providerInfo.Multiaddrs)
		if err != nil {
			if errors.As(err, &requesterror.BogonIPError{}) ||
				errors.As(err, &requesterror.InvalidIPError{}) ||
				errors.As(err, &requesterror.HostLookupError{}) ||
				errors.As(err, &requesterror.NoValidMultiAddrError{}) {
				results = addErrorResults(requester, ipInfo, results, document, providerInfo, location,
					task.NoValidMultiAddrs, err.Error())
			} else {
				logger.With("provider", document.MinerAddr, "err", err).
					Error("failed to resolve provider location")
			}
			continue
		}

		// 校验 PeerID
		_, err = peer.Decode(providerInfo.PeerId)
		if err != nil {
			logger.With("provider", document.MinerAddr, "peerID", providerInfo.PeerId, "err", err).
				Info("failed to decode peerID")
			results = addErrorResults(requester, ipInfo, results, document, providerInfo, location,
				task.InvalidPeerID, err.Error())
			continue
		}

		// 仅添加 HTTP piece 检索任务（使用 DataCID）
		tasks = append(tasks, task.Task{
			Requester: requester,
			Module:    task.HTTP,
			Metadata: map[string]string{
				"client":        document.ClientAddr,
				"retrieve_type": "piece",
				"retrieve_size": "1048576",
			},
			Provider: task.Provider{
				ID:         document.MinerAddr,
				PeerID:     providerInfo.PeerId,
				Multiaddrs: convert.MultiaddrsBytesToStringArraySkippingError(providerInfo.Multiaddrs),
				City:       location.City,
				Region:     location.Region,
				Country:    location.Country,
				Continent:  location.Continent,
			},
			Content: task.Content{
				CID: document.DataCID,
			},
			CreatedAt: time.Now().UTC(),
			Timeout:   env.GetDuration(env.FilplusIntegrationTaskTimeout, 15*time.Second),
		})
	}

	logger.With("count", len(tasks)).Info("inserted tasks")
	//nolint:nakedret
	return
}

// 仅保留与当前逻辑匹配的元数据（去掉 deal_id/label）
var moduleMetadataMap = map[task.ModuleName]map[string]string{
	task.GraphSync: {
		"assume_label":  "true",
		"retrieve_type": "root_block",
	},
	task.Bitswap: {
		"assume_label":  "true",
		"retrieve_type": "root_block",
	},
	task.HTTP: {
		"retrieve_type": "piece",
		"retrieve_size": "1048576",
	},
}

func addErrorResults(
	requester string,
	ipInfo resolver.IPInfo,
	results []interface{},
	document model.DBClaim,
	providerInfo resolver.MinerInfo,
	location resolver.IPInfo,
	errorCode task.ErrorCode,
	errorMessage string,
) []interface{} {
	for module, metadata := range moduleMetadataMap {
		newMetadata := make(map[string]string)
		for k, v := range metadata {
			newMetadata[k] = v
		}
		// 不再包含 deal_id；client 改为 DBClaim.ClientAddr
		newMetadata["client"] = document.ClientAddr

		results = append(results, task.Result{
			Task: task.Task{
				Requester: requester,
				Module:    module,
				Metadata:  newMetadata,
				Provider: task.Provider{
					ID:         document.MinerAddr,
					PeerID:     providerInfo.PeerId,
					Multiaddrs: convert.MultiaddrsBytesToStringArraySkippingError(providerInfo.Multiaddrs),
					City:       location.City,
					Region:     location.Region,
					Country:    location.Country,
					Continent:  location.Continent,
				},
				Content: task.Content{
					// 统一用 DataCID
					CID: document.DataCID,
				},
				CreatedAt: time.Now().UTC(),
				Timeout:   env.GetDuration(env.FilplusIntegrationTaskTimeout, 15*time.Second),
			},
			Retriever: task.Retriever{
				PublicIP:  ipInfo.IP,
				City:      ipInfo.City,
				Region:    ipInfo.Region,
				Country:   ipInfo.Country,
				Continent: ipInfo.Continent,
				ASN:       ipInfo.ASN,
				ISP:       ipInfo.ISP,
				Latitude:  ipInfo.Latitude,
				Longitude: ipInfo.Longitude,
			},
			Result: task.RetrievalResult{
				Success:      false,
				ErrorCode:    errorCode,
				ErrorMessage: errorMessage,
				TTFB:         0,
				Speed:        0,
				Duration:     0,
				Downloaded:   0,
			},
			CreatedAt: time.Now().UTC(),
		})
	}
	return results
}
