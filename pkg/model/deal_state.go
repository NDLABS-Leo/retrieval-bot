package model

import (
	"time"
)

// -----------------------------
// 常量：Filecoin Epoch 与 Unix 时间换算
// 主网创世时间：2020-08-25 22:00:00 UTC → 1598306400
// 1 epoch = 30 秒
// -----------------------------
const (
	filecoinGenesisUnix = int64(1598306400)
	epochDurationSec    = int64(30)
)

// -----------------------------
// int32 版本（兼容旧逻辑）
// -----------------------------
func EpochToTime(epoch int32) time.Time {
	if epoch < 0 {
		return time.Time{}
	}
	return time.Unix(int64(epoch*30+1598306400), 0).UTC()
}

func TimeToEpoch(t time.Time) int32 {
	if t.IsZero() {
		return -1
	}
	return int32(t.Unix()-1598306400) / 30
}

// -----------------------------
// int64 版本（推荐新逻辑）
// -----------------------------
func EpochToTime64(epoch int64) time.Time {
	if epoch < 0 {
		return time.Time{}
	}
	return time.Unix(epoch*epochDurationSec+filecoinGenesisUnix, 0).UTC()
}

func TimeToEpoch64(t time.Time) int64 {
	if t.IsZero() {
		return -1
	}
	return (t.UTC().Unix() - filecoinGenesisUnix) / epochDurationSec
}

// -----------------------------
// 新模型：DBClaim
// -----------------------------
type DBClaim struct {
	ClaimID    int64          `bson:"claim_id"`    // verifreg.ClaimId
	ProviderID int64          `bson:"provider_id"` // abi.ActorID
	ClientID   int64          `bson:"client_id"`   // abi.ActorID
	ClientAddr string         `bson:"client_addr"` // f1/f3... address
	DataCID    string         `bson:"data_cid"`    // cid string
	Size       int64          `bson:"size"`        // padded piece size (bytes)
	TermMin    int64          `bson:"term_min"`    // epochs
	TermMax    int64          `bson:"term_max"`    // epochs
	TermStart  int64          `bson:"term_start"`  // epoch
	Sector     uint64         `bson:"sector"`      // sector number
	MinerAddr  string         `bson:"miner_addr"`  // f0... 矿工ID地址
	UpdatedAt  time.Time      `bson:"updated_at"`  // upsert 时间(UTC)
	Meta       map[string]any `bson:"meta,omitempty"`
}

// 便捷方法：TermStart 的实际时间
func (c DBClaim) TermStartTime() time.Time {
	return EpochToTime64(c.TermStart)
}

// 便捷方法：TermMin/TermMax 转换为时长
func (c DBClaim) TermMinDuration() time.Duration {
	if c.TermMin <= 0 {
		return 0
	}
	return time.Duration(c.TermMin*epochDurationSec) * time.Second
}

func (c DBClaim) TermMaxDuration() time.Duration {
	if c.TermMax <= 0 {
		return 0
	}
	return time.Duration(c.TermMax*epochDurationSec) * time.Second
}

// 便捷方法：从 TermStart 到现在的“年龄”（年）
func (c DBClaim) AgeInYears() float64 {
	ts := c.TermStartTime()
	if ts.IsZero() {
		return 0
	}
	return time.Since(ts).Hours() / 24.0 / 365.0
}

// 设置更新时间为当前 UTC
func (c *DBClaim) Touch() {
	c.UpdatedAt = time.Now().UTC()
}
