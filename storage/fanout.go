// Copyright 2017 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"context"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/common/model"

	"github.com/prometheus/prometheus/pkg/labels"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
)

// fanout 用于兼容本地存储和远端存储. 该接口同样实现对远端存储进行数据写入的 Appender 接口.
// 当执行 fanout 中的方法（如 Add）时, fanout 接口会先执行本地存储（primary）的 Add 方法,
// 然后遍历执行每个远端存储（secondaries）的 Add 方法. 当需要获取 Prometheus 保存时间最长
// 的监控数据时, 也会分别调用本地存储和远端存储的 startTime 方法, 获取各自的最长保存时间点,
// 选择最大值作为整个存储的最长保存时间.
type fanout struct {
	logger log.Logger

	primary     Storage   // 本地存储, 实际是 ReadyStorage 实例
	secondaries []Storage // 多个远端存储
}

// NewFanout returns a new fanout Storage, which proxies reads and writes
// through to multiple underlying storages.
//
// The difference between primary and secondary Storage is only for read (Querier) path and it goes as follows:
// * If the primary querier returns an error, then any of the Querier operations will fail.
// * If any secondary querier returns an error the result from that queries is discarded. The overall operation will succeed,
// and the error from the secondary querier will be returned as a warning.
//
// NOTE: In the case of Prometheus, it treats all remote storages as secondary / best effort.
func NewFanout(logger log.Logger, primary Storage, secondaries ...Storage) Storage {
	return &fanout{
		logger:      logger,
		primary:     primary,
		secondaries: secondaries,
	}
}

// StartTime implements the Storage interface.
func (f *fanout) StartTime() (int64, error) {
	// StartTime of a fanout should be the earliest StartTime of all its storages,
	// both primary and secondaries.
	firstTime, err := f.primary.StartTime()
	if err != nil {
		return int64(model.Latest), err
	}

	for _, s := range f.secondaries {
		t, err := s.StartTime()
		if err != nil {
			return int64(model.Latest), err
		}
		if t < firstTime {
			firstTime = t
		}
	}
	return firstTime, nil
}

func (f *fanout) Querier(ctx context.Context, mint, maxt int64) (Querier, error) {
	primary, err := f.primary.Querier(ctx, mint, maxt)
	if err != nil {
		return nil, err
	}

	secondaries := make([]Querier, 0, len(f.secondaries))
	for _, storage := range f.secondaries {
		querier, err := storage.Querier(ctx, mint, maxt)
		if err != nil {
			// Close already open Queriers, append potential errors to returned error.
			errs := tsdb_errors.NewMulti(err, primary.Close())
			for _, q := range secondaries {
				errs.Add(q.Close())
			}
			return nil, errs.Err()
		}
		secondaries = append(secondaries, querier)
	}
	return NewMergeQuerier([]Querier{primary}, secondaries, ChainedSeriesMerge), nil
}

func (f *fanout) ChunkQuerier(ctx context.Context, mint, maxt int64) (ChunkQuerier, error) {
	primary, err := f.primary.ChunkQuerier(ctx, mint, maxt)
	if err != nil {
		return nil, err
	}

	secondaries := make([]ChunkQuerier, 0, len(f.secondaries))
	for _, storage := range f.secondaries {
		querier, err := storage.ChunkQuerier(ctx, mint, maxt)
		if err != nil {
			// Close already open Queriers, append potential errors to returned error.
			errs := tsdb_errors.NewMulti(err, primary.Close())
			for _, q := range secondaries {
				errs.Add(q.Close())
			}
			return nil, errs.Err()
		}
		secondaries = append(secondaries, querier)
	}
	return NewMergeChunkQuerier([]ChunkQuerier{primary}, secondaries, NewCompactingChunkSeriesMerger(ChainedSeriesMerge)), nil
}

// Appender 将本地存储关联的 Appender 实例以及远端存储关联的 Appender 封装成 fanoutAppender 实例并返回.
func (f *fanout) Appender(ctx context.Context) Appender {
	primary := f.primary.Appender(ctx) // 获取本地存储关联的 Appender 实例
	secondaries := make([]Appender, 0, len(f.secondaries))
	// 获取远端存储关联的 Appender 实例
	for _, storage := range f.secondaries {
		secondaries = append(secondaries, storage.Appender(ctx))
	}
	return &fanoutAppender{ // 将本地 Appender 和远端 Appender 封装成 fanoutAppender
		logger:      f.logger,
		primary:     primary,
		secondaries: secondaries,
	}
}

// Close closes the storage and all its underlying resources.
func (f *fanout) Close() error {
	errs := tsdb_errors.NewMulti(f.primary.Close())
	for _, s := range f.secondaries {
		errs.Add(s.Close())
	}
	return errs.Err()
}

// fanoutAppender implements Appender.
type fanoutAppender struct {
	logger log.Logger

	primary     Appender   // 本地存储关联的 Appender
	secondaries []Appender // 远端存储关联的 Appender
}

func (f *fanoutAppender) Add(l labels.Labels, t int64, v float64) (uint64, error) {
	// 将时序点写入本地存储
	ref, err := f.primary.Add(l, t, v)
	if err != nil {
		return ref, err
	}

	// 将时序点写入全部远端存储
	for _, appender := range f.secondaries {
		if _, err := appender.Add(l, t, v); err != nil {
			return 0, err
		}
	}
	return ref, nil
}

func (f *fanoutAppender) AddFast(ref uint64, t int64, v float64) error {
	if err := f.primary.AddFast(ref, t, v); err != nil {
		return err
	}

	for _, appender := range f.secondaries {
		if err := appender.AddFast(ref, t, v); err != nil {
			return err
		}
	}
	return nil
}

func (f *fanoutAppender) Commit() (err error) {
	err = f.primary.Commit()

	for _, appender := range f.secondaries {
		if err == nil {
			err = appender.Commit()
		} else {
			if rollbackErr := appender.Rollback(); rollbackErr != nil {
				level.Error(f.logger).Log("msg", "Squashed rollback error on commit", "err", rollbackErr)
			}
		}
	}
	return
}

func (f *fanoutAppender) Rollback() (err error) {
	err = f.primary.Rollback()

	for _, appender := range f.secondaries {
		rollbackErr := appender.Rollback()
		if err == nil {
			err = rollbackErr
		} else if rollbackErr != nil {
			level.Error(f.logger).Log("msg", "Squashed rollback error on rollback", "err", rollbackErr)
		}
	}
	return nil
}
