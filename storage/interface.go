// Copyright 2014 The Prometheus Authors
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
	"errors"

	"github.com/prometheus/prometheus/pkg/labels"
)

// The errors exposed.
var (
	ErrNotFound                    = errors.New("not found")
	ErrOutOfOrderSample            = errors.New("out of order sample")
	ErrDuplicateSampleForTimestamp = errors.New("duplicate sample for timestamp")
	ErrOutOfBounds                 = errors.New("out of bounds")
)

// Storage ingests and manages samples, along with various indexes. All methods
// are goroutine-safe. Storage implements storage.SampleAppender.
type Storage interface {
	Queryable

	// StartTime returns the oldest timestamp stored in the storage.
	StartTime() (int64, error)

	// Appender returns a new appender against the storage.
	Appender() (Appender, error)

	// Close closes the storage and all its underlying resources.
	Close() error
}

// A Queryable handles queries against a storage.
type Queryable interface {
	// Querier returns a new Querier on the storage.
	Querier(ctx context.Context, mint, maxt int64) (Querier, error)
}

// Querier provides reading access to time series data.
// Querier 监控数据的查询接口
type Querier interface {
	// Select returns a set of series that matches the given label matchers.
	// 根据标签查询对应的时序数据
	Select(*SelectParams, ...*labels.Matcher) (SeriesSet, Warnings, error)

	// LabelValues returns all potential values for a label name.
	// 根据标签名称查询标签的值
	LabelValues(name string) ([]string, Warnings, error)

	// LabelNames returns all the unique label names present in the block in sorted order.
	// 返回所有标签名
	LabelNames() ([]string, Warnings, error)

	// Close releases the resources of the Querier.
	// 关闭查询请求
	Close() error
}

// SelectParams specifies parameters passed to data selections.
type SelectParams struct {
	Start int64 // Start time in milliseconds for this select.
	End   int64 // End time in milliseconds for this select.

	Step int64  // Query step size in milliseconds.
	Func string // String representation of surrounding function or aggregation.

	Grouping []string // List of label names used in aggregation.
	By       bool     // Indicate whether it is without or by.
	Range    int64    // Range vector selector range in milliseconds.
}

// QueryableFunc is an adapter to allow the use of ordinary functions as
// Queryables. It follows the idea of http.HandlerFunc.
type QueryableFunc func(ctx context.Context, mint, maxt int64) (Querier, error)

// Querier calls f() with the given parameters.
func (f QueryableFunc) Querier(ctx context.Context, mint, maxt int64) (Querier, error) {
	return f(ctx, mint, maxt)
}

// Appender provides batched appends against a storage.
// Appender 数据写入接口
type Appender interface {
	// 将给定的样本数据添加到对应的序列中, 并返回索引
	// Add 方法需要根据传入的标签通过 Hash 算法找到(如果不存在则创建)序列 ID,
	// 然后将样本值保存到序列中.
	Add(l labels.Labels, t int64, v float64) (uint64, error)

	// 通过给定的索引快速添加指标
	// AddFast 方法则直接传入序列 ID, 省去了进行序列查找或者序列 ID 生成的步骤
	AddFast(l labels.Labels, ref uint64, t int64, v float64) error

	// Commit submits the collected samples and purges the batch.
	// 批量提交, 用于提交多个 Add 方法或将 AddFast 方法的结果持久化(如保存到 WAL 中)
	Commit() error

	// 回滚
	Rollback() error
}

// SeriesSet contains a set of series.
type SeriesSet interface {
	Next() bool
	At() Series
	Err() error
}

// Series represents a single time series.
type Series interface {
	// Labels returns the complete set of labels identifying the series.
	Labels() labels.Labels

	// Iterator returns a new iterator of the data of the series.
	Iterator() SeriesIterator
}

// SeriesIterator iterates over the data of a time series.
type SeriesIterator interface {
	// Seek advances the iterator forward to the value at or after
	// the given timestamp.
	Seek(t int64) bool
	// At returns the current timestamp/value pair.
	At() (t int64, v float64)
	// Next advances the iterator by one.
	Next() bool
	// Err returns the current error.
	Err() error
}

type Warnings []error
