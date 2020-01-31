// Copyright 2018 The Prometheus Authors

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

package wal

import (
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/prometheus/prometheus/tsdb/fileutil"
	"github.com/prometheus/prometheus/tsdb/record"
	"github.com/prometheus/prometheus/tsdb/tombstones"
)

// CheckpointStats returns stats about a created checkpoint.
type CheckpointStats struct {
	DroppedSeries     int
	DroppedSamples    int
	DroppedTombstones int
	TotalSeries       int // Processed series including dropped ones.
	TotalSamples      int // Processed samples including dropped ones.
	TotalTombstones   int // Processed tombstones including dropped ones.
}

// LastCheckpoint returns the directory name and index of the most recent checkpoint.
// If dir does not contain any checkpoints, ErrNotFound is returned.
// LastCheckpoint 查找指定目录下最新的 checkpoint
func LastCheckpoint(dir string) (string, int, error) {
	files, err := ioutil.ReadDir(dir) // 读取指定目录下的全部文件名并将它们排序后返回
	if err != nil {
		return "", 0, err
	}
	// Traverse list backwards since there may be multiple checkpoints left.
	for i := len(files) - 1; i >= 0; i-- { // 倒序遍历
		fi := files[i]

		// 忽略非 "checkpoint." 前缀开头的文件
		if !strings.HasPrefix(fi.Name(), checkpointPrefix) {
			continue
		}
		if !fi.IsDir() { // 忽略非目录文件
			return "", 0, errors.Errorf("checkpoint %s is not a directory", fi.Name())
		}
		// 获取 Checkpoint 文件名的后缀数字
		idx, err := strconv.Atoi(fi.Name()[len(checkpointPrefix):])
		if err != nil {
			continue
		}
		return filepath.Join(dir, fi.Name()), idx, nil
	}
	return "", 0, record.ErrNotFound
}

// DeleteCheckpoints deletes all checkpoints in a directory below a given index.
// DeleteCheckpoints 删除编号 maxIndex 之前全部的 Checkpoint 目录
func DeleteCheckpoints(dir string, maxIndex int) error {
	var errs tsdb_errors.MultiError

	files, err := ioutil.ReadDir(dir) // 读取指定目录下的全部文件名并将它们排序后返回
	if err != nil {
		return err
	}
	for _, fi := range files {
		// 忽略非 "checkpoint." 前缀开头的文件
		if !strings.HasPrefix(fi.Name(), checkpointPrefix) {
			continue
		}
		// 获取 Checkpoint 文件名的后缀数字
		index, err := strconv.Atoi(fi.Name()[len(checkpointPrefix):])
		if err != nil || index >= maxIndex { // 编号 maxIndex 之后的 Checkpoint 目录会被忽略
			continue
		}
		// 编号 maxIndex 之前的 Checkpoint 目录将会被删除
		if err := os.RemoveAll(filepath.Join(dir, fi.Name())); err != nil {
			errs.Add(err)
		}
	}
	return errs.Err()
}

const checkpointPrefix = "checkpoint."

// Checkpoint creates a compacted checkpoint of segments in range [first, last] in the given WAL.
// It includes the most recent checkpoint if it exists.
// All series not satisfying keep and samples below mint are dropped.
//
// The checkpoint is stored in a directory named checkpoint.N in the same
// segmented format as the original WAL itself.
// This makes it easy to read it through the WAL package and concatenate
// it with the original WAL.
func Checkpoint(w *WAL, from, to int, keep func(id uint64) bool, mint int64) (*CheckpointStats, error) {
	stats := &CheckpointStats{}
	var sgmReader io.ReadCloser

	{

		var sgmRange []SegmentRange
		dir, idx, err := LastCheckpoint(w.Dir()) // 获取 WAL 目录下最新的 Checkpoint 目录
		if err != nil && err != record.ErrNotFound {
			return nil, errors.Wrap(err, "find last checkpoint")
		}
		last := idx + 1
		if err == nil {
			if from > last {
				return nil, fmt.Errorf("unexpected gap to last checkpoint. expected:%v, requested:%v", last, from)
			}
			// Ignore WAL files below the checkpoint. They shouldn't exist to begin with.
			from = last

			sgmRange = append(sgmRange, SegmentRange{Dir: dir, Last: math.MaxInt32})
		}

		// 为编号 from~to 的 Segment 文件创建对应的 segmentBufReader
		sgmRange = append(sgmRange, SegmentRange{Dir: w.Dir(), First: from, Last: to})
		sgmReader, err = NewSegmentsRangeReader(sgmRange...)
		if err != nil {
			return nil, errors.Wrap(err, "create segment reader")
		}
		defer sgmReader.Close()
	}

	// 创建临时 Checkpoint 目录, 并获取其读写权限
	cpdir := filepath.Join(w.Dir(), fmt.Sprintf(checkpointPrefix+"%06d", to))
	cpdirtmp := cpdir + ".tmp"

	if err := os.MkdirAll(cpdirtmp, 0777); err != nil {
		return nil, errors.Wrap(err, "create checkpoint dir")
	}
	// 创建该临时 Checkpoint 目录对应的 WAL 实例
	cp, err := New(nil, nil, cpdirtmp, w.CompressionEnabled())
	if err != nil {
		return nil, errors.Wrap(err, "open checkpoint")
	}

	// Ensures that an early return caused by an error doesn't leave any tmp files.
	defer func() {
		cp.Close()
		os.RemoveAll(cpdirtmp)
	}()

	r := NewReader(sgmReader) // 将 segmentBufReader 实例 sgmReader 封装成 wal.Reader

	var (
		series  []record.RefSeries // 从 Record 中反序列化得到的 RefSeries 会暂存在其中
		samples []record.RefSample // 从 Record 中反序列化得到的 RefSample 会暂存在其中
		tstones []tombstones.Stone // 从 Record 中反序列化得到的 Stone 会暂存在其中
		dec     record.Decoder
		enc     record.Encoder
		buf     []byte   // Record 缓冲区, 只有当其达到 1MB 时才会清空
		recs    [][]byte // 需要保留的 Record 记录
	)
	for r.Next() {
		series, samples, tstones = series[:0], samples[:0], tstones[:0]

		// We don't reset the buffer since we batch up multiple records
		// before writing them to the checkpoint.
		// Remember where the record for this iteration starts.
		start := len(buf) // buf 在 start 之前的空间已经被占用
		rec := r.Record() // 读取一条 Record

		switch dec.Type(rec) { // 根据 Record 中的第一个字节确定其类型
		case record.Series:
			series, err = dec.Series(rec, series) // 反序列化得到其中的 RefSeries 集合
			if err != nil {
				return nil, errors.Wrap(err, "decode series")
			}
			// Drop irrelevant series in place.
			repl := series[:0]
			for _, s := range series {
				if keep(s.Ref) { // 判断该时序是否需要保留, 若需要保留, 则将其添加到 repl 中
					repl = append(repl, s)
				}
			}
			if len(repl) > 0 { // 将保留的 RefSeries 集合序列化到 buf 缓冲区中, 等待写入
				buf = enc.Series(repl, buf)
			}
			stats.TotalSeries += len(series)
			stats.DroppedSeries += len(series) - len(repl)

		case record.Samples:
			samples, err = dec.Samples(rec, samples) // 反序列化得到其中的 RefSamples 集合
			if err != nil {
				return nil, errors.Wrap(err, "decode samples")
			}
			// Drop irrelevant samples in place.
			repl := samples[:0]
			for _, s := range samples {
				if s.T >= mint { // 判断该点是否需要保留, 若需要暴力流, 则将其添加到 repl 中
					repl = append(repl, s)
				}
			}
			if len(repl) > 0 { // 将保留的 RefSamples 集合序列化到 buf 缓冲区中, 等待写入
				buf = enc.Samples(repl, buf)
			}
			stats.TotalSamples += len(samples)
			stats.DroppedSamples += len(samples) - len(repl)

		case record.Tombstones:
			tstones, err = dec.Tombstones(rec, tstones) // 反序列化得到其中的 Stone 集合
			if err != nil {
				return nil, errors.Wrap(err, "decode deletes")
			}
			// Drop irrelevant tombstones in place.
			repl := tstones[:0]
			for _, s := range tstones {
				// 遍历 Stone 中的每个时间范围, 确定该 Stone 是否需要保留
				for _, iv := range s.Intervals {
					if iv.Maxt >= mint {
						repl = append(repl, s)
						break
					}
				}
			}
			if len(repl) > 0 { // 将保留的 Stone 集合序列化到 buf 缓冲区中, 等待写入
				buf = enc.Tombstones(repl, buf)
			}
			stats.TotalTombstones += len(tstones)
			stats.DroppedTombstones += len(tstones) - len(repl)

		default: // 返回异常
			return nil, errors.New("invalid record type")
		}
		if len(buf[start:]) == 0 { // buf 为空, 表示所有的内容都将被丢弃
			continue // All contents discarded.
		}
		// 将 buf 中缓冲的 Record 数据作为一条单独的 Record 记录到 recs 中,
		// 注意, 这不会清空 buf 缓冲区, 在处理下一条 Record 时, 会继续向 buf 缓冲区写入
		recs = append(recs, buf[start:])

		// Flush records in 1 MB increments.
		if len(buf) > 1*1024*1024 { // buf 缓冲区超过 1MB, 则开始批量写入 Checkpoint
			if err := cp.Log(recs...); err != nil {
				return nil, errors.Wrap(err, "flush records")
			}
			buf, recs = buf[:0], recs[:0] // 写入完成后, 才会清空 buf 缓冲区和 recs 缓冲区
		}
	}
	// If we hit any corruption during checkpointing, repairing is not an option.
	// The head won't know which series records are lost.
	if r.Err() != nil {
		return nil, errors.Wrap(r.Err(), "read segments")
	}

	// Flush remaining records.
	if err := cp.Log(recs...); err != nil {
		return nil, errors.Wrap(err, "flush records")
	}
	if err := cp.Close(); err != nil {
		return nil, errors.Wrap(err, "close checkpoint")
	}
	// 将临时目录更改为正式的 Checkpoint 目录
	if err := fileutil.Replace(cpdirtmp, cpdir); err != nil {
		return nil, errors.Wrap(err, "rename checkpoint directory")
	}

	return stats, nil
}
