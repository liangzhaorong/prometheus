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

package tombstones

import (
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/tsdb/encoding"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/prometheus/prometheus/tsdb/fileutil"
)

const TombstonesFilename = "tombstones"

const (
	// MagicTombstone is 4 bytes at the head of a tombstone file.
	MagicTombstone = 0x0130BA30

	tombstoneFormatV1 = 1
)

// The table gets initialized with sync.Once but may still cause a race
// with any other use of the crc32 package anywhere. Thus we initialize it
// before.
var castagnoliTable *crc32.Table

func init() {
	castagnoliTable = crc32.MakeTable(crc32.Castagnoli)
}

// newCRC32 initializes a CRC32 hash with a preconfigured polynomial, so the
// polynomial may be easily changed in one location at a later time, if necessary.
func newCRC32() hash.Hash32 {
	return crc32.New(castagnoliTable)
}

// Reader gives access to tombstone intervals by series reference.
type Reader interface {
	// Get returns deletion intervals for the series with the given reference.
	// 根据传入的时序编号 ref 获取待删除部分的时间范围集合 Intervals
	Get(ref uint64) (Intervals, error)

	// Iter calls the given function for each encountered interval.
	// 迭代全部的 tombstone 记录, 并对每个 tombstone 调用一次给定的回调函数
	Iter(func(uint64, Intervals) error) error

	// Total returns the total count of tombstones.
	Total() uint64 // 统计全部 tombstone 的个数

	// Close any underlying resources
	Close() error
}

func WriteFile(logger log.Logger, dir string, tr Reader) (int64, error) {
	path := filepath.Join(dir, TombstonesFilename)
	tmp := path + ".tmp" // 创建 ".tmp" 后缀的文件
	hash := newCRC32()
	var size int

	f, err := os.Create(tmp) // 打开临时 tombstones 文件
	if err != nil {
		return 0, err
	}
	defer func() { // 函数结束时关闭并移除临时 tombstone 文件
		if f != nil {
			if err := f.Close(); err != nil {
				level.Error(logger).Log("msg", "close tmp file", "err", err.Error())
			}
		}
		if err := os.RemoveAll(tmp); err != nil {
			level.Error(logger).Log("msg", "remove tmp file", "err", err.Error())
		}
	}()

	// 创建 encoding.Encbuf 缓冲区
	buf := encoding.Encbuf{B: make([]byte, 3*binary.MaxVarintLen64)}
	buf.Reset() // 清空 buf 缓冲区
	// Write the meta.
	buf.PutBE32(MagicTombstone)    // 向 buf 写入 tombstone 文件头
	buf.PutByte(tombstoneFormatV1) // 向 buf 写入版本号
	n, err := f.Write(buf.Get())   // 将 buf 中缓冲的数据写入文件中
	if err != nil {
		return 0, err
	}
	size += n // 更新写入文件中的字节数

	mw := io.MultiWriter(f, hash) // 在写入文件的同时, 会计算 tombstones 文件对应的 CRC32 校验码

	// 遍历 Reader 中待删除的全部时序信息, 并为每个 tombstone 调用传入的函数
	if err := tr.Iter(func(ref uint64, ivs Intervals) error {
		for _, iv := range ivs {
			buf.Reset() // 清空 buf

			buf.PutUvarint64(ref)    // 写入时序编号(series reference)
			buf.PutVarint64(iv.Mint) // 写入该时序待删除部分的起止时间戳
			buf.PutVarint64(iv.Maxt)

			n, err = mw.Write(buf.Get()) // 将 buf 缓冲的数据写入文件中
			if err != nil {
				return err
			}
			size += n // 更新写入文件的字节数
		}
		return nil
	}); err != nil {
		return 0, fmt.Errorf("error writing tombstones: %v", err)
	}

	n, err = f.Write(hash.Sum(nil)) // 写入 CRC32 校验码
	if err != nil {
		return 0, err
	}
	size += n // 更新写入文件的字节数

	var merr tsdb_errors.MultiError
	if merr.Add(f.Sync()); merr.Err() != nil {
		merr.Add(f.Close())
		return 0, merr.Err()
	}

	if err = f.Close(); err != nil { // 关闭临时 tombstone 文件
		return 0, err
	}
	f = nil
	return int64(size), fileutil.Replace(tmp, path) // 重命名临时文件, 将 ".tmp" 后缀去掉
}

// Stone holds the information on the posting and time-range
// that is deleted.
// Stone 是 WAL 文件中对待删除时序的标识的抽象
type Stone struct {
	Ref       uint64    // 待删除的时序编号
	Intervals Intervals // 该时序待删除的时间范围
}

func ReadTombstones(dir string) (Reader, int64, error) {
	// 读取指定 block 目录下 tombstone 文件的全部内容
	b, err := ioutil.ReadFile(filepath.Join(dir, TombstonesFilename))
	if os.IsNotExist(err) {
		return NewMemTombstones(), 0, nil
	} else if err != nil {
		return nil, 0, err
	}

	if len(b) < 5 { // 检测 tombstone 文件的长度
		return nil, 0, errors.Wrap(encoding.ErrInvalidSize, "tombstones header")
	}

	// 获取文件中 tombstone 的具体内容, 最后 4 个字节为 CRC32 校验码
	d := &encoding.Decbuf{B: b[:len(b)-4]}    // 4 for the checksum.
	if mg := d.Be32(); mg != MagicTombstone { // 检测开头是否为固定的 MagicTombstone 文件头
		return nil, 0, fmt.Errorf("invalid magic number %x", mg)
	}
	if flag := d.Byte(); flag != tombstoneFormatV1 { // 检测 tombstone 文件的版本
		return nil, 0, fmt.Errorf("invalid tombstone format %x", flag)
	}

	if d.Err() != nil {
		return nil, 0, d.Err()
	}

	// Verify checksum.
	// 检测校验码是否正确
	hash := newCRC32()
	if _, err := hash.Write(d.Get()); err != nil {
		return nil, 0, errors.Wrap(err, "write to hash")
	}
	if binary.BigEndian.Uint32(b[len(b)-4:]) != hash.Sum32() {
		return nil, 0, errors.New("checksum did not match")
	}

	// 创建 memTombstones 实例, 用于记录时序编号与待删除的 Intervals 之间的关系
	stonesMap := NewMemTombstones()

	// 读取时序编号(series reference) 以及待删除的时间范围, 然后将这些映射信息记录到
	// memTombstones 实例 stonesMap 中用于缓存
	for d.Len() > 0 {
		k := d.Uvarint64()
		mint := d.Varint64()
		maxt := d.Varint64()
		if d.Err() != nil {
			return nil, 0, d.Err()
		}

		stonesMap.AddInterval(k, Interval{mint, maxt})
	}

	return stonesMap, int64(len(b)), nil
}

// memTombstones 是上面 Reader 接口的实现, 可理解为 tombstone 文件的缓存
type memTombstones struct {
	intvlGroups map[uint64]Intervals // 维护了时序编号与 Intervals 之间的映射关系
	mtx         sync.RWMutex         // 在读写 intvlGroups 时, 需要获取该锁进行同步
}

// NewMemTombstones creates new in memory Tombstone Reader
// that allows adding new intervals.
func NewMemTombstones() *memTombstones {
	return &memTombstones{intvlGroups: make(map[uint64]Intervals)}
}

func NewTestMemTombstones(intervals []Intervals) *memTombstones {
	ret := NewMemTombstones()
	for i, intervalsGroup := range intervals {
		for _, interval := range intervalsGroup {
			ret.AddInterval(uint64(i+1), interval)
		}
	}
	return ret
}

// Get 传入的参数 ref 是时序编号 (series reference), 返回值为该时序对应的 Intervals(该待删除部分的时间范围集合)
func (t *memTombstones) Get(ref uint64) (Intervals, error) {
	t.mtx.RLock()
	defer t.mtx.RUnlock()
	return t.intvlGroups[ref], nil
}

// Iter 迭代全部的 tombstone 记录, 并对每个 tombstone 调用一次给定的回调函数
func (t *memTombstones) Iter(f func(uint64, Intervals) error) error {
	t.mtx.RLock()
	defer t.mtx.RUnlock()
	for ref, ivs := range t.intvlGroups {
		if err := f(ref, ivs); err != nil {
			return err
		}
	}
	return nil
}

// Total 统计全部 tombstones 的个数
func (t *memTombstones) Total() uint64 {
	t.mtx.RLock()
	defer t.mtx.RUnlock()

	total := uint64(0)
	for _, ivs := range t.intvlGroups {
		total += uint64(len(ivs))
	}
	return total
}

// AddInterval to an existing memTombstones.
func (t *memTombstones) AddInterval(ref uint64, itvs ...Interval) {
	t.mtx.Lock()
	defer t.mtx.Unlock()
	for _, itv := range itvs {
		t.intvlGroups[ref] = t.intvlGroups[ref].Add(itv)
	}
}

func (*memTombstones) Close() error {
	return nil
}

// Interval represents a single time-interval.
type Interval struct {
	Mint, Maxt int64 // 表示时序待删除的部分
}

func (tr Interval) InBounds(t int64) bool {
	return t >= tr.Mint && t <= tr.Maxt
}

func (tr Interval) IsSubrange(dranges Intervals) bool {
	for _, r := range dranges {
		if r.InBounds(tr.Mint) && r.InBounds(tr.Maxt) {
			return true
		}
	}

	return false
}

// Intervals represents	a set of increasing and non-overlapping time-intervals.
type Intervals []Interval

// Add the new time-range to the existing ones.
// The existing ones must be sorted.
// Add 添加待删除时序的时间范围, 并且会按照 Min 值对 Interval 进行排序,
// 在添加过程中会尝试将重叠的 Interval 进行合并.
func (itvs Intervals) Add(n Interval) Intervals {
	for i, r := range itvs {
		// TODO(gouthamve): Make this codepath easier to digest.
		if r.InBounds(n.Mint-1) || r.InBounds(n.Mint) {
			if n.Maxt > r.Maxt {
				itvs[i].Maxt = n.Maxt
			}

			j := 0
			for _, r2 := range itvs[i+1:] {
				if n.Maxt < r2.Mint {
					break
				}
				j++
			}
			if j != 0 {
				if itvs[i+j].Maxt > n.Maxt {
					itvs[i].Maxt = itvs[i+j].Maxt
				}
				itvs = append(itvs[:i+1], itvs[i+j+1:]...)
			}
			return itvs
		}

		if r.InBounds(n.Maxt+1) || r.InBounds(n.Maxt) {
			if n.Mint < r.Maxt {
				itvs[i].Mint = n.Mint
			}
			return itvs
		}

		if n.Mint < r.Mint {
			newRange := make(Intervals, i, len(itvs[:i])+1)
			copy(newRange, itvs[:i])
			newRange = append(newRange, n)
			newRange = append(newRange, itvs[i:]...)

			return newRange
		}
	}

	itvs = append(itvs, n)
	return itvs
}
