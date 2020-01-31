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

package tsdb

import (
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/prometheus/prometheus/tsdb/fileutil"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/tsdb/tombstones"
)

// ExponentialBlockRanges returns the time ranges based on the stepSize.
// minSize 是该 Prometheus TSDB 中 block 目录跨域的最小时间范围, 默认是 2h(其单位是毫秒)
func ExponentialBlockRanges(minSize int64, steps, stepSize int) []int64 {
	ranges := make([]int64, 0, steps)
	curRange := minSize
	for i := 0; i < steps; i++ { // 计算每个层级中 block 目录所能跨越的时间范围, 每个范围递增 5 倍
		ranges = append(ranges, curRange)
		curRange = curRange * int64(stepSize)
	}

	return ranges // 默认返回值是 [2h, 2h*5, 2h*5*5]
}

// Compactor provides compaction against an underlying storage
// of time series data.
type Compactor interface {
	// Plan returns a set of directories that can be compacted concurrently.
	// The directories can be overlapping.
	// Results returned when compactions are in progress are undefined.
	Plan(dir string) ([]string, error) // 返回当前可以压缩的目录集合

	// Write persists a Block into a directory.
	// No Block is written when resulting Block has 0 samples, and returns empty ulid.ULID{}.
	//
	// 将传入的 Block 实例写入指定的目录中, 返回值是该 block 的唯一标识
	Write(dest string, b BlockReader, mint, maxt int64, parent *BlockMeta) (ulid.ULID, error)

	// Compact runs compaction against the provided directories. Must
	// only be called concurrently with results of Plan().
	// Can optionally pass a list of already open blocks,
	// to avoid having to reopen them.
	// When resulting Block has 0 samples
	//  * No block is written.
	//  * The source dirs are marked Deletable.
	//  * Returns empty ulid.ULID{}.
	//
	// 将多个 block 目录(dirs 参数) 进行压缩, 压缩后的 block 目录的路径为 dest
	Compact(dest string, dirs []string, open []*Block) (ulid.ULID, error)
}

// LeveledCompactor 会对 block 目录进行多层压缩, 每一层压缩后的 block 目录的时间跨度都有所不同,
// 默认分为 3 个层级, 每个层级压缩后的 block 的跨度是 2h、2h*5 和 2h*5*5.
// 压缩层级是在 Prometheus TSDB 启动时调用 ExponentialBlockRanges() 方法计算得到的.
//
// LeveledCompactor implements the Compactor interface.
type LeveledCompactor struct {
	metrics   *compactorMetrics
	logger    log.Logger
	ranges    []int64       // 压缩层级
	chunkPool chunkenc.Pool // 用于记录可复用的 Chunk 实例
	ctx       context.Context
}

type compactorMetrics struct {
	ran               prometheus.Counter
	populatingBlocks  prometheus.Gauge
	overlappingBlocks prometheus.Counter
	duration          prometheus.Histogram
	chunkSize         prometheus.Histogram
	chunkSamples      prometheus.Histogram
	chunkRange        prometheus.Histogram
}

func newCompactorMetrics(r prometheus.Registerer) *compactorMetrics {
	m := &compactorMetrics{}

	m.ran = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_compactions_total",
		Help: "Total number of compactions that were executed for the partition.",
	})
	m.populatingBlocks = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "prometheus_tsdb_compaction_populating_block",
		Help: "Set to 1 when a block is currently being written to the disk.",
	})
	m.overlappingBlocks = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_vertical_compactions_total",
		Help: "Total number of compactions done on overlapping blocks.",
	})
	m.duration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "prometheus_tsdb_compaction_duration_seconds",
		Help:    "Duration of compaction runs",
		Buckets: prometheus.ExponentialBuckets(1, 2, 10),
	})
	m.chunkSize = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "prometheus_tsdb_compaction_chunk_size_bytes",
		Help:    "Final size of chunks on their first compaction",
		Buckets: prometheus.ExponentialBuckets(32, 1.5, 12),
	})
	m.chunkSamples = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "prometheus_tsdb_compaction_chunk_samples",
		Help:    "Final number of samples on their first compaction",
		Buckets: prometheus.ExponentialBuckets(4, 1.5, 12),
	})
	m.chunkRange = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "prometheus_tsdb_compaction_chunk_range_seconds",
		Help:    "Final time range of chunks on their first compaction",
		Buckets: prometheus.ExponentialBuckets(100, 4, 10),
	})

	if r != nil {
		r.MustRegister(
			m.ran,
			m.populatingBlocks,
			m.overlappingBlocks,
			m.duration,
			m.chunkRange,
			m.chunkSamples,
			m.chunkSize,
		)
	}
	return m
}

// NewLeveledCompactor returns a LeveledCompactor.
func NewLeveledCompactor(ctx context.Context, r prometheus.Registerer, l log.Logger, ranges []int64, pool chunkenc.Pool) (*LeveledCompactor, error) {
	if len(ranges) == 0 {
		return nil, errors.Errorf("at least one range must be provided")
	}
	if pool == nil {
		pool = chunkenc.NewPool()
	}
	if l == nil {
		l = log.NewNopLogger()
	}
	return &LeveledCompactor{
		ranges:    ranges,
		chunkPool: pool,
		logger:    l,
		metrics:   newCompactorMetrics(r),
		ctx:       ctx,
	}, nil
}

type dirMeta struct {
	dir  string
	meta *BlockMeta
}

// Plan returns a list of compactable blocks in the provided directory.
func (c *LeveledCompactor) Plan(dir string) ([]string, error) {
	dirs, err := blockDirs(dir)
	if err != nil {
		return nil, err
	}
	if len(dirs) < 1 {
		return nil, nil
	}

	var dms []dirMeta
	for _, dir := range dirs {
		meta, _, err := readMetaFile(dir)
		if err != nil {
			return nil, err
		}
		dms = append(dms, dirMeta{dir, meta})
	}
	return c.plan(dms)
}

// Prometheus TSDB 压缩操作的第一步是生成压缩计划, 即计算哪些 block 目录会参与此次的压缩操作.
// 压缩计划是由 LeveledCompactor.plan() 生成的, 它会读取当前 Prometheus TSDB 下的全部 block
// 目录, 并从每个 block 目录下的 meta.json 文件中加载元数据, 然后将这些元数据信息封装成 dirMeta
// 集合交给 plan() 方法进行后续的筛选.
func (c *LeveledCompactor) plan(dms []dirMeta) ([]string, error) {
	sort.Slice(dms, func(i, j int) bool { // 按照 MinTime 对 dirMeta 集合进行排序
		return dms[i].meta.MinTime < dms[j].meta.MinTime
	})

	res := c.selectOverlappingDirs(dms)
	if len(res) > 0 {
		return res, nil
	}
	// No overlapping blocks, do compaction the usual way.
	// We do not include a recently created block with max(minTime), so the block which was just created from WAL.
	// This gives users a window of a full block size to piece-wise backup new data without having to care about data overlap.
	dms = dms[:len(dms)-1]

	for _, dm := range c.selectDirs(dms) { // 获取一组可压缩的 block 目录
		res = append(res, dm.dir)
	}
	if len(res) > 0 { // 检测是否存在可压缩的 block 目录, 若存在任意一组可压缩的 block 目录, 则从该方法返回
		return res, nil
	}

	// 如果没有找到任何一组可压缩的 block 目录, 则查找需要删除时序数据的 block 目录
	// Compact any blocks with big enough time range that have >5% tombstones.
	for i := len(dms) - 1; i >= 0; i-- {
		meta := dms[i].meta
		if meta.MaxTime-meta.MinTime < c.ranges[len(c.ranges)/2] {
			break // 如果前遍历到的 block 时间跨度较小, 则不进行压缩
		}
		// tombstone 个数达到一定阈值(包含 5%)且时间跨度较大的 block 目录才会触发此处的删除操作
		if float64(meta.Stats.NumTombstones)/float64(meta.Stats.NumSeries+1) > 0.05 {
			return []string{dms[i].dir}, nil
		}
	}

	return nil, nil
}

// selectDirs returns the dir metas that should be compacted into a single new block.
// If only a single block range is configured, the result is always nil.
func (c *LeveledCompactor) selectDirs(ds []dirMeta) []dirMeta {
	if len(c.ranges) < 2 || len(ds) < 1 {
		return nil
	}

	highTime := ds[len(ds)-1].meta.MinTime

	for _, iv := range c.ranges[1:] {
		// 获取多组可压缩的 block 目录集合
		parts := splitByRange(ds, iv)
		if len(parts) == 0 {
			// 在低压缩级别中, 找不到可压缩的 block 目录组, 则继续检查下一个压缩级别,
			// 默认是 [2h, 2h*5, 2h*5*5] 这个 3 个压缩层次
			continue
		}

	Outer:
		for _, p := range parts { // 遍历所有 block 分组
			// Do not select the range if it has a block whose compaction failed.
			// 如果当前 block 分组中存在压缩失败的 block 目录, 则跳过当前分组
			for _, dm := range p {
				if dm.meta.Compaction.Failed {
					continue Outer
				}
			}

			// 当前分组的时间范围, mint 是该组 block 中最小的 MinTime, maxt 是该组 block 中最大的 MaxTime
			mint := p[0].meta.MinTime
			maxt := p[len(p)-1].meta.MaxTime
			// 满足下面任一条件, 当前分组即可被压缩
			// 1) 当前分组已写满或当前激活的 block 不在当前分组中, 即不会有新数据写入当前分组中
			// 2) 当前分组中有多个 block 需要压缩, 如果只有一个 block 目录, 则压缩就没意义了
			// Pick the range of blocks if it spans the full range (potentially with gaps)
			// or is before the most recent block.
			// This ensures we don't compact blocks prematurely when another one of the same
			// size still fits in the range.
			if (maxt-mint == iv || maxt <= highTime) && len(p) > 1 {
				return p
			}
		}
	}

	return nil
}

// selectOverlappingDirs returns all dirs with overlapping time ranges.
// It expects sorted input by mint and returns the overlapping dirs in the same order as received.
func (c *LeveledCompactor) selectOverlappingDirs(ds []dirMeta) []string {
	if len(ds) < 2 {
		return nil
	}
	var overlappingDirs []string
	globalMaxt := ds[0].meta.MaxTime
	for i, d := range ds[1:] {
		if d.meta.MinTime < globalMaxt {
			if len(overlappingDirs) == 0 { // When it is the first overlap, need to add the last one as well.
				overlappingDirs = append(overlappingDirs, ds[i].dir)
			}
			overlappingDirs = append(overlappingDirs, d.dir)
		} else if len(overlappingDirs) > 0 {
			break
		}
		if d.meta.MaxTime > globalMaxt {
			globalMaxt = d.meta.MaxTime
		}
	}
	return overlappingDirs
}

// splitByRange splits the directories by the time range. The range sequence starts at 0.
//
// For example, if we have blocks [0-10, 10-20, 50-60, 90-100] and the split range tr is 30
// it returns [0-10, 10-20], [50-60], [90-100].
func splitByRange(ds []dirMeta, tr int64) [][]dirMeta {
	var splitDirs [][]dirMeta

	for i := 0; i < len(ds); {
		var (
			group []dirMeta
			t0    int64
			m     = ds[i].meta
		)
		// Compute start of aligned time range of size tr closest to the current block's start.
		if m.MinTime >= 0 { // 对齐 MinTime 时间戳
			t0 = tr * (m.MinTime / tr)
		} else {
			t0 = tr * ((m.MinTime - tr + 1) / tr)
		}
		// Skip blocks that don't fall into the range. This can happen via mis-alignment or
		// by being the multiple of the intended range.
		// 没有完全在当前 [t0, t0+tr] 范围内的 block 目录, 都不会被分配到当前分组中
		if m.MaxTime > t0+tr {
			i++
			continue
		}

		// Add all dirs to the current group that are within [t0, t0+tr].
		// 将在 [t0, t0+tr] 区域内的 block 目录分为一组
		for ; i < len(ds); i++ { // 注意, 因为这里循环变量使用的是 i, 所以每个 block 只能被分到一个 group
			// Either the block falls into the next range or doesn't fit at all (checked above).
			if ds[i].meta.MaxTime > t0+tr {
				break
			}
			group = append(group, ds[i]) // 记录能够完全落到 group 分组内的 block 目录
		}

		if len(group) > 0 {
			splitDirs = append(splitDirs, group) // 记录所有 group 分组
		}
	}

	return splitDirs
}

func compactBlockMetas(uid ulid.ULID, blocks ...*BlockMeta) *BlockMeta {
	res := &BlockMeta{
		ULID:    uid,
		MinTime: blocks[0].MinTime,
	}

	sources := map[ulid.ULID]struct{}{}
	// For overlapping blocks, the Maxt can be
	// in any block so we track it globally.
	maxt := int64(math.MinInt64)

	for _, b := range blocks {
		if b.MaxTime > maxt {
			maxt = b.MaxTime
		}
		if b.Compaction.Level > res.Compaction.Level {
			res.Compaction.Level = b.Compaction.Level
		}
		for _, s := range b.Compaction.Sources {
			sources[s] = struct{}{}
		}
		res.Compaction.Parents = append(res.Compaction.Parents, BlockDesc{
			ULID:    b.ULID,
			MinTime: b.MinTime,
			MaxTime: b.MaxTime,
		})
	}
	res.Compaction.Level++

	for s := range sources {
		res.Compaction.Sources = append(res.Compaction.Sources, s)
	}
	sort.Slice(res.Compaction.Sources, func(i, j int) bool {
		return res.Compaction.Sources[i].Compare(res.Compaction.Sources[j]) < 0
	})

	res.MaxTime = maxt
	return res
}

// Compact creates a new block in the compactor's directory from the blocks in the
// provided directories.
func (c *LeveledCompactor) Compact(dest string, dirs []string, open []*Block) (uid ulid.ULID, err error) {
	var (
		blocks []BlockReader
		bs     []*Block
		metas  []*BlockMeta
		uids   []string
	)
	start := time.Now()

	for _, d := range dirs {
		meta, _, err := readMetaFile(d)
		if err != nil {
			return uid, err
		}

		var b *Block

		// Use already open blocks if we can, to avoid
		// having the index data in memory twice.
		for _, o := range open {
			if meta.ULID == o.Meta().ULID {
				b = o
				break
			}
		}

		if b == nil {
			var err error
			b, err = OpenBlock(c.logger, d, c.chunkPool)
			if err != nil {
				return uid, err
			}
			defer b.Close()
		}

		metas = append(metas, meta)
		blocks = append(blocks, b)
		bs = append(bs, b)
		uids = append(uids, meta.ULID.String())
	}

	entropy := rand.New(rand.NewSource(time.Now().UnixNano()))
	uid = ulid.MustNew(ulid.Now(), entropy)

	meta := compactBlockMetas(uid, metas...)
	err = c.write(dest, meta, blocks...)
	if err == nil {
		if meta.Stats.NumSamples == 0 {
			for _, b := range bs {
				b.meta.Compaction.Deletable = true
				n, err := writeMetaFile(c.logger, b.dir, &b.meta)
				if err != nil {
					level.Error(c.logger).Log(
						"msg", "Failed to write 'Deletable' to meta file after compaction",
						"ulid", b.meta.ULID,
					)
				}
				b.numBytesMeta = n
			}
			uid = ulid.ULID{}
			level.Info(c.logger).Log(
				"msg", "compact blocks resulted in empty block",
				"count", len(blocks),
				"sources", fmt.Sprintf("%v", uids),
				"duration", time.Since(start),
			)
		} else {
			level.Info(c.logger).Log(
				"msg", "compact blocks",
				"count", len(blocks),
				"mint", meta.MinTime,
				"maxt", meta.MaxTime,
				"ulid", meta.ULID,
				"sources", fmt.Sprintf("%v", uids),
				"duration", time.Since(start),
			)
		}
		return uid, nil
	}

	var merr tsdb_errors.MultiError
	merr.Add(err)
	if err != context.Canceled {
		for _, b := range bs {
			if err := b.setCompactionFailed(); err != nil {
				merr.Add(errors.Wrapf(err, "setting compaction failed for block: %s", b.Dir()))
			}
		}
	}

	return uid, merr
}

func (c *LeveledCompactor) Write(dest string, b BlockReader, mint, maxt int64, parent *BlockMeta) (ulid.ULID, error) {
	start := time.Now()

	entropy := rand.New(rand.NewSource(time.Now().UnixNano()))
	uid := ulid.MustNew(ulid.Now(), entropy)

	meta := &BlockMeta{
		ULID:    uid,
		MinTime: mint,
		MaxTime: maxt,
	}
	meta.Compaction.Level = 1
	meta.Compaction.Sources = []ulid.ULID{uid}

	if parent != nil {
		meta.Compaction.Parents = []BlockDesc{
			{ULID: parent.ULID, MinTime: parent.MinTime, MaxTime: parent.MaxTime},
		}
	}

	err := c.write(dest, meta, b)
	if err != nil {
		return uid, err
	}

	if meta.Stats.NumSamples == 0 {
		return ulid.ULID{}, nil
	}

	level.Info(c.logger).Log(
		"msg", "write block",
		"mint", meta.MinTime,
		"maxt", meta.MaxTime,
		"ulid", meta.ULID,
		"duration", time.Since(start),
	)
	return uid, nil
}

// instrumentedChunkWriter is used for level 1 compactions to record statistics
// about compacted chunks.
type instrumentedChunkWriter struct {
	ChunkWriter

	size    prometheus.Histogram
	samples prometheus.Histogram
	trange  prometheus.Histogram
}

func (w *instrumentedChunkWriter) WriteChunks(chunks ...chunks.Meta) error {
	for _, c := range chunks {
		w.size.Observe(float64(len(c.Chunk.Bytes())))
		w.samples.Observe(float64(c.Chunk.NumSamples()))
		w.trange.Observe(float64(c.MaxTime - c.MinTime))
	}
	return w.ChunkWriter.WriteChunks(chunks...)
}

// write creates a new block that is the union of the provided blocks into dir.
// It cleans up all files of the old blocks after completing successfully.
func (c *LeveledCompactor) write(dest string, meta *BlockMeta, blocks ...BlockReader) (err error) {
	dir := filepath.Join(dest, meta.ULID.String())
	tmp := dir + ".tmp"
	var closers []io.Closer
	defer func(t time.Time) {
		var merr tsdb_errors.MultiError
		merr.Add(err)
		merr.Add(closeAll(closers))
		err = merr.Err()

		// RemoveAll returns no error when tmp doesn't exist so it is safe to always run it.
		if err := os.RemoveAll(tmp); err != nil {
			level.Error(c.logger).Log("msg", "removed tmp folder after failed compaction", "err", err.Error())
		}
		c.metrics.ran.Inc()
		c.metrics.duration.Observe(time.Since(t).Seconds())
	}(time.Now())

	if err = os.RemoveAll(tmp); err != nil {
		return err
	}

	if err = os.MkdirAll(tmp, 0777); err != nil {
		return err
	}

	// Populate chunk and index files into temporary directory with
	// data of all blocks.
	var chunkw ChunkWriter

	chunkw, err = chunks.NewWriter(chunkDir(tmp))
	if err != nil {
		return errors.Wrap(err, "open chunk writer")
	}
	closers = append(closers, chunkw)
	// Record written chunk sizes on level 1 compactions.
	if meta.Compaction.Level == 1 {
		chunkw = &instrumentedChunkWriter{
			ChunkWriter: chunkw,
			size:        c.metrics.chunkSize,
			samples:     c.metrics.chunkSamples,
			trange:      c.metrics.chunkRange,
		}
	}

	indexw, err := index.NewWriter(c.ctx, filepath.Join(tmp, indexFilename))
	if err != nil {
		return errors.Wrap(err, "open index writer")
	}
	closers = append(closers, indexw)

	if err := c.populateBlock(blocks, meta, indexw, chunkw); err != nil {
		return errors.Wrap(err, "write compaction")
	}

	select {
	case <-c.ctx.Done():
		return c.ctx.Err()
	default:
	}

	// We are explicitly closing them here to check for error even
	// though these are covered under defer. This is because in Windows,
	// you cannot delete these unless they are closed and the defer is to
	// make sure they are closed if the function exits due to an error above.
	var merr tsdb_errors.MultiError
	for _, w := range closers {
		merr.Add(w.Close())
	}
	closers = closers[:0] // Avoid closing the writers twice in the defer.
	if merr.Err() != nil {
		return merr.Err()
	}

	// Populated block is empty, so exit early.
	if meta.Stats.NumSamples == 0 {
		return nil
	}

	if _, err = writeMetaFile(c.logger, tmp, meta); err != nil {
		return errors.Wrap(err, "write merged meta")
	}

	// Create an empty tombstones file.
	if _, err := tombstones.WriteFile(c.logger, tmp, tombstones.NewMemTombstones()); err != nil {
		return errors.Wrap(err, "write new tombstones file")
	}

	df, err := fileutil.OpenDir(tmp)
	if err != nil {
		return errors.Wrap(err, "open temporary block dir")
	}
	defer func() {
		if df != nil {
			df.Close()
		}
	}()

	if err := df.Sync(); err != nil {
		return errors.Wrap(err, "sync temporary dir file")
	}

	// Close temp dir before rename block dir (for windows platform).
	if err = df.Close(); err != nil {
		return errors.Wrap(err, "close temporary dir")
	}
	df = nil

	// Block successfully written, make visible and remove old ones.
	if err := fileutil.Replace(tmp, dir); err != nil {
		return errors.Wrap(err, "rename block dir")
	}

	return nil
}

// populateBlock fills the index and chunk writers with new data gathered as the union
// of the provided blocks. It returns meta information for the new block.
// It expects sorted blocks input by mint.
func (c *LeveledCompactor) populateBlock(blocks []BlockReader, meta *BlockMeta, indexw IndexWriter, chunkw ChunkWriter) (err error) {
	if len(blocks) == 0 {
		return errors.New("cannot populate block from no readers")
	}

	var (
		set         ChunkSeriesSet
		symbols     index.StringIter
		closers     = []io.Closer{}
		overlapping bool
	)
	defer func() {
		var merr tsdb_errors.MultiError
		merr.Add(err)
		merr.Add(closeAll(closers))
		err = merr.Err()
		c.metrics.populatingBlocks.Set(0)
	}()
	c.metrics.populatingBlocks.Set(1)

	globalMaxt := blocks[0].Meta().MaxTime
	for i, b := range blocks {
		select {
		case <-c.ctx.Done():
			return c.ctx.Err()
		default:
		}

		if !overlapping {
			if i > 0 && b.Meta().MinTime < globalMaxt {
				c.metrics.overlappingBlocks.Inc()
				overlapping = true
				level.Warn(c.logger).Log("msg", "found overlapping blocks during compaction", "ulid", meta.ULID)
			}
			if b.Meta().MaxTime > globalMaxt {
				globalMaxt = b.Meta().MaxTime
			}
		}

		indexr, err := b.Index()
		if err != nil {
			return errors.Wrapf(err, "open index reader for block %s", b)
		}
		closers = append(closers, indexr)

		chunkr, err := b.Chunks()
		if err != nil {
			return errors.Wrapf(err, "open chunk reader for block %s", b)
		}
		closers = append(closers, chunkr)

		tombsr, err := b.Tombstones()
		if err != nil {
			return errors.Wrapf(err, "open tombstone reader for block %s", b)
		}
		closers = append(closers, tombsr)

		k, v := index.AllPostingsKey()
		all, err := indexr.Postings(k, v)
		if err != nil {
			return err
		}
		all = indexr.SortedPostings(all)

		s := newCompactionSeriesSet(indexr, chunkr, tombsr, all)
		syms := indexr.Symbols()

		if i == 0 {
			set = s
			symbols = syms
			continue
		}
		set, err = newCompactionMerger(set, s)
		if err != nil {
			return err
		}
		symbols = newMergedStringIter(symbols, syms)
	}

	for symbols.Next() {
		if err := indexw.AddSymbol(symbols.At()); err != nil {
			return errors.Wrap(err, "add symbol")
		}
	}
	if symbols.Err() != nil {
		return errors.Wrap(symbols.Err(), "next symbol")
	}

	delIter := &deletedIterator{}
	ref := uint64(0)
	for set.Next() {
		select {
		case <-c.ctx.Done():
			return c.ctx.Err()
		default:
		}

		lset, chks, dranges := set.At() // The chunks here are not fully deleted.
		if overlapping {
			// If blocks are overlapping, it is possible to have unsorted chunks.
			sort.Slice(chks, func(i, j int) bool {
				return chks[i].MinTime < chks[j].MinTime
			})
		}

		// Skip the series with all deleted chunks.
		if len(chks) == 0 {
			continue
		}

		for i, chk := range chks {
			// Re-encode head chunks that are still open (being appended to) or
			// outside the compacted MaxTime range.
			// The chunk.Bytes() method is not safe for open chunks hence the re-encoding.
			// This happens when snapshotting the head block.
			//
			// Block time range is half-open: [meta.MinTime, meta.MaxTime) and
			// chunks are closed hence the chk.MaxTime >= meta.MaxTime check.
			//
			// TODO think how to avoid the typecasting to verify when it is head block.
			if _, isHeadChunk := chk.Chunk.(*safeChunk); isHeadChunk && chk.MaxTime >= meta.MaxTime {
				dranges = append(dranges, tombstones.Interval{Mint: meta.MaxTime, Maxt: math.MaxInt64})

			} else
			// Sanity check for disk blocks.
			// chk.MaxTime == meta.MaxTime shouldn't happen as well, but will brake many users so not checking for that.
			if chk.MinTime < meta.MinTime || chk.MaxTime > meta.MaxTime {
				return errors.Errorf("found chunk with minTime: %d maxTime: %d outside of compacted minTime: %d maxTime: %d",
					chk.MinTime, chk.MaxTime, meta.MinTime, meta.MaxTime)
			}

			if len(dranges) > 0 {
				// Re-encode the chunk to not have deleted values.
				if !chk.OverlapsClosedInterval(dranges[0].Mint, dranges[len(dranges)-1].Maxt) {
					continue
				}
				newChunk := chunkenc.NewXORChunk()
				app, err := newChunk.Appender()
				if err != nil {
					return err
				}

				delIter.it = chk.Chunk.Iterator(delIter.it)
				delIter.intervals = dranges

				var (
					t int64
					v float64
				)
				for delIter.Next() {
					t, v = delIter.At()
					app.Append(t, v)
				}
				if err := delIter.Err(); err != nil {
					return errors.Wrap(err, "iterate chunk while re-encoding")
				}

				chks[i].Chunk = newChunk
				chks[i].MaxTime = t
			}
		}

		mergedChks := chks
		if overlapping {
			mergedChks, err = chunks.MergeOverlappingChunks(chks)
			if err != nil {
				return errors.Wrap(err, "merge overlapping chunks")
			}
		}
		if err := chunkw.WriteChunks(mergedChks...); err != nil {
			return errors.Wrap(err, "write chunks")
		}

		if err := indexw.AddSeries(ref, lset, mergedChks...); err != nil {
			return errors.Wrap(err, "add series")
		}

		meta.Stats.NumChunks += uint64(len(mergedChks))
		meta.Stats.NumSeries++
		for _, chk := range mergedChks {
			meta.Stats.NumSamples += uint64(chk.Chunk.NumSamples())
		}

		for _, chk := range mergedChks {
			if err := c.chunkPool.Put(chk.Chunk); err != nil {
				return errors.Wrap(err, "put chunk")
			}
		}

		ref++
	}
	if set.Err() != nil {
		return errors.Wrap(set.Err(), "iterate compaction set")
	}

	return nil
}

type compactionSeriesSet struct {
	p          index.Postings
	index      IndexReader
	chunks     ChunkReader
	tombstones tombstones.Reader

	l         labels.Labels
	c         []chunks.Meta
	intervals tombstones.Intervals
	err       error
}

func newCompactionSeriesSet(i IndexReader, c ChunkReader, t tombstones.Reader, p index.Postings) *compactionSeriesSet {
	return &compactionSeriesSet{
		index:      i,
		chunks:     c,
		tombstones: t,
		p:          p,
	}
}

func (c *compactionSeriesSet) Next() bool {
	if !c.p.Next() {
		return false
	}
	var err error

	c.intervals, err = c.tombstones.Get(c.p.At())
	if err != nil {
		c.err = errors.Wrap(err, "get tombstones")
		return false
	}

	if err = c.index.Series(c.p.At(), &c.l, &c.c); err != nil {
		c.err = errors.Wrapf(err, "get series %d", c.p.At())
		return false
	}

	// Remove completely deleted chunks.
	if len(c.intervals) > 0 {
		chks := make([]chunks.Meta, 0, len(c.c))
		for _, chk := range c.c {
			if !(tombstones.Interval{Mint: chk.MinTime, Maxt: chk.MaxTime}.IsSubrange(c.intervals)) {
				chks = append(chks, chk)
			}
		}

		c.c = chks
	}

	for i := range c.c {
		chk := &c.c[i]

		chk.Chunk, err = c.chunks.Chunk(chk.Ref)
		if err != nil {
			c.err = errors.Wrapf(err, "chunk %d not found", chk.Ref)
			return false
		}
	}

	return true
}

func (c *compactionSeriesSet) Err() error {
	if c.err != nil {
		return c.err
	}
	return c.p.Err()
}

func (c *compactionSeriesSet) At() (labels.Labels, []chunks.Meta, tombstones.Intervals) {
	return c.l, c.c, c.intervals
}

type compactionMerger struct {
	a, b ChunkSeriesSet

	aok, bok  bool
	l         labels.Labels
	c         []chunks.Meta
	intervals tombstones.Intervals
}

func newCompactionMerger(a, b ChunkSeriesSet) (*compactionMerger, error) {
	c := &compactionMerger{
		a: a,
		b: b,
	}
	// Initialize first elements of both sets as Next() needs
	// one element look-ahead.
	c.aok = c.a.Next()
	c.bok = c.b.Next()

	return c, c.Err()
}

func (c *compactionMerger) compare() int {
	if !c.aok {
		return 1
	}
	if !c.bok {
		return -1
	}
	a, _, _ := c.a.At()
	b, _, _ := c.b.At()
	return labels.Compare(a, b)
}

func (c *compactionMerger) Next() bool {
	if !c.aok && !c.bok || c.Err() != nil {
		return false
	}
	// While advancing child iterators the memory used for labels and chunks
	// may be reused. When picking a series we have to store the result.
	var lset labels.Labels
	var chks []chunks.Meta

	d := c.compare()
	if d > 0 {
		lset, chks, c.intervals = c.b.At()
		c.l = append(c.l[:0], lset...)
		c.c = append(c.c[:0], chks...)

		c.bok = c.b.Next()
	} else if d < 0 {
		lset, chks, c.intervals = c.a.At()
		c.l = append(c.l[:0], lset...)
		c.c = append(c.c[:0], chks...)

		c.aok = c.a.Next()
	} else {
		// Both sets contain the current series. Chain them into a single one.
		l, ca, ra := c.a.At()
		_, cb, rb := c.b.At()

		for _, r := range rb {
			ra = ra.Add(r)
		}

		c.l = append(c.l[:0], l...)
		c.c = append(append(c.c[:0], ca...), cb...)
		c.intervals = ra

		c.aok = c.a.Next()
		c.bok = c.b.Next()
	}

	return true
}

func (c *compactionMerger) Err() error {
	if c.a.Err() != nil {
		return c.a.Err()
	}
	return c.b.Err()
}

func (c *compactionMerger) At() (labels.Labels, []chunks.Meta, tombstones.Intervals) {
	return c.l, c.c, c.intervals
}

func newMergedStringIter(a index.StringIter, b index.StringIter) index.StringIter {
	return &mergedStringIter{a: a, b: b, aok: a.Next(), bok: b.Next()}
}

type mergedStringIter struct {
	a        index.StringIter
	b        index.StringIter
	aok, bok bool
	cur      string
}

func (m *mergedStringIter) Next() bool {
	if (!m.aok && !m.bok) || (m.Err() != nil) {
		return false
	}

	if !m.aok {
		m.cur = m.b.At()
		m.bok = m.b.Next()
	} else if !m.bok {
		m.cur = m.a.At()
		m.aok = m.a.Next()
	} else if m.b.At() > m.a.At() {
		m.cur = m.a.At()
		m.aok = m.a.Next()
	} else if m.a.At() > m.b.At() {
		m.cur = m.b.At()
		m.bok = m.b.Next()
	} else { // Equal.
		m.cur = m.b.At()
		m.aok = m.a.Next()
		m.bok = m.b.Next()
	}

	return true
}
func (m mergedStringIter) At() string { return m.cur }
func (m mergedStringIter) Err() error {
	if m.a.Err() != nil {
		return m.a.Err()
	}
	return m.b.Err()
}
