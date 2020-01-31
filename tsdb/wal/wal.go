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

package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/golang/snappy"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb/fileutil"
)

const (
	DefaultSegmentSize = 128 * 1024 * 1024 // 128 MB
	pageSize           = 32 * 1024         // 32KB
	recordHeaderSize   = 7
)

// The table gets initialized with sync.Once but may still cause a race
// with any other use of the crc32 package anywhere. Thus we initialize it
// before.
var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

// page is an in memory buffer used to batch disk writes.
// Records bigger than the page size are split and flushed separately.
// A flush is triggered when a single records doesn't fit the page size or
// when the next record can't fit in the remaining free page space.
type page struct {
	alloc   int            // 当前 page 已经使用的字节数
	flushed int            // buf 的下标, flushed 之前的数据都已经刷新到磁盘中
	buf     [pageSize]byte // 用于存储当前 page 的数据
}

// remaining 检测 page 实例的剩余空间
func (p *page) remaining() int {
	return pageSize - p.alloc
}

// full 检测 page 是否已满
func (p *page) full() bool {
	return pageSize-p.alloc < recordHeaderSize
}

func (p *page) reset() {
	for i := range p.buf {
		p.buf[i] = 0
	}
	p.alloc = 0
	p.flushed = 0
}

// Segment represents a segment file.
type Segment struct {
	*os.File        // 内嵌 os.File 以提供读写文件的能力
	dir      string // WAL 日志所在的目录位置
	i        int    // 当前 segment 文件在 WAL 目录下所有 segment 文件中的编号
}

// Index returns the index of the segment.
func (s *Segment) Index() int {
	return s.i
}

// Dir returns the directory of the segment.
func (s *Segment) Dir() string {
	return s.dir
}

// CorruptionErr is an error that's returned when corruption is encountered.
type CorruptionErr struct {
	Dir     string
	Segment int
	Offset  int64
	Err     error
}

func (e *CorruptionErr) Error() string {
	if e.Segment < 0 {
		return fmt.Sprintf("corruption after %d bytes: %s", e.Offset, e.Err)
	}
	return fmt.Sprintf("corruption in segment %s at %d: %s", SegmentName(e.Dir, e.Segment), e.Offset, e.Err)
}

// OpenWriteSegment opens segment k in dir. The returned segment is ready for new appends.
func OpenWriteSegment(logger log.Logger, dir string, k int) (*Segment, error) {
	segName := SegmentName(dir, k)
	f, err := os.OpenFile(segName, os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	// If the last page is torn, fill it with zeros.
	// In case it was torn after all records were written successfully, this
	// will just pad the page and everything will be fine.
	// If it was torn mid-record, a full read (which the caller should do anyway
	// to ensure integrity) will detect it as a corruption by the end.
	if d := stat.Size() % pageSize; d != 0 {
		level.Warn(logger).Log("msg", "last page of the wal is torn, filling it with zeros", "segment", segName)
		if _, err := f.Write(make([]byte, pageSize-d)); err != nil {
			f.Close()
			return nil, errors.Wrap(err, "zero-pad torn page")
		}
	}
	return &Segment{File: f, i: k, dir: dir}, nil
}

// CreateSegment creates a new segment k in dir.
// CreateSegment 在指定的目录中创建编号为 k 的 Segment 文件
func CreateSegment(dir string, k int) (*Segment, error) {
	// 创建指定编号的 Segment 文件
	f, err := os.OpenFile(SegmentName(dir, k), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	return &Segment{File: f, i: k, dir: dir}, nil // 创建 Segment 实例并返回
}

// OpenReadSegment opens the segment with the given filename.
func OpenReadSegment(fn string) (*Segment, error) {
	k, err := strconv.Atoi(filepath.Base(fn))
	if err != nil {
		return nil, errors.New("not a valid filename")
	}
	f, err := os.Open(fn)
	if err != nil {
		return nil, err
	}
	return &Segment{File: f, i: k, dir: filepath.Dir(fn)}, nil
}

// WAL is a write ahead log that stores records in segment files.
// It must be read from start to end once before logging new data.
// If an error occurs during read, the repair procedure must be called
// before it's safe to do further writes.
//
// Segments are written to in pages of 32KB, with records possibly split
// across page boundaries.
// Records are never split across segments to allow full segments to be
// safely truncated. It also ensures that torn writes never corrupt records
// beyond the most recent segment.
type WAL struct {
	dir         string // WAL 日志所在的目录位置
	logger      log.Logger
	segmentSize int                // 每个 segment 文件大小的上限值, 默认是 128MB
	mtx         sync.RWMutex       // 在写入 WAL 日志时, 需要获取该锁进行同步
	segment     *Segment           // Active segment. 当前正在使用的 Segment 实例, 当前的 WAL 日志都会被写入其对应的日志文件中
	donePages   int                // Pages written to the segment. 已写入当前 Segment 的 page 个数
	page        *page              // Active page. // 当前写入正在使用的 page 实例
	stopc       chan chan struct{} // 当 WAL 关闭时使用的通道, 使用该通道协调文件关闭时的磁盘刷新等操作
	actorc      chan func()        // 在进行 Segment 文件切换的时候, 异步刷新 Segment 文件用到的通道
	closed      bool               // To allow calling Close() more than once without blocking.
	compress    bool
	snappyBuf   []byte

	metrics *walMetrics
}

type walMetrics struct {
	fsyncDuration   prometheus.Summary
	pageFlushes     prometheus.Counter
	pageCompletions prometheus.Counter
	truncateFail    prometheus.Counter
	truncateTotal   prometheus.Counter
	currentSegment  prometheus.Gauge
}

func newWALMetrics(w *WAL, r prometheus.Registerer) *walMetrics {
	m := &walMetrics{}

	m.fsyncDuration = prometheus.NewSummary(prometheus.SummaryOpts{
		Name:       "prometheus_tsdb_wal_fsync_duration_seconds",
		Help:       "Duration of WAL fsync.",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	})
	m.pageFlushes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_wal_page_flushes_total",
		Help: "Total number of page flushes.",
	})
	m.pageCompletions = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_wal_completed_pages_total",
		Help: "Total number of completed pages.",
	})
	m.truncateFail = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_wal_truncations_failed_total",
		Help: "Total number of WAL truncations that failed.",
	})
	m.truncateTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_wal_truncations_total",
		Help: "Total number of WAL truncations attempted.",
	})
	m.currentSegment = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "prometheus_tsdb_wal_segment_current",
		Help: "WAL segment index that TSDB is currently writing to.",
	})

	if r != nil {
		r.MustRegister(
			m.fsyncDuration,
			m.pageFlushes,
			m.pageCompletions,
			m.truncateFail,
			m.truncateTotal,
			m.currentSegment,
		)
	}

	return m
}

// New returns a new WAL over the given directory.
func New(logger log.Logger, reg prometheus.Registerer, dir string, compress bool) (*WAL, error) {
	return NewSize(logger, reg, dir, DefaultSegmentSize, compress)
}

// NewSize returns a new WAL over the given directory.
// New segments are created with the specified size.
func NewSize(logger log.Logger, reg prometheus.Registerer, dir string, segmentSize int, compress bool) (*WAL, error) {
	// 检测 segmentSize 参数是否合法, 该值指定了每个 Segment 的大小, 它必须是 pageSize 的整数倍
	if segmentSize%pageSize != 0 {
		return nil, errors.New("invalid segment size")
	}
	if err := os.MkdirAll(dir, 0777); err != nil { // 有足够的权限操作 WAL 日志所在的目录
		return nil, errors.Wrap(err, "create dir")
	}
	if logger == nil {
		logger = log.NewNopLogger()
	}
	w := &WAL{ // 创建 WAL 实例
		dir:         dir,
		logger:      logger,
		segmentSize: segmentSize,
		page:        &page{},
		actorc:      make(chan func(), 100),
		stopc:       make(chan chan struct{}),
		compress:    compress,
	}
	w.metrics = newWALMetrics(w, reg)

	// WAL.Segments() 方法会读取 WAL 目录, 返回第一个以及最后一个 Segment 文件的编号
	_, last, err := w.Segments()
	if err != nil {
		return nil, errors.Wrap(err, "get segment range")
	}

	// Index of the Segment we want to open and write to.
	writeSegmentIndex := 0 // 若 last == -1, 表示当前目录为空, 则会创建第一个 Segment 文件
	// If some segments already exist create one with a higher index than the last segment.
	if last != -1 { // 否则当前目录中已存在 Segment 文件, 则打开/创建最后一个 Segemnt 文件
		writeSegmentIndex = last + 1
	}

	// 创建 Segment 实例, 其中封装了对 Segment 文件的操作
	segment, err := CreateSegment(w.dir, writeSegmentIndex)
	if err != nil {
		return nil, err
	}

	// 根据 Segment 文件的状态, 更新当前 Segment 文件已写入的 page 个数
	if err := w.setSegment(segment); err != nil {
		return nil, err
	}

	// 启动一个单独的 goroutine 来执行 WAL.run() 方法
	go w.run()

	return w, nil
}

// Open an existing WAL.
func Open(logger log.Logger, reg prometheus.Registerer, dir string) (*WAL, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	w := &WAL{
		dir:    dir,
		logger: logger,
	}

	return w, nil
}

// CompressionEnabled returns if compression is enabled on this WAL.
func (w *WAL) CompressionEnabled() bool {
	return w.compress
}

// Dir returns the directory of the WAL.
func (w *WAL) Dir() string {
	return w.dir
}

func (w *WAL) run() {
Loop:
	for {
		select {
		// 监听 actorc 通道, 并执行从该通道监听到的函数(这些函数是由 nextSegment() 方法发送过来的),
		// 其作用就是异步刷新并关闭 Segment 文件
		case f := <-w.actorc:
			f()

		// 监听 stopc 通道, 在 WAL 关闭时, 会通过 stopc 通道告知 run goroutine -- 关闭 actorc 通道
		// 并执行 actorc 中堆积的函数, 待这些堆积的刷新操作全部执行完毕后, WAL 才算正常关闭.
		case donec := <-w.stopc:
			close(w.actorc)
			defer close(donec)
			break Loop
		}
	}
	// Drain and process any remaining functions.
	for f := range w.actorc { // 执行 actorc 通道中堆积的函数
		f()
	}
}

// Repair attempts to repair the WAL based on the error.
// It discards all data after the corruption.
// Repair 最大限度地恢复损坏的 Segment 文件中可用的 Record 数据.
// 该方法会将损坏点后的全部 Segment 文件删除, 因为这些文件中记录的 Record 可能已经损坏,
// 然后读取损坏点所在 Segment 文件并保存其中可用的 Record 记录, 最后会删除损坏的 Segment 文件.
func (w *WAL) Repair(origErr error) error {
	// We could probably have a mode that only discards torn records right around
	// the corruption to preserve as data much as possible.
	// But that's not generally applicable if the records have any kind of causality.
	// Maybe as an extra mode in the future if mid-WAL corruptions become
	// a frequent concern.
	err := errors.Cause(origErr) // So that we can pick up errors even if wrapped.

	cerr, ok := err.(*CorruptionErr) // 对传入的异常进行类型转换
	if !ok {
		return errors.Wrap(origErr, "cannot handle error")
	}
	if cerr.Segment < 0 {
		return errors.New("corruption error does not specify position")
	}
	level.Warn(w.logger).Log("msg", "starting corruption repair",
		"segment", cerr.Segment, "offset", cerr.Offset)

	// All segments behind the corruption can no longer be used.
	segs, err := listSegments(w.dir) // 获取 WAL 目录下的全部 Segment 文件
	if err != nil {
		return errors.Wrap(err, "list segments")
	}
	level.Warn(w.logger).Log("msg", "deleting all segments newer than corrupted segment", "segment", cerr.Segment)

	for _, s := range segs {
		// 如果当前可写的 Segment 文件发生损坏, 则需要进行关闭
		if w.segment.i == s.index {
			// The active segment needs to be removed,
			// close it first (Windows!). Can be closed safely
			// as we set the current segment to repaired file
			// below.
			if err := w.segment.Close(); err != nil {
				return errors.Wrap(err, "close active segment")
			}
		}
		// 无须处理损坏点之前的全部 Segment 文件, 其后的 Segment 文件将会被删除
		if s.index <= cerr.Segment {
			continue
		}
		// 删除损坏点之后的全部 Segment 文件
		if err := os.Remove(filepath.Join(w.dir, s.name)); err != nil {
			return errors.Wrapf(err, "delete segment:%v", s.index)
		}
	}
	// Regardless of the corruption offset, no record reaches into the previous segment.
	// So we can safely repair the WAL by removing the segment and re-inserting all
	// its records up to the corruption.
	level.Warn(w.logger).Log("msg", "rewrite corrupted segment", "segment", cerr.Segment)

	// 下面开始处理损坏点所在的 Segment 文件
	// 首先将损坏点所在的 Segment 文件进行重命名(添加 ".repair" 后缀)
	fn := SegmentName(w.dir, cerr.Segment)
	tmpfn := fn + ".repair"

	if err := fileutil.Rename(fn, tmpfn); err != nil {
		return err
	}
	// Create a clean segment and make it the active one.
	// 创建一个新的 Segment 文件, 其名称与原来损坏的 Segment 文件同名, 这个全新的
	// Segment 文件用于记录已损坏的 Segment 文件中可用的 Record
	s, err := CreateSegment(w.dir, cerr.Segment)
	if err != nil {
		return err
	}
	// 将这个全新的 Segment 文件作为当前可写的 Segment 文件
	if err := w.setSegment(s); err != nil {
		return err
	}

	// 打开已损坏的 Segment 文件, 并读取其中的 Record 数据
	f, err := os.Open(tmpfn)
	if err != nil {
		return errors.Wrap(err, "open segment")
	}
	defer f.Close()

	// wal.Reader 用于读取当前 Segment 文件, 这里传入的 io.Reader 实例只会读取损坏的 Segment 文件
	r := NewReader(bufio.NewReader(f))

	for r.Next() { // 将读取到的 Record 数据写入当前可写的 Segment 文件中
		// Add records only up to the where the error was.
		if r.Offset() >= cerr.Offset { // 丢弃当前 Segment 文件中损坏点之后的 Record 记录
			break
		}
		if err := w.Log(r.Record()); err != nil {
			return errors.Wrap(err, "insert record")
		}
	}
	// We expect an error here from r.Err(), so nothing to handle.
	// r.Err() 在这里出现 error, 因此没有什么可处理的

	// We need to pad to the end of the last page in the repaired segment
	// 我们需要在修复的 segment 中填充到最后一页的末尾
	if err := w.flushPage(true); err != nil {
		return errors.Wrap(err, "flush page in repair")
	}

	// We explicitly close even when there is a defer for Windows to be
	// able to delete it. The defer is in place to close it in-case there
	// are errors above.
	if err := f.Close(); err != nil { // 修复完成后关闭并移除 ".repair" 文件
		return errors.Wrap(err, "close corrupted file")
	}
	if err := os.Remove(tmpfn); err != nil {
		return errors.Wrap(err, "delete corrupted segment")
	}

	// Explicitly close the segment we just repaired to avoid issues with Windows.
	s.Close()

	// We always want to start writing to a new Segment rather than an existing
	// Segment, which is handled by NewSize, but earlier in Repair we're deleting
	// all segments that come after the corrupted Segment. Recreate a new Segment here.
	s, err = CreateSegment(w.dir, cerr.Segment+1)
	if err != nil {
		return err
	}
	if err := w.setSegment(s); err != nil {
		return err
	}
	return nil
}

// SegmentName builds a segment name for the directory.
func SegmentName(dir string, i int) string {
	return filepath.Join(dir, fmt.Sprintf("%08d", i))
}

// NextSegment creates the next segment and closes the previous one.
func (w *WAL) NextSegment() error {
	w.mtx.Lock()
	defer w.mtx.Unlock()
	return w.nextSegment()
}

// nextSegment creates the next segment and closes the previous one.
func (w *WAL) nextSegment() error {
	// Only flush the current page if it actually holds data.
	// 当前 page(Segment 的最后一个 page)中已有数据写入, 则需要将该 page 刷新到磁盘中
	if w.page.alloc > 0 {
		if err := w.flushPage(true); err != nil {
			return err
		}
	}
	// 创建新的 Segment
	next, err := CreateSegment(w.dir, w.segment.Index()+1)
	if err != nil {
		return errors.Wrap(err, "create new segment file")
	}
	prev := w.segment
	// 更新 WAL.segment 字段, 并根据 Segment 状态更新当前 Segment 文件(即 next)已写入的 page 个数
	if err := w.setSegment(next); err != nil {
		return err
	}

	// Don't block further writes by fsyncing the last segment.
	// 通过 actorc 通道通知 run goroutine, 完成上一个 Segment 文件的磁盘刷新
	w.actorc <- func() { // 该函数是由 run goroutine 执行的
		if err := w.fsync(prev); err != nil { // 将上一个 Segment 文件刷新到磁盘
			level.Error(w.logger).Log("msg", "sync previous segment", "err", err)
		}
		if err := prev.Close(); err != nil { // 关闭上一个 Segment 文件
			level.Error(w.logger).Log("msg", "close previous segment", "err", err)
		}
	}
	return nil
}

func (w *WAL) setSegment(segment *Segment) error {
	w.segment = segment

	// Correctly initialize donePages.
	stat, err := segment.Stat() // 根据 Segment 文件的状态, 更新当前 Segment 文件已写入的 page 个数
	if err != nil {
		return err
	}
	w.donePages = int(stat.Size() / pageSize)
	w.metrics.currentSegment.Set(float64(segment.Index()))
	return nil
}

// flushPage writes the new contents of the page to disk. If no more records will fit into
// the page, the remaining bytes will be set to zero and a new page will be started.
// If clear is true, this is enforced regardless of how many bytes are left in the page.
// flushPage 将当前内存中的 page 数据写入对应的 Segment 文件中, 并尝试清空该 page
func (w *WAL) flushPage(clear bool) error {
	w.metrics.pageFlushes.Inc()

	p := w.page
	clear = clear || p.full() // 根据 clear 参数以及当前 page 是否已满来判断是否应清空该 page 中的数据

	// No more data will fit into the page or an implicit clear.
	// Enqueue and clear it.
	if clear {
		p.alloc = pageSize // Write till end of page.
	}
	// 补充 page 中未写入 Segment 文件的数据(flushed 为上次写入的位置, alloc 为当前已使用的位置)
	n, err := w.segment.Write(p.buf[p.flushed:p.alloc])
	if err != nil {
		return err
	}
	p.flushed += n // 推进 flushed, 下次 flushPage 调用将从该位置开始写入

	// We flushed an entire page, prepare a new one.
	if clear { // 清空 page 中的数据, 为写入下一个 page 做准备
		p.reset()     // 重置 alloc 和 flushed
		w.donePages++ // donePages 记录了当前 Segment 文件中写入的 page 个数
		w.metrics.pageCompletions.Inc()
	}
	return nil
}

// First Byte of header format:
// [ 4 bits unallocated] [1 bit snappy compression flag] [ 3 bit record type ]
const (
	snappyMask  = 1 << 3
	recTypeMask = snappyMask - 1
)

type recType uint8

const (
	recPageTerm recType = 0 // Rest of page is empty.
	recFull     recType = 1 // Full record.
	recFirst    recType = 2 // First fragment of a record.
	recMiddle   recType = 3 // Middle fragments of a record.
	recLast     recType = 4 // Final fragment of a record.
)

func recTypeFromHeader(header byte) recType {
	return recType(header & recTypeMask)
}

func (t recType) String() string {
	switch t {
	case recPageTerm:
		return "zero"
	case recFull:
		return "full"
	case recFirst:
		return "first"
	case recMiddle:
		return "middle"
	case recLast:
		return "last"
	default:
		return "<invalid>"
	}
}

func (w *WAL) pagesPerSegment() int {
	return w.segmentSize / pageSize
}

// Log writes the records into the log.
// Multiple records can be passed at once to reduce writes and increase throughput.
func (w *WAL) Log(recs ...[]byte) error {
	w.mtx.Lock()
	defer w.mtx.Unlock()
	// Callers could just implement their own list record format but adding
	// a bit of extra logic here frees them from that overhead.
	for i, r := range recs {
		if err := w.log(r, i == len(recs)-1); err != nil {
			return err
		}
	}
	return nil
}

// log writes rec to the log and forces a flush of the current page if:
// - the final record of a batch
// - the record is bigger than the page size
// - the current page is full.
func (w *WAL) log(rec []byte, final bool) error {
	// When the last page flush failed the page will remain full.
	// When the page is full, need to flush it before trying to add more records to it.
	if w.page.full() {
		if err := w.flushPage(true); err != nil {
			return err
		}
	}
	// If the record is too big to fit within the active page in the current
	// segment, terminate the active segment and advance to the next one.
	// This ensures that records do not cross segment boundaries.
	// 计算当前 page 剩余的空间
	left := w.page.remaining() - recordHeaderSize // Free space in the active page.
	// 计算当前 Segment 剩余的空间
	left += (pageSize - recordHeaderSize) * (w.pagesPerSegment() - w.donePages - 1) // Free pages in the active segment.

	if len(rec) > left {
		// 当前 Segment 的剩余空间无法容纳要写入的 Record, 则调用 nextSegment 方法
		// 切换到下一个 Segment 文件, 再开始后续的写入
		if err := w.nextSegment(); err != nil {
			return err
		}
	}

	compressed := false
	if w.compress && len(rec) > 0 { // 需要进行压缩
		// The snappy library uses `len` to calculate if we need a new buffer.
		// In order to allocate as few buffers as possible make the length
		// equal to the capacity.
		w.snappyBuf = w.snappyBuf[:cap(w.snappyBuf)]
		w.snappyBuf = snappy.Encode(w.snappyBuf, rec)
		if len(w.snappyBuf) < len(rec) {
			rec = w.snappyBuf
			compressed = true
		}
	}

	// Populate as many pages as necessary to fit the record.
	// Be careful to always do one pass to ensure we write zero-length records.
	// 一个 Record 是可以跨越多个 page 的, 因此在遇到当前 page 无法容纳的 Record 时,
	// 会将其多次写入不同的 page 中, 直至整个 Record 被写入完成
	for i := 0; i == 0 || len(rec) > 0; i++ {
		p := w.page

		// Find how much of the record we can fit into the page.
		var (
			l    = min(len(rec), (pageSize-p.alloc)-recordHeaderSize) // 计算当前 page 能写入的最大字节数
			part = rec[:l]
			buf  = p.buf[p.alloc:] // 当前 page 剩余的空间
			typ  recType
		)

		switch {
		case i == 0 && len(part) == len(rec):
			// 第一次写入该 Record, 且可以将其完全写入当前的 page 中
			typ = recFull
		case len(part) == len(rec):
			// 并不是第一次写入该 Record(即该 Record 的前半部分已经被写入到
			// 上一个 page 中), 且剩余部分能够被完整写入当前 page 中
			typ = recLast
		case i == 0:
			// 第一次写入该 Record, 当无法被完整写入当前的 page 中(Record 的后半部分会被
			// 写入到下一个 page 中)
			typ = recFirst
		default:
			// 如果不满足上述场景, 则表示该 Record 将其中间部分写入当前的 page 中,
			// 即该 Reaord 的前半部分被写入上一个 page, 中间部分占用当前整个 page(或是连续
			// 多个完整的 page), 后半部分被写入下一个 page
			typ = recMiddle
		}
		if compressed {
			typ |= snappyMask
		}

		// 下面开始构建 Record 的头信息
		buf[0] = byte(typ) // 使用 buf 中的第一个字节记录当前 Record 的写入范围
		crc := crc32.Checksum(part, castagnoliTable)
		binary.BigEndian.PutUint16(buf[1:], uint16(len(part))) // 记录 Record 的长度
		binary.BigEndian.PutUint32(buf[3:], crc)               // 记录 CRC32 校验码

		copy(buf[recordHeaderSize:], part)      // 写入 Record
		p.alloc += len(part) + recordHeaderSize // 更新当前 page 已使用的字节数

		// 当一个 page 被写满时, 将触发 WAL.flushPage 方法将当前 page 写入对应的 Segment 文件中
		if w.page.full() {
			if err := w.flushPage(true); err != nil {
				return err
			}
		}
		rec = rec[l:] // 更新 Record 剩余未写入的部分, 下次循环继续写入
	}

	// If it's the final record of the batch and the page is not empty, flush it.
	// 或者完成当前的批量写入时, 将触发 WAL.flushPage 方法将当前 page 写入对应的 Segment 文件中
	if final && w.page.alloc > 0 {
		if err := w.flushPage(false); err != nil {
			return err
		}
	}

	return nil
}

// Segments returns the range [first, n] of currently existing segments.
// If no segments are found, first and n are -1.
func (w *WAL) Segments() (first, last int, err error) {
	refs, err := listSegments(w.dir)
	if err != nil {
		return 0, 0, err
	}
	if len(refs) == 0 {
		return -1, -1, nil
	}
	return refs[0].index, refs[len(refs)-1].index, nil
}

// 随着 Prometheus TSDB 的运转, WAL 日志量会不断增加, 旧的 WAL 日志对应的时序数据已经
// 刷新到磁盘上, 而这些 WAL 日志现在已经无效了, 那么就需要对其进行定期清理, 释放磁盘空间.
// Truncate 删除指定编号之前的所有 Segment 文件, 实现 WAL 清理的目的.
// Truncate drops all segments before i.
func (w *WAL) Truncate(i int) (err error) {
	w.metrics.truncateTotal.Inc()
	defer func() {
		if err != nil {
			w.metrics.truncateFail.Inc()
		}
	}()
	refs, err := listSegments(w.dir) // 获取 WAL 目录下的全部 Segment 文件
	if err != nil {
		return err
	}
	for _, r := range refs {
		if r.index >= i { // 忽略编号 i 之后的文件 Segment 文件
			break
		}
		// 将编号 i 之前的 Segment 文件删除
		if err = os.Remove(filepath.Join(w.dir, r.name)); err != nil {
			return err
		}
	}
	return nil
}

func (w *WAL) fsync(f *Segment) error {
	start := time.Now()
	err := f.File.Sync()
	w.metrics.fsyncDuration.Observe(time.Since(start).Seconds())
	return err
}

// Close flushes all writes and closes active segment.
func (w *WAL) Close() (err error) {
	w.mtx.Lock()
	defer w.mtx.Unlock()

	if w.closed {
		return errors.New("wal already closed")
	}

	// Flush the last page and zero out all its remaining size.
	// We must not flush an empty page as it would falsely signal
	// the segment is done if we start writing to it again after opening.
	if w.page.alloc > 0 {
		if err := w.flushPage(true); err != nil {
			return err
		}
	}

	donec := make(chan struct{})
	w.stopc <- donec
	<-donec

	if err = w.fsync(w.segment); err != nil {
		level.Error(w.logger).Log("msg", "sync previous segment", "err", err)
	}
	if err := w.segment.Close(); err != nil {
		level.Error(w.logger).Log("msg", "close previous segment", "err", err)
	}
	w.closed = true
	return nil
}

type segmentRef struct {
	name  string
	index int
}

// listSegments 获取 WAL 目录下所有 Segment 文件的引用(其中包含编号信息)
func listSegments(dir string) (refs []segmentRef, err error) {
	// 获取该目录下所有文件
	files, err := fileutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var last int
	for _, fn := range files {
		k, err := strconv.Atoi(fn) // 将 Segment 文件名转换成对应的数字
		if err != nil {
			continue
		}
		if len(refs) > 0 && k > last+1 {
			return nil, errors.New("segments are not sequential")
		}
		// 将文件名以及对应的序号封装成 segmentRef 实例, 并记录到 refs 中
		refs = append(refs, segmentRef{name: fn, index: k})
		last = k
	}
	sort.Slice(refs, func(i, j int) bool { // 根据 Segment 文件编号进行排序
		return refs[i].index < refs[j].index
	})
	return refs, nil
}

// SegmentRange groups segments by the directory and the first and last index it includes.
type SegmentRange struct {
	Dir         string
	First, Last int
}

// NewSegmentsReader returns a new reader over all segments in the directory.
func NewSegmentsReader(dir string) (io.ReadCloser, error) {
	return NewSegmentsRangeReader(SegmentRange{dir, -1, -1})
}

// NewSegmentsRangeReader returns a new reader over the given WAL segment ranges.
// If first or last are -1, the range is open on the respective end.
func NewSegmentsRangeReader(sr ...SegmentRange) (io.ReadCloser, error) {
	var segs []*Segment

	for _, sgmRange := range sr {
		refs, err := listSegments(sgmRange.Dir)
		if err != nil {
			return nil, errors.Wrapf(err, "list segment in dir:%v", sgmRange.Dir)
		}

		for _, r := range refs {
			if sgmRange.First >= 0 && r.index < sgmRange.First {
				continue
			}
			if sgmRange.Last >= 0 && r.index > sgmRange.Last {
				break
			}
			s, err := OpenReadSegment(filepath.Join(sgmRange.Dir, r.name))
			if err != nil {
				return nil, errors.Wrapf(err, "open segment:%v in dir:%v", r.name, sgmRange.Dir)
			}
			segs = append(segs, s)
		}
	}
	return NewSegmentBufReader(segs...), nil
}

// WAL 日志的读取依赖于 segmentBufReader 结构体. segmentBufReader 可以一次读取多个 page,
// 也支持跨越多个 Segment 文件进行读取.
//
// segmentBufReader is a buffered reader that reads in multiples of pages.
// The main purpose is that we are able to track segment and offset for
// corruption reporting.  We have to be careful not to increment curr too
// early, as it is used by Reader.Err() to tell Repair which segment is corrupt.
// As such we pad the end of non-page align segments with zeros.
type segmentBufReader struct {
	buf *bufio.Reader // 底层读取 Segment 文件的 bufio.Reader 实例, 其中自带 16 个 page 的缓冲区
	// 当前 SegmentBufReader 实例可以读取的 Segment 文件, 一般会指定读取范围, 例如读取编号 m~n 的
	// Segment, segmentBufReader 无法读取超出这个范围的 Segment 文件
	segs []*Segment
	cur  int // Index into segs. 当前正在读取的 Segment 文件编号
	// 当前 Segment 文件中是否有可以继续读取的数据
	off int // Offset of read data into current segment.
}

func NewSegmentBufReader(segs ...*Segment) *segmentBufReader {
	return &segmentBufReader{
		buf:  bufio.NewReaderSize(segs[0], 16*pageSize),
		segs: segs,
	}
}

func (r *segmentBufReader) Close() (err error) {
	for _, s := range r.segs {
		if e := s.Close(); e != nil {
			err = e
		}
	}
	return err
}

// Read implements io.Reader.
func (r *segmentBufReader) Read(b []byte) (n int, err error) {
	n, err = r.buf.Read(b) // 从 Segment 文件读取数据到 b 这个缓冲区中
	r.off += n             // 递增 off

	// If we succeeded, or hit a non-EOF, we can stop.
	if err == nil || err != io.EOF {
		return n, err
	}

	// We hit EOF; fake out zero padding at the end of short segments, so we
	// don't increment curr too early and report the wrong segment as corrupt.
	if r.off%pageSize != 0 {
		i := 0
		for ; n+i < len(b) && (r.off+i)%pageSize != 0; i++ {
			b[n+i] = 0
		}

		// Return early, even if we didn't fill b.
		r.off += i
		return n + i, nil
	}

	// There is no more deta left in the curr segment and there are no more
	// segments left.  Return EOF.
	if r.cur+1 >= len(r.segs) { // 已读取完全部的 Segment 文件
		return n, io.EOF
	}

	// Move to next segment.
	r.cur++                    // 后移 curr, 读取下一个 Segment 文件
	r.off = 0                  // 重置 off, 准备读取下一个 Segment 文件
	r.buf.Reset(r.segs[r.cur]) // 清空缓冲区中的数据并准备从新的 Segment 文件中读取数据
	return n, nil
}

// Computing size of the WAL.
// We do this by adding the sizes of all the files under the WAL dir.
func (w *WAL) Size() (int64, error) {
	return fileutil.DirSize(w.Dir())
}
