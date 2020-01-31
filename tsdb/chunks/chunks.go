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

package chunks

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/prometheus/prometheus/tsdb/fileutil"
)

// Segment header fields constants.
const (
	// MagicChunks is 4 bytes at the head of a series file.
	MagicChunks = 0x85BD40DD
	// MagicChunksSize is the size in bytes of MagicChunks.
	MagicChunksSize          = 4
	chunksFormatV1           = 1
	ChunksFormatVersionSize  = 1
	segmentHeaderPaddingSize = 3
	// SegmentHeaderSize defines the total size of the header part.
	SegmentHeaderSize = MagicChunksSize + ChunksFormatVersionSize + segmentHeaderPaddingSize
)

// Chunk fields constants.
const (
	// MaxChunkLengthFieldSize defines the maximum size of the data length part.
	MaxChunkLengthFieldSize = binary.MaxVarintLen32
	// ChunkEncodingSize defines the size of the chunk encoding part.
	ChunkEncodingSize = 1
)

// Meta 存储 Chunk 实例关联的元数据信息
// Meta holds information about a chunk of data.
type Meta struct {
	// Ref and Chunk hold either a reference that can be used to retrieve
	// chunk data or the data itself.
	// When it is a reference it is the segment offset at which the chunk bytes start.
	// Generally, only one of them is set.
	Ref   uint64         // 记录关联 Chunk 在磁盘上的位置信息, 主要用于读取
	Chunk chunkenc.Chunk // 指向 XORChunk 实例, 在 ChunkWriter 方法中, 在将 Chunk 中时序数据持久化到文件时, 该字段必须有值

	// Time range the data covers.
	// When MaxTime == math.MaxInt64 the chunk is still open and being appended to.
	MinTime, MaxTime int64 // 记录 Chunk 实例所覆盖的时间范围
}

// writeHash writes the chunk encoding and raw data into the provided hash.
// writeHash 为关联的 Chunk 计算 Hash 值
func (cm *Meta) writeHash(h hash.Hash, buf []byte) error {
	buf = append(buf[:0], byte(cm.Chunk.Encoding()))
	if _, err := h.Write(buf[:1]); err != nil {
		return err
	}
	if _, err := h.Write(cm.Chunk.Bytes()); err != nil {
		return err
	}
	return nil
}

// OverlapsClosedInterval Returns true if the chunk overlaps [mint, maxt].
// OverlapsClosedInterval 用于确定给定的时间范围是否与关联 Chunk 实例所覆盖的时间范围有重合
func (cm *Meta) OverlapsClosedInterval(mint, maxt int64) bool {
	// The chunk itself is a closed interval [cm.MinTime, cm.MaxTime].
	return cm.MinTime <= maxt && mint <= cm.MaxTime
}

var (
	errInvalidSize = fmt.Errorf("invalid size")
)

var castagnoliTable *crc32.Table

func init() {
	castagnoliTable = crc32.MakeTable(crc32.Castagnoli)
}

// newCRC32 initializes a CRC32 hash with a preconfigured polynomial, so the
// polynomial may be easily changed in one location at a later time, if necessary.
func newCRC32() hash.Hash32 {
	return crc32.New(castagnoliTable)
}

// Writer implements the ChunkWriter interface for the standard
// serialization format.
type Writer struct {
	dirFile *os.File // 磁盘上存储时序数据的目录
	// dirFile 目录下存储时序数据的 segment 文件集合, 其中只有最后一个 segment 文件是当前
	// 有效的, 即当前可以写入数据的 segment 文件, 之前的 segment 文件不可写
	files []*os.File
	wbuf  *bufio.Writer // 用于写文件的 bufio.Writer, 该 Writer 是带缓冲区的
	n     int64         // 当前分段已经写入的字节数
	crc32 hash.Hash     // CRC32 校验码, 每一个写入的 Chunk 都会生成一个校验码
	buf   [binary.MaxVarintLen32]byte

	segmentSize int64 // 每个分段文件的大小上限, 默认是 512*1024*1024
}

const (
	// DefaultChunkSegmentSize is the default chunks segment size.
	DefaultChunkSegmentSize = 512 * 1024 * 1024
)

// NewWriterWithSegSize returns a new writer against the given directory
// and allows setting a custom size for the segments.
func NewWriterWithSegSize(dir string, segmentSize int64) (*Writer, error) {
	return newWriter(dir, segmentSize)
}

// NewWriter returns a new writer against the given directory
// using the default segment size.
func NewWriter(dir string) (*Writer, error) {
	return newWriter(dir, DefaultChunkSegmentSize)
}

func newWriter(dir string, segmentSize int64) (*Writer, error) {
	if segmentSize <= 0 {
		segmentSize = DefaultChunkSegmentSize
	}

	// 创建 dir 参数指定的目录, 并给予足够的权限
	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil, err
	}
	dirFile, err := fileutil.OpenDir(dir)
	if err != nil {
		return nil, err
	}
	return &Writer{ // 初始化 Writer 实例
		dirFile:     dirFile,
		n:           0,
		crc32:       newCRC32(), // 创建复用的 CRC32 循环校验码
		segmentSize: segmentSize,
	}, nil
}

func (w *Writer) tail() *os.File {
	if len(w.files) == 0 {
		return nil
	}
	return w.files[len(w.files)-1]
}

// finalizeTail writes all pending data to the current tail file,
// truncates its size, and closes it.
//
// finalizeTail 将已写入当前 segment 文件的时序数据刷新到磁盘中, 并对当前
// segment 文件中预分配但是未使用的部分进行截断, 最后关闭文件.
func (w *Writer) finalizeTail() error {
	tf := w.tail() // 获取 files 集合中的最后一个文件, 即当前有效的写入文件
	if tf == nil {
		return nil
	}

	// 调用 wbuf 字段(bufio.Writer)的 Flush() 方法将数据刷新到磁盘中
	if err := w.wbuf.Flush(); err != nil {
		return err
	}
	if err := tf.Sync(); err != nil {
		return err
	}
	// As the file was pre-allocated, we truncate any superfluous zero bytes.
	// 在创建文件时会进行预分配, 这里获取当前写入的位置, 并调用 Truncate() 方法进行截断
	off, err := tf.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	if err := tf.Truncate(off); err != nil {
		return err
	}

	return tf.Close() // 关闭当前文件
}

func (w *Writer) cut() error {
	// Sync current tail to disk and close.
	// 通过 finializeTail() 方法完成当前文件的写入
	if err := w.finalizeTail(); err != nil {
		return err
	}

	p, _, err := nextSequenceFile(w.dirFile.Name()) // 计算下一个写入的新 segment 文件的名称
	if err != nil {
		return err
	}
	f, err := os.OpenFile(p, os.O_WRONLY|os.O_CREATE, 0666) // 创建新的 segment
	if err != nil {
		return err
	}
	// 按照 segment 文件的上限进行预分配
	if err = fileutil.Preallocate(f, w.segmentSize, true); err != nil {
		return err
	}
	if err = w.dirFile.Sync(); err != nil { // 将上述 segment 文件创建以及预分配操作同步到磁盘
		return err
	}

	// Write header metadata for new file.
	metab := make([]byte, SegmentHeaderSize)                         // 创建 segment 文件头, 8 字节
	binary.BigEndian.PutUint32(metab[:MagicChunksSize], MagicChunks) // 将前 4 个字节写入固定头信息
	metab[4] = chunksFormatV1                                        // 写入版本信息

	n, err := f.Write(metab) // 将 8 字节文件头写入 segment 文件中
	if err != nil {
		return err
	}
	w.n = int64(n) // 更新当前 segment 文件中的总字节数

	w.files = append(w.files, f) // 将新建的 segment 文件记录到 Writer.files 集合中
	if w.wbuf != nil {
		w.wbuf.Reset(f) // 将 wbuf 从上一个文件指向新建的文件
	} else {
		w.wbuf = bufio.NewWriterSize(f, 8*1024*1024)
	}

	return nil
}

// wbuf 指向 bufio.Writer(即指向 sengment 文件)
func (w *Writer) write(b []byte) error {
	n, err := w.wbuf.Write(b)
	w.n += int64(n)
	return err
}

// MergeOverlappingChunks removes the samples whose timestamp is overlapping.
// The last appearing sample is retained in case there is overlapping.
// This assumes that `chks []Meta` is sorted w.r.t. MinTime.
func MergeOverlappingChunks(chks []Meta) ([]Meta, error) {
	if len(chks) < 2 {
		return chks, nil
	}
	newChks := make([]Meta, 0, len(chks)) // Will contain the merged chunks.
	newChks = append(newChks, chks[0])
	last := 0
	for _, c := range chks[1:] {
		// We need to check only the last chunk in newChks.
		// Reason: (1) newChks[last-1].MaxTime < newChks[last].MinTime (non overlapping)
		//         (2) As chks are sorted w.r.t. MinTime, newChks[last].MinTime < c.MinTime.
		// So never overlaps with newChks[last-1] or anything before that.
		if c.MinTime > newChks[last].MaxTime {
			newChks = append(newChks, c)
			last++
			continue
		}
		nc := &newChks[last]
		if c.MaxTime > nc.MaxTime {
			nc.MaxTime = c.MaxTime
		}
		chk, err := MergeChunks(nc.Chunk, c.Chunk)
		if err != nil {
			return nil, err
		}
		nc.Chunk = chk
	}

	return newChks, nil
}

// MergeChunks vertically merges a and b, i.e., if there is any sample
// with same timestamp in both a and b, the sample in a is discarded.
func MergeChunks(a, b chunkenc.Chunk) (*chunkenc.XORChunk, error) {
	newChunk := chunkenc.NewXORChunk()
	app, err := newChunk.Appender()
	if err != nil {
		return nil, err
	}
	ait := a.Iterator(nil)
	bit := b.Iterator(nil)
	aok, bok := ait.Next(), bit.Next()
	for aok && bok {
		at, av := ait.At()
		bt, bv := bit.At()
		if at < bt {
			app.Append(at, av)
			aok = ait.Next()
		} else if bt < at {
			app.Append(bt, bv)
			bok = bit.Next()
		} else {
			app.Append(bt, bv)
			aok = ait.Next()
			bok = bit.Next()
		}
	}
	for aok {
		at, av := ait.At()
		app.Append(at, av)
		aok = ait.Next()
	}
	for bok {
		bt, bv := bit.At()
		app.Append(bt, bv)
		bok = bit.Next()
	}
	if ait.Err() != nil {
		return nil, ait.Err()
	}
	if bit.Err() != nil {
		return nil, bit.Err()
	}
	return newChunk, nil
}

// WriteChunks writes as many chunks as possible to the current segment,
// cuts a new segment when the current segment is full and
// writes the rest of the chunks in the new segment.
//
// WriteChunks 写入多个 chunks 到当前 segment 中, 若当前 segment 已满则新建一个新的
// segment 并将剩余的 chunks 写入到新的 segment 中
func (w *Writer) WriteChunks(chks ...Meta) error {
	var (
		batchSize  = int64(0)
		batchStart = 0
		batches    = make([][]Meta, 1) // batches 在下面遍历后有多少维即表示需要在多少个 segment 中写入本批 chks
		batchID    = 0
		firstBatch = true // 为 false 表示需要在多个 segment 中写入本批 chunks
	)

	for i, chk := range chks { // 计算待写入的所有 Chunk 实例的字节总数
		// Each chunk contains: data length + encoding + the data itself + crc32
		chkSize := int64(MaxChunkLengthFieldSize) // The data length is a variable length field so use the maximum possible value.
		chkSize += ChunkEncodingSize              // The chunk encoding.
		chkSize += int64(len(chk.Chunk.Bytes()))  // The data itself.
		chkSize += crc32.Size                     // The 4 bytes of crc32.
		batchSize += chkSize

		// Cut a new batch when it is not the first chunk(to avoid empty segments) and
		// the batch is too large to fit in the current segment.
		// 不为首个 chunk(避免空 segment) 且待写入的所有 Chunk 实例数据总字节数超过一个 segment 所能
		// 写入的最大字节数, 则切换到一个新的 segment 继续写入
		cutNewBatch := (i != 0) && (batchSize+SegmentHeaderSize > w.segmentSize)

		// When the segment already has some data than
		// the first batch size calculation should account for that.
		// segment 中存在已写入的数据
		if firstBatch && w.n > SegmentHeaderSize {
			cutNewBatch = batchSize+w.n > w.segmentSize
			if cutNewBatch { // 待写入的数据加上已有数据超过了 segment 最大字节数限制
				firstBatch = false
			}
		}

		if cutNewBatch { // cutNewBatch 为 true 表示需要新建一个 segment
			batchStart = i
			batches = append(batches, []Meta{})
			batchID++
			batchSize = chkSize // 重置 batchSize 为当前遍历的 Chunk 的大小, 重新开始计数
		}
		batches[batchID] = chks[batchStart : i+1]
	}

	// Create a new segment when one doesn't already exist.
	if w.n == 0 { // 首次写入 Chunk, 则新建一个 segment
		if err := w.cut(); err != nil {
			return err
		}
	}

	for i, chks := range batches {
		if err := w.writeChunks(chks); err != nil {
			return err
		}
		// Cut a new segment only when there are more chunks to write.
		// Avoid creating a new empty segment at the end of the write.
		if i < len(batches)-1 { // 当前 segment 已满时切换新的 segment 继续写入
			if err := w.cut(); err != nil {
				return err
			}
		}
	}
	return nil
}

// writeChunks writes the chunks into the current segment irrespective
// of the configured segment size limit. A segment should have been already
// started before calling this.
func (w *Writer) writeChunks(chks []Meta) error {
	if len(chks) == 0 {
		return nil
	}

	// 将当前 segment 文件在 Writer.files 集合中的下标记录到 seq 变量的高 32 位中
	var seq = uint64(w.seq()) << 32
	for i := range chks { // 将 chunk 逐个写入到 segment 文件中
		chk := &chks[i]

		// The reference is set to the segment index and the offset where
		// the data starts for this chunk.
		//
		// The upper 4 bytes are for the segment index and
		// the lower 4 bytes are for the segment offset where to start reading this chunk.
		//
		// 更新 Ref 字段, 其中高 32 位明确了该 Chunk 在哪个 segment 文件中,
		// 低 32 位记录了该 Chunk 在 segment 文件中的字节偏移量
		chk.Ref = seq | uint64(w.n)

		// 统计该 Chunk 的字节数
		n := binary.PutUvarint(w.buf[:], uint64(len(chk.Chunk.Bytes())))

		if err := w.write(w.buf[:n]); err != nil { // 将当前 chunk 的字节数值写入到 segment 中
			return err
		}
		w.buf[0] = byte(chk.Chunk.Encoding()) // 将 Chunk 的编码类型写入 segment 文件中
		if err := w.write(w.buf[:1]); err != nil {
			return err
		}
		// 将 Chunk 中记录的时序数据写入 segment 文件中
		if err := w.write(chk.Chunk.Bytes()); err != nil {
			return err
		}

		// 计算该 Chunk 的 CRC32 校验码并写入 segment 文件中
		w.crc32.Reset()
		if err := chk.writeHash(w.crc32, w.buf[:]); err != nil {
			return err
		}
		if err := w.write(w.crc32.Sum(w.buf[:0])); err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) seq() int {
	return len(w.files) - 1
}

func (w *Writer) Close() error {
	if err := w.finalizeTail(); err != nil {
		return err
	}

	// close dir file (if not windows platform will fail on rename)
	return w.dirFile.Close()
}

// ByteSlice abstracts a byte slice.
type ByteSlice interface {
	Len() int
	Range(start, end int) []byte
}

type realByteSlice []byte

func (b realByteSlice) Len() int {
	return len(b)
}

func (b realByteSlice) Range(start, end int) []byte {
	return b[start:end]
}

func (b realByteSlice) Sub(start, end int) ByteSlice {
	return b[start:end]
}

// Reader implements a ChunkReader for a serialized byte stream
// of series data.
// Reader 是 ChunkReader 接口的实现之一
type Reader struct {
	// The underlying bytes holding the encoded series data.
	// Each slice holds the data for a different segment.
	//
	// ByteSlice 接口是对 byte 切片的抽象, 它提供了两个方法, 一个是 Len() 方法, 用于返回底层 byte
	// 切片的长度; 另一个是 Range() 方法, 用于返回底层 byte 切片在指定区间内的数据. ByteSlice 接口的
	// 实现是 realByteSlice, realByteSlice 则是 []byte 的类型别名.
	// bs 字段存储的是时序数据, 其中每个 ByteSlice 实例都对应一个 segment 文件的数据
	bs []ByteSlice
	// 当前 Reader 实例能够读取的文件集合, 其中每个元素都对应一个 segment 文件
	cs   []io.Closer   // Closers for resources behind the byte slices.
	size int64         // The total size of bytes in the reader.
	pool chunkenc.Pool // 用于存储可重用的 Chunk 实例
}

func newReader(bs []ByteSlice, cs []io.Closer, pool chunkenc.Pool) (*Reader, error) {
	cr := Reader{pool: pool, bs: bs, cs: cs}
	var totalSize int64

	for i, b := range cr.bs {
		if b.Len() < SegmentHeaderSize {
			return nil, errors.Wrapf(errInvalidSize, "invalid segment header in segment %d", i)
		}
		// Verify magic number.
		if m := binary.BigEndian.Uint32(b.Range(0, MagicChunksSize)); m != MagicChunks {
			return nil, errors.Errorf("invalid magic number %x", m)
		}

		// Verify chunk format version.
		if v := int(b.Range(MagicChunksSize, MagicChunksSize+ChunksFormatVersionSize)[0]); v != chunksFormatV1 {
			return nil, errors.Errorf("invalid chunk format version %d", v)
		}
		totalSize += int64(b.Len())
	}
	cr.size = totalSize
	return &cr, nil
}

// NewDirReader returns a new Reader against sequentially numbered files in the
// given directory.
func NewDirReader(dir string, pool chunkenc.Pool) (*Reader, error) {
	// 读取指定 chunks 文件夹中的 segment 文件并按照文件名进行排序
	files, err := sequenceFiles(dir)
	if err != nil {
		return nil, err
	}
	if pool == nil { // 初始化 Chunk 池
		pool = chunkenc.NewPool()
	}

	var (
		bs   []ByteSlice
		cs   []io.Closer
		merr tsdb_errors.MultiError
	)
	for _, fn := range files {
		f, err := fileutil.OpenMmapFile(fn) // 通过 mmap 系统调用将当前整个 segment 文件映射到内存
		if err != nil {
			merr.Add(errors.Wrap(err, "mmap files"))
			merr.Add(closeAll(cs))
			return nil, merr
		}
		cs = append(cs, f)                        // 将映射得到的 MmapFile 实例追加到 cs 切片中
		bs = append(bs, realByteSlice(f.Bytes())) // 将 segment 文件映射到 bs 切片中
	}

	reader, err := newReader(bs, cs, pool) // 其中完成文件头的校验以及 Reader 实例的创建
	if err != nil {
		merr.Add(err)
		merr.Add(closeAll(cs))
		return nil, merr
	}
	return reader, nil
}

func (s *Reader) Close() error {
	return closeAll(s.cs)
}

// Size returns the size of the chunks.
func (s *Reader) Size() int64 {
	return s.size
}

// Chunk returns a chunk from a given reference.
//
// Chunk 根据传入的 ref 参数在当前 chunks 目录中查找对应 Chunk 数据的位置, 然后从
// Chunk 池中获取一个空闲的 Chunk 实例, 最后从文件中读取时序数据填充到 Chunk 实例中,
// 并将其返回.
func (s *Reader) Chunk(ref uint64) (chunkenc.Chunk, error) {
	var (
		// Get the upper 4 bytes.
		// These contain the segment index.
		sgmIndex = int(ref >> 32) // 从 ref 参数的高 32 位中获取对应 chunk 所在的 segment 文件编号
		// Get the lower 4 bytes.
		// These contain the segment offset where the data for this chunk starts.
		// 从 ref 参数的低 32 位中获取 Chunk 在该 segment 文件中的字节偏移量
		chkStart = int((ref << 32) >> 32)
		chkCRC32 = newCRC32()
	)

	// 检测编号是否合法, 即检测 seq 编号是否大于 chunks 目录中的最大编号
	if sgmIndex >= len(s.bs) {
		return nil, errors.Errorf("segment index %d out of range", sgmIndex)
	}

	sgmBytes := s.bs[sgmIndex] // 获取指定编号对应的 segment 文件数据

	// 查找到正确的 segment 文件后, 检测 chkStart 偏移量是否合法, 即检测 chkStart 偏移量是否
	// 超过了该 segment 文件的大小
	if chkStart+MaxChunkLengthFieldSize > sgmBytes.Len() {
		return nil, errors.Errorf("segment doesn't include enough bytes to read the chunk size data field - required:%v, available:%v", chkStart+MaxChunkLengthFieldSize, sgmBytes.Len())
	}
	// With the minimum chunk length this should never cause us reading
	// over the end of the slice.
	// 读取 Chunk 在文件中所占的字节数
	c := sgmBytes.Range(chkStart, chkStart+MaxChunkLengthFieldSize)
	chkDataLen, n := binary.Uvarint(c)
	if n <= 0 {
		return nil, errors.Errorf("reading chunk length failed with %d", n)
	}

	chkEncStart := chkStart + n
	chkEnd := chkEncStart + ChunkEncodingSize + int(chkDataLen) + crc32.Size
	chkDataStart := chkEncStart + ChunkEncodingSize
	chkDataEnd := chkEnd - crc32.Size

	// 要读取的 Chunk 的超出了该 segment 文件的字节数限制
	if chkEnd > sgmBytes.Len() {
		return nil, errors.Errorf("segment doesn't include enough bytes to read the chunk - required:%v, available:%v", chkEnd, sgmBytes.Len())
	}

	// 获取 ref 对应的时序数据的 crc32 校验码
	sum := sgmBytes.Range(chkDataEnd, chkEnd)
	if _, err := chkCRC32.Write(sgmBytes.Range(chkEncStart, chkDataEnd)); err != nil {
		return nil, err
	}

	// 根据 CRC32 检测读取到的 Chunk 数据是否有效
	if act := chkCRC32.Sum(nil); !bytes.Equal(act, sum) {
		return nil, errors.Errorf("checksum mismatch expected:%x, actual:%x", sum, act)
	}

	// 读取 ref 对应的时序数据
	chkData := sgmBytes.Range(chkDataStart, chkDataEnd)
	chkEnc := sgmBytes.Range(chkEncStart, chkEncStart+ChunkEncodingSize)[0] // 获取时序数据对应的编码类型
	// 从 Chunk 池中获取一个空闲的 Chunk 实例, 并将 Encoding 方式以及时序数据填充进去
	return s.pool.Get(chunkenc.Encoding(chkEnc), chkData)
}

// nextSequenceFile 计算下一个 segment 文件的名称
func nextSequenceFile(dir string) (string, int, error) {
	names, err := fileutil.ReadDir(dir)
	if err != nil {
		return "", 0, err
	}

	i := uint64(0)
	for _, n := range names {
		j, err := strconv.ParseUint(n, 10, 64)
		if err != nil {
			continue
		}
		i = j
	}
	return filepath.Join(dir, fmt.Sprintf("%0.6d", i+1)), int(i + 1), nil
}

func sequenceFiles(dir string) ([]string, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var res []string

	for _, fi := range files {
		if _, err := strconv.ParseUint(fi.Name(), 10, 64); err != nil {
			continue
		}
		res = append(res, filepath.Join(dir, fi.Name()))
	}
	return res, nil
}

func closeAll(cs []io.Closer) error {
	var merr tsdb_errors.MultiError

	for _, c := range cs {
		merr.Add(c.Close())
	}
	return merr.Err()
}
