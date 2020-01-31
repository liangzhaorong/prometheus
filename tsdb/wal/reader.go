// Copyright 2019 The Prometheus Authors
//
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
	"encoding/binary"
	"hash/crc32"
	"io"

	"github.com/golang/snappy"
	"github.com/pkg/errors"
)

// Reader 通过其中封装的 io.Reader 实例读取数据, 而是否跨越多个 Segment 文件进行数据
// 读取由其中封装的 io.Reader 决定.
// Reader reads WAL records from an io.Reader.
type Reader struct {
	rdr       io.Reader // 底层真正读取数据的 io.Reader 实例
	err       error
	rec       []byte // 读取 Reader 的缓冲区, 会被循环使用
	snappyBuf []byte
	buf       [pageSize]byte // 读取 page 的缓冲区, 会被循环使用
	total     int64          // Total bytes processed. 已读取的总字节数
	curRecTyp recType        // Used for checking that the last record is not torn.
}

// NewReader returns a new reader.
func NewReader(r io.Reader) *Reader {
	return &Reader{rdr: r}
}

// Next advances the reader to the next records and returns true if it exists.
// It must not be called again after it returned false.
func (r *Reader) Next() bool {
	err := r.next()
	if errors.Cause(err) == io.EOF {
		// The last WAL segment record shouldn't be torn(should be full or last).
		// The last record would be torn after a crash just before
		// the last record part could be persisted to disk.
		if r.curRecTyp == recFirst || r.curRecTyp == recMiddle {
			r.err = errors.New("last record is torn")
		}
		return false
	}
	r.err = err
	return r.err == nil
}

func (r *Reader) next() (err error) {
	// We have to use r.buf since allocating byte arrays here fails escape
	// analysis and ends up on the heap, even though it seemingly should not.
	hdr := r.buf[:recordHeaderSize] // 用于记录 Record 的头信息
	buf := r.buf[recordHeaderSize:] // 用于记录 Record 的数据

	r.rec = r.rec[:0]
	r.snappyBuf = r.snappyBuf[:0]

	i := 0
	for {
		// 从当前 Segment 文件中读取第一个字节
		if _, err = io.ReadFull(r.rdr, hdr[:1]); err != nil {
			return errors.Wrap(err, "read first header byte")
		}
		r.total++
		// Record 头信息的第一个字节是该 Record 的写入状态, 即前面写入过程中的 recType
		r.curRecTyp = recTypeFromHeader(hdr[0])
		compressed := hdr[0]&snappyMask != 0

		// Gobble up zero bytes.
		if r.curRecTyp == recPageTerm { // 若为 recPageTerm 类型, 则表示当前的 page 已经读取完
			// recPageTerm is a single byte that indicates the rest of the page is padded.
			// If it's the first byte in a page, buf is too small and
			// needs to be resized to fit pageSize-1 bytes.
			buf = r.buf[1:]

			// We are pedantic and check whether the zeros are actually up
			// to a page boundary.
			// It's not strictly necessary but may catch sketchy state early.
			k := pageSize - (r.total % pageSize) // 当前 page 已读取的字节数
			if k == pageSize {                   // 正好读取到当前 page 的最后一个字节
				continue // Initial 0 byte was last page byte.
			}
			// 如果未读取到当前 page 的末尾, 则将剩余字节全部读取出来
			n, err := io.ReadFull(r.rdr, buf[:k])
			if err != nil {
				return errors.Wrap(err, "read remaining zeros")
			}
			r.total += int64(n)

			// 检测当前 page 中是否都为空字节, 如果不是, 则说明当前 page 中的数据存在异常, 会抛出异常
			for _, c := range buf[:k] {
				if c != 0 {
					return errors.New("unexpected non-zero byte in padded page")
				}
			}
			continue
		}
		n, err := io.ReadFull(r.rdr, hdr[1:]) // 读取 Record 头信息中剩余的字节
		if err != nil {
			return errors.Wrap(err, "read remaining header")
		}
		r.total += int64(n) // 统计已读取到的字节数

		var (
			length = binary.BigEndian.Uint16(hdr[1:]) // 从头信息中获取整个 Record 的长度
			crc    = binary.BigEndian.Uint32(hdr[3:]) // 从头信息中获取该 Record 的 CRC32 校验码
		)

		if length > pageSize-recordHeaderSize {
			return errors.Errorf("invalid record size %d", length)
		}
		n, err = io.ReadFull(r.rdr, buf[:length]) // 读取当前的 Record 数据
		if err != nil {
			return err
		}
		r.total += int64(n) // 更新 total, 记录已读取到的字节数

		// 检测 Record 长度以及 CRC32 校验码是否正确
		if n != int(length) {
			return errors.Errorf("invalid size: expected %d, got %d", length, n)
		}
		if c := crc32.Checksum(buf[:length], castagnoliTable); c != crc {
			return errors.Errorf("unexpected checksum %x, expected %x", c, crc)
		}

		if compressed {
			r.snappyBuf = append(r.snappyBuf, buf[:length]...)
		} else {
			r.rec = append(r.rec, buf[:length]...) // 将获取的 Record 数据记录到 Recoder.rec 字段
		}

		if err := validateRecord(r.curRecTyp, i); err != nil {
			return err
		}
		// 若为 recLast, 则表示多次读取后, 最终读取到一个完整的 Record
		if r.curRecTyp == recLast || r.curRecTyp == recFull {
			if compressed && len(r.snappyBuf) > 0 {
				// The snappy library uses `len` to calculate if we need a new buffer.
				// In order to allocate as few buffers as possible make the length
				// equal to the capacity.
				r.rec = r.rec[:cap(r.rec)]
				r.rec, err = snappy.Decode(r.rec, r.snappyBuf)
				return err
			}
			return nil
		}

		// Only increment i for non-zero records since we use it
		// to determine valid content record sequences.
		i++ // 未读取到一个完整的 Record, 则会继续进行读取
	}
}

// Err returns the last encountered error wrapped in a corruption error.
// If the reader does not allow to infer a segment index and offset, a total
// offset in the reader stream will be provided.
func (r *Reader) Err() error {
	if r.err == nil {
		return nil
	}
	if b, ok := r.rdr.(*segmentBufReader); ok {
		return &CorruptionErr{
			Err:     r.err,
			Dir:     b.segs[b.cur].Dir(),
			Segment: b.segs[b.cur].Index(),
			Offset:  int64(b.off),
		}
	}
	return &CorruptionErr{
		Err:     r.err,
		Segment: -1,
		Offset:  r.total,
	}
}

// Record returns the current record. The returned byte slice is only
// valid until the next call to Next.
func (r *Reader) Record() []byte {
	return r.rec
}

// Segment returns the current segment being read.
func (r *Reader) Segment() int {
	if b, ok := r.rdr.(*segmentBufReader); ok {
		return b.segs[b.cur].Index()
	}
	return -1
}

// Offset returns the current position of the segment being read.
func (r *Reader) Offset() int64 {
	if b, ok := r.rdr.(*segmentBufReader); ok {
		return int64(b.off)
	}
	return r.total
}
