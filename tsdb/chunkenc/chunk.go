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

package chunkenc

import (
	"fmt"
	"sync"

	"github.com/pkg/errors"
)

// Encoding is the identifier for a chunk encoding.
type Encoding uint8

func (e Encoding) String() string {
	switch e {
	case EncNone:
		return "none"
	case EncXOR:
		return "XOR"
	}
	return "<unknown>"
}

// The different available chunk encodings.
const (
	EncNone Encoding = iota
	EncXOR
)

// 磁盘存储中的 block 的时序数据存储在 Chunk 文件里, Prometheus TSDB 中对应的则是 Chunk 接口, 它表示一组时序点的集合.
// Chunk holds a sequence of sample pairs that can be iterated over and appended to.
type Chunk interface {
	Bytes() []byte               // 存储时序点的 byte 切片, 通过 bstream 结构体完成读写
	Encoding() Encoding          // 编码类型, 目前只有 XOR 这一种编码类型
	Appender() (Appender, error) // 返回该 Chunk 关联的 Appender 实例
	// The iterator passed as argument is for re-use.
	// Depending on implementation, the iterator can
	// be re-used or a new iterator can be allocated.
	Iterator(Iterator) Iterator // 返回该 Chunk 关联的 Iterator 实例
	NumSamples() int            // 返回该 Chunk 中保存的时序点的个数
}

// Appender adds sample pairs to a chunk.
type Appender interface {
	// 用于向 Chunk 实例中追加一个时序点, 其接受的参数分别是时序点的 timestamp 和 value 值
	Append(int64, float64) // 每个时序点都是由对应的 timestmap 和 value 值组成的
}

// Iterator is a simple iterator that can only get the next value.
type Iterator interface { // 可通过 Iterator 接口迭代 Chunk 中存储的时序点
	At() (int64, float64) // 返回当前时序点的 timestamp 和 value 值
	Err() error           // 返回迭代过程中发生的异常
	Next() bool           // 检测后续是否有时序点可以继续迭代
}

// NewNopIterator returns a new chunk iterator that does not hold any data.
func NewNopIterator() Iterator {
	return nopIterator{}
}

type nopIterator struct{}

func (nopIterator) At() (int64, float64) { return 0, 0 }
func (nopIterator) Next() bool           { return false }
func (nopIterator) Err() error           { return nil }

// Pool is used to create and reuse chunk references to avoid allocations.
type Pool interface {
	Put(Chunk) error                         // 将 Chunk 实例放回到池中
	Get(e Encoding, b []byte) (Chunk, error) // 根据指定的 Encoding 从池中获取 chunk 实例
}

// pool is a memory pool of chunk objects.
type pool struct {
	xor sync.Pool
}

// NewPool returns a new pool.
func NewPool() Pool {
	return &pool{
		xor: sync.Pool{
			// 如果调用 Pool.Get() 方法从池中获取对象时没有可用的 Chunk 实例, 则会通过该函数
			// 创建新的 XORChunk 实例返回
			New: func() interface{} {
				return &XORChunk{b: bstream{}}
			},
		},
	}
}

func (p *pool) Get(e Encoding, b []byte) (Chunk, error) {
	switch e {
	case EncXOR:
		c := p.xor.Get().(*XORChunk) // 从 Pool 中获取 XORChunk 实例
		c.b.stream = b               // 填充 bstream
		c.b.count = 0
		return c, nil
	}
	return nil, errors.Errorf("invalid encoding %q", e)
}

func (p *pool) Put(c Chunk) error {
	switch c.Encoding() {
	case EncXOR:
		xc, ok := c.(*XORChunk) // 检测传入的 Chunk 实例的实际类型
		// This may happen often with wrapped chunks. Nothing we can really do about
		// it but returning an error would cause a lot of allocations again. Thus,
		// we just skip it.
		if !ok {
			return nil
		}
		xc.b.stream = nil // 清空 XORChunk 底层的 bstream
		xc.b.count = 0
		p.xor.Put(c) // 将 XORChunk 实例放入 Pool 中
	default:
		return errors.Errorf("invalid encoding %q", c.Encoding())
	}
	return nil
}

// FromData returns a chunk from a byte slice of chunk data.
// This is there so that users of the library can easily create chunks from
// bytes.
func FromData(e Encoding, d []byte) (Chunk, error) {
	switch e {
	case EncXOR:
		return &XORChunk{b: bstream{count: 0, stream: d}}, nil
	}
	return nil, fmt.Errorf("unknown chunk encoding: %d", e)
}
