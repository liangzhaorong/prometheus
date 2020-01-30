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

// The code in this file was largely written by Damian Gryski as part of
// https://github.com/dgryski/go-tsz and published under the license below.
// It was modified to accommodate reading from byte slices without modifying
// the underlying bytes, which would panic when reading from mmap'd
// read-only byte slices.

// Copyright (c) 2015,2016 Damian Gryski <damian@gryski.com>
// All rights reserved.

// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:

// * Redistributions of source code must retain the above copyright notice,
// this list of conditions and the following disclaimer.
//
// * Redistributions in binary form must reproduce the above copyright notice,
// this list of conditions and the following disclaimer in the documentation
// and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
// CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
// OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package chunkenc

import (
	"encoding/binary"
	"math"
	"math/bits"
)

// XORChunk 是 Prometheus TSDB 实现中 Chunk 接口的唯一实现
// XORChunk holds XOR encoded sample data.
type XORChunk struct {
	b bstream // 存储时序数据
}

// NewXORChunk returns a new chunk with XOR encoding of the given size.
func NewXORChunk() *XORChunk {
	b := make([]byte, 2, 128)
	return &XORChunk{b: bstream{stream: b, count: 0}}
}

// Encoding returns the encoding type.
func (c *XORChunk) Encoding() Encoding {
	return EncXOR
}

// Bytes returns the underlying byte slice of the chunk.
func (c *XORChunk) Bytes() []byte {
	return c.b.bytes()
}

// NumSamples returns the number of samples in the chunk.
func (c *XORChunk) NumSamples() int {
	return int(binary.BigEndian.Uint16(c.Bytes()))
}

// Appender implements the Chunk interface.
func (c *XORChunk) Appender() (Appender, error) {
	it := c.iterator(nil) // 创建 xorIterator 迭代器

	// To get an appender we must know the state it would have if we had
	// appended all existing data from scratch.
	// We iterate through the end and populate via the iterator's state.
	for it.Next() { // 迭代 XORChunk 中已有的全部时序点, 直至结束, 这样才能得到可以写入的全部状态
	}
	if err := it.Err(); err != nil {
		return nil, err
	}

	a := &xorAppender{ // 根据 xorIterator 迭代器的状态创建 xorAppender 实例
		b:        &c.b,
		t:        it.t,
		v:        it.val,
		tDelta:   it.tDelta,
		leading:  it.leading,
		trailing: it.trailing,
	}
	if binary.BigEndian.Uint16(a.b.bytes()) == 0 { // 如果是空的 XORChunk, 则初始化 leading
		a.leading = 0xff
	}
	return a, nil
}

// iterator 创建 xorIterator 实例, 该实例用于迭代 XORChunk 中的时序点
func (c *XORChunk) iterator(it Iterator) *xorIterator {
	// Should iterators guarantee to act on a copy of the data so it doesn't lock append?
	// When using striped locks to guard access to chunks, probably yes.
	// Could only copy data if the chunk is not completed yet.
	if xorIter, ok := it.(*xorIterator); ok {
		xorIter.Reset(c.b.bytes())
		return xorIter
	}
	return &xorIterator{
		// The first 2 bytes contain chunk headers.
		// We skip that for actual samples.
		// 因为 bstream 中前两个 byte 元素存储的是 XORChunk 中时序点的个数, 所以这里跳过
		br: newBReader(c.b.bytes()[2:]),
		// 读取 bstream 中的前两个 byte 元素, 获取 XORChunk 实例中存储的时序点的个数
		numTotal: binary.BigEndian.Uint16(c.b.bytes()),
	}
}

// Iterator implements the Chunk interface.
func (c *XORChunk) Iterator(it Iterator) Iterator {
	return c.iterator(it)
}

// xorAppender 为 Appender 接口的实现
type xorAppender struct {
	b *bstream // bstream 实例, 存储写入的时序点数据

	t      int64   // 记录上次写入时序点对应的 timestamp
	v      float64 // 记录上次写入时序点对应的 value 值
	tDelta uint64  // 记录当前点与前一个点的 timestamp 差值

	leading  uint8 // 记录当前 XOR 运算结果中前置 "0" 的个数
	trailing uint8 // 记录当前 XOR 运算结果中后置 "0" 的个数
}

// 参数为待写入时序点的 timestamp 和 value 值
// 这里会按照 delta-of-delta 时间戳压缩方式存储 timestamp
// 按照 XOR 压缩方式存储 value 值
func (a *xorAppender) Append(t int64, v float64) {
	var tDelta uint64
	// XORChunk 会使用 bstream 中前两个 byte 记录写入的时序点的个数, 这里就是读取该值
	num := binary.BigEndian.Uint16(a.b.bytes())

	if num == 0 { // XORChunk 中需要完整记录第一个点的 timestamp 和 value 值
		buf := make([]byte, binary.MaxVarintLen64)         // 创建一个足够存储 timestamp 的 byte 切片
		for _, b := range buf[:binary.PutVarint(buf, t)] { // 将 timestamp 完整写入 bstream 中
			a.b.writeByte(b)
		}
		a.b.writeBits(math.Float64bits(v), 64) // 将第一个时序点的 value 值写入 bstream 中

	} else if num == 1 { // 根据 num 判断, 此次写入的是第二个时序点
		tDelta = uint64(t - a.t) // 计算该点与前一个时序点的 timestamp 差值

		// 下面将当前时序点与前一个时序点的 timestamp 差值写入 bstream 中
		buf := make([]byte, binary.MaxVarintLen64)
		for _, b := range buf[:binary.PutUvarint(buf, tDelta)] {
			a.b.writeByte(b)
		}

		// 计算该时序点与前一个时序点 value 值的 XOR 值, 并按照 XOR 压缩方式记录到 bstream 中
		a.writeVDelta(v)

	} else { // 根据 num 判断, 写入第三个以及之后的时序点
		tDelta = uint64(t - a.t)        // 计算该时序点与前一个时序点的 timestamp 差值
		dod := int64(tDelta - a.tDelta) // 计算两个 timestamp 的 dod(delta-of-delta) 值

		// Gorilla has a max resolution of seconds, Prometheus milliseconds.
		// Thus we use higher value range steps with larger bit size.
		switch {
		case dod == 0: // 如果 dod 差值为 0, 则只需要记录一个值为 "0" 的 bit 值
			a.b.writeBit(zero)
		case bitRange(dod, 14):
			// 如果 dod 值在 [-8191, 8192] 范围中, 则使用 "10" 作为标识, 然后使用 14bit 存储 dod 值
			a.b.writeBits(0x02, 2) // '10'
			a.b.writeBits(uint64(dod), 14)
		case bitRange(dod, 17):
			// 如果 dod 值在 [-65535, 65536] 范围中, 则使用 "110" 作为标识, 然后使用 17bit 存储 dod 值
			a.b.writeBits(0x06, 3) // '110'
			a.b.writeBits(uint64(dod), 17)
		case bitRange(dod, 20):
			// 如果 dod 值在 [-524287, 524288] 范围中, 则使用 "1110" 作为标识, 然后使用 20bit 存储 dod 值
			a.b.writeBits(0x0e, 4) // '1110'
			a.b.writeBits(uint64(dod), 20)
		default:
			// 如果 dod 值超出了上述范围, 则使用 "1111" 作为标识, 然后使用 64bit 存储 dod 值
			a.b.writeBits(0x0f, 4) // '1111'
			a.b.writeBits(uint64(dod), 64)
		}

		a.writeVDelta(v) // 计算当前时序点与前一个时序点 value 值的 XOR, 并记录到 bstream 中
	}

	a.t = t // 更新 t、v 字段, 记录当前时序点的 timestamp 和 value 值, 为下一个时序点的写入做准备
	a.v = v
	binary.BigEndian.PutUint16(a.b.bytes(), num+1) // 更新该 XORChunk 已写入的点的个数
	// 记录当前时序点与前一个时序点的 timestamp 差值, 为下次计算 dod 值做准备
	a.tDelta = tDelta
}

func bitRange(x int64, nbits uint8) bool {
	return -((1<<(nbits-1))-1) <= x && x <= 1<<(nbits-1)
}

// writeVDelta 计算当前时序点的 value 值与前一个时序点的 value 值的 XOR 值,
// 并根据 XOR 运算结果中前置 "0" 和后置 "0" 的个数进行相应的压缩存储
func (a *xorAppender) writeVDelta(v float64) {
	// 计算当前时序点的 value 值与前一个时序点的 value 值之间的 XOR 值
	vDelta := math.Float64bits(v) ^ math.Float64bits(a.v)

	if vDelta == 0 { // 如果两个时序点的 value 值相同, 则只写入一个值为 "0" 的 bit
		a.b.writeBit(zero)
		return
	}
	a.b.writeBit(one) // 写入控制位的第 1bit, 该位的值为 "1"

	leading := uint8(bits.LeadingZeros64(vDelta))   // 返回 vDelta 中前置 "0" 的个数
	trailing := uint8(bits.TrailingZeros64(vDelta)) // 返回 vDelta 中后置 "0" 的个数

	// Clamp number of leading zeros to avoid overflow when encoding.
	if leading >= 32 {
		leading = 31
	}

	// 该 vDelta 值的前置 "0" 和后置 "0" 的个数都比上一次写入得到的 XOR 值多
	if a.leading != 0xff && leading >= a.leading && trailing >= a.trailing {
		a.b.writeBit(zero) // 写入控制位的第 2bit, 该位的值为 "0"
		// 这里只需要记录去除前置 "0" 和后置 "0" 的部分即可
		a.b.writeBits(vDelta>>a.trailing, 64-int(a.leading)-int(a.trailing))
	} else { // 该 vDelta 值的前置 "0" 或后置 "0" 比上一次写入得到的 XOR 值少
		// 更新 xorAppender 的 leading 和 trailing 字段, 分别记录此次写入时得到的 XOR 值中
		// 前置 "0" 和后置 "0" 个数, 这主要是为下一个时序点的写入做准备
		a.leading, a.trailing = leading, trailing

		a.b.writeBit(one)                 // 写入控制位的第 2bit, 该位的值为 "1"
		a.b.writeBits(uint64(leading), 5) // 用 5bit 来存储 XOR 中前置 "0" 的个数

		// Note that if leading == trailing == 0, then sigbits == 64.  But that value doesn't actually fit into the 6 bits we have.
		// Luckily, we never need to encode 0 significant bits, since that would put us in the other case (vdelta == 0).
		// So instead we write out a 0 and adjust it back to 64 on unpacking.
		sigbits := 64 - leading - trailing
		a.b.writeBits(uint64(sigbits), 6)             // 用 6bit 来存储 XOR 值中间非 0 位的长度
		a.b.writeBits(vDelta>>trailing, int(sigbits)) // 存储中间非 0 位的值
	}
}

type xorIterator struct {
	br       bstream // 关联 XORChunk 实例中的 b 字段, 存储了 XORChunk 实例中的时序数据
	numTotal uint16  // 关联 XORChunk 中存储的时序点的个数
	numRead  uint16  // 通过该 xorIterator 实例读取的时序点的个数

	t   int64   // 当前读取的时序点的 timestamp
	val float64 // 当前读取的时序点的 value 值

	leading  uint8 // 当前读取到的 XOR 值的前置 "0" 个数
	trailing uint8 // 当前读取到的 XOR 值的后置 "0" 个数

	tDelta uint64 // 记录当前时序点与前一个时序点的 timestamp 的差值
	err    error
}

func (it *xorIterator) At() (int64, float64) {
	return it.t, it.val
}

func (it *xorIterator) Err() error {
	return it.err
}

func (it *xorIterator) Reset(b []byte) {
	// The first 2 bytes contain chunk headers.
	// We skip that for actual samples.
	it.br = newBReader(b[2:])
	it.numTotal = binary.BigEndian.Uint16(b)

	it.numRead = 0
	it.t = 0
	it.val = 0
	it.leading = 0
	it.trailing = 0
	it.tDelta = 0
	it.err = nil
}

// Next 会根据当前读取的是第几个时序点来决定如何返回正确的 timestamp 和 value 值
func (it *xorIterator) Next() bool {
	// 检测迭代过程中是否出现异常, 如果出现异常, 则返回 false, 并终止整个迭代过程
	if it.err != nil || it.numRead == it.numTotal {
		return false
	}

	if it.numRead == 0 { // 读取 XORChunk 实例中的第一个时序点
		t, err := binary.ReadVarint(&it.br) // 从 bstream 中读取第一个时序点的完整 timestamp
		if err != nil {
			it.err = err
			return false
		}
		v, err := it.br.readBits(64) // 从 bstream 中读取第一个时序点的完整 value 值
		if err != nil {
			it.err = err
			return false
		}
		it.t = t // 更新 t、val 字段, 记录当前时序点的 timestamp 和 value 值, 在 At() 方法中会返回这两个值
		it.val = math.Float64frombits(v)

		it.numRead++ // 递增已读取的时序点的个数
		return true
	}
	if it.numRead == 1 { // 读取 XORChunk 实例中的第二个时序点
		// 从 bstream 中读取第二个点与第一个点的 timestamp 差值
		tDelta, err := binary.ReadUvarint(&it.br)
		if err != nil {
			it.err = err
			return false
		}
		it.tDelta = tDelta             // 更新 tDelta 字段, 记录 timestamp 差值
		it.t = it.t + int64(it.tDelta) // 计算第二个时序点对应的 timestamp 值

		// 读取第二个时序点的 value 值
		return it.readValue()
	}
	// 读取 XORChunk 实例中的第三个时序点以及之后的时序点时, 执行如下逻辑

	var d byte
	// read delta-of-delta
	for i := 0; i < 4; i++ { // 首先读取标识位
		d <<= 1 // 将 d 左移一位, 为读取下一位做准备
		bit, err := it.br.readBit()
		if err != nil {
			it.err = err
			return false
		}
		if bit == zero { // 如果在读取标识位的过程中遇到 "0" 位, 则表示标识位已经读取结束
			break
		}
		d |= 1 // 该 bit 位不为 "0", 则将对应 bit 位设置为 1
	}
	var sz uint8 // 后续需要读取多少个 bit 位, 才能得到 dod(delta-of-delta) 值
	var dod int64
	switch d {
	case 0x00: // 如果标识位为 "0", 则表示时间戳的 dod(delta-of-delta) 值为 0
		// dod == 0
	case 0x02: // 如果标识位为 "10", 则表示时间戳的 dod 值在 [-8191,8192] 范围中, 需要读取 14bit
		sz = 14
	case 0x06: // 如果标识位为 "110", 则表示时间戳的 dod 值在 [-65535,65536] 范围中, 需要读取 17bit
		sz = 17
	case 0x0e: // 如果标识位为 "1110", 则表示时间戳的 dod 值在 [-524287,524288] 范围中, 需要读取 20bit
		sz = 20
	case 0x0f: // 如果标识位为 "1111", 则表示时间戳的 dod 值超出了上述范围, 需要读取 64bit
		bits, err := it.br.readBits(64)
		if err != nil {
			it.err = err
			return false
		}

		dod = int64(bits)
	}

	if sz != 0 { // 如果标识位为 "10" "110" "1110", 则读取指定数量的 bit, 获得 dod 值
		bits, err := it.br.readBits(int(sz))
		if err != nil {
			it.err = err
			return false
		}
		if bits > (1 << (sz - 1)) {
			// or something
			bits = bits - (1 << sz)
		}
		dod = int64(bits)
	}

	it.tDelta = uint64(int64(it.tDelta) + dod) // 计算两个点的时间戳的差值
	it.t = it.t + int64(it.tDelta)             // 根据上一点的时间戳计算当前点的时间戳

	return it.readValue() // 读取当前时序点的 value 值
}

func (it *xorIterator) readValue() bool {
	bit, err := it.br.readBit() // 读取控制位的第 1bit
	if err != nil {
		it.err = err
		return false
	}

	if bit == zero { // 如果控制位的第 1bit 为 "0", 则表示当前时序点的 value 值与前一个点的 value 值相同
		// it.val = it.val
	} else {
		bit, err := it.br.readBit() // 如果控制位的第 1bit 为 "1", 则需要读取第二个控制位
		if err != nil {
			it.err = err
			return false
		}
		if bit == zero {
			// 控制位为 "10", 则表示可以直接读取 XOR 值的中间非 0 部分(因为其前置 "0" 和后置 "0"
			// 与前一个 XOR 结果的个数相同)
			// reuse leading/trailing zero bits
			// it.leading, it.trailing = it.leading, it.trailing
		} else { // 控制位为 "11", 则表示 XOR 结果中前置 "0" 与后置 "0" 与前一个 XOR 值的个数相同
			bits, err := it.br.readBits(5) // 读取 XOR 结果中前置 "0" 的个数(5bit)
			if err != nil {
				it.err = err
				return false
			}
			it.leading = uint8(bits) // 更新 leading 字段, 记录前置 "0" 的个数

			bits, err = it.br.readBits(6) // 读取 XOR 结果中非 0 部分的长度(6bit)
			if err != nil {
				it.err = err
				return false
			}
			mbits := uint8(bits)
			// 0 significant bits here means we overflowed and we actually need 64; see comment in encoder
			if mbits == 0 {
				mbits = 64
			}
			it.trailing = 64 - it.leading - mbits // 计算 XOR 结果中后置 "0" 的个数
		}

		mbits := int(64 - it.leading - it.trailing) // 计算 XOR 值中非 0 部分的位数
		bits, err := it.br.readBits(mbits)          // 读取 XOR 结果中的非 0 部分
		if err != nil {
			it.err = err
			return false
		}
		// 根据前一个时序点的 value 值以及 XOR 值, 得到当前点的 value 值
		vbits := math.Float64bits(it.val)
		vbits ^= (bits << it.trailing)
		it.val = math.Float64frombits(vbits) // 更新 val 字段
	}

	it.numRead++ // 此次读取完成, 递增 numRead 字段
	return true
}
