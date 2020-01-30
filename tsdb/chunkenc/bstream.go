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
// It received minor modifications to suit Prometheus's needs.

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

import "io"

// bstream 是 Prometheus TSDB 中的基础组件之一, 它是对 byte 切片的封装, 提供了
// 读写位的功能, 主要用于读写时序数据.
// bstream is a stream of bits.
type bstream struct {
	// 用于记录数据的 byte 切片
	stream []byte // the data stream
	// 在写入数据时, 是逐个 byte 进行操作的, count 字段用来记录当前 byte 中有多少 bit 是可以写入的;
	// 在读取数据时, 表示当前 byte 中有多少 bit 是可读的. 也就是说, count 字段类似于控制写入/读取
	// 位置的下标
	count uint8 // how many bits are valid in current byte
}

func newBReader(b []byte) bstream {
	return bstream{stream: b, count: 8}
}

func (b *bstream) bytes() []byte {
	return b.stream
}

type bit bool

const (
	zero bit = false
	one  bit = true
)

func (b *bstream) writeBit(bit bit) {
	if b.count == 0 {
		// 当前 bstream 已经完整写完一个 byte, 需要向 stream 切片中追加新的 byte 元素来完成此次写入
		b.stream = append(b.stream, 0)
		b.count = 8
	}

	i := len(b.stream) - 1 // 最后一个 byte 元素的下标

	if bit { // 如果 bit 为 1, 则需要将该 byte 元素中对应的位设置为 1; 如果为 0, 则不需要设置
		b.stream[i] |= 1 << (b.count - 1)
	}

	b.count-- // 写入 1bit 后, 更新当前 byte 可用位的数量
}

func (b *bstream) writeByte(byt byte) {
	if b.count == 0 {
		// 当前 bstream 已经完整写完一个 byte, 需要向 stream 切片中追加新的 byte 元素来完成此次写入
		b.stream = append(b.stream, 0)
		b.count = 8
	}

	i := len(b.stream) - 1

	// fill up b.b with b.count bits from byt
	b.stream[i] |= byt >> (8 - b.count) // 在 stream 切片末尾写入 byt

	// 如果 stream 切片中最后一个 byte 元素剩余的位不足 8bit, 则需要再追加 1byte, 写入 byt 剩余的位
	b.stream = append(b.stream, 0)
	i++
	b.stream[i] = byt << b.count
}

func (b *bstream) writeBits(u uint64, nbits int) {
	u <<= (64 - uint(nbits))
	for nbits >= 8 { // 当该值所占的位数超过 8 时, 首先调用 writeByte 方法, 按照每 8 位 1byte 方式写入
		byt := byte(u >> 56)
		b.writeByte(byt)
		u <<= 8
		nbits -= 8
	}

	for nbits > 0 {
		b.writeBit((u >> 63) == 1)
		u <<= 1
		nbits--
	}
}

func (b *bstream) readBit() (bit, error) {
	// 检测当前 stream 是否为空
	if len(b.stream) == 0 {
		return false, io.EOF
	}

	if b.count == 0 { // count 为 0 表示 stream 切片中第一个 byte 元素已读取完毕
		b.stream = b.stream[1:] // 截掉第一个 byte 元素

		if len(b.stream) == 0 { // 重新检测 stream 是否为空
			return false, io.EOF
		}
		b.count = 8 // 将 count 重置为 8, 表示有当前 byte 有 8bit 可读
	}

	d := (b.stream[0] << (8 - b.count)) & 0x80 // 读取第一个 bit 位
	b.count--                                  // 递减 count 字段, 表示该 byte 中剩余可读取的 bit 数
	return d != 0, nil
}

func (b *bstream) ReadByte() (byte, error) {
	return b.readByte()
}

func (b *bstream) readByte() (byte, error) {
	// 检测当前 stream 切片是否为空
	if len(b.stream) == 0 {
		return 0, io.EOF
	}

	if b.count == 0 { // count 为 0 表示当前 stream 切片中第一个元素已经被读取完毕
		b.stream = b.stream[1:] // 截掉 stream 切片中的第一个 byte 元素

		if len(b.stream) == 0 { // 重新检测 stream 是否为空
			return 0, io.EOF
		}
		return b.stream[0], nil // 返回 stream 切片中第一个元素
	}

	if b.count == 8 { // count 为 8 表示读取 stream 切片中的第一个元素
		b.count = 0 // 将 count 更新为 0
		return b.stream[0], nil
	}

	// 如果 count 不等于 0 或 8, 则此次读取的 8bit 需要跨两个 byte 元素
	byt := b.stream[0] << (8 - b.count) // 从第一个 byte 元素中读取剩余可读取的 bit
	b.stream = b.stream[1:]             // 截掉 stream 切片中的第一个 byte 元素

	// 检测 stream 切片是否为空
	if len(b.stream) == 0 {
		return 0, io.EOF
	}

	// We just advanced the stream and can assume the shift to be 0.
	byt |= b.stream[0] >> b.count // 截断之后, 再次读取 strream 中的第一个 byte 元素, 凑齐 8bit

	return byt, nil
}

func (b *bstream) readBits(nbits int) (uint64, error) {
	var u uint64

	for nbits >= 8 { // 当读取的 bit 个数超过 8 时, 调用 readByte 方法按照 byte 进行读取
		byt, err := b.readByte()
		if err != nil {
			return 0, err
		}

		u = (u << 8) | uint64(byt)
		nbits -= 8
	}

	if nbits == 0 {
		return u, nil
	}

	if nbits > int(b.count) {
		u = (u << uint(b.count)) | uint64((b.stream[0]<<(8-b.count))>>(8-b.count))
		nbits -= int(b.count)
		b.stream = b.stream[1:]
		if len(b.stream) == 0 {
			return 0, io.EOF
		}
		b.count = 8
	}

	u = (u << uint(nbits)) | uint64((b.stream[0]<<(8-b.count))>>(8-uint(nbits)))
	b.count -= uint8(nbits)
	return u, nil
}
