// Protocol Buffers - Google's data interchange format
// Copyright 2008 Google Inc.  All rights reserved.
// https://developers.google.com/protocol-buffers/
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

/**
 * @fileoverview varint helpers directly inferred from Google's Protocol Buffers library.
 */

const TWO_TO_32 = 4294967296;

/**
 * Decodes a varint, but limited to 32 bits. Throws otherwise.
 */
export function readVarint32(readByte: () => number): number {
  let temp = readByte();
  let x = temp & 0x7f;
  if (temp < 128) {
    return x;
  }

  temp = readByte();
  x |= (temp & 0x7f) << 7;
  if (temp < 128) {
    return x;
  }

  temp = readByte();
  x |= (temp & 0x7f) << 14;
  if (temp < 128) {
    return x;
  }

  temp = readByte();
  x |= (temp & 0x7f) << 21;
  if (temp < 128) {
    return x;
  }

  temp = readByte();
  x |= (temp & 0x0f) << 28;
  if (temp < 128) {
    // We're reading the high bits of an unsigned varint. The byte we just read
    // also contains bits 33 through 35, which we're going to discard.
    return x >>> 0;
  }
  // If we get here, we need to truncate coming bytes. However we need to make
  // sure cursor place is correct.
  if (
    readByte() >= 128 &&
    readByte() >= 128 &&
    readByte() >= 128 &&
    readByte() >= 128 &&
    readByte() >= 128
  ) {
    // If we get here, the varint is too long.
    throw new TypeError(`Too much data for int32`);
  }

  return x;
}

/**
 * Returns a zigzag varint but probably throws at >53 bits (i.e., {@link Number.MAX_SAFE_INTEGER}).
 */
export function readZigZagVarint53(readByte: () => number): number {
  let { low, high } = readSplitVarint64(readByte);

  // 64 bit math is:
  //   signmask = (zigzag & 1) ? -1 : 0;
  //   twosComplement = (zigzag >> 1) ^ signmask;
  //
  // To work with 32 bit, we can operate on both but "carry" the lowest bit
  // from the high word by shifting it up 31 bits to be the most significant bit
  // of the low word.
  const signFlipMask = -(low & 1);
  low = ((low >>> 1) | (high << 31)) ^ signFlipMask;
  high = (high >>> 1) ^ signFlipMask;

  return joinInt64(low, high);
}

export function readZigZagVarint32(readByte: () => number): number {
  const n = readVarint32(readByte);
  return (n >>> 1) ^ (-1 * (n & 1));
}

/**
 * Decodes a varint32, returning the value and number of bytes consumed.
 */
export function decodeVarint32(source: Uint8Array, offset = 0): { value: number; size: number } {
  let size = 0;
  const readByte = () => {
    const byte = source[offset + size];
    ++size;
    return byte;
  };
  const value = readVarint32(readByte);
  return { value, size };
}

function joinInt64(bitsLow: number, bitsHigh: number) {
  // If the high bit is set, do a manual two's complement conversion.
  const sign = (bitsHigh & 0x80000000);
  if (sign) {
    bitsLow = (~bitsLow + 1) >>> 0;
    bitsHigh = ~bitsHigh >>> 0;
    if (bitsLow == 0) {
      bitsHigh = (bitsHigh + 1) >>> 0;
    }
  }

  if (bitsHigh & 0x3ff80000) {
    throw new RangeError(`got int > 53 bits of data`);
  }

  const join = bitsHigh * TWO_TO_32 + (bitsLow >>> 0);
  return sign ? -join : join;
}

function readSplitVarint64(readByte: () => number) {
  let temp = 128;
  let lowBits = 0;
  let highBits = 0;
  // Read the first four bytes of the varint, stopping at the terminator if we
  // see it.
  for (let i = 0; i < 4 && temp >= 128; i++) {
    temp = readByte();
    lowBits |= (temp & 0x7F) << (i * 7);
  }
  if (temp >= 128) {
    // Read the fifth byte, which straddles the low and high dwords.
    temp = readByte();
    lowBits |= (temp & 0x7F) << 28;
    highBits |= (temp & 0x7F) >> 4;
  }
  if (temp >= 128) {
    // Read the sixth through tenth byte.
    for (let i = 0; i < 5 && temp >= 128; i++) {
      temp = readByte();
      highBits |= (temp & 0x7F) << (i * 7 + 3);
    }
  }
  if (temp >= 128) {
    throw new TypeError(`Invalid encoding`)
  }

  return { low: lowBits >>> 0, high: highBits >>> 0 };
};