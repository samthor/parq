import { decodeVarint32 } from '../varint.js';

const fromRightMask = [
  0b11111111, 0b01111111, 0b00111111, 0b00011111, 0b00001111, 0b00000111, 0b00000011, 0b00000001,
];

export type IntArray = Int8Array | Int16Array | Int32Array;

/**
 * Decode a bitpacked run by shifting within u8's.
 *
 * TODO: it might be faster in reverse order (it's big-endian encoded)
 */
function decodeRunBitpacked_shifter(
  arr: Uint8Array,
  offset: number,
  count8: number,
  typeLength: number,
  target: IntArray,
  targetOffset: number,
): number {
  let bitOffset = 0;
  const byteLength = typeLength * count8;
  const finalOffset = offset + byteLength;

  let haveBits = 0;
  let value = 0;

  while (offset < finalOffset) {
    const hadBits = haveBits;

    const byte = arr[offset];
    let rightPart = byte >>> bitOffset;
    haveBits += 8 - bitOffset;

    // Do we have enough bits?
    if (haveBits >= typeLength) {
      // Drop any excess bits in this byte's value.
      const excessBits = haveBits - typeLength;
      rightPart &= fromRightMask[excessBits];

      // The final part goes on the far left of the current accumulated value.
      value |= rightPart << hadBits;

      target[targetOffset] = value;
      ++targetOffset;
      haveBits = 0;
      value = 0;

      if (excessBits === 0) {
        ++offset;
        bitOffset = 0;
      } else {
        bitOffset = 8 - excessBits;
      }
      continue;
    }

    // We don't have enough bits, accumulate.
    // This is separate to above because we DON'T have excess bits here, so no need to mask.
    value |= rightPart << hadBits;

    bitOffset = 0;
    ++offset;
  }

  return finalOffset;
}

/**
 * Decode a bitpacked run of values.
 *
 * This is faster than the naïve solution but not than the bit shifting approach. Left for history.
 */
function decodeRunBitpacked_u8(
  arr: Uint8Array,
  offset: number,
  count8: number,
  typeLength: number,
  target: Int8Array | Int16Array | Int32Array,
  targetOffset: number,
): number {
  const byteLength = typeLength * count8;
  let bc = 0;
  let targetValue = 0;

  for (let b8 = 0; b8 < byteLength; ++b8) {
    const sourceValue = arr[offset];
    ++offset;

    // bit 1
    if (sourceValue & 1) {
      targetValue |= 1 << bc;
    }
    if (++bc === typeLength) {
      bc = 0;
      target[targetOffset] = targetValue;
      targetValue = 0;
      ++targetOffset;
    }

    // bit 2
    if (sourceValue & 2) {
      targetValue |= 1 << bc;
    }
    if (++bc === typeLength) {
      bc = 0;
      target[targetOffset] = targetValue;
      targetValue = 0;
      ++targetOffset;
    }

    // bit 3
    if (sourceValue & 4) {
      targetValue |= 1 << bc;
    }
    if (++bc === typeLength) {
      bc = 0;
      target[targetOffset] = targetValue;
      targetValue = 0;
      ++targetOffset;
    }

    // bit 4
    if (sourceValue & 8) {
      targetValue |= 1 << bc;
    }
    if (++bc === typeLength) {
      bc = 0;
      target[targetOffset] = targetValue;
      targetValue = 0;
      ++targetOffset;
    }

    // bit 5
    if (sourceValue & 16) {
      targetValue |= 1 << bc;
    }
    if (++bc === typeLength) {
      bc = 0;
      target[targetOffset] = targetValue;
      targetValue = 0;
      ++targetOffset;
    }

    // bit 6
    if (sourceValue & 32) {
      targetValue |= 1 << bc;
    }
    if (++bc === typeLength) {
      bc = 0;
      target[targetOffset] = targetValue;
      targetValue = 0;
      ++targetOffset;
    }

    // bit 7
    if (sourceValue & 64) {
      targetValue |= 1 << bc;
    }
    if (++bc === typeLength) {
      bc = 0;
      target[targetOffset] = targetValue;
      targetValue = 0;
      ++targetOffset;
    }

    // bit 8
    if (sourceValue & 128) {
      targetValue |= 1 << bc;
    }
    if (++bc === typeLength) {
      bc = 0;
      target[targetOffset] = targetValue;
      targetValue = 0;
      ++targetOffset;
    }
  }

  target[targetOffset] = targetValue;
  return offset;
}

function readRunRepeated_u8(arr: Uint8Array, offset: number): number {
  return arr[offset];
}

function readRunRepeated_u16(arr: Uint8Array, offset: number): number {
  return (arr[offset + 1] << 8) + arr[offset];
}

function readRunRepeated_u24(arr: Uint8Array, offset: number): number {
  return (arr[offset + 2] << 16) + (arr[offset + 1] << 8) + arr[offset];
}

function readRunRepeated_u32(arr: Uint8Array, offset: number): number {
  return (arr[offset + 3] << 24) + (arr[offset + 2] << 16) + (arr[offset + 1] << 8) + arr[offset];
}

function buildOut(typeLength: number, count: number): IntArray {
  if (typeLength <= 8) {
    return new Int8Array(count);
  } else if (typeLength <= 16) {
    return new Int16Array(count);
  } else if (typeLength <= 32) {
    return new Int32Array(count);
  } else {
    throw new Error(`TODO: RLE does not support 64-bit values yet`);
  }
}

const readRunRepeatedTable = [
  undefined,
  readRunRepeated_u8,
  readRunRepeated_u16,
  // TODO: 3-byte may not exist; need to find in the wild
  readRunRepeated_u24,
  readRunRepeated_u32,
];

const throwReadRunRepeated = () => {
  throw new Error(`Invalid typeLength`);
};

/**
 * Traverse a RLE-encoded data stream by yielding it.
 */
export function* yieldDataRLE(
  arr: Uint8Array,
  totalCount: number,
  typeLength: number,
): Generator<{ value: number; count: number }, number, void> {
  const typeLengthBytes = ((typeLength - 1) >> 3) + 1;
  const readRunRepeated = readRunRepeatedTable[typeLengthBytes] ?? throwReadRunRepeated;
  let offset = 0;

  let j = 0;
  while (j < totalCount) {
    const o = decodeVarint32(arr);
    const header = o.value;
    offset += o.size;

    const partCount = header >> 1;
    if (header & 1) {
      const count8 = partCount << 3;

      // This is a bit gross (always Int32) but probably not a big deal.
      // Yielded data rarely has this.
      const resultArr = new Int32Array(count8);
      offset = decodeRunBitpacked_shifter(arr, offset, partCount, typeLength, resultArr, 0);

      // Aggregate same packed bits.
      let i = 0;
      while (i < count8) {
        const value = resultArr[i];
        let localCount = 0;
        ++i;

        // It would be possible to read more than we have (because count8 packed).
        // We have to min this out so we don't yield more than the consumer expects.
        while (i < count8 && i + j < totalCount && resultArr[i] === value) {
          ++localCount;
          ++i;
        }

        yield { value, count: localCount };
      }

      j += count8;
    } else {
      const value = readRunRepeated(arr, offset);
      offset += typeLengthBytes;

      yield { value, count: partCount };

      j += partCount;
    }
  }

  if (j > totalCount + 8) {
    // Allowing 8 extra seems sane, because bitpacked numbers have to be 8 at a time at minimum.
    throw new Error(`yield RLE got invalid number of values: wanted=${totalCount} found=${j}`);
  }

  return offset;
}

/**
 * Builds a {@link DataResult} from a RLE-encoded data stream.
 */
export function processDataRLE(
  arr: Uint8Array,
  totalCount: number,
  typeLength: number,
): { int: IntArray; offset: number } {
  const typeLengthBytes = ((typeLength - 1) >> 3) + 1;
  const readRunRepeated = readRunRepeatedTable[typeLengthBytes] ?? throwReadRunRepeated;

  const resultArr = buildOut(typeLength, totalCount + 8);
  let offset = 0;

  let j = 0;
  while (j < totalCount) {
    const o = decodeVarint32(arr, offset);
    const header = o.value;
    offset += o.size;

    const partCount = header >> 1;
    if (header & 1) {
      const count8 = partCount << 3;
      offset = decodeRunBitpacked_shifter(arr, offset, partCount, typeLength, resultArr, j);
      j += count8;
    } else {
      const value = readRunRepeated(arr, offset);
      offset += typeLengthBytes;
      const end = j + partCount;
      resultArr.fill(value, j, end);
      j = end;
    }
  }

  if (j > totalCount + 8) {
    // Allowing 8 extra seems sane, because bitpacked numbers have to be 8 at a time at minimum.
    throw new Error(`process RLE got invalid number of values: wanted=${totalCount} found=${j}`);
  }

  // Remove any trailing data (since we allow +8 for bitpacked).
  const int = resultArr.subarray(0, totalCount);

  return { int, offset };
}
