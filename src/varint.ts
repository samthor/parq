/**
 * Decodes a varint, but limited to 32 bits. Throws otherwise.
 */
export function readVarint32(readByte: () => number): number {
  let num = 0;
  let shift = 0;
  while (true) {
    const b = readByte();
    num = num | ((b & 0x7f) << shift);
    shift += 7;
    if (!(b & 0x80)) {
      break; // if first bit not set
    }
    if (shift >= 28) {
      const final = readByte();
      if (final & 0b11110000) {
        // Can only read 4 more bits (28 -> 32)
        throw new Error(`Too much data for varint32`);
      }
      num = num | (b << shift);
      break;
    }
  }
  return num;
}

/**
 * Returns a varint but throws at >53 bits (i.e., {@link Number.MAX_SAFE_INTEGER}).
 */
export function readVarint53(readByte: () => number): number {
  let num = 0;
  let shift = 0;
  while (true) {
    const b = readByte();
    num = num | ((b & 0x7f) << shift);
    shift += 7;
    if (!(b & 0x80)) {
      break; // if first bit not set
    }
    if (shift >= 49) {
      const final = readByte();
      if (final & 0b11110000) {
        // Can only read 4 more bits (49 -> 53)
        throw new Error(`Too much data for varint53`);
      }
      num = num | (b << shift);
      break;
    }
  }
  return num;
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
