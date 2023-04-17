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
      throw new TypeError(`More than 5 bytes of int32`);
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
      // TODO: we can consume 4 further bits
      throw new TypeError(`More than 7 bytes of int53`);
    }
  }
  return num;
}

/**
 * Decodes a varint64, returning its parts in two numbers.
 */
export function readVarint64(readByte: () => number): { lo: number; hi: number } {
  let rsize = 0;
  let lo = 0;
  let hi = 0;
  let shift = 0;
  while (true) {
    const b = readByte();
    rsize++;
    if (shift <= 25) {
      lo = lo | ((b & 0x7f) << shift);
    } else if (25 < shift && shift < 32) {
      lo = lo | ((b & 0x7f) << shift);
      hi = hi | ((b & 0x7f) >>> (32 - shift));
    } else {
      hi = hi | ((b & 0x7f) << (shift - 32));
    }
    shift += 7;
    if (!(b & 0x80)) {
      break;
    }
    if (rsize >= 10) {
      throw new TypeError(`More than 10 bytes of int64`);
    }
  }
  return { lo, hi };
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
