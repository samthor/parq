// thrift-tools conveniently provides this for us
import { readVarint32 } from 'thrift-tools';
export { readVarint32 };

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
