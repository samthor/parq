import { typedArrayView } from './view.js';

/**
 * Iterate over data encoded as `[uint32le length, data of length]+`.
 *
 * Each yielded {@link Uint8Array} is a slice of the source buffer, so its data position can be
 * derived by subtracting its `.byteOffset` property from the same property on the source.
 */
export function* iterateLengthByteArray(buf: Uint8Array): Generator<Uint8Array, void, void> {
  const dv = typedArrayView(buf);
  let offset = 0;

  while (offset < buf.length) {
    const length = dv.getUint32(offset, true);
    offset += 4;
    const end = offset + length;

    const part = buf.slice(offset, end);
    yield part;

    offset = end;
  }
}
