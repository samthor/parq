import { decodeVarint32 } from '../varint.js';

const numberOfBytesMask: number[] = [0, 0xff, 0xffff, 0xffffff, 0xffffffff];
const TYPED_ARRAY_THRESHOLD = 8192;

export function copyBytes(
  source: Uint8Array,
  sourceOffset: number,
  target: Uint8Array,
  targetOffset: number,
  copyLength: number,
) {
  if (copyLength > TYPED_ARRAY_THRESHOLD) {
    target.set(source.subarray(sourceOffset, sourceOffset + copyLength), targetOffset);
  } else {
    for (let i = 0; i < copyLength; ++i) {
      target[targetOffset + i] = source[sourceOffset + i];
    }
  }
}

function copyBytesWithin(
  target: Uint8Array,
  targetOffset: number,
  copyOffset: number,
  copyLength: number,
) {
  if (copyLength > TYPED_ARRAY_THRESHOLD) {
    const start = targetOffset - copyOffset;
    target.copyWithin(targetOffset, start, start + copyLength);
  } else {
    for (let i = 0; i < copyLength; ++i) {
      target[targetOffset + i] = target[targetOffset - copyOffset + i];
    }
  }
}

/**
 * Decompresses a Snappy byte stream synchronously.
 *
 * Note that the typed array methods don't always outperform naïve copies (for some reason), so
 * they're only used when the number of bytes is large.
 */
export function decompress(source: Uint8Array): Uint8Array {
  const dv = new DataView(source.buffer, source.byteOffset);

  // Snappy compressions start with a varint of the actual target size.
  const o = decodeVarint32(source);
  const length = o.value;
  let sourceOffset = o.size;

  const target = new Uint8Array(length);
  let targetOffset = 0;

  let copyLength = 0;
  let copyOffset = 0;

  const { length: sourceLength } = source;
  while (sourceOffset < sourceLength) {
    const c = source[sourceOffset];
    ++sourceOffset;

    const control = c & 0x3;
    const controlRest = c >>> 2;

    switch (control) {
      case 0: {
        // Literal data
        let literalLength = controlRest + 1;
        if (literalLength > 60) {
          if (sourceOffset + 63 >= sourceLength) {
            throw new RangeError(`Literal length storage extends past buffer length`);
          }

          // Too big, the length is in a uint32 here (but only 63, 62, 61, 60 bytes worth)
          const numberOfBytes = literalLength - 60;
          const mask = numberOfBytesMask[numberOfBytes];
          if (mask === undefined) {
            throw new TypeError(`Got invalid literal length: ${numberOfBytes}`);
          }

          literalLength = (dv.getUint32(sourceOffset, true) & mask) + 1;
          sourceOffset += numberOfBytes;
        }

        if (sourceOffset + literalLength > sourceLength) {
          throw new RangeError(`Literal length extends past buffer length`);
        }
        copyBytes(source, sourceOffset, target, targetOffset, literalLength);

        targetOffset += literalLength;
        sourceOffset += literalLength;
        continue;
      }

      case 1:
        if (sourceOffset + 0 >= sourceLength) {
          throw new RangeError(`Could not read copy with 1-byte`);
        }

        // Copy with 1-byte offset
        copyLength = (controlRest & 0x7) + 4;
        copyOffset = source[sourceOffset] + ((c >>> 5) << 8);
        sourceOffset += 1;
        break;

      case 2:
        if (sourceOffset + 1 >= sourceLength) {
          throw new RangeError(`Could not read copy with 2-byte`);
        }

        // Copy with 2-byte offset
        copyLength = controlRest + 1;
        copyOffset = dv.getUint16(sourceOffset, true);
        sourceOffset += 2;
        break;

      case 3:
        if (sourceOffset + 4 >= sourceLength) {
          throw new RangeError(`Could not read copy with 4-byte`);
        }

        // Copy with 4-byte offset
        copyLength = controlRest + 1;
        copyOffset = dv.getUint32(sourceOffset, true);
        sourceOffset += 4;
        break;
    }

    if (copyOffset === 0 || targetOffset < copyOffset) {
      throw new RangeError(
        `Snappy offset goes past start of source: copyOffset=${copyOffset} targetOffset=${targetOffset}`,
      );
    }

    copyBytesWithin(target, targetOffset, copyOffset, copyLength);
    targetOffset += copyLength;
  }

  return target;
}
