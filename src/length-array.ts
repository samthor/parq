import { typedArrayView } from './view.js';
import type { DataType } from '../types.js';

/**
 * Index data encoded in {@link DataType.LENGTH_BYTE_ARRAY}.
 */
export function indexLengthTypeArray(buf: Uint8Array): { at: number; length: number }[] {
  const out: { at: number; length: number }[] = [];

  const dv = typedArrayView(buf);
  let offset = 0;

  while (offset < buf.length) {
    const length = dv.getUint32(offset, true);
    offset += 4;

    out.push({ at: offset, length });

    offset += length;
  }

  return out;
}

/**
 * Convert data encoded in {@link DataType.LENGTH_BYTE_ARRAY} to an array of {@link Uint8Array}.
 */
export function arrayLengthTypeArray(buf: Uint8Array): Uint8Array[] {
  const out: Uint8Array[] = [];

  const dv = typedArrayView(buf);
  let offset = 0;

  while (offset < buf.length) {
    const length = dv.getUint32(offset, true);
    offset += 4;
    const end = offset + length;

    out.push(buf.slice(offset, end));

    offset = end;
  }

  return out;
}
