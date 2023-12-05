import { Data, ParquetReader, Part, UintArray } from '../types.ts';
import * as pq from '../dep/thrift/parquet-code.ts';
import { iterateLengthByteArray } from './length-array.ts';

export type FlatRead = {
  start: number;
  end: number;
  bitLength: number;
} & ({ index: Uint8Array[]; raw: undefined } | { index: undefined; raw: Uint8Array });

/**
 * flatRead generates highly-usable data from a {@link ParquetReader} by flattening out dictionary
 * data into output arrays.
 *
 * This operates in a special-case for {@link pq.Type.BYTE_ARRAY}, as it's _more work_ to put its
 * data back into its original format. Returns an index of all byte buffers (probably strings).
 *
 * This is unashamedly slow if you don't need all the data.
 */
export async function flatRead(
  r: ParquetReader,
  columnNo: number,
  start: number,
  end: number,
): Promise<FlatRead> {
  const c = r.info().columns.at(columnNo);
  if (c === undefined) {
    throw new Error(`bad columnNo`);
  }

  let size = 0;
  switch (c.physicalType) {
    case pq.Type.BOOLEAN:
      size = 1;
      break;
    case pq.Type.INT32:
    case pq.Type.FLOAT:
      size = 32;
      break;
    case pq.Type.INT64:
    case pq.Type.DOUBLE:
      size = 64;
      break;
    case pq.Type.INT96:
      size = 96;
      break;
    case pq.Type.FIXED_LEN_BYTE_ARRAY:
      size = c.typeLength * 8;
      break;
    case pq.Type.BYTE_ARRAY:
      break;
    default:
      throw new Error(`bad physicalType: ${c.physicalType}`);
  }

  const lookups = new Map<number, Promise<UintArray>>();
  const reads = new Map<number, Promise<Data>>();
  const parts: Part[] = [];

  const gen = r.loadRange(columnNo, start, end);
  for await (const part of gen) {
    if (part.lookup) {
      lookups.set(part.lookup, r.lookupAt(part.lookup));
    }
    reads.set(part.at, r.readAt(part.at));
    parts.push(part);
  }

  // nothing was read?
  if (parts.length === 0) {
    if (c.physicalType === pq.Type.BYTE_ARRAY) {
      return { bitLength: size, start, end: start, index: [], raw: undefined };
    } else {
      return { bitLength: size, start, end: start, index: undefined, raw: new Uint8Array() };
    }
  }

  start = parts[0].start;
  end = parts.at(-1)!.end;
  const count = end - start;

  if (c.physicalType === pq.Type.BYTE_ARRAY) {
    // size is just total because it's not fixed
    // do two passes; one for size, one for output

    const out: Uint8Array[] = [];

    for (const part of parts) {
      const read = await reads.get(part.at)!;
      const idx = [...iterateLengthByteArray(read.raw)];

      if (part.lookup === 0) {
        out.push(...idx);
        continue;
      }

      // have to index read the dictionary
      const lookup = await lookups.get(part.lookup)!;
      for (const indexAt of lookup) {
        out.push(idx[indexAt]);
      }
    }

    return { bitLength: size, start, end, index: out, raw: undefined };
  }

  // fast-path for single non-dict part, don't do more allocs
  if (size % 8 === 0 && parts.length === 1 && parts[0].lookup === 0) {
    const part = parts[0];
    const read = await reads.get(part.at)!;
    return { bitLength: size, start, end, index: undefined, raw: read.raw };
  }

  const bitsNeeded = count * size;
  const out = new Uint8Array((bitsNeeded + 7) >> 3);
  let outAt = 0;

  for (const part of parts) {
    const read = await reads.get(part.at)!;

    // real data, just blat it in place
    if (part.lookup === 0) {
      if (size % 8) {
        const count = part.end - part.start;
        if (count % 8) {
          // TODO: a part has excess single-bit data
          // fixing this involves bit-shifting _everything_
          throw new Error(`got misaligned BOOLEAN data`);
        }
      }

      out.set(read.raw, outAt);
      outAt += read.raw.length;
      continue;
    }

    if (size % 8) {
      // TODO: this should probably never happen - BOOLEAN doing lookup?
      throw new Error(`TODO: boolean/dict lookup`);
    }

    // dict lookup data
    const lookup = await lookups.get(part.lookup)!;
    const bytesPer = size >> 3;

    for (const indexAt of lookup) {
      const at = indexAt * bytesPer;
      const lookupValue = read.raw.slice(at, at + bytesPer);
      out.set(lookupValue, outAt);
      outAt += bytesPer;
    }
  }

  return { bitLength: size, start, end, index: undefined, raw: out };
}
