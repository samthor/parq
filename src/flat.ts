import { Data, ParquetReader, Part, UintArray } from '../types.ts';
import * as pq from '../dep/thrift/parquet-code.ts';
import { iterateLengthByteArray } from './length-array.ts';

function bitLength(t: pq.Type, typeLength: number) {
  switch (t) {
    case pq.Type.BOOLEAN:
      return 1;
    case pq.Type.INT32:
    case pq.Type.FLOAT:
      return 32;
    case pq.Type.INT64:
    case pq.Type.DOUBLE:
      return 64;
    case pq.Type.INT96:
      return 96;
    case pq.Type.FIXED_LEN_BYTE_ARRAY:
      return typeLength * 8;
    case pq.Type.BYTE_ARRAY:
      return 0;
    default:
      throw new Error(`bad physicalType: ${t}`);
  }
}

export async function* flatIterate(
  r: ParquetReader,
  columnNo: number,
  start: number,
  end: number,
): AsyncGenerator<Uint8Array, void, void> {
  // FIXME: doesn't filter to start/end or even announce it

  const c = r.info().columns.at(columnNo);
  if (c === undefined) {
    throw new Error(`bad columnNo`);
  }

  const size = bitLength(c.physicalType, c.typeLength);
  const bytesPer = size >> 3;

  const gen = r.loadRange(columnNo, start, end);
  for await (const part of gen) {
    const readPromise = r.readAt(part.at);

    if (part.lookup) {
      const lookup = await r.lookupAt(part.lookup);
      const read = await readPromise;

      if (c.physicalType === pq.Type.BYTE_ARRAY) {
        const readIndex = [...iterateLengthByteArray(read.raw)];
        for (const index of lookup) {
          if (index >= readIndex.length) {
            throw new Error(`can't yield past end of data`);
          }
          yield readIndex[index];
        }
        continue;
      }

      for (const index of lookup) {
        if (c.physicalType === pq.Type.BOOLEAN) {
          throw new Error(`BOOLEAN lookup`);
        }
//        console.info('yielding lookup data', { read, lookup, index, bytesPer, size, type: c.physicalType, raw: read.raw });
        yield read.raw.slice(bytesPer * index, bytesPer * (index + 1));
      }

      continue;
    }

    // normal operation
    const read = await readPromise;
    if (c.physicalType === pq.Type.BYTE_ARRAY) {
      yield* iterateLengthByteArray(read.raw);
      continue;
    } else if (c.physicalType === pq.Type.BOOLEAN) {
      const u = new Uint8Array(1);

      for (let i = 0; i < read.count; ++i) {
        const byte = i >> 3;
        const off = 1 << i % 8;
        u[0] = read.raw[byte] & off ? 1 : 0;
        yield u;
      }
      continue;
    }

    for (let i = 0; i < read.count; ++i) {
//      console.info('yielding simple data', { i, bytesPer, raw: read.raw });
      yield read.raw.slice(bytesPer * i, bytesPer * (i + 1));
    }
  }
}

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

  const size = bitLength(c.physicalType, c.typeLength);

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
    }
    return { bitLength: size, start, end: start, index: undefined, raw: new Uint8Array() };
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
