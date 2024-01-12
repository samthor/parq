import { Data, ParquetReader, Part, UintArray, bitLength } from '../types.ts';
import * as pq from '../dep/thrift/parquet-code.ts';
import { iterateLengthByteArray } from './length-array.ts';

/**
 * Iterate over the range of values for the given column.
 *
 * This will only ever yield at most `end` - `start` values.
 */
export async function* flatIterate(
  r: ParquetReader,
  columnNo: number,
  start: number,
  end: number,
): AsyncGenerator<Uint8Array, void, void> {
  const gen = r.loadRange(columnNo, start, end);

  const res = await gen.next();
  if (res.done) {
    return; // nothing to do - bad read
  }

  if (start < 0) {
    throw new Error(`can't iterate with -ve start`);
  }

  const first = res.value;
  if (start > first.end) {
    throw new Error(`cannot skip past first`);
  }
  const toSkip = start - first.start;
  let toEmit = end - start;

  const gen2 = flatIterateInternal(r, columnNo);

  for await (const val of gen2(first, toSkip)) {
    if (toEmit === 0) {
      return;
    }
    yield val;
    --toEmit;
  }

  for (;;) {
    if (toEmit === 0) {
      return; // check again in case on boundary
    }

    const next = await gen.next();
    if (next.done) {
      return;
    }
    const part = next.value;

    const length = part.end - part.start;
    if (length > toEmit) {
      yield* gen2(part);
      toEmit -= length;
    } else {
      for await (const val of gen2(part)) {
        yield val;
        --toEmit;
        if (toEmit === 0) {
          return;
        }
      }
    }
  }
}

function flatIterateInternal(r: ParquetReader, columnNo: number) {
  const c = r.info().columns.at(columnNo)!;
  if (c === undefined) {
    throw new Error(`bad columnNo`);
  }

  const size = bitLength(c.physicalType, c.typeLength);
  const bytesPer = size >> 3;

  async function* gen2(part: Part, skip: number = 0) {
    const readPromise = r.readAt(part.at);

    if (part.lookup) {
      const lookup = await r.lookupAt(part.lookup);
      const read = await readPromise;

      if (c.physicalType === pq.Type.BYTE_ARRAY) {
        const readIndex = [...iterateLengthByteArray(read.raw)];
        for (const index of lookup.subarray(skip)) {
          if (index >= readIndex.length) {
            throw new Error(`can't yield past end of data`);
          }
          yield readIndex[index];
        }
        return;
      }

      for (const index of lookup.subarray(skip)) {
        if (c.physicalType === pq.Type.BOOLEAN) {
          throw new Error(`BOOLEAN lookup`);
        }
        //        console.info('yielding lookup data', { read, lookup, index, bytesPer, size, type: c.physicalType, raw: read.raw });
        yield read.raw.subarray(bytesPer * index, bytesPer * (index + 1));
      }
      return;
    }

    // normal operation
    const read = await readPromise;
    if (c.physicalType === pq.Type.BYTE_ARRAY) {
      yield* iterateLengthByteArray(read.raw, skip);
      return;
    } else if (c.physicalType === pq.Type.BOOLEAN) {
      const u = new Uint8Array(1);

      for (let i = skip; i < read.count; ++i) {
        const byte = i >> 3;
        const off = 1 << i % 8;
        u[0] = read.raw[byte] & off ? 1 : 0;
        yield u;
      }
      return;
    }

    for (let i = skip; i < read.count; ++i) {
      //      console.info('yielding simple data', { i, bytesPer, raw: read.raw });
      yield read.raw.subarray(bytesPer * i, bytesPer * (i + 1));
    }
  }

  return gen2;
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
 * Unlike {@link flatIterate} this does _not_ filter to start/end, instead, it returs whole chunks.
 * You'll often get more data than you wanted.
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
      const lookupValue = read.raw.subarray(at, at + bytesPer);
      out.set(lookupValue, outAt);
      outAt += bytesPer;
    }
  }

  return { bitLength: size, start, end, index: undefined, raw: out };
}
