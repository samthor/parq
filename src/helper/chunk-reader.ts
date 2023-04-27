import type { Reader } from '../../types.js';

// TODO: This might help in some cases (web - avoid moving tons of data?) but eeeh

export type Options = {
  power?: number;
  reader: Reader;
  size: number;
};

/**
 * Returns a reader which internally caches whole chunks. Default is 32mb.
 */
export function buildChunkReader(o: Options): Reader {
  const { power = 24, reader, size } = o;
  if (power > 30 || power < 12) {
    throw new RangeError(`Power probably out of range: ${power}`);
  }
  const cacheSize = 100; // TODO: random number
  const chunkSize = 2 ** power;

  const chunks = new Map<number, Uint8Array>();
  const chunkFor = (at: number) => Math.floor(at / chunkSize);

  const ensureChunk = async (c: number) => {
    const prev = chunks.get(c);
    if (prev !== undefined) {
      chunks.delete(c);
      chunks.set(c, prev);
      return prev;
    }

    const readAt = c * chunkSize;
    const data = await reader(readAt, readAt + chunkSize);
    chunks.set(c, data);

    while (chunks.size >= cacheSize) {
      for (const old of chunks.keys()) {
        chunks.delete(old);
        break;
      }
    }

    return data;
  };

  return async (start: number, end?: number): Promise<Uint8Array> => {
    if (start < 0) {
      start += size;
    }
    if (end === undefined) {
      end = size;
    } else if (end < 0) {
      end += size;
    }

    const startChunk = chunkFor(start);
    const endChunk = chunkFor(end);

    if (startChunk === endChunk) {
      const data = await ensureChunk(startChunk);
      const readAt = startChunk * chunkSize;
      return data.subarray(start - readAt, end - readAt);
    } else if (endChunk === startChunk + 1) {
      const low = await ensureChunk(startChunk);
      const high = await ensureChunk(endChunk);

      const out = new Uint8Array(end - start);

      // Copy left side. Can read "forever" and read at zero.
      const lowReadAt = startChunk * chunkSize;
      const lowPart = low.subarray(start - lowReadAt);
      out.set(lowPart);

      // Copy right side.
      const highLength = out.length - lowPart.length;
      const highPart = high.subarray(0, highLength);
      out.set(highPart, lowPart.length);

      return out;
    } else {
      console.warn('Want data', { start, end, startChunk, endChunk, size: end - start });
      throw 'TODO';
    }
  };
}
