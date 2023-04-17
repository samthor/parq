import { ParquetReader } from './src/read';
import * as fs from 'node:fs';

const readerFor = (f: fs.promises.FileHandle) => {
  return async (start: number, end?: number) => {
    const stat = await f.stat();
    const { size } = stat;

    if (start < 0) {
      start += size;
    }
    if (end === undefined) {
      end = size;
    } else if (end < 0) {
      end += size;
    }

    const length = end - start;
    const out = await f.read({ position: start, length });

    const { buffer } = out;
    if (buffer.length > length) {
      // Node always seems to read 16k of data, regardless of how much we wanted
      console.debug('discarding buffer length=', buffer.length, 'only need', length);
      return buffer.subarray(0, length);
    }

    return buffer;
  };
};

async function demo(p: string) {
  const f = await fs.promises.open(p);
  const r = readerFor(f);

  try {
    const reader = new ParquetReader(r);
    await reader.init();

    const cols = await reader.columns();

    console.info('done', { reader, cols });
  } finally {
    await f.close();
  }
}

// from https://www.synthcity.xyz/download.html
await demo('sample/area1.parquet');
