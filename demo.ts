import { ParquetReader } from './src/read.js';
import * as fs from 'node:fs';

const fileSize = (raw: number | { size: number }) => {
  let size: number;
  if (typeof raw === 'number') {
    size = raw;
  } else {
    size = raw.size;
  }

  if (size > 1024 * 1024) {
    return (size / (1024 * 1024)).toFixed(2) + 'mb';
  } else if (size > 1024) {
    return (size / 1024).toFixed(2) + 'kb';
  } else {
    return size + 'b';
  }
};

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

    // Have to create buffer otherwise Node only creates a 16k one.
    const length = end - start;
    const buffer = new Uint8Array(length);
    const out = await f.read(buffer, 0, length, start);
    console.debug('read', fileSize(length));

    if (out.bytesRead !== length) {
      throw new Error(`Could not read desired length=${length} bytesRead=${out.bytesRead}`);
    }
    return buffer;
  };
};

async function demo(p: string) {
  const f = await fs.promises.open(p);
  const stat = await f.stat();
  console.info('operating on', p, 'size', fileSize(stat))
  const r = readerFor(f);
  await r(-1000); // cheese it

  try {
    const reader = new ParquetReader(r);
    console.time('init');
    await reader.init();
    console.timeEnd('init');

    console.time('read');
    const cols = await reader.columns();
    for (let g = 0; g < reader.groups; ++g) {
      const col = reader.readColumn(0, g);
      console.info('got col', col);
      for await (const part of col) {
        console.info('got part', part);
      }
    }
    console.timeEnd('read');
  } finally {
    await f.close();
  }
}

// from https://www.synthcity.xyz/download.html
// Notes:
//  - creates ~300mb buffer to read column from disk, but decompresses Snappy about ~1mb each time
await demo(process.argv[2] || 'sample/complete.parquet');
