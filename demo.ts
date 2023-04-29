import { ParquetReader } from './src/read.js';
import { AsyncGeneratorCache } from 'thorish';
import { ParquetIndexer } from './src/helper/caching.js';
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

    if (out.bytesRead !== length) {
      if (size - start === out.bytesRead) {
        // This was an "overread" of the end of the file, ignore
      } else {
        throw new Error(`Could not read desired length=${length} bytesRead=${out.bytesRead}`);
      }
    }

    return buffer;
  };
};

async function demo(p: string) {
  const f = await fs.promises.open(p);
  const stat = await f.stat();
  console.info('operating on', p, 'size', fileSize(stat));
  const r = readerFor(f);

  try {
    const reader = new ParquetReader(r);
    console.time('init');
    await reader.init();
    console.timeEnd('init');

    // // Read all parts of 0th group for demo.
    // console.time('index');
    // const cols = await reader.columns();
    // const tasks = [...cols].map(async (c, i) => {
    //   const gen = reader.indexColumnGroup(i, 0);
    //   let readDict = false;
    //   let p = 0;
    //   for await (const part of gen) {
    //     const data = await part.read();

    //     if ('lookup' in part) {
    //       if (readDict) {
    //         throw new Error(`read dict >1 time`);
    //       }
    //       const dictPart = await reader.dictForColumnGroup(i, 0);
    //       const dictData = await dictPart!.read();
    //       console.info('got lookup data', { c, group: 0, page: p, id: part.id }, data, dictData);
    //       readDict = true;
    //     } else {
    //       console.info('got inline data', { c, group: 0, page: p, id: part.id }, data);
    //     }

    //     ++p;
    //     // XXX
    //   }
    // });
    // await Promise.all(tasks);
    // console.timeEnd('index');

    const i = new ParquetIndexer(reader, 2, (part) => {
      console.debug('found new part', part.id, part.count);
    });
    console.info('source data has rows', reader.rows(), 'groups', reader.groups);

    console.time('find');
    const arg = { start: 152_400, end: 234_450 };
    const out = await i.findRange(arg);
    console.timeEnd('find');

    console.info('got data', arg, { start: out.start }, out.data);

    console.time('find');
    const out2 = await i.findRange(arg);
    console.timeEnd('find');

    // const c = new AsyncGeneratorCache(reader.indexColumnGroup(0, 0));

    // for await (const part of c.read()) {
    //   console.info('got part a', part);
    // }
    // for await (const part of c.read()) {
    //   console.info('got part b', part);
    // }
  } finally {
    await f.close();
  }
}

// from https://www.synthcity.xyz/download.html
await demo(process.argv[2] || 'sample/complete.parquet');
