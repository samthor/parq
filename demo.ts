import { ParquetReader } from './src/read.js';
import { fileSize } from './src/helper/format.js';
import { ParquetIndexer } from './src/indexer.js';
import * as fs from 'node:fs';
import { readerFor } from './src/helper/node-reader.js';

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

    const i = new ParquetIndexer(reader, 0, (part) => {
      //      console.debug('found new part', part.id, part.count);
    });
    console.info('source data has rows', reader.rows(), 'groups', reader.groups);

    console.time('find');
    const arg = { start: 0, end: 100 };
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
