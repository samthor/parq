import { buildReader } from './src/read.js';
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
    console.time('init');
    const reader = await buildReader(r);
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

    const i = new ParquetIndexer(reader, 0);
    console.info('source data has groups', reader.groups().length);

    console.time('find');
    const arg = { start: 0, end: 200_000 };
    const out = await i.findRange(arg);
    console.timeEnd('find');

    console.info('got data', arg, { start: out.start }, out.part);

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
