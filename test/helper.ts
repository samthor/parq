import { Reader } from '../types.js';
import * as fs from 'node:fs';
import * as url from 'node:url';
import * as path from 'node:path';

const dataDir = url.fileURLToPath(new URL('./data/', import.meta.url));

export function readerForData(filename: string): Reader {
  const allBytes = fs.readFileSync(path.join(dataDir, filename));
  const allBytesArray = new Uint8Array(allBytes);

  return async (start, end) => allBytesArray.slice(start, end);
}

export async function flattenAsyncIterator<K>(it: AsyncIterable<K>): Promise<K[]> {
  const out: K[] = [];

  for await (const next of it) {
    out.push(next);
  }

  return out;
}