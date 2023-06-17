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
