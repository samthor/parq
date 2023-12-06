import { Reader } from '../types.js';
import * as fs from 'node:fs';
import * as url from 'node:url';
import * as path from 'node:path';
import { readerFor } from '../src/helper/node-reader.js';

const dataDir = url.fileURLToPath(new URL('./data/', import.meta.url));

const handlesToCleanup = new Set<fs.promises.FileHandle>();

export async function readerForData(filename: string): Promise<Reader> {
  const t = path.join(dataDir, filename);
  const handle = await fs.promises.open(t);
  handlesToCleanup.add(handle);
  return readerFor(handle);
}

export async function cleanupHandles() {
  for (const handle of handlesToCleanup) {
    await handle.close();
  }
}