import * as fs from 'node:fs';
import { Reader } from '../index.js';

/**
 * Convert a {@link fs.promises.FileHandle} into a {@link Reader}.
 */
export const readerFor = (f: fs.promises.FileHandle): Reader => {
  let statPromise = f.stat();

  return async (start: number, end?: number) => {
    const stat = await statPromise;
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
