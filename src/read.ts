import type * as parquet from '../dep/thrift/gen-nodejs/parquet.js';
import { Encoding, PageType } from './const.js';
import { decompress } from './decompress.js';
import { Chunk, parseFileMetadata, type FileMetadata } from './parts/file-metadata.js';
import type { SchemaLeafNode } from './parts/schema.js';
import { typedArrayView } from './view.js';
import { DataResult, DataType, IdDataResult } from '../types.js';
import {
  countForPageHeader,
  processTypeDataPage,
  processTypeDataPageV2,
  readPage,
} from './parts/page.js';
import { processDataPlain } from './parts/process.js';

export type Reader = (start: number, end?: number) => Promise<Uint8Array>;

export class ParquetReader {
  r: Reader;
  metadata?: FileMetadata;

  rows() {
    return this.metadata!.rows;
  }

  constructor(r: Reader) {
    this.r = r;
  }

  async columns(): Promise<Iterable<string>> {
    return this.metadata!.columns.map((x) => x.schema.name);
  }

  async init() {
    const td = new TextDecoder('utf-8');

    const headerPromise = this.r(0, 4);
    const footerPromise = this.r(-8);

    const header = await headerPromise;
    if (td.decode(header) !== 'PAR1') {
      throw new Error(`Not a parquet file: ${this.r}`);
    }

    const footer = await footerPromise;
    if (td.decode(footer.subarray(4)) !== 'PAR1') {
      throw new Error(`Not a parquet file: ${this.r}`);
    }

    const v = typedArrayView(footer);
    const metadataSize = v.getUint32(0, true);
    const metadata = await this.r(-8 - metadataSize, -8);

    this.metadata = parseFileMetadata(metadata);
  }

  /**
   * Process a chunk from the Parquet file. Each chunk contains an optional dictionary and a number
   * of data pages, of which any may refer back to the dictionary.
   */
  private async processChunk(chunk: Chunk, schema: SchemaLeafNode) {
    const arr = await this.r(chunk.begin, chunk.end);
    let offset = 0;

    // This reads the dictionary/pages in parallel. The only efficiency here will be if the data
    // is gzip-encoded, which can be done in parallel by Node or the browser.
    let dictPromise = Promise.resolve<undefined | IdDataResult>(undefined);
    const pagePromises: Promise<IdDataResult>[] = [];

    if (chunk.hasDictionary) {
      const out = readPage(arr);
      const count = countForPageHeader(out.header);

      const data = arr.subarray(out.begin, out.end);
      offset = out.end;

      dictPromise = (async () => {
        if (out.type !== PageType.DICTIONARY_PAGE) {
          throw new Error(`Got non-dictionary first page: ${out.header.type}`);
        }
        const dictionaryHeader = out.header.dictionary_page_header as InstanceType<
          typeof parquet.DictionaryPageHeader
        >;
        if (dictionaryHeader.encoding !== Encoding.PLAIN_DICTIONARY) {
          // TODO: Is this always true? This lets us just pass the data back as a buffer.
          throw new Error(`Unexpected dictionary encoding: ${dictionaryHeader.encoding}`);
        }
        const count = dictionaryHeader.num_values as number;

        const raw = await decompress(data, chunk.codec);
        const dr = processDataPlain(raw, count, schema.type);
        return {
          id: out.begin,
          ...dr,
        };
      })();
    }

    while (offset < arr.length) {
      const out = readPage(arr, offset);
      const count = countForPageHeader(out.header);

      const data = arr.subarray(out.begin, out.end);
      offset = out.end;

      // Read pages in async in case there's any efficiencies to be found.
      const p = (async () => {
        const raw = await decompress(data, chunk.codec);

        switch (out.type) {
          case PageType.DATA_PAGE:
            return processTypeDataPage(out.header, schema, raw);

          case PageType.DATA_PAGE_V2:
            return processTypeDataPageV2(out.header, schema, raw);

          default:
            throw new Error(`Unsupported page type: ${out.header.type}`);
        }
      })();
      pagePromises.push(p.then((dr) => ({ id: out.begin, ...dr })));
    }

    return {
      dict: await dictPromise,
      pages: await Promise.all(pagePromises),
    };
  }

  columnLength() {
    return this.metadata!.columns.length;
  }

  async readColumn(columnNo: number, start: number) {
    const metadata = this.metadata!;

    if (metadata.groups.length !== 1) {
      console.warn('XXX chosing single group from', metadata.groups.length);
      // throw new Error(`TODO: only support single group for now, found: ${metadata.groups.length}`);
    }
    const group = metadata.groups[0];
//    console.info('got group', group);
    const chunk = group.columns[columnNo];
    if (!chunk) {
      throw new Error(`Bad column index: ${columnNo}`);
    }
    const column = metadata.columns[columnNo];

    const result = await this.processChunk(chunk, column.schema);

    // Called whenever a page arrives; all pages should have same type.
    let type: DataType | undefined;
    const updateType = (t: DataType) => {
      if (type === undefined) {
        type = t;
      } else if (t !== type) {
        throw new Error(`Mismatched final data type in Parquet column: ${columnNo}`);
      }
    };

    const parts = result.pages.map((p) => {
      // Actual data. Return simple mode.
      if (!('lookup' in p)) {
        updateType(p.type);

        return {
          isDictLookup: false,
          data: p,
        };
      }

      // Sanity-check dictionary lookup.
      if (!(p.type === DataType.INT8 || p.type === DataType.INT16 || p.type === DataType.INT32)) {
        throw new Error(`Got invalid dict lookup type: ${(p as DataResult).type}`);
      } else if (!result.dict) {
        throw new Error(`Dictionary lookup without dict, can't index?`);
      }
      updateType(result.dict.type);

      return {
        isDictLookup: true,
        data: p,
        dict: result.dict,
      };
    });

    return {
      name: metadata.columns[columnNo].schema.name,
      gen: (async function* () {
        // TODO: not actually efficient
        for (const part of parts) {
          yield part;
        }
      })(),
      type: type ?? DataType.INT32, // there's no data
    };
  }
}
