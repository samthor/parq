import type * as parquet from '../dep/thrift/gen-nodejs/parquet.js';
import { Encoding, PageType } from './const.js';
import { decompress } from './decompress.js';
import { Chunk, parseFileMetadata, type FileMetadata } from './parts/file-metadata.js';
import type { SchemaLeafNode } from './parts/schema.js';
import { typedArrayView } from './view.js';
import { DataResult, DataType, ReadColumnPart } from '../types.js';
import {
  countForPageHeader,
  isDictLookup,
  processTypeDataPage,
  processTypeDataPageV2,
  readPage,
} from './parts/page.js';
import { processDataPlain } from './parts/process.js';

export type Reader = (start: number, end?: number) => Promise<Uint8Array>;

type SimplePart = {
  id: number;
  count: number;
  read(): Promise<DataResult>;
};

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

    const headerBytes = await headerPromise;
    if (td.decode(headerBytes) !== 'PAR1') {
      throw new Error(`Not a parquet file: ${this.r}`);
    }

    const footerBytes = await footerPromise;
    if (td.decode(footerBytes.subarray(4)) !== 'PAR1') {
      throw new Error(`Not a parquet file: ${this.r}`);
    }

    const v = typedArrayView(footerBytes);
    const metadataSize = v.getUint32(0, true);
    const metadataBytes = await this.r(-8 - metadataSize, -8);

    this.metadata = parseFileMetadata(metadataBytes);
  }

  /**
   * Process a chunk from the Parquet file. Each chunk contains an optional dictionary and a number
   * of data pages, of which any may refer back to the dictionary.
   */
  private async *processChunk(
    chunk: Chunk,
    schema: SchemaLeafNode,
    row: number,
  ): AsyncGenerator<ReadColumnPart, void, void> {
    let dictPartPromise: Promise<ReadColumnPart> | undefined;
    const dictId = chunk.begin;

    if (chunk.dictionarySize) {
      dictPartPromise = (async () => {
        const arr = await this.r(chunk.begin, chunk.begin + chunk.dictionarySize);
        const page = readPage(arr);
        const count = countForPageHeader(page.header);
        const data = arr.subarray(page.begin, page.end);

        if (page.type !== PageType.DICTIONARY_PAGE) {
          throw new Error(`Got non-dictionary first page: ${page.header.type}`);
        }
        const dictionaryHeader = page.header.dictionary_page_header as InstanceType<
          typeof parquet.DictionaryPageHeader
        >;
        if (dictionaryHeader.encoding !== Encoding.PLAIN_DICTIONARY) {
          // TODO: Is this always true? This lets us just pass the data back as a buffer.
          throw new Error(`Unexpected dictionary encoding: ${dictionaryHeader.encoding}`);
        }

        return {
          id: dictId,
          count,
          async read(): Promise<DataResult> {
            const raw = await decompress(data, chunk.codec);
            return processDataPlain(raw, count, schema.type);
          },
          dict: true,
        };
      })();
    }

    const arr = await this.r(chunk.begin + chunk.dictionarySize, chunk.end);
    let offset = 0;
    while (offset < arr.length) {
      console.debug('reading page at', arr.length, offset);
      const page = readPage(arr, offset);
      const count = countForPageHeader(page.header);

      const data = arr.subarray(page.begin, page.end);
      offset = page.end;

      const dict = isDictLookup(page.header);
      const begin = row;
      row += count;
      const end = row;

      if (dict && dictPartPromise) {
        const dictPart = await dictPartPromise;
        yield dictPart;
        dictPartPromise = undefined;
      }

      yield {
        id: chunk.begin + chunk.dictionarySize + offset,
        begin,
        end,
        count,
        dict,
        async read(): Promise<DataResult> {
          const raw = await decompress(data, chunk.codec);

          switch (page.type) {
            case PageType.DATA_PAGE:
              return processTypeDataPage(page.header, schema, raw);

            case PageType.DATA_PAGE_V2:
              return processTypeDataPageV2(page.header, schema, raw);

            default:
              throw new Error(`Unsupported page type: ${page.header.type}`);
          }
        },
      };
    }
  }

  columnLength() {
    return this.metadata!.columns.length;
  }

  get groups() {
    return this.metadata!.groups.length;
  }

  async *readColumn(columnNo: number, groupNo: number): AsyncGenerator<ReadColumnPart, void, void> {
    const metadata = this.metadata!;

    if (groupNo < 0 || groupNo >= metadata.groups.length) {
      throw new RangeError(`Invalid group`);
    }
    const group = metadata.groups[groupNo];

    const chunk = group.columns[columnNo];
    if (!chunk) {
      throw new Error(`Bad column index: ${columnNo}`);
    }
    const column = metadata.columns[columnNo];

    yield* this.processChunk(chunk, column.schema, group.start);
  }
}
