import * as parquet from '../dep/thrift/gen-nodejs/parquet.js';
import { Encoding, PageType } from './const.js';
import { decompress } from './decompress.js';
import { parseFileMetadata, type FileMetadata } from './parts/file-metadata.js';
import { typedArrayView } from './view.js';
import { DataResult, ReadColumnPart, Reader } from '../types.js';
import {
  countForPageHeader,
  isDictLookup,
  processTypeDataPage,
  processTypeDataPageV2,
} from './parts/page.js';
import { processDataPlain } from './parts/process.js';
import {
  TCompactProtocolReaderBuffer,
  TCompactProtocolReaderPoll,
  TCompactProtocolReaderPoll_OutOfData,
} from './thrift/reader.js';

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
   * Reads the dictionary part for the given column/group.
   *
   * Unlike {@link #indexColumnGroup}, this reads all the dictionary row data right away.
   */
  async dictForColumnGroup(columnNo: number, groupNo: number): Promise<ReadColumnPart | null> {
    const column = this.metadata!.columns[columnNo];
    const chunk = column.chunks[groupNo];
    if (chunk.dictionarySize === 0) {
      return null;
    }

    const buffer = await this.r(chunk.begin, chunk.begin + chunk.dictionarySize);
    const header = new parquet.PageHeader();
    const reader = new TCompactProtocolReaderBuffer(buffer);
    header.read(reader);

    const count = countForPageHeader(header);
    const compressed = buffer.subarray(reader.at, reader.at + header.compressed_page_size);

    if (header.type !== PageType.DICTIONARY_PAGE) {
      throw new Error(`Got non-dictionary first page: ${header.type}`);
    }
    const dictionaryHeader = header.dictionary_page_header as InstanceType<
      typeof parquet.DictionaryPageHeader
    >;
    if (dictionaryHeader.encoding !== Encoding.PLAIN_DICTIONARY) {
      // TODO: Is this always true? This lets us just pass the data back as a buffer.
      throw new Error(`Unexpected dictionary encoding: ${dictionaryHeader.encoding}`);
    }

    return {
      id: chunk.begin,
      count,
      async read(): Promise<DataResult> {
        const buf = await decompress(compressed, chunk.codec);
        return processDataPlain(buf, count, column.schema.type);
      },
      dict: true,
    };
  }

  /**
   * Provides a {@link AsyncGenerator} over the pages within this column/group. This doesn't read
   * the underlying data from the source until `read()` is called.
   */
  async *indexColumnGroup(
    columnNo: number,
    groupNo: number,
  ): AsyncGenerator<ReadColumnPart, void, void> {
    const group = this.metadata!.groups[groupNo];
    const column = this.metadata!.columns[columnNo];
    const chunk = column.chunks[groupNo];

    let position = group.start;
    let offset = chunk.begin + chunk.dictionarySize;
    while (offset < chunk.end) {
      let header: InstanceType<typeof parquet.PageHeader> = new parquet.PageHeader();
      let consumed = 0;

      // This assumes an increasing number of bytes to try to consume the header
      // It reads 128, 256, 512, 1024, 2048, 4196, before giving up.
      // I've not found headers in the wild that are >=64 bytes.
      for (let i = 7; i <= 12; ++i) {
        const guess = await this.r(offset, offset + (1 << i));
        const reader = new TCompactProtocolReaderPoll(guess);

        try {
          header.read(reader);
        } catch (e) {
          if (i !== 12 && e instanceof TCompactProtocolReaderPoll_OutOfData) {
            header = new parquet.PageHeader();
            continue;
          }
          throw e;
        }
        consumed = reader.consumed;
        break;
      }

      if (header.compressed_page_size <= 0) {
        throw new Error(`Could not find valid page while indexing`);
      }

      const count = countForPageHeader(header);
      const dataBegin = offset + consumed;
      const dataEnd = dataBegin + header.compressed_page_size;

      const begin = position;
      position += count;
      const end = position;

      const read = async (): Promise<DataResult> => {
        const compressed = await this.r(dataBegin, dataEnd);
        const buf = await decompress(compressed, chunk.codec);

        switch (header.type) {
          case PageType.DATA_PAGE:
            return processTypeDataPage(header, column.schema, buf);

          case PageType.DATA_PAGE_V2:
            return processTypeDataPageV2(header, column.schema, buf);

          default:
            throw new Error(`Unsupported page type: ${header.type}`);
        }
      };

      const o: ReadColumnPart = { id: offset, begin, end, count, dict: false, read };
      if (isDictLookup(header)) {
        if (!chunk.dictionarySize) {
          throw new Error(`got dict lookup without dict`);
        }
        o.lookup = chunk.begin; // id is start of chunk
      }
      yield o;

      offset += consumed;
      offset += header.compressed_page_size;
    }
  }

  columnLength() {
    return this.metadata!.columns.length;
  }

  get groups() {
    return this.metadata!.groups.length;
  }
}
