import * as pq from '../dep/thrift/parquet-code.js';
import { decompress } from './decompress.js';
import { parseFileMetadata, type FileMetadata } from './parts/file-metadata.js';
import { typedArrayView } from './view.js';
import {
  ColumnDataResult,
  ColumnDataResultLookup,
  ReadColumnPart,
  ReadDictPart,
  Reader,
} from '../types.js';
import {
  countForPageHeader,
  isDictLookup,
  pollPageHeader,
  processTypeDataPage,
  processTypeDataPageV2,
} from './parts/page.js';
import { processDataPlain } from './parts/process.js';

export class ParquetReader {
  r: Reader;
  metadata?: FileMetadata;

  constructor(r: Reader) {
    this.r = r;
  }

  async columns(): Promise<Array<string>> {
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
   * This reads just the header immediately, like {@link #indexColumnGroup}.
   */
  async dictForColumnGroup(columnNo: number, groupNo: number): Promise<ReadDictPart | null> {
    const column = this.metadata!.columns[columnNo];
    const chunk = column.chunks[groupNo];

    if (chunk.dictionarySize === 0) {
      // Check if there was actually a dictionary in the 1st position.
      const { header, consumed } = await pollPageHeader(this.r, chunk.begin);
      if (header.type !== pq.PageType.DICTIONARY_PAGE) {
        return null;
      }
      chunk.dictionarySize = consumed + header.compressed_page_size;
    }

    const { header, consumed } = await pollPageHeader(this.r, chunk.begin);
    const count = countForPageHeader(header);
    const dataBegin = chunk.begin + consumed;
    const dataEnd = dataBegin + header.compressed_page_size;

    if (dataEnd !== chunk.begin + chunk.dictionarySize) {
      throw new Error(`Got inconsistent dictionary dize`);
    }
    if (header.type !== pq.PageType.DICTIONARY_PAGE) {
      throw new Error(`Got invalid type for dict: ${header.type}`);
    }
    const dictionaryHeader = header.dictionary_page_header!;
    if (dictionaryHeader.encoding !== pq.Encoding.PLAIN_DICTIONARY) {
      // TODO: Is this always true? This lets us just pass the data back as a buffer.
      throw new Error(`Unexpected dictionary encoding: ${dictionaryHeader.encoding}`);
    }

    const read = async (): Promise<ColumnDataResult> => {
      const compressed = await this.r(dataBegin, dataEnd);
      const buf = await decompress(compressed, chunk.codec);
      return processDataPlain(buf, count, column.schema.type);
    };

    return { id: chunk.begin, count, read, dict: true };
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
      const { header, consumed } = await pollPageHeader(this.r, offset);

      const start = position;
      const count = countForPageHeader(header);
      const dataBegin = offset + consumed;
      const dataEnd = dataBegin + header.compressed_page_size;

      // Some files don't have dictionarySize in the footer, so we have to catch it here.
      if (header.type === pq.PageType.DICTIONARY_PAGE) {
        if (chunk.dictionarySize) {
          throw new Error(`Found multiple dictionary pages`);
        }
        if (offset !== chunk.begin) {
          throw new Error(`Dictionary is not the first part`);
        }
        chunk.dictionarySize = dataEnd - chunk.begin;
        offset = dataEnd;
        continue;
      }

      offset = dataEnd;

      const read = async (): Promise<ColumnDataResult> => {
        const compressed = await this.r(dataBegin, dataEnd);
        const buf = await decompress(compressed, chunk.codec);

        switch (header.type) {
          case pq.PageType.DATA_PAGE:
            return processTypeDataPage(header, column.schema, buf);

          case pq.PageType.DATA_PAGE_V2:
            return processTypeDataPageV2(header, column.schema, buf);

          default:
            throw new Error(`Unsupported page type: ${header.type}`);
        }
      };

      // If this id a dict lookup, include the lookup 'id' and change the read type.
      if (isDictLookup(header)) {
        if (!chunk.dictionarySize) {
          throw new Error(`got dict lookup without dict`);
        }
        yield {
          id: offset,
          count,
          dict: false,
          lookup: chunk.begin,
          start,
          read: read as () => Promise<ColumnDataResultLookup>,
        };
      } else {
        yield { id: offset, count, dict: false, start, read };
      }

      position += count;
    }
  }

  columnLength() {
    return this.metadata!.columns.length;
  }

  rows() {
    return this.metadata!.rows;
  }

  groupsAt() {
    return this.metadata!.groups.map(({ start, end }) => {
      return { start, end };
    });
  }

  get groups() {
    return this.metadata!.groups.length;
  }
}
