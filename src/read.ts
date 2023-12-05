import * as pq from '../dep/thrift/parquet-code.js';
import { decompress } from './decompress.js';
import { parseFileMetadata, type FileMetadata } from './parts/file-metadata.js';
import { toUint16Array, toUint32Array, typedArrayView } from './view.js';
import { ColumnInfo, Part, ParquetReader, Reader, Data } from '../types.js';
import {
  countForPageHeader,
  isDictLookup,
  pollPageHeader,
  processTypeDataPage,
  processTypeDataPageV2,
} from './parts/page.js';

export async function buildReader(r: Reader): Promise<ParquetReader> {
  const td = new TextDecoder('utf-8');

  const headerPromise = r(0, 4);
  const footerPromise = r(-8);

  const headerBytes = await headerPromise;
  if (td.decode(headerBytes) !== 'PAR1') {
    throw new Error(`Not a parquet file: ${r}`);
  }

  const footerBytes = await footerPromise;
  if (td.decode(footerBytes.subarray(4)) !== 'PAR1') {
    throw new Error(`Not a parquet file: ${r}`);
  }

  const v = typedArrayView(footerBytes);
  const metadataSize = v.getUint32(0, true);
  const metadataBytes = await r(-8 - metadataSize, -8);

  const metadata = parseFileMetadata(metadataBytes);

  return new ParquetReaderImpl(r, metadata);
}

type ReadDesc = {
  columnNo: number;
  groupNo: number;
  header: pq.PageHeader;
  part?: Part;
  at: number;
  lookup: boolean;
};

export class ParquetReaderImpl implements ParquetReader {
  constructor(public r: Reader, public metadata: FileMetadata) {}

  private refs = new Map<number, ReadDesc>();

  /**
   * Reads the dictionary part for the given column/group.
   *
   * This reads just the header immediately, like {@link #indexColumnGroup}.
   */
  async ensureDictFor(columnNo: number, groupNo: number): Promise<boolean> {
    const column = this.metadata.columns[columnNo];
    const chunk = column.chunks[groupNo];

    if (this.refs.has(chunk.begin)) {
      return false;
    }

    const { header, consumed } = await pollPageHeader(this.r, chunk.begin);

    if (chunk.dictionarySize === 0) {
      // Check if there was actually a dictionary in the 1st position.
      if (header.type !== pq.PageType.DICTIONARY_PAGE) {
        return false;
      }
      chunk.dictionarySize = consumed + header.compressed_page_size;
    }

    const dataBegin = chunk.begin + consumed;
    const dataEnd = dataBegin + header.compressed_page_size;

    if (dataEnd !== chunk.begin + chunk.dictionarySize) {
      throw new Error(`Got inconsistent dictionary size`);
    }
    if (header.type !== pq.PageType.DICTIONARY_PAGE) {
      throw new Error(`Got invalid type for dict: ${header.type}`);
    }
    const dictionaryHeader = header.dictionary_page_header!;
    if (
      dictionaryHeader.encoding !== pq.Encoding.PLAIN_DICTIONARY &&
      dictionaryHeader.encoding !== pq.Encoding.PLAIN
    ) {
      // TODO: Is this always true? This lets us just pass the data back as a buffer.
      throw new Error(`Unexpected dictionary encoding: ${dictionaryHeader.encoding}`);
    }

    this.refs.set(chunk.begin, {
      columnNo,
      groupNo,
      header,
      at: dataBegin,
      lookup: false,
    });

    return true;
  }

  /**
   * Provides a {@link AsyncGenerator} over the pages within this column/group. This doesn't read
   * the underlying data from the source until `read()` is called.
   */
  async *load(columnNo: number, groupNo: number): AsyncGenerator<Part, void, void> {
    const group = this.metadata.groups[groupNo];
    const column = this.metadata.columns[columnNo];
    const chunk = column.chunks[groupNo];

    if (!group || !column) {
      throw new Error(`invalid column/group`);
    }

    let position = group.start;
    let offset = chunk.begin + chunk.dictionarySize;
    while (offset < chunk.end) {
      const prev = this.refs.get(offset);
      if (prev !== undefined) {
        if (prev.part !== undefined) {
          // skips dict-only parts
          yield { ...prev.part };
        }
        offset += prev.header.compressed_page_size;
        continue;
      }

      const { header, consumed } = await pollPageHeader(this.r, offset);

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

      const part: Part = {
        at: offset,
        start: position,
        end: position + count,
        lookup: 0,
      };
      let lookup = false;

      if (isDictLookup(header)) {
        // Ensure that we're valid.
        await this.ensureDictFor(columnNo, groupNo);
        part.lookup = dataBegin;
        part.at = chunk.begin;
        lookup = true;
      }
      this.refs.set(dataBegin, {
        groupNo,
        columnNo,
        header,
        at: dataBegin,
        part,
        lookup,
      });
      yield { ...part };

      position += count;
    }
  }

  info() {
    const columns = this.metadata.columns.map((x): ColumnInfo => {
      const raw = x.schema.raw;
      return {
        name: x.schema.name,
        typeLength: x.schema.typeLength,
        physicalType: raw.type!,

        // TODO: this is generally not populated, but there is a documented mapping (...for most things)
        //   https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
        logicalType: raw.logicalType,
      };
    });

    const groups = this.metadata.groups.map(({ start, end }) => ({ start, end }));

    return { columns, groups };
  }

  async internalRead(desc: ReadDesc) {
    const column = this.metadata.columns[desc.columnNo];
    const chunk = column.chunks[desc.groupNo];

    const end = desc.at + desc.header.compressed_page_size;
    const compressed = await this.r(desc.at, end);
    const buf = await decompress(compressed, chunk.codec);

    switch (desc.header.type) {
      case pq.PageType.DICTIONARY_PAGE:
        return { raw: buf, count: countForPageHeader(desc.header) };

      case pq.PageType.DATA_PAGE:
        return processTypeDataPage(desc.header, column.schema, buf);

      case pq.PageType.DATA_PAGE_V2:
        return processTypeDataPageV2(desc.header, column.schema, buf);

      default:
        throw new Error(`Unsupported page type: ${desc.header.type}`);
    }
  }

  async readAt(at: number): Promise<Data> {
    const desc = this.refs.get(at);
    if (desc === undefined || desc.lookup) {
      // can't read real data
      throw new Error(`no data for ${at}, load() first`);
    }
    return this.internalRead(desc);
  }

  async lookupAt(at: number): Promise<ArrayLike<number>> {
    const desc = this.refs.get(at);
    if (desc === undefined || !desc.lookup) {
      // can't read lookup data
      throw new Error(`no lookup data for ${at}, load() first (lookup=${desc?.lookup})`);
    }
    const data = await this.internalRead(desc);

    // shouldn't get rem, just in case
    const rem = data.raw.length % data.count;
    const bytesPer = (data.raw.length - rem) / data.count;

    switch (bytesPer) {
      case 1:
        return data.raw;
      case 2:
        return toUint16Array(data.raw);
      case 4:
        return toUint32Array(data.raw);
    }
    throw new Error(`invalid bytesPer for index: ${bytesPer}`);
  }
}
