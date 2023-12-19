import * as pq from '../dep/thrift/parquet-code.js';
import { decompress } from './decompress.js';
import { parseFileMetadata, type FileMetadata } from './parts/file-metadata.js';
import { toUint16Array, toUint32Array, typedArrayView } from './view.js';
import { ColumnInfo, Part, ParquetReader, Reader, Data, UintArray } from '../types.js';
import {
  countForPageHeader,
  isDictLookup,
  pollPageHeader,
  processTypeDataPage,
  processTypeDataPageV2,
} from './parts/page.js';

export async function buildReader(r: Reader | Promise<Reader>): Promise<ParquetReader> {
  r = await r;
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
  lookup: boolean;
  cache?: Promise<Data>;

  // where the read actually happens (not header)
  dataBegin: number;
  dataEnd: number; // ... but can be used to skip this ReadDesc
};

export class ParquetReaderImpl implements ParquetReader {
  constructor(public r: Reader, public metadata: FileMetadata) {}

  private refs = new Map<number, ReadDesc>();

  /**
   * Provides a {@link AsyncGenerator} over the pages within this column/group. This doesn't read
   * the underlying data from the source, but lets the caller invoke `readAt` or `lookupAt`.
   */
  async *load(columnNo: number, groupNo: number): AsyncGenerator<Part, void, void> {
    const group = this.metadata.groups[groupNo];
    const column = this.metadata.columns[columnNo];
    const chunk = column.chunks[groupNo];

    if (!group || !column) {
      throw new Error(`invalid columnNo=${columnNo} groupNo=${groupNo}`);
    }

    let position = group.start;
    let offset = chunk.begin;

    while (offset < chunk.end) {
      const prev = this.refs.get(offset);
      if (prev !== undefined) {
        if (prev.part !== undefined) {
          // skips dict-only parts
          yield { ...prev.part };
        }
        offset = prev.dataEnd;
        continue;
      }

      const { header, consumed } = await pollPageHeader(this.r, offset);
      const dataBegin = offset + consumed;
      const dataEnd = dataBegin + header.compressed_page_size;

      // Detect and read dictionary pages. It should only be the 1st page, because other pages
      // have no way to reference a "specific" dictionary - it's only the start.
      // There should only be one, so check that it's at the start.
      if (header.type === pq.PageType.DICTIONARY_PAGE) {
        if (offset !== chunk.begin) {
          throw new Error(`Dictionary is not the first part`);
        }
        const dictionaryHeader = header.dictionary_page_header;
        if (
          dictionaryHeader?.encoding !== pq.Encoding.PLAIN_DICTIONARY &&
          dictionaryHeader?.encoding !== pq.Encoding.PLAIN
        ) {
          // TODO: Is this always true? This lets us just pass the data back as a buffer.
          throw new Error(`Unexpected dictionary encoding: ${dictionaryHeader?.encoding}`);
        }

        this.refs.set(offset, {
          columnNo,
          groupNo,
          header,
          dataBegin,
          dataEnd,
          lookup: false,
        });
        offset = dataEnd;
        continue;
      }

      const count = countForPageHeader(header);

      // Part is directly yielded to the end-user.
      const part: Part = {
        at: offset,
        start: position,
        end: position + count,
        lookup: 0,
      };
      let lookup = false;

      if (isDictLookup(header)) {
        part.lookup = offset; // lookup data is _here_
        part.at = chunk.begin; // "normal" is dict, which is at the 1st chunk
        lookup = true;
      }
      this.refs.set(offset, {
        groupNo,
        columnNo,
        header,
        dataBegin,
        dataEnd,
        part,
        lookup,
      });
      yield { ...part };

      position += count;
      offset = dataEnd;
    }
  }

  groupIndex(row: number) {
    // TODO: binary search
    return this.metadata.groups.findIndex((g) => {
      return row >= g.start && row < g.end;
    });
  }

  async *loadRange(columnNo: number, start: number, end: number): AsyncGenerator<Part, void, void> {
    start = Math.max(0, start);
    end = Math.min(this.metadata.rows, end);

    if (end <= start) {
      return; // nothing to yield
    }

    // TODO: this could be very efficient but now we just scan and find the range.

    const startGroup = this.groupIndex(start);
    const endGroup = this.groupIndex(end - 1);
    if (startGroup === -1 || endGroup === -1) {
      throw new Error(`invalid group data`);
    }

    let hasPrefix = false;

    for (let g = startGroup; g <= endGroup; ++g) {
      const gen = this.load(columnNo, g);
      // TODO: this just scans for the start/end
      for await (const part of gen) {
        if (!hasPrefix) {
          if (part.start > start) {
            continue;
          }
          hasPrefix = true;
        }

        if (part.start >= end) {
          return;
        }

        yield part;
      }
    }
  }

  info() {
    const columns = this.metadata.columns.map(({ schema }): ColumnInfo => {
      return {
        name: schema.name,
        typeLength: schema.typeLength,
        physicalType: schema.type,
        logicalType: schema.logicalType,
      };
    });

    const groups = this.metadata.groups.map(({ start, end }) => ({ start, end }));

    return { columns, groups, rows: this.metadata.rows };
  }

  async internalRead(desc: ReadDesc): Promise<Data> {
    const column = this.metadata.columns[desc.columnNo];
    const chunk = column.chunks[desc.groupNo];

    const compressed = await this.r(desc.dataBegin, desc.dataEnd);

    const buf = await decompress(compressed, chunk.codec, chunk.uncompressedSize);
    if (buf.length === 0 && compressed.length !== 0) {
      // zstddec can sometimes return nothing?
      throw new Error(`got zero decompress srcLength=${compressed.length} codec=${chunk.codec}`);
    }

    switch (desc.header.type) {
      case pq.PageType.DICTIONARY_PAGE:
        return { raw: buf, count: countForPageHeader(desc.header) };

      case pq.PageType.DATA_PAGE:
        return processTypeDataPage(desc.header, column.schema, buf);

      case pq.PageType.DATA_PAGE_V2:
        return processTypeDataPageV2(desc.header, column.schema, buf);
    }

    throw new Error(`Unsupported page type: ${desc.header.type}`);
  }

  cacheRead(desc: ReadDesc): Promise<Data> {
    if (desc.cache) {
      return desc.cache.then((d) => {
        // This checks for detached/transfered data, if the length is now zero.
        // TODO: could use `.detached` but it's VERY NEW (Dec-2023)
        if (d.count !== 0 && d.raw.length === 0) {
          desc.cache = undefined;
          return this.cacheRead(desc);
        }
        return d;
      });
    }
    const c = this.internalRead(desc);
    desc.cache = c;
    return c;
  }

  async readAt(at: number): Promise<Data> {
    const desc = this.refs.get(at);
    if (desc === undefined || desc.lookup) {
      // can't read real data
      throw new Error(`no data for ${at}, load() first (lookup=${desc?.lookup})`);
    }
    return this.cacheRead(desc);
  }

  async lookupAt(at: number): Promise<UintArray> {
    const desc = this.refs.get(at);
    if (desc === undefined || !desc.lookup) {
      // can't read lookup data
      throw new Error(`no lookup data for ${at}, load() first (lookup=${desc?.lookup})`);
    }
    const data = await this.cacheRead(desc);

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

  purge() {
    // this just clears the large(?) buffers we might be holding
    for (const o of this.refs.values()) {
      o.cache = undefined;
    }
  }
}
