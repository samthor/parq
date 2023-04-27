import { TCompactProtocolReader } from '../thrift/reader.js';
import * as parquet from '../../dep/thrift/gen-nodejs/parquet.js';
import { decodeSchema, type SchemaLeafNode } from './schema.js';
import type { CompressionCodec } from '../const.js';

export type Chunk = {
  begin: number;
  end: number;
  dictionarySize: number;
  codec: CompressionCodec;
  columnNo: number;
};

export type FileMetadata = {
  columns: {
    chunks: Chunk[];
    schema: SchemaLeafNode;
  }[];

  groups: {
    start: number;
    end: number;
    columns: Chunk[];
  }[];

  rows: number;
};

/**
 * Parse a metadata section of a Parquet file.
 */
export function parseFileMetadata(buf: Uint8Array): FileMetadata {
  const reader = new TCompactProtocolReader(buf);

  const s = new parquet.FileMetaData();
  s.read(reader); // TODO: for ~48mb metadata, this takes ~500ms - maybe that's fine?

  const schemaNode = decodeSchema(s.schema);
  const allColumns: FileMetadata['columns'] = schemaNode.columns.map((schema) => {
    return {
      chunks: [],
      schema,
    };
  });

  // Most small (~mb) Parquet files just have a single group.
  let currentRow = 0;
  const groups = (s.row_groups as InstanceType<typeof parquet.RowGroup>[]).map((group) => {
    if (typeof group.num_rows !== 'number') {
      throw new Error(`Got non-row'ed group: ${group.num_rows}`);
    }

    const start = currentRow;
    const end = (currentRow += group.num_rows);

    const rawColumnChunks = group.columns as InstanceType<typeof parquet.ColumnChunk>[];

    const columnChunks = allColumns.map((o, i) => {
      const rawChunk = rawColumnChunks[i];
      const metadata = rawChunk.meta_data as InstanceType<typeof parquet.ColumnMetaData>;

      if (metadata.type !== o.schema.type) {
        throw new TypeError(`Got chunk type=${metadata.type}, schema type=${o.schema.type}`);
      } else if (!metadata.data_page_offset) {
        throw new Error(`Did not find page offset`);
      }

      // TODO: The dictionary is always the first-N bytes, but it could be identified here so that
      // the caller can read in parallel.
      const begin = metadata.dictionary_page_offset ?? metadata.data_page_offset;
      let dictionarySize = 0;
      if (metadata.dictionary_page_offset) {
        dictionarySize = metadata.data_page_offset; // this is _from_ the dictionary
      }

      const chunk: Chunk = {
        begin,
        end: rawChunk.file_offset,
        dictionarySize,
        codec: metadata.codec,
        columnNo: i,
      };
      o.chunks.push(chunk);
      return chunk;
    });

    return {
      start,
      end,
      columns: columnChunks,
    };
  });

  return {
    columns: allColumns,
    groups,
    rows: s.num_rows,
  };
}
