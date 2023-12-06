import { CompactProtocolReader } from 'thrift-tools';
import * as pq from '../../dep/thrift/parquet-code.js';
import { decodeSchema, type SchemaLeafNode } from './schema.js';

export type Chunk = {
  begin: number;
  end: number;
  dictionarySize: number;
  codec: pq.CompressionCodec;
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
  const reader = new CompactProtocolReader(buf);

  const s = new pq.FileMetaData();
  s.read(reader); // TODO: for ~48mb metadata, this takes ~500ms - maybe that's fine?

  if (reader.at !== buf.length) {
    throw new Error(`did not consume all metadata: at=${reader.at} len=${buf.length}`);
  }

  const schemaNode = decodeSchema(s.schema);
  const allColumns: FileMetadata['columns'] = schemaNode.columns.map((schema) => {
    return {
      chunks: [],
      schema,
    };
  });

  // Most small (~mb) Parquet files just have a single group.
  let currentRow = 0;
  const groups = s.row_groups.map((group) => {
    if (typeof group.num_rows !== 'number') {
      throw new Error(`Got non-row'ed group: ${group.num_rows}`);
    }

    const start = currentRow;
    const end = (currentRow += group.num_rows);

    const rawColumnChunks = group.columns;

    const columnChunks = allColumns.map((o, i) => {
      const rawChunk = rawColumnChunks[i];
      const metadata = rawChunk.meta_data!;

      if (metadata.type !== o.schema.type) {
        throw new TypeError(`Got chunk type=${metadata.type}, schema type=${o.schema.type}`);
      } else if (!metadata.data_page_offset) {
        throw new Error(`Did not find page offset`);
      }

      const begin = metadata.dictionary_page_offset ?? metadata.data_page_offset;
      if (begin < 0) {
        // This happened a lot when the zigzag decoder was borked.
        throw new Error(`got -ve begin for page location: ${begin}`);
      }

      let dictionarySize = 0;
      if (metadata.dictionary_page_offset) {
        dictionarySize = metadata.data_page_offset - metadata.dictionary_page_offset;
      }

      // Some files don't properly indicate 'end', so calculate from begin + size.
      let end = rawChunk.file_offset;
      if (end === begin || end === 0) {
        if (metadata.total_compressed_size <= 0) {
          throw new Error(`got bad total_compressed_size=${metadata.total_compressed_size}`);
        }
        end = begin + metadata.total_compressed_size;
      }

      const chunk: Chunk = {
        begin,
        end,
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
