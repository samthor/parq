import * as parquet from '../../dep/thrift/gen-nodejs/parquet.js';
import { TCompactProtocolReader } from '../thrift/reader.js';
import { Encoding, PageType } from '../const.js';
import { SchemaLeafNode } from './schema.js';
import { typedArrayView } from '../view.js';
import { yieldDataRLE } from './process-rle.js';
import { processData } from './process.js';
import { DataResult } from '../../types.js';

export type RawPage = {
  header: InstanceType<typeof parquet.PageHeader>;
  type: PageType;
  begin: number;
  end: number;
};

/**
 * Reads a single page (Thrift-encoded header + byte range for data) from the passed data.
 */
export function readPage(arr: Uint8Array, offset: number = 0): RawPage {
  const reader = new TCompactProtocolReader(arr, offset);

  const header = new parquet.PageHeader();
  header.read(reader);

  const begin = reader.at;
  const end = begin + (header.compressed_page_size ?? 0);

  return {
    header,
    type: header.type as PageType,
    begin,
    end,
  };
}

/**
 * Return the number of rows in this page.
 */
export function countForPageHeader(header: InstanceType<typeof parquet.PageHeader>) {
  if (header.data_page_header) {
    const dpHeader = header.data_page_header as InstanceType<typeof parquet.DataPageHeader>;
    return dpHeader.num_values as number;
  }

  if (header.data_page_header_v2) {
    const dpHeader = header.data_page_header_v2 as InstanceType<typeof parquet.DataPageHeaderV2>;
    return dpHeader.num_rows as number;
  }

  if (header.dictionary_page_header) {
    const dictHeader = header.dictionary_page_header as InstanceType<typeof parquet.DictionaryPageHeader>;
    return dictHeader.num_values as number;
  }

  throw new Error(`Could not handle type of PageHeader: ${header.type}`);
}

/**
 * Does the data under this header refer to the group dictionary?
 */
export function isDictLookup(header: InstanceType<typeof parquet.PageHeader>): boolean {
  const encodings = [Encoding.PLAIN_DICTIONARY, Encoding.RLE_DICTIONARY];

  if (header.data_page_header) {
    const dpHeader = header.data_page_header as InstanceType<typeof parquet.DataPageHeader>;
    return encodings.includes(dpHeader.encoding);
  }

  if (header.data_page_header_v2) {
    const dpHeader = header.data_page_header_v2 as InstanceType<typeof parquet.DataPageHeaderV2>;
    return encodings.includes(dpHeader.encoding);
  }

  throw new Error(`Could not handle type of PageHeader: ${header.type}`);
}

/**
 * Processes a {@link PageType.DATA_PAGE}.
 */
export function processTypeDataPage(
  header: InstanceType<typeof parquet.PageHeader>,
  schema: SchemaLeafNode,
  data: Uint8Array,
): DataResult {
  const dpHeader = header.data_page_header as InstanceType<typeof parquet.DataPageHeader>;
  const valueCount = dpHeader.num_values as number;

  const rlEncoding = dpHeader.repetition_level_encoding as Encoding;
  const dlEncoding = dpHeader.definition_level_encoding as Encoding;

  if (rlEncoding !== Encoding.RLE || dlEncoding !== Encoding.RLE) {
    // In V2, this is always the case.
    throw new Error(`expected DL/RL encoding of RLE`);
  }

  const encoding = dpHeader.encoding as Encoding;

  let effectiveValueCount = valueCount;

  if (schema.rl) {
    const dv = typedArrayView(data);
    const offset = dv.getUint32(0, true);
    data = data.subarray(offset + 4);

    // // TODO: We don't use these right now, so just skip them
    // const rls = processData(
    //   rlEncoding,
    //   data,
    //   valueCount,
    //   ParquetType.INT32,
    //   getBitWidth(schema.rl),
    // );
    // data = data.subarray(rls.sourceLength);
  }

  if (schema.dl) {
    const dv = typedArrayView(data);
    const offset = dv.getUint32(0, true);

    // This is likely sparse so don't expand into an array, just use a generator.
    const gen = yieldDataRLE(data.subarray(4), valueCount, getBitWidth(schema.dl));
    for (const v of gen) {
      if (v.value !== schema.dl) {
        effectiveValueCount -= v.count;
      }
    }

    data = data.subarray(offset + 4);
  }

  return processData(encoding, data, effectiveValueCount, schema.type, schema.typeLength);
}

/**
 * Processes a {@link PageType.DATA_PAGE_V2}.
 */
export function processTypeDataPageV2(
  header: InstanceType<typeof parquet.PageHeader>,
  schema: SchemaLeafNode,
  data: Uint8Array,
): DataResult {
  throw new Error('TODO: V2');
}

/**
 * Return the number of bits required to store this number. Negative will return `NaN`.
 */
function getBitWidth(n: number) {
  return Math.ceil(Math.log2(n + 1));
}
