import * as parquet from '../../dep/thrift/gen-nodejs/parquet';
import { TCompactProtocolReader } from '../thrift/reader';
import { Encoding, PageType } from '../const';
import { SchemaLeafNode } from './schema';
import { typedArrayView } from '../view';
import { yieldDataRLE } from './process-rle';
import { processData } from './process';
import { DataResult } from '../../types';

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
