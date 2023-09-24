import * as pq from '../../dep/thrift/parquet-code.js';
import type { SchemaLeafNode } from './schema.js';
import { typedArrayView } from '../view.js';
import { yieldDataRLE } from './process-rle.js';
import { processData } from './process.js';
import type { ColumnDataResult, Reader } from '../../types.js';
import { CompactProtocolReaderPoll, CompactProtocolReaderPoll_OutOfData } from 'thrift-tools';

export type RawPage = {
  header: pq.PageHeader;
  type: pq.PageType;
  begin: number;
  end: number;
};

const POLL_BY2_START = 6;
const POLL_BY2_END = 12;

/**
 * Poll the reader at the given location for a {@link parquet.PageHeader}. Basically we don't know
 * how long it's going to be but it's _likely_ to be pretty small (everything in the wild seems
 * to be <64 bytes).
 */
export async function pollPageHeader(r: Reader, at: number) {
  let header = new pq.PageHeader();
  let consumed = 0;

  // This assumes an increasing number of bytes to try to consume the header
  // It reads 64, 256, 1024, 4192, before giving up.
  // Most headers in the wild seem to be <=64, some are <=128.
  for (let i = POLL_BY2_START; i <= POLL_BY2_END; i += 2) {
    const guess = await r(at, at + (1 << i));
    const reader = new CompactProtocolReaderPoll(guess);

    try {
      header.read(reader);
    } catch (e) {
      if (i !== POLL_BY2_END && e instanceof CompactProtocolReaderPoll_OutOfData) {
        header = new pq.PageHeader();
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

  return { header, consumed };
}

/**
 * Return the number of rows in this page.
 */
export function countForPageHeader(header: pq.PageHeader) {
  if (header.data_page_header) {
    return header.data_page_header.num_values;
  }

  if (header.data_page_header_v2) {
    return header.data_page_header_v2.num_rows;
  }

  if (header.dictionary_page_header) {
    return header.dictionary_page_header.num_values;
  }

  throw new Error(`Could not handle type of PageHeader: ${header.type}`);
}

/**
 * Does the data under this header refer to the group dictionary?
 */
export function isDictLookup(header: pq.PageHeader): boolean {
  const encodings = [pq.Encoding.PLAIN_DICTIONARY, pq.Encoding.RLE_DICTIONARY];

  if (header.data_page_header) {
    return encodings.includes(header.data_page_header.encoding);
  }

  if (header.data_page_header_v2) {
    return encodings.includes(header.data_page_header_v2.encoding);
  }

  if (header.type === pq.PageType.DICTIONARY_PAGE) {
    throw new Error(`isDictLookup cannot be true for DICTIONARY_PAGE`);
  }

  throw new Error(`Could not handle type of PageHeader: ${header.type}`);
}

/**
 * Processes a {@link PageType.DATA_PAGE}.
 */
export function processTypeDataPage(
  header: pq.PageHeader,
  schema: SchemaLeafNode,
  data: Uint8Array,
): ColumnDataResult {
  const dpHeader = header.data_page_header!;
  const valueCount = dpHeader.num_values;

  const rlEncoding = dpHeader.repetition_level_encoding;
  const dlEncoding = dpHeader.definition_level_encoding;

  if (rlEncoding !== pq.Encoding.RLE || dlEncoding !== pq.Encoding.RLE) {
    // In V2, this is always the case.
    throw new Error(`expected DL/RL encoding of RLE`);
  }

  let effectiveValueCount = valueCount;

  if (schema.rl) {
    const dv = typedArrayView(data);
    const offset = dv.getUint32(0, true);
    data = data.subarray(offset + 4);

    // // TODO: We don't use these right now, so just skip decoding them
    // const rls = processData(
    //   rlEncoding,
    //   data,
    //   valueCount,
    //   Type.INT32,
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

  return processData(dpHeader.encoding, data, effectiveValueCount, schema.type, schema.typeLength);
}

/**
 * Processes a {@link PageType.DATA_PAGE_V2}.
 */
export function processTypeDataPageV2(
  header: pq.PageHeader,
  schema: SchemaLeafNode,
  data: Uint8Array,
): ColumnDataResult {
  throw new Error('TODO: V2');
}

/**
 * Return the number of bits required to store this number. Negative will return `NaN`.
 */
function getBitWidth(n: number) {
  return Math.ceil(Math.log2(n + 1));
}
