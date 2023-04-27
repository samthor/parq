import { Encoding, ParquetType } from '../const.js';
import { processDataRLE } from './process-rle.js';
import { typedArrayView } from '../view.js';
import { DataResult, DataType } from '../../types.js';

/**
 * Preprocesses a binary blob of data from Parquet so that it can be efficiently indexed by a
 * renderer. This only does the minimum required that might be 'slow' before returning the data.
 */
export function processData(
  encoding: Encoding,
  arr: Uint8Array,
  count: number,
  type: ParquetType,
  typeLength: number,
): DataResult {
  switch (encoding) {
    case Encoding.PLAIN: {
      return processDataPlain(arr, count, type);
    }

    case Encoding.PLAIN_DICTIONARY: {
      // PLAIN_DICTIONARY is just RLE but with an initial typeLength byte instead of an expected
      // offset. The offset isn't "known" because RLE is just that, not a fixed up-front size.
      //
      // Note that this is the encoding of a data page that references into the dictionary, not the
      // dictionary itself (which is probably always `Encoding.PLAIN`).
      //
      // "Data page format: the bit width used to encode the entry ids stored as 1 byte (max bit
      // width = 32), followed by the values encoded using RLE/Bit packed described above (with the
      // given bit width)."
      //   https://parquet.apache.org/docs/file-format/data-pages/encodings/#dictionary-encoding-plain_dictionary--2-and-rle_dictionary--8
      const typeLength = arr[0];
      if (typeLength > 32) {
        throw new Error(`Bad PLAIN_DICTIONARY: typeLength=${typeLength}`);
      }

      const out = processDataRLE(arr.subarray(1), count, typeLength);
      return {
        lookup: 0, // indicate this will index something else
        ...out.pt,
      };
    }

    case Encoding.RLE: {
      const dv = typedArrayView(arr);
      const expectedOffset = dv.getUint32(0, true);

      const out = processDataRLE(arr.subarray(4), count, typeLength);
      if (out.offset !== expectedOffset) {
        throw new Error(`Got unexpected RLE length: ${out.offset}`);
      }

      return out.pt;
    }

    default:
      throw new Error(`Unsupported encoding: ${encoding}`);
  }
}

/**
 * Maps a plain section of Parquet data (i.e., values stored in regular bytes) to a matching JS
 * typed array view.
 */
export function processDataPlain(arr: Uint8Array, count: number, type: ParquetType): DataResult {
  switch (type) {
    case ParquetType.INT32: {
      return {
        type: DataType.INT32,
        arr: new Int32Array(arr.buffer, arr.byteOffset, arr.byteLength >>> 2),
      };
    }

    case ParquetType.INT64: {
      return {
        type: DataType.INT64,
        arr: new BigInt64Array(arr.buffer, arr.byteOffset, arr.byteLength >>> 3),
      };
    }

    case ParquetType.FLOAT: {
      return {
        type: DataType.FLOAT,
        arr: new Float32Array(arr.buffer, arr.byteOffset, arr.byteLength >>> 2),
      };
    }

    case ParquetType.DOUBLE: {
      return {
        type: DataType.DOUBLE,
        arr: new Float64Array(arr.buffer, arr.byteOffset, arr.byteLength >>> 3),
      };
    }

    case ParquetType.BYTE_ARRAY: {
      // TODO: The data here is [uint32 length + bytes, ...]
      // It could be indexed here.
      return {
        type: DataType.BYTE_ARRAY,
        arr,
      };
    }
  }

  throw new Error(`Unsupported PLAIN type: ${type}`);
}
