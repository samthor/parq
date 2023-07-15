import { Encoding, Type } from '../../dep/thrift/parquet-code.js';
import { processDataRLE } from './process-rle.js';
import { typedArrayView } from '../view.js';
import { type ColumnDataResult, DataType } from '../../types.js';

/**
 * Preprocesses a binary blob of data from Parquet so that it can be efficiently indexed by a
 * renderer. This only does the minimum required that might be 'slow' before returning the data.
 */
export function processData(
  encoding: Encoding,
  arr: Uint8Array,
  count: number,
  type: Type,
  typeLength: number,
): ColumnDataResult {
  switch (encoding) {
    case Encoding.PLAIN: {
      return processDataPlain(arr, count, type);
    }

    case Encoding.PLAIN_DICTIONARY: {
      // PLAIN_DICTIONARY is just RLE but with an initial typeLength byte instead of an expected
      // offset. The offset isn't "known" because RLE is just that, not a fixed up-front size.
      //
      // Note that this is the encoding of a data page that references into the dictionary, not the
      // dictionary itself (which is probably always `Encoding.PLAIN_DICTIONARY`).
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
        lookup: true, // indicate this will index something else
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
export function processDataPlain(arr: Uint8Array, count: number, type: Type): ColumnDataResult {
  switch (type) {
    case Type.BOOLEAN: {
      // The docs are super unclear; are these int32's, or is it bit-encoded.
      throw new Error(
        `TODO: found Type.BOOLEAN, how long is this? count=${count} arr.length=${arr.length}`,
      );
    }

    case Type.FIXED_LEN_BYTE_ARRAY: {
      return {
        type: DataType.FIXED_LENGTH_BYTE_ARRAY,
        arr,
      };
    }

    case Type.INT32: {
      return {
        type: DataType.INT32,
        arr: new Int32Array(arr.buffer, arr.byteOffset, arr.byteLength >>> 2),
      };
    }

    case Type.INT64: {
      return {
        type: DataType.INT64,
        arr: new BigInt64Array(arr.buffer, arr.byteOffset, arr.byteLength >>> 3),
      };
    }

    case Type.INT96: {
      // INT96 is deprecated: https://issues.apache.org/jira/browse/PARQUET-323
      // It's used to store "nanosec timestamp" only.
      // This return type is a bit funky for that reason.
      return {
        type: DataType.BIG_BYTE_ARRAY,
        size: 12,
        arr,
      };
    }

    case Type.FLOAT: {
      return {
        type: DataType.FLOAT,
        arr: new Float32Array(arr.buffer, arr.byteOffset, arr.byteLength >>> 2),
      };
    }

    case Type.DOUBLE: {
      return {
        type: DataType.DOUBLE,
        arr: new Float64Array(arr.buffer, arr.byteOffset, arr.byteLength >>> 3),
      };
    }

    case Type.BYTE_ARRAY: {
      // TODO: The data here is [uint32 length + bytes, ...]
      // It could be indexed here.
      return {
        type: DataType.LENGTH_BYTE_ARRAY,
        arr,
      };
    }
  }

  throw new Error(`Unsupported PLAIN type: ${type}`);
}
