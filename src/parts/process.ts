import { Encoding, Type } from '../../dep/thrift/parquet-code.js';
import { type IntArray, processDataRLE } from './process-rle.js';
import { typedArrayView } from '../view.js';
import type { ColumnData } from '../../types.js';

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
): ColumnData {
  let int: IntArray;

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
      if (typeLength > 32 || typeLength <= 0) {
        throw new Error(`Bad PLAIN_DICTIONARY: typeLength=${typeLength}`);
      }

      const out = processDataRLE(arr.subarray(1), count, typeLength);
      int = out.int;
      break;
    }

    case Encoding.RLE: {
      const dv = typedArrayView(arr);
      const expectedOffset = dv.getUint32(0, true);

      const out = processDataRLE(arr.subarray(4), count, typeLength);
      if (out.offset !== expectedOffset) {
        throw new Error(`Got unexpected RLE length: ${out.offset}`);
      }
      int = out.int;
      break;
    }

    default:
      // TODO: implement these (e.g., DELTA_BINARY_PACKED)
      throw new Error(`Unsupported encoding: ${encoding}`);
  }

  return {
    bitLength: int.BYTES_PER_ELEMENT * 8,
    fp: false,
    raw: int instanceof Uint8Array ? int : new Uint8Array(int.buffer),
    count,
    index: false, // replaced later
  };
}

/**
 * Maps a plain section of Parquet data (i.e., values stored in regular bytes) to a matching JS
 * typed array view.
 */
export function processDataPlain(arr: Uint8Array, count: number, type: Type): ColumnData {
  switch (type) {
    case Type.BOOLEAN: {
      // The docs basically say this is bit-encoded in a sane way.
      return {
        type,
        bitLength: 1,
        fp: false,
        raw: arr,
        count,
        index: false, // replaced later
      };
    }

    case Type.FIXED_LEN_BYTE_ARRAY: {
      // TODO: this might be derivable here, _but_, it's also in the schema

      throw new Error(`TODO: this is a fixed size? arr.length=${arr.length} count=${count}`);
    }

    case Type.INT32: {
      return {
        type,
        bitLength: 32,
        fp: false,
        raw: arr,
        count,
        index: false, // replaced later
      };
    }

    case Type.INT64: {
      return {
        type,
        bitLength: 64,
        fp: false,
        raw: arr,
        count,
        index: false, // replaced later
      };
    }

    case Type.INT96: {
      // INT96 is deprecated: https://issues.apache.org/jira/browse/PARQUET-323
      // It's used to store "nanosec timestamp" only.
      return {
        type,
        bitLength: 96,
        fp: false,
        raw: arr,
        count,
        index: false, // replaced later
      };
    }

    case Type.FLOAT: {
      return {
        type,
        bitLength: 32,
        fp: true,
        raw: arr,
        count,
        index: false, // replaced later
      };
    }

    case Type.DOUBLE: {
      return {
        type,
        bitLength: 64,
        fp: true,
        raw: arr,
        count,
        index: false, // replaced later
      };
    }

    case Type.BYTE_ARRAY: {
      // TODO: The data here is [uint32 length + bytes, ...]
      // It could be indexed here.
      return {
        type,
        bitLength: 0,
        fp: false,
        raw: arr,
        count,
        index: false, // replaced later
      };
    }
  }

  throw new Error(`Unsupported PLAIN type: ${type}`);
}
