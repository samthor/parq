import { Encoding, Type } from '../../dep/thrift/parquet-code.js';
import { processDataRLE } from './process-rle.js';
import { toUint8Array, typedArrayView } from '../view.js';
import type { Data } from '../../types.js';

/**
 * Preprocesses a binary blob of data from Parquet via its encoding, converting it to flat data.
 */
export function processData(
  encoding: Encoding,
  arr: Uint8Array,
  count: number,
  type: Type,
  typeLength: number,
): Data {
  switch (encoding) {
    case Encoding.PLAIN: {
      return { raw: arr, count };
    }

    case Encoding.PLAIN_DICTIONARY:
    case Encoding.RLE_DICTIONARY: {
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
      //
      // RLE_DICTIONARY *seems* to purely be a synonym.
      const typeLength = arr[0];
      if (typeLength > 32 || typeLength <= 0) {
        throw new Error(`Bad PLAIN_DICTIONARY: typeLength=${typeLength}`);
      }

      const out = processDataRLE(arr.subarray(1), count, typeLength);
      return { raw: toUint8Array(out.int), count };
    }

    case Encoding.RLE: {
      const dv = typedArrayView(arr);
      const expectedOffset = dv.getUint32(0, true);

      const out = processDataRLE(arr.subarray(4), count, typeLength);
      if (out.offset !== expectedOffset) {
        throw new Error(`Got unexpected RLE length: ${out.offset}`);
      }
      return { raw: toUint8Array(out.int), count };
    }

    case Encoding.BYTE_STREAM_SPLIT: {
      // This is "collate" byte encoding and should only happen for FLOAT/DOUBLE.
      if (type !== Type.FLOAT && type !== Type.DOUBLE) {
        throw new Error(`cannot BYTE_STREAM_SPLIT on anything but float`);
      }

      // TODO: there's probably a way to do this in-place, but lazy.
      const copy = new Uint8Array(arr.length);

      const count2 = count + count;
      const count3 = count2 + count;
      for (let i = 0; i < count; ++i) {
        const t = i << 2;
        copy[t] = arr[i];
        copy[t + 1] = arr[count + i];
        copy[t + 2] = arr[count2 + i];
        copy[t + 3] = arr[count3 + i];
      }

      return { raw: copy, count };
    }

    case Encoding.DELTA_BINARY_PACKED: {
      throw new Error(`TODO: build DELTA_BINARY_PACKED`);
    }

    case Encoding.DELTA_BYTE_ARRAY: {
      throw new Error(`TODO: build DELTA_BYTE_ARRAY`); // this uses DELTA_BINARY_PACKED internally
    }
  }

  throw new Error(`Unsupported encoding: ${encoding}`);
}
