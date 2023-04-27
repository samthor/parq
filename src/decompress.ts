import { CompressionCodec } from './const.js';
import { decompress as snappyDecompress } from './snappy/decompress.js';

/**
 * Decompress a page of Parquet data encoded in one of {@link CompressionCodec}.
 *
 * If the data is uncompressed, this may return the input value.
 */
export async function decompress(arr: Uint8Array, codec: CompressionCodec): Promise<Uint8Array> {
  switch (codec) {
    case CompressionCodec.UNCOMPRESSED:
      return arr;

    case CompressionCodec.SNAPPY:
      console.time('snappy-' + arr.length);
      try {
        return snappyDecompress(arr);
      } finally {
        console.timeEnd('snappy-' + arr.length);
      }

    case CompressionCodec.GZIP:
      throw new Error(`TODO: implement via browser/node support`);

    default:
      throw new Error(`Unsuppored compression: ${codec}`);
  }
}
