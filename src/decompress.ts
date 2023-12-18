import { CompressionCodec } from '../dep/thrift/parquet-code.js';
import { streamToBytes } from './helper/bytes.ts';
import { decompress as snappyDecompress } from './snappy/decompress.js';

type DecompressFn = (raw: Uint8Array, uncompressedSize?: number) => Uint8Array;

let zstdPromise: Promise<DecompressFn> | void;

async function prepareZstd(): Promise<DecompressFn> {
  const zstd = await import('zstddec');
  const dec = new zstd.ZSTDDecoder();
  await dec.init();
  return (arr, uncompressedSize) => dec.decode(arr, uncompressedSize);
}

/**
 * Decompress a page of Parquet data encoded in one of {@link CompressionCodec}.
 *
 * If the data is uncompressed, this may return the input value.
 */
export async function decompress(
  arr: Uint8Array,
  codec: CompressionCodec,
  uncompressedSize: number,
): Promise<Uint8Array> {

  switch (codec) {
    case CompressionCodec.UNCOMPRESSED:
      return arr;

    case CompressionCodec.GZIP: {
      const s = new DecompressionStream('gzip');
      s.writable.getWriter().write(arr); // TODO: await?
      const reader: ReadableStreamDefaultReader<Uint8Array> = s.readable.getReader();
      return streamToBytes(reader);
    }
  
    case CompressionCodec.SNAPPY:
      return snappyDecompress(arr);

    case CompressionCodec.ZSTD: {
      if (zstdPromise === undefined) {
        zstdPromise = prepareZstd();
      }
      const decompress = await zstdPromise;

      // TODO: sometimes `uncompressedSize` is the wrong size, and zstddec just returns an
      // empty array - don't pass it through for now

      return decompress(arr);
    }
  }

  throw new Error(`Unsupported codec: ${codec}`)
}
