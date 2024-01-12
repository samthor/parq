
parq is a Parquet reader in JavaScript.
Install from NPM via "parq".

## Usage

You can build a reader and then iterate over its contents, yielding a `Uint8Array` for each value:

```js
import { buildReader, flatIterate } from 'parq';

const bytes = /* Uint8Array from somewhere */;
const pr = await buildReader(bytes);

// iterate over the data in rows 100-200 of column zero
const it = flatIterate(pr, 0, 100, 200);

let i = 100;
for await (const value of it) {
  console.info(`col0 row${i}=`, value);
  ++i;
}
```

It's a bit awkward to receive a `Uint8Array` per-value, but it matches how Parquet works: it has a variety of primitive data types _as well as_ the `BYTE_ARRAY` type which has variable length.
This type is usually used for UTF-8 encoded strings.

To find out what type is used per-column, check `pr.info().columns` for their name, type, and so on, before indexing.

### Advanced Usage

You can access the low-level methods on `ParquetReader` to read raw page data directly.
These need a little bit of work to eventually render, but this means you can process the data more efficiently.

You can also pass a `Reader` implementation to `buildReader` instead of raw bytes.
This is a method which reads bytes in a specific range, useful if you are processing large files and don't want to read it from disk or network all at once.

## Support

This is missing support for Parquet files that use:

- data pages v2
- compression codecs `LZO`, `BROTLI`, `LZ4`, `LZ4_RAW`
- possibly complex nested schemas.

It supports compressions `SNAPPY`, `GZIP`, and `ZSTD` _via_ a dynamic import of the [zstddec](https://www.npmjs.com/package/zstddec) package.
If you need `ZSTD`, install "ztsdec" and instruct your bundler to use it.
(I can see adding [brotli-wasm](https://www.npmjs.com/package/brotli-wasm) for `BROTLI` if it's needed in the same way.)

## Demo

There's a simple demo [on GitHub Pages](https://samthor.github.io/parq/), with the source in [demo](./demo).
This uses a `Worker` to process Parquet data remotely, which means that this code can trivially handle gigabyte or more file sizes.
It implements a remote `ParquetReader` that connects to the worker.
