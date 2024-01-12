import type * as pq from './dep/thrift/parquet-code.ts';
import type { iterateLengthByteArray } from './src/length-array.ts';

// nb. not actually a type, an enum
export { Type as PhysicalType } from './dep/thrift/parquet-code.ts';
import { Type as PhysicalTypeEnum } from './dep/thrift/parquet-code.ts';

export type UintArray = Uint8Array | Uint16Array | Uint32Array;

type PhysicalType = pq.Type;

/**
 * Returns the length, in bits, of the passed Parquet physical type and `typeLength` argument.
 * This is in bits because {@link pq.Type.BOOLEAN} is a single bit, and you need to index into that data using bitwise operations.
 *
 * The type length is only used for {@link pq.Type.FIXED_LEN_BYTE_ARRAY}.
 *
 * Returns zero for {@link pq.Type.BYTE_ARRAY}.
 */
export function bitLength(t: pq.Type, typeLength: number) {
  switch (t) {
    case PhysicalTypeEnum.BOOLEAN:
      return 1;
    case PhysicalTypeEnum.INT32:
    case PhysicalTypeEnum.FLOAT:
      return 32;
    case PhysicalTypeEnum.INT64:
    case PhysicalTypeEnum.DOUBLE:
      return 64;
    case PhysicalTypeEnum.INT96:
      return 96;
    case PhysicalTypeEnum.FIXED_LEN_BYTE_ARRAY:
      return typeLength * 8;
    case PhysicalTypeEnum.BYTE_ARRAY:
      return 0;
    default:
      throw new Error(`bad physicalType: ${t}`);
  }
}

export type ParquetReader = {
  info(): ParquetInfo;

  /**
   * Purge any caches used by this reader.
   */
  purge(): void;

  /**
   * Loads a specific column/group. This itself may internally have many parts.
   */
  load(columnNo: number, groupNo: number): AsyncGenerator<Part, void, void>;

  /**
   * Loads the parts for a specific range.
   */
  loadRange(columnNo: number, start: number, end: number): AsyncGenerator<Part, void, void>;

  /**
   * Reads the Parquet data at the given location. This may be dictionary data.
   *
   * This is _not_ already indexed! For all types except {@link pq.Type.BYTE_ARRAY}, the length can
   * be known by indexing into the resulting data based on {@link bitLength}. For a byte array, you
   * can use {@link iterateLengthByteArray}.
   */
  readAt(at: number): Promise<Data>;

  /**
   * Reads the lookup data at the given location. This always indexes into dictionary data, but
   * with that data's rules (e.g., for {@link pq.Type.BYTE_ARRAY}, you have to index twice).
   */
  lookupAt(at: number): Promise<UintArray>;
};

export type ParquetInfo = {
  groups: GroupInfo[];
  columns: ColumnInfo[];
  rows: number;
}

export type GroupInfo = {
  start: number;
  end: number;
};

export type ColumnInfo = {
  name: string;

  /**
   * Non-zero if the physical type is {@link pq.Type.FIXED_LEN_BYTE_ARRAY}. Specifies the length in
   * bytes.
   */
  typeLength: number;

  /**
   * Parquet's underlying physical type.
   */
  physicalType: PhysicalType;

  /**
   * Any logical type set alongside the physical type.
   */
  logicalType?: pq.LogicalType;
};

export type Data = {
  raw: Uint8Array;
  count: number;
};

export type Part = {

  /**
   * An opaque ID that can be used to read this part's underlying data later. If {@link #lookup} is
   * non-zero, then this actually refers to dictionary data.
   */
  at: number;

  /**
   * The row start of this part, inclusive.
   */
  start: number;

  /**
   * The row end of this part, exclusive.
   */
  end: number;

  /**
   * If non-zero, this can be fetched with {@link ParquetReader#lookupAt} and used to index into
   * the underlying data.
   */
  lookup: number;

}

export type Reader = (start: number, end?: number) => Promise<Uint8Array>;
