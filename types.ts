import type * as pq from './dep/thrift/parquet-code.ts';

// nb. not actually a type, an enum
export { Type as PhysicalType } from './dep/thrift/parquet-code.ts';

export type UintArray = Uint8Array | Uint16Array | Uint32Array;

type PhysicalType = pq.Type;

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
   */
  readAt(at: number): Promise<Data>;

  /**
   * Reads the lookup data at the given location. This always indexes into a dictionary.
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
  typeLength: number;
  physicalType: PhysicalType;
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
