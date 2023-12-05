import type * as pq from './dep/thrift/parquet-code.ts';

export type ParquetReader = {
  info(): ParquetInfo;
  load(columnNo: number, groupNo: number): AsyncGenerator<Part, void, void>;
  readAt(at: number): Promise<Data>;
  lookupAt(at: number): Promise<ArrayLike<number>>;
};

export type ParquetInfo = {
  groups: GroupInfo[];
  columns: ColumnInfo[];
}

export type GroupInfo = {
  start: number;
  end: number;
};

export type ColumnInfo = {
  name: string;
  typeLength: number;
  physicalType: pq.Type;
  logicalType?: pq.LogicalType;
};

export type Data = {
  raw: Uint8Array;
  count: number;
};

export type Part = {

  /**
   * An opaque ID that can be used to read this part's underlying data later.
   * (It's the byte location.)
   */
  at: number;

  /**
   * The row start of this part.
   */
  start: number;

  /**
   * The row end of this part.
   */
  end: number;

  /**
   * If non-zero, this can be fetched with {@link ParquetReader#lookupAt} and used to index into the underlying data.
   */
  lookup: number;

}

export type Reader = (start: number, end?: number) => Promise<Uint8Array>;
