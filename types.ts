import type { ConvertedType, LogicalType, Type } from './dep/thrift/parquet-code.ts';

export type ParquetReader = {
  dictFor(columnNo: number, groupNo: number): Promise<ReadDictPart | null>;
  load(columnNo: number, groupNo: number): AsyncGenerator<ReadColumnPart, void, void>;
  rows(): number;
  columns(): Array<ColumnInfo>;
  groups(): Array<GroupInfo>;
};

export type GroupInfo = {
  start: number;
  end: number;
}

export type ColumnInfo = {
  name: string;
  typeLength: number;
  physicalType: Type,
  logicalType?: LogicalType;
};

export type ColumnData = {
  raw: Uint8Array;
  count: number;
  index: false;

  // TODO: the following stuff is probably same per-group header
  // probably the same per schema too

  type?: Type;
  convertedType?: ConvertedType;
  bitLength: number;
  fp: boolean;
};

export type IndexColumnData = {
  ptr: ArrayLike<number>;
  index: true;
};

export type ReadDictPart = {
  id: number;
  dict: true;
  start: 0;
  count: number;
  lookup: undefined;
  read(): Promise<ColumnData>;
};

/**
 * Metadata and read helper for a part of data. This is basically wrapping the smallest contiguous
 * chunk of column data from the underlying data source.
 *
 * This may be dictionary index data, or be regular data which optionally has a lookup _into_
 * dictionary data.
 */
export type ReadColumnPart = {
  id: number;
  dict: false;
  start: number;
  count: number;
} & (
  | {
      lookup: undefined;
      read(): Promise<ColumnData>;
    }
  | {
      lookup: number;
      read(): Promise<IndexColumnData>;
    }
);

export type ReadPart = ReadDictPart | ReadColumnPart;

export type Reader = (start: number, end?: number) => Promise<Uint8Array>;
