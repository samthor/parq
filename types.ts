export enum DataType {
  INT8 = 1,
  INT16,
  INT32,
  INT64,
  FLOAT,
  DOUBLE,

  /**
   * This is used for INT96, which can't be represented in any/most languages easily.
   */
  BIG_BYTE_ARRAY,

  /**
   * This is just bytes.
   */
  FIXED_LENGTH_BYTE_ARRAY,

  /**
   * This contains `[uint32 length + bytes, ...]`.
   */
  LENGTH_BYTE_ARRAY,
}

/**
 * Data that may be used to lookup other data in a dictionary.
 */
export type ColumnDataResultLookup = { lookup?: true } & (
  | {
      type: DataType.INT8;
      arr: Int8Array;
    }
  | {
      type: DataType.INT16;
      arr: Int16Array;
    }
  | {
      type: DataType.INT32;
      arr: Int32Array;
    }
);

/**
 * Any data that has been expanded for return from reading some encoded file.
 */
export type ColumnDataResult =
  | {
      type: DataType.INT8;
      arr: Int8Array;
    }
  | {
      type: DataType.INT16;
      arr: Int16Array;
    }
  | {
      type: DataType.INT32;
      arr: Int32Array;
    }
  | {
      type: DataType.INT64;
      arr: BigInt64Array;
    }
  | {
      type: DataType.FLOAT;
      arr: Float32Array;
    }
  | {
      type: DataType.DOUBLE;
      arr: Float64Array;
    }
  | {
      type: DataType.BIG_BYTE_ARRAY;
      size: number;
      arr: Uint8Array;
    }
  | {
      type: DataType.FIXED_LENGTH_BYTE_ARRAY;
      arr: Uint8Array;
    }
  | {
      type: DataType.LENGTH_BYTE_ARRAY;
      arr: Uint8Array;
    };

export type ReadDictPart = {
  id: number;
  count: number;
  dict: true;
  read(): Promise<ColumnDataResult>;
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
  count: number;
  dict: false;
  start: number;
} & (
  | {
      lookup: number;
      read(): Promise<ColumnDataResultLookup>;
    }
  | {
      read(): Promise<ColumnDataResult>;
    }
);

export type ReadPart = ReadDictPart | ReadColumnPart;

export type Reader = (start: number, end?: number) => Promise<Uint8Array>;
