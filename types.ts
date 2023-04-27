export enum DataType {
  INT8,
  INT16,
  INT32,
  INT64,
  FLOAT,
  DOUBLE,
  BYTE_ARRAY,
}

/**
 * Data that may optionally be used to lookup other data in a dictionary.
 */
export type DataResultDictLookup = { lookup?: true } & (
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
export type DataResult =
  | DataResultDictLookup
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
      type: DataType.BYTE_ARRAY;
      arr: Uint8Array;
    };

export type ReadColumnPart = {
  id: number;
  count: number;
  read(): Promise<DataResult>;
} & (
  | {
      dict: true;
    }
  | {
      dict: false;
      lookup?: number;

      /**
       * The first row of data found here.
       */
      begin: number;

      /**
       * The row after the last row of data found here.
       */
      end: number;
    }
);

export type Reader = (start: number, end?: number) => Promise<Uint8Array>;

