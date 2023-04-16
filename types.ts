export enum DataType {
  INT8,
  INT16,
  INT32,
  INT64,
  FLOAT,
  DOUBLE,
  BYTE_ARRAY,
}

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
