import * as pq from '../../dep/thrift/parquet-code.js';

function build(arg: (lt: pq.LogicalType) => any): pq.LogicalType {
  const lt = new pq.LogicalType();
  arg(lt);
  return lt;
}

function buildInt(bitWidth: number, isSigned: boolean): pq.LogicalType {
  const lt = new pq.LogicalType();
  lt.INTEGER = new pq.IntType();
  lt.INTEGER.bitWidth = bitWidth;
  lt.INTEGER.isSigned = isSigned;
  return lt;
}

export const convertedToLogical = new Map<pq.ConvertedType, pq.LogicalType>([
  [pq.ConvertedType.UTF8, build((lt) => (lt.STRING = new pq.StringType()))],
  [pq.ConvertedType.INT_8, buildInt(8, true)],
  [pq.ConvertedType.INT_16, buildInt(16, true)],
  [pq.ConvertedType.INT_32, buildInt(32, true)],
  [pq.ConvertedType.INT_64, buildInt(64, true)],
  [pq.ConvertedType.UINT_8, buildInt(8, false)],
  [pq.ConvertedType.UINT_16, buildInt(16, false)],
  [pq.ConvertedType.UINT_32, buildInt(32, false)],
  [pq.ConvertedType.UINT_64, buildInt(64, false)],
  [
    pq.ConvertedType.TIME_MICROS,
    build((lt) => {
      lt.TIME = new pq.TimeType();
      lt.TIME.isAdjustedToUTC = true;
      lt.TIME.unit.MICROS = new pq.MicroSeconds();
    }),
  ],
  [
    pq.ConvertedType.TIME_MILLIS,
    build((lt) => {
      lt.TIME = new pq.TimeType();
      lt.TIME.isAdjustedToUTC = true;
      lt.TIME.unit.MILLIS = new pq.MilliSeconds();
    }),
  ],
  [
    pq.ConvertedType.TIMESTAMP_MICROS,
    build((lt) => {
      lt.TIMESTAMP = new pq.TimestampType();
      lt.TIMESTAMP.isAdjustedToUTC = true;
      lt.TIMESTAMP.unit.MICROS = new pq.MicroSeconds();
    }),
  ],
  [
    pq.ConvertedType.TIMESTAMP_MILLIS,
    build((lt) => {
      lt.TIMESTAMP = new pq.TimestampType();
      lt.TIMESTAMP.isAdjustedToUTC = true;
      lt.TIMESTAMP.unit.MILLIS = new pq.MicroSeconds();
    }),
  ],
  [pq.ConvertedType.JSON, build((lt) => (lt.JSON = new pq.JsonType()))],
  [pq.ConvertedType.BSON, build((lt) => (lt.BSON = new pq.BsonType()))],
]);
