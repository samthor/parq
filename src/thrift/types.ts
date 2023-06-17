
/**
 * This reflects the binary protocol: https://github.com/apache/thrift/blob/master/doc/specs/thrift-binary-protocol.md
 */
export enum ThriftType {
  STOP = 0,
  VOID = 1,
  BOOL = 2,
  I08 = 3,
  BYTE = 3,
  DOUBLE = 4,
  I16 = 6,
  I32 = 8,
  I64 = 10,
  BYTES = 11,
  STRUCT = 12,
  MAP = 13,
  SET = 14,
  LIST = 15,
  UUID = 16,
}
