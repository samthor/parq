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

type FieldInfo = {
  ftype: ThriftType;
  fid: number;
};

/**
 * Low-level Thrift reader.
 */
export interface ThriftReader {
  skip(type: ThriftType): void;

  readListBegin(): { etype: ThriftType; size: number };
  readListEnd(): void;

  readSetBegin(): { etype: ThriftType; size: number };
  readSetEnd(): void;

  readMapBegin(): { ktype: ThriftType; vtype: ThriftType; size: number };
  readMapEnd(): void;

  readStructBegin(): void;
  readStructEnd(): void;

  readFieldBegin(): FieldInfo;
  readFieldEnd(): void;

  readByte(): number;
  readI16(): number;
  readI32(): number;
  readI64(): number;
  readDouble(): number;

  readBool(): boolean;
  readUUID(): Uint8Array;
  readBinary(): Uint8Array;
  readString(): string;
}

export function readList(input: ThriftReader, type: ThriftType, reader: () => any) {
  const { etype, size } = input.readListBegin();

  if (etype !== type || size === 0) {
    for (let i = 0; i < size; ++i) {
      input.skip(etype);
    }
    input.readListEnd();
    return [];
  }

  // prealloc array gives small speedup
  const out = new Array(size);
  for (let i = 0; i < size; ++i) {
    out[i] = reader();
  }
  input.readListEnd();
  return out;
}

export function readFastList(input: ThriftReader, type: ThriftType, reader: () => any) {
  const { etype, size } = input.readListBegin();

  if (etype !== type || size === 0) {
    for (let i = 0; i < size; ++i) {
      input.skip(etype);
    }
    // input.readListEnd();
    return [];
  }

  // prealloc array gives small speedup
  const out = new Array(size);
  for (let i = 0; i < size; ++i) {
    out[i] = reader();
  }
  // input.readListEnd();
  return out;
}
