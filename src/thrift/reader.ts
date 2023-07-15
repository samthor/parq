/**
 * @fileoverview Better implementation of the TCompactProtocolReader from Thrift.
 */

import { readVarint32, readZigZagVarint32, readZigZagVarint53 } from '../varint.js';
import { type ThriftReader, ThriftType } from '../../dep/thrift/compiler-deps.js';

const dec = new TextDecoder();

export enum CompactProtocolType {
  CT_STOP = 0x00,
  CT_BOOLEAN_TRUE = 0x01,
  CT_BOOLEAN_FALSE = 0x02,
  CT_BYTE = 0x03,
  CT_I16 = 0x04,
  CT_I32 = 0x05,
  CT_I64 = 0x06,
  CT_DOUBLE = 0x07,
  CT_BINARY = 0x08,
  CT_LIST = 0x09,
  CT_SET = 0x0a,
  CT_MAP = 0x0b,
  CT_STRUCT = 0x0c,
  CT_UUID = 0x0d, // always 16 bytes long
}

export type FieldInfo = {
  ftype: ThriftType;
  fid: number;
};

const ttypeMap = new Map([
  [CompactProtocolType.CT_STOP, ThriftType.STOP],
  [CompactProtocolType.CT_BOOLEAN_FALSE, ThriftType.BOOL],
  [CompactProtocolType.CT_BOOLEAN_TRUE, ThriftType.BOOL],
  [CompactProtocolType.CT_BYTE, ThriftType.BYTE],
  [CompactProtocolType.CT_I16, ThriftType.I16],
  [CompactProtocolType.CT_I32, ThriftType.I32],
  [CompactProtocolType.CT_I64, ThriftType.I64],
  [CompactProtocolType.CT_DOUBLE, ThriftType.DOUBLE],
  [CompactProtocolType.CT_BINARY, ThriftType.BYTES],
  [CompactProtocolType.CT_LIST, ThriftType.LIST],
  [CompactProtocolType.CT_SET, ThriftType.SET],
  [CompactProtocolType.CT_MAP, ThriftType.MAP],
  [CompactProtocolType.CT_STRUCT, ThriftType.STRUCT],
  [CompactProtocolType.CT_UUID, ThriftType.UUID],
]);

export abstract class TCompactProtocolReader implements ThriftReader {
  private fieldId = 0;
  private fieldIdStack: number[] = [];

  private pendingBool: boolean | undefined;

  private readVarint32: () => number;
  private readZigZagVarint32: () => number;
  private readZigZagVarint53: () => number;

  constructor() {
    const readByteBind = this.readByte.bind(this);
    this.readVarint32 = readVarint32.bind(null, readByteBind);
    this.readZigZagVarint32 = readZigZagVarint32.bind(null, readByteBind);
    this.readZigZagVarint53 = readZigZagVarint53.bind(null, readByteBind);
  }

  readUUID(): Uint8Array {
    return this.readBytes(16);
  }

  skipVarint() {
    for (;;) {
      const b = this.readByte();
      if (!(b & 0x80)) {
        break;
      }
    }
  }

  skip(type: ThriftType) {
    switch (type) {
      case ThriftType.BOOL:
        if (this.pendingBool !== undefined) {
          this.pendingBool = undefined;
          break;
        }
      // fall-through
      case ThriftType.BYTE:
        this.readByte();
        break;
      case ThriftType.I16:
      case ThriftType.I32:
      case ThriftType.I64:
        this.skipVarint();
        break;
      case ThriftType.DOUBLE:
        this.skipBytes(8);
        break;
      case ThriftType.BYTES: {
        const size = this.readVarint32();
        this.skipBytes(size);
        break;
      }
      case ThriftType.STRUCT:
        while (true) {
          const ftype = this.readFieldForSkip();
          if (ftype === ThriftType.STOP) {
            break;
          }
          this.skip(ftype);
        }
        break;
      case ThriftType.MAP: {
        const info = this.readMapBegin();
        for (let i = 0; i < info.size; ++i) {
          this.skip(info.ktype);
          this.skip(info.vtype);
        }
        // mapEnd is noop
        break;
      }
      case ThriftType.SET:
      case ThriftType.LIST: {
        const info = this.readListBegin();
        for (let i = 0; i < info.size; ++i) {
          this.skip(info.etype);
        }
        // listEnd is noop
        break;
      }
      case ThriftType.UUID:
        this.skipBytes(16);
        break;
      default:
        throw new Error(`TODO skip: ${type}`);
    }
  }

  readListBegin() {
    const head = this.readByte();

    let size = (head >>> 4) & 0x0000000f;
    if (size === 15) {
      size = this.readVarint32(); // too long
      if (size < 0) {
        throw new TypeError(`got -ve list size: ${size}`);
      }
    }

    const thriftFieldType = this.getTType(head & 0x0000000f);
    return { etype: thriftFieldType, size };
  }

  /**
   * Noop
   */
  readListEnd() {}

  readSetBegin() {
    return this.readListBegin();
  }

  /**
   * Noop
   */
  readSetEnd() {}

  readMapBegin() {
    const size = this.readVarint32();
    if (size === 0) {
      return { ktype: ThriftType.STOP, vtype: ThriftType.STOP, size };
    } else if (size < 0) {
      throw new TypeError(`got -ve map size: ${size}`);
    }

    const head = this.readByte();
    const ktype = this.getTType((head & 0xf0) >>> 4);
    const vtype = this.getTType(head & 0xf);
    return { ktype, vtype, size };
  }

  /**
   * Noop
   */
  readMapEnd() {}

  readStructBegin() {
    this.fieldIdStack.push(this.fieldId);
    this.fieldId = 0;
  }

  readStructEnd() {
    this.fieldId = this.fieldIdStack.pop()!;
  }

  /**
   * Read a field starter purely for skipping it. Does not modify/care about fieldId.
   */
  private readFieldForSkip(): ThriftType {
    const b = this.readByte();
    if (!b) {
      return ThriftType.STOP;
    }

    const protocolType: CompactProtocolType = b & 0x0f;
    if (protocolType === CompactProtocolType.CT_BOOLEAN_TRUE) {
      this.pendingBool = true;
    } else if (protocolType === CompactProtocolType.CT_BOOLEAN_FALSE) {
      this.pendingBool = false;
    }
    return this.getTType(protocolType);
  }

  readFieldKey(): number {
    const b = this.readByte();
    const protocolType: CompactProtocolType = b & 0x0f;
    if (protocolType === 0) {
      return 0;
    }

    const modifier = (b & 0x000000f0) >>> 4;
    if (modifier === 0) {
      // This is a new field ID.
      this.fieldId = this.readI16();
    } else {
      // This is a delta encoded in the type byte.
      this.fieldId += modifier;
    }

    if (protocolType === CompactProtocolType.CT_BOOLEAN_TRUE) {
      this.pendingBool = true;
    } else if (protocolType === CompactProtocolType.CT_BOOLEAN_FALSE) {
      this.pendingBool = false;
    }

    const thriftFieldType = this.getTType(protocolType);
    return (this.fieldId << 8) + thriftFieldType;
  }

  /**
   * Reads a struct or struct-like field.
   */
  readFieldBegin(): FieldInfo {
    const b = this.readByte();
    const protocolType: CompactProtocolType = b & 0x0f;

    if (protocolType === CompactProtocolType.CT_STOP) {
      return { ftype: ThriftType.STOP, fid: 0 };
    }

    const modifier = (b & 0x000000f0) >>> 4;
    if (modifier === 0) {
      // This is a new field ID.
      this.fieldId = this.readI16();
    } else {
      // This is a delta encoded in the type byte.
      this.fieldId += modifier;
    }

    if (protocolType === CompactProtocolType.CT_BOOLEAN_TRUE) {
      this.pendingBool = true;
    } else if (protocolType === CompactProtocolType.CT_BOOLEAN_FALSE) {
      this.pendingBool = false;
    }

    const thriftFieldType = this.getTType(protocolType);
    return { ftype: thriftFieldType, fid: this.fieldId };
  }

  /**
   * Ends reading a struct or struct-like field. Noop.
   */
  readFieldEnd() {}

  readBool(): boolean {
    if (this.pendingBool !== undefined) {
      const out = this.pendingBool;
      this.pendingBool = undefined;
      return out;
    }

    const b = this.readByte();
    return b === CompactProtocolType.CT_BOOLEAN_TRUE;
  }

  abstract readByte(): number;
  abstract readBytes(size: number): Uint8Array;
  abstract skipBytes(size: number): void;

  readI16(): number {
    return this.readZigZagVarint32(); // lol
  }

  readI32(): number {
    return this.readZigZagVarint32();
  }

  readI64(): number {
    return this.readZigZagVarint53();
  }

  /**
   * Reads a double. This is always 8 bytes. Little-endian.
   */
  readDouble() {
    const bytes = this.readBytes(8);
    const dv = new DataView(bytes.buffer, bytes.byteOffset);
    return dv.getFloat64(0, true);
  }

  readUuid() {
    return this.readBytes(16);
  }

  readNumber(type: ThriftType): number {
    switch (type) {
      case ThriftType.BYTE:
        return this.readByte();
      case ThriftType.I16:
      case ThriftType.I32:
        return this.readZigZagVarint32();
      case ThriftType.I64:
        return this.readZigZagVarint53();
      case ThriftType.DOUBLE:
        return this.readDouble();
    }

    throw new Error(`not a number type: ${type}`);
  }

  private getTType(type: CompactProtocolType): ThriftType {
    const o = ttypeMap.get(type);
    if (o === undefined) {
      throw new TypeError(`Unknown protocol type: ${type}`);
    }
    return o;
  }

  readBinary() {
    const size = this.readVarint32();
    return this.readBytes(size);
  }

  readString() {
    const b = this.readBinary();
    return dec.decode(b);
  }
}

/**
 * Reads a Thrift-compact encoded stream from a concrete buffer.
 */
export class TCompactProtocolReaderBuffer extends TCompactProtocolReader {
  private buf: Uint8Array;
  at: number;

  constructor(buf: Uint8Array, at = 0) {
    super();
    this.buf = buf;
    this.at = at;
  }

  readByte(): number {
    return this.buf[this.at++];
  }

  readBytes(size: number): Uint8Array {
    const end = this.at + size;
    const out = this.buf.subarray(this.at, end);
    this.at = end;
    return out;
  }

  skipBytes(size: number): void {
    this.at += size;
  }

  readDouble(): number {
    const dv = new DataView(this.buf.buffer, this.buf.byteOffset + this.at, 8);
    const out = dv.getFloat64(0, true);
    this.at += 8;
    return out;
  }
}

export class TCompactProtocolReaderPoll_OutOfData extends Error {}

/**
 * Reads a Thrift-compact encoded stream from a source which may be polled for additional bytes.
 *
 * If there are no more bytes available, throws {@link TCompactProtocolReaderPoll_OutOfData}. This
 * is not async, so it's probably not possible to "get more" from a source inline.
 */
export class TCompactProtocolReaderPoll extends TCompactProtocolReader {
  private more: (min: number) => Uint8Array;
  private pending: Uint8Array = new Uint8Array();
  private at = 0;
  private _consumed = 0;

  get consumed() {
    return this._consumed;
  }

  /**
   * @param more To provide more bytes. Must always return >= min request.
   */
  constructor(arg: Uint8Array | ((min: number) => Uint8Array)) {
    super();

    if (arg instanceof Uint8Array) {
      this.pending = arg;
      this.more = () => {
        throw new TCompactProtocolReaderPoll_OutOfData();
      };
    } else {
      this.more = arg;
    }
  }

  private ensure(min: number) {
    if (this.at + min <= this.pending.length) {
      return; // ok!
    }

    const suffix = this.pending.subarray(this.at);
    min -= suffix.length;

    const update = this.more(min);
    if (update.length < min) {
      throw new TCompactProtocolReaderPoll_OutOfData();
    }

    if (suffix.length) {
      // Need to combine the remaining suffix with new data. Just create a new buffer, oh well.
      this.pending = new Uint8Array(suffix.length + update.length);
      this.pending.set(suffix);
      this.pending.set(update, suffix.length);
    } else {
      // Can use verbatim, no prior data to keep.
      this.pending = update;
    }
    this.at = 0;
  }

  readByte(): number {
    this.ensure(1);
    const out = this.pending[this.at];
    ++this.at;
    ++this._consumed;
    return out;
  }

  readBytes(size: number): Uint8Array {
    if (size === 0) {
      return new Uint8Array();
    } else if (size < 0) {
      throw new TypeError(`Got -ve binary size: ${size}`);
    }
    this.ensure(size);
    this._consumed += size;

    const end = this.at + size;
    const out = this.pending.subarray(this.at, end);
    this.at = end;
    return out;
  }

  skipBytes(size: number): void {
    if (size < 0) {
      throw new TypeError(`cannot skip -ve bytes: ${size}`);
    }
    this.ensure(size);
    this._consumed += size;
    this.at += size;
  }
}
