/**
 * @fileoverview Better implementation of the TCompactProtocolReader from Thrift.
 */

import { readVarint32, readZigZagVarint32, readZigZagVarint53 } from '../varint.js';
import { type ThriftReader, ThriftType } from '../../dep/thrift/compiler-deps.js';

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
        this.readBool();
        break;
      case ThriftType.BYTE:
        this.readByte();
        break;
      case ThriftType.I16:
      case ThriftType.I32:
      case ThriftType.I64:
        this.skipVarint();
        break;
      case ThriftType.DOUBLE:
        this.readDouble();
        break;
      case ThriftType.BYTES:
        this.readBinary(); // skip decode step, we don't know if it's a string
        break;
      case ThriftType.STRUCT:
        this.readStructBegin();
        while (true) {
          const r = this.readFieldBegin();
          if (r.ftype === ThriftType.STOP) {
            break;
          }
          this.skip(r.ftype);
          this.readFieldEnd();
        }
        this.readStructEnd();
        break;
      case ThriftType.MAP: {
        const info = this.readMapBegin();
        for (let i = 0; i < info.size; ++i) {
          this.skip(info.ktype);
          this.skip(info.vtype);
        }
        this.readMapEnd();
        break;
      }
      case ThriftType.SET:
      case ThriftType.LIST: {
        const info = this.readListBegin();
        for (let i = 0; i < info.size; ++i) {
          this.skip(info.etype);
        }
        this.readListEnd();
        break;
      }
      case ThriftType.UUID:
        this.readBytes(16);
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
   * Reads a struct or struct-like field.
   */
  readFieldBegin(): FieldInfo {
    const b = this.readByte();
    const protocolType = b & 0x0f;

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

  readI16(): number {
    return this.readI32(); // lol
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
      case ThriftType.I16:
      case ThriftType.I32:
      case ThriftType.I64:
        return this.readZigZagVarint53();

      case ThriftType.DOUBLE:
        return this.readDouble();
    }

    throw new Error(`not a number type: ${type}`);
  }

  private getTType(type: CompactProtocolType): ThriftType {
    switch (type) {
      case CompactProtocolType.CT_STOP:
        return ThriftType.STOP;
      case CompactProtocolType.CT_BOOLEAN_FALSE:
      case CompactProtocolType.CT_BOOLEAN_TRUE:
        return ThriftType.BOOL;
      case CompactProtocolType.CT_BYTE:
        return ThriftType.BYTE;
      case CompactProtocolType.CT_I16:
        return ThriftType.I16;
      case CompactProtocolType.CT_I32:
        return ThriftType.I32;
      case CompactProtocolType.CT_I64:
        return ThriftType.I64;
      case CompactProtocolType.CT_DOUBLE:
        return ThriftType.DOUBLE;
      case CompactProtocolType.CT_BINARY:
        return ThriftType.BYTES;
      case CompactProtocolType.CT_LIST:
        return ThriftType.LIST;
      case CompactProtocolType.CT_SET:
        return ThriftType.SET;
      case CompactProtocolType.CT_MAP:
        return ThriftType.MAP;
      case CompactProtocolType.CT_STRUCT:
        return ThriftType.STRUCT;
      case CompactProtocolType.CT_UUID:
        return ThriftType.UUID; // always 16 bytes long
      default:
        throw new TypeError(`Unknown protocol type: ${type}`);
    }
    return ThriftType.STOP;
  }

  readBinary() {
    const size = this.readVarint32();
    return this.readBytes(size);
  }

  readString() {
    const b = this.readBinary();
    return new TextDecoder().decode(b);
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
    const out = this.buf[this.at];
    ++this.at;
    return out;
  }

  readBytes(size: number): Uint8Array {
    if (size === 0) {
      return new Uint8Array();
    } else if (size < 0) {
      throw new TypeError(`Got -ve binary size: ${size}`);
    }

    const end = this.at + size;
    const out = this.buf.subarray(this.at, end);
    this.at = end;
    return out;
  }

  readDouble(): number {
    const dv = new DataView(this.buf.buffer, this.buf.byteOffset);
    const out = dv.getFloat64(this.at, true);
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
}
