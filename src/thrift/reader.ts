/**
 * @fileoverview Better implementation of the TCompactProtocolReader from Thrift.
 */

import { readVarint32, readZigZagVarint53 } from '../varint.js';
import { ThriftType } from './types.js';

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
  CT_UUID = 0x0d,
}

function zigzagToInt(n: number) {
  return (n >>> 1) ^ (-1 * (n & 1));
}

export class TCompactProtocolReader {
  private fieldId = 0;
  private fieldIdStack: number[] = [];

  private buf: Uint8Array;
  at = 0;

  private pendingBool: boolean | undefined;

  private readVarint32: () => number;
  private readZigZagVarint53: () => number;

  constructor(buf: Uint8Array, at = 0) {
    this.buf = buf;
    this.at = at;

    const readByteBind = this.readByte.bind(this);
    this.readVarint32 = readVarint32.bind(null, readByteBind);
    this.readZigZagVarint53 = readZigZagVarint53.bind(null, readByteBind);
  }

  skipVarint() {
    while (this.buf[this.at] & 0x80) {
      ++this.at;
    }
    ++this.at;
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
      case ThriftType.UTF8:
      case ThriftType.STRING:
        this.readBinary(); // skip decode step
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
  readFieldBegin() {
    let fieldId = 0;
    const b = this.readByte();
    const protocolType = b & 0x0f;

    if (protocolType === CompactProtocolType.CT_STOP) {
      return { fname: null, ftype: ThriftType.STOP, fid: 0 };
    }

    const modifier = (b & 0x000000f0) >>> 4;
    if (modifier === 0) {
      // This is a new field ID.
      fieldId = this.readI16();
    } else {
      // This is a delta encoded in the type byte.
      fieldId = this.fieldId + modifier;
    }
    this.fieldId = fieldId;

    if (protocolType === CompactProtocolType.CT_BOOLEAN_TRUE) {
      this.pendingBool = true;
    } else if (protocolType === CompactProtocolType.CT_BOOLEAN_FALSE) {
      this.pendingBool = false;
    }

    const thriftFieldType = this.getTType(protocolType);
    return { fname: null, ftype: thriftFieldType, fid: fieldId };
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

  readByte(): number {
    const out = this.buf[this.at];
    ++this.at;
    return out;
  }

  readI16(): number {
    return this.readI32(); // lol
  }

  readI32(): number {
    return zigzagToInt(this.readVarint32());
  }

  readI64(): number {
    return this.readZigZagVarint53();
  }

  private readBytes(size: number) {
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

  /**
   * Reads a double. This is always 8 bytes. Little-endian.
   */
  readDouble() {
    const dv = new DataView(this.buf.buffer, this.buf.byteOffset);
    return dv.getFloat64(this.at, true);
  }

  getTType(type: CompactProtocolType): ThriftType {
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
        return ThriftType.STRING;
      case CompactProtocolType.CT_LIST:
        return ThriftType.LIST;
      case CompactProtocolType.CT_SET:
        return ThriftType.SET;
      case CompactProtocolType.CT_MAP:
        return ThriftType.MAP;
      case CompactProtocolType.CT_STRUCT:
        return ThriftType.STRUCT;
      case CompactProtocolType.CT_UUID:
        throw new Error(`TODO: found UUID protocol type`);
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
