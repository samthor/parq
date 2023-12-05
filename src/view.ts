/**
 * Returns a {@link DataView} from the source.
 */
export function typedArrayView(x: Uint8Array | ArrayBuffer) {
  if (x instanceof Uint8Array) {
    return new DataView(x.buffer, x.byteOffset, x.byteLength);
  }
  return new DataView(x);
}

export function toUint8Array(y: { buffer: ArrayBuffer, byteOffset: number, byteLength: number }) {
  if (y instanceof Uint8Array) {
    return y;
  }
  return new Uint8Array(y.buffer, y.byteOffset, y.byteLength);
}

export function toUint16Array(y: { buffer: ArrayBuffer, byteOffset: number, byteLength: number }) {
  if (y instanceof Uint16Array) {
    return y;
  }
  if (y.byteLength & 0x1) {
    throw new Error(`invalid U16 length: ${y.byteLength}`);
  }
  return new Uint16Array(y.buffer, y.byteOffset, y.byteLength >> 1);
}

export function toUint32Array(y: { buffer: ArrayBuffer, byteOffset: number, byteLength: number }) {
  if (y instanceof Uint32Array) {
    return y;
  }
  if (y.byteLength & 0x2) {
    throw new Error(`invalid U32 length: ${y.byteLength}`);
  }
  return new Uint32Array(y.buffer, y.byteOffset, y.byteLength >> 2);
}