/**
 * Returns a {@link DataView} from the source.
 */
export function typedArrayView(x: Uint8Array | ArrayBuffer) {
  if (x instanceof Uint8Array) {
    return new DataView(x.buffer, x.byteOffset, x.byteLength);
  }
  return new DataView(x);
}
