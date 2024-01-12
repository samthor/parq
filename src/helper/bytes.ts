import { Reader } from "../index.ts";

let concatBytes: (chunks: Uint8Array[]) => Uint8Array;

if (typeof Buffer !== 'undefined') {
  concatBytes = (chunks) => Buffer.concat(chunks);
} else {
  concatBytes = (chunks) => {
    let length = 0;
    for (const chunk of chunks) {
      length += chunk.length;
    }
    const out = new Uint8Array(length);
    let offset = 0;
    for (const chunk of chunks) {
      out.set(chunk, offset);
      offset += chunk.length;
    }
    return out;
  };
}

export { concatBytes };

export async function streamToBytes(
  reader: ReadableStreamDefaultReader<Uint8Array>,
): Promise<Uint8Array> {
  const chunks: Uint8Array[] = [];
  while (true) {
    const { done, value } = await reader.read();
    if (done) {
      break;
    }
    chunks.push(value);
  }
  return concatBytes(chunks);
}