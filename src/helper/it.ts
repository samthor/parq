export async function flattenAsyncIterator<K>(it: AsyncIterable<K>): Promise<K[]> {
  const out: K[] = [];

  for await (const next of it) {
    out.push(next);
  }

  return out;
}
