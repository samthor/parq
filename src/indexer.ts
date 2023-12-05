import type { ParquetReader, ReadColumnPart, ReadDictPart, ReadPart } from '../types.js';

type IndexEntry = {
  at: number;
  r?: ReadColumnPart;

  groupNo?: number;
  pending?: Promise<void>;
};

const clamp = (value: number, lo: number, hi: number) => Math.min(Math.max(value, lo), hi);

/**
 * Indexer for columns of data within a Parquet file.
 */
export class ParquetIndexer {
  private index: IndexEntry[];
  private listener: (r: ReadPart) => void = () => {};
  private rows: number;

  constructor(private r: ParquetReader, private columnNo: number) {
    const groups = this.r.groups();

    // These groups are always contiguous (they have a `num_rows` field, not start/end).
    this.index = groups.map((g, groupNo) => {
      return { at: g.start, groupNo };
    });
    this.rows = groups.at(-1)?.end ?? 0;
    this.index.push({ at: this.rows });
  }

  /**
   * Adds a read listener to this indexer. This is called whenever new data is indexed.
   */
  addListener(listener: (r: ReadPart) => void) {
    const prev = this.listener;
    this.listener = (r) => {
      prev(r);
      listener(r);
    };
  }

  /**
   * Binary search to find the best index location for this target.
   */
  #find(at: number): number {
    let lo = 0;
    let hi = this.index.length;

    while (lo !== hi) {
      const halfLength = (hi - lo) >>> 1;
      const i = lo + halfLength;

      const mat = this.index[i].at;
      if (at >= mat && (i + 1 === this.index.length || at < this.index[i + 1].at)) {
        return i;
      }

      if (at < mat) {
        hi = i;
      } else {
        lo = i;
      }
    }

    return lo;
  }

  /**
   * Expand the given group to all its reader parts.
   *
   * TODO: could go linearly forward and bail early, but tricker to implement.
   */
  async #expandGroup(index: number): Promise<boolean> {
    const groupEntry = this.index[index];
    if (groupEntry.groupNo === undefined) {
      if (groupEntry.pending) {
        await groupEntry.pending;
        return true;
      }
      return false;
    }

    const { groupNo } = groupEntry;
    delete groupEntry.groupNo;

    await (groupEntry.pending = (async () => {
      const updates: IndexEntry[] = [];
      let dictPartPromise: Promise<ReadDictPart | null> = Promise.resolve(null);

      const gen = this.r.load(this.columnNo, groupNo);
      for await (const next of gen) {
        // TODO: technically the last part could not be "complete" to the next group?
        updates.push({
          at: next.start,
          r: next,
        });
        this.listener(next);

        if ('lookup' in next && !dictPartPromise) {
          // We don't really need to index this _until_ the caller wants the data but it's simpler
          // this way.
          dictPartPromise = this.r.dictFor(this.columnNo, groupNo);
        }
      }

      // We might have moved since this started (only +ve), find ourselves again. Sadly O(n).
      const selfIndex = this.index.indexOf(groupEntry, index);
      if (selfIndex === -1) {
        throw new Error(`? removed from index early`);
      }
      this.index.splice(selfIndex, 1, ...updates);

      const dictPart = await dictPartPromise;
      if (dictPart) {
        this.listener(dictPart);
      }
    })());

    return true;
  }

  /**
   * Finds the range of data that must be read to satisfy the query. Does not actually read or
   * cache the underlying data, just the index.
   */
  async findRange({
    start,
    end,
  }: {
    start: number;
    end: number;
  }): Promise<{ start: number; end: number, part: ReadColumnPart[], ids: number[] }> {
    start = clamp(start, 0, this.rows);
    end = clamp(end, start, this.rows);

    if (end === start) {
      return { start: 0, end: 0, part: [], ids: [] };
    }

    for (;;) {
      const si = this.#find(start);
      const ei = this.#find(end - 1); // not inclusive, and we know end > start

      const indexParts = this.index.slice(si, ei + 1);
      const expandTasks = await Promise.all(indexParts.map((e, i) => this.#expandGroup(si + i)));
      if (expandTasks.some((x) => x)) {
        // something changed, be lazy and run again
        continue;
      }

      const lastPart = indexParts.at(-1);
      const lastPartReader = lastPart?.r!;

      if (!lastPartReader) {
        throw new Error(`Can't read underlying data, no reader`);
      }

      return {
        start: indexParts[0].at,
        end: lastPartReader.start + lastPartReader.count,
        // TODO: just return IDs?
        part: indexParts.map(({ r }) => r!),
        ids: indexParts.map(({ r }) => r!.id),
      };
    }
  }
}
