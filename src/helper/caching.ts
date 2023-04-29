import type { ColumnDataResult, ReadColumnPart } from '../../types.js';
import type { ParquetReader } from '../read.js';

type IndexEntry = {
  at: number;
  r?: ReadColumnPart;

  groupNo?: number;
  pending?: Promise<void>;
};

const clamp = (value: number, lo: number, hi: number) => {
  return Math.min(Math.max(value, lo), hi);
};

/**
 * Indexer for columns of data within a Parquet file.
 */
export class ParquetIndexer {
  private r: ParquetReader;
  private index: IndexEntry[];
  private columnNo: number;

  get _index() {
    return this.index;
  }

  constructor(r: ParquetReader, columnNo: number) {
    this.r = r;
    this.columnNo = columnNo;

    // These groups are always contiguous (they have a `num_rows` field, not start/end).
    this.index = this.r.groupsAt().map((g, groupNo) => {
      return { at: g.start, groupNo };
    });
    this.index.push({ at: this.r.rows() });
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

      const gen = this.r.indexColumnGroup(this.columnNo, groupNo);
      for await (const next of gen) {
        // TODO: technically the last part could not be "complete" to the next group?
        updates.push({
          at: next.start,
          r: next,
        });
      }

      // We might have moved since this started, find ourselves again. Unfortunately O(n).
      const selfIndex = this.index.indexOf(groupEntry);
      if (selfIndex === -1) {
        throw new Error(`? removed from index early`);
      }
      this.index.splice(selfIndex, 1, ...updates);
    })());

    return true;
  }

  async readRange({
    start,
    end,
  }: {
    start: number;
    end: number;
  }): Promise<{ start: number; data: ColumnDataResult[] }> {
    const rows = this.r.rows();
    start = clamp(start, 0, rows);
    end = clamp(end, start, rows);

    if (end === start) {
      return { start: 0, data: [] };
    }

    const si = this.#find(start);
    const ei = this.#find(end - 1); // not inclusive, and we know end > start

    const indexParts = this.index.slice(si, ei + 1);
    const expandTasks = await Promise.all(indexParts.map((e, i) => this.#expandGroup(si + i)));
    if (expandTasks.some((x) => x)) {
      // something changed, be lazy and run again
      return this.readRange({ start, end });
    }

    // TODO: "CompletionList", which allows AsyncGenerator as well as Promise.all semantics

    const tasks = indexParts.map((e) => e.r!.read());
    const columnData = await Promise.all(tasks);

    return {
      start: indexParts[0].at,
      data: columnData,
    };
  }
}
