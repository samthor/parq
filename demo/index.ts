import { Data, ParquetInfo, ParquetReader, Part, UintArray } from '../types';
import { DemoTableElement } from './demo-table';
import { WorkerRequest, WorkerReply } from './worker';
import { buildRpcClient } from './worker-api';
import * as thorish from 'thorish';

const helpNode = document.querySelector('#help')!;

window.addEventListener('dragover', (e) => e.preventDefault());

window.addEventListener('drop', async (e) => {
  e.preventDefault();
  const files = e.dataTransfer?.files;
  if (!files) {
    return;
  }

  for (let i = 0; i < files.length; ++i) {
    const f = files[i];

    const w = new Worker('./worker.ts', { type: 'module' });
    const rp = await RemoteParquetReader.create(w, f);

    const table = new DemoTableElement(rp);
    document.body.append(table);

    table.signal.addEventListener('abort', () => {
      table.remove();
      w.terminate();
    });

    helpNode.remove();
  }
});

export class RemoteParquetReader implements ParquetReader {
  static async create(w: Worker, f: File) {
    const rpc = buildRpcClient<WorkerRequest, WorkerReply>(w);
    const reply = await rpc({
      type: 'init',
      blob: f,
      name: f.name,
    });
    if (reply.type !== 'init') {
      throw new Error(`unexpected RPC reply`);
    }
    return new RemoteParquetReader(rpc, reply.id, reply.info);
  }

  private constructor(
    private rpc: (r: WorkerRequest) => Promise<WorkerReply>,
    private id: string,
    private _info: ParquetInfo,
  ) {}

  private readAtCache = new thorish.SimpleCache<number, Promise<Data>>(async (at) => {
    const reply = await this.rpc({ type: 'readAt', id: this.id, at });
    if (reply.type !== 'readAt') {
      throw new Error(`unexpected RPC reply`);
    }
    return reply.data;
  });

  private lookupAtCache = new thorish.SimpleCache<number, Promise<UintArray>>(async (at) => {
    const reply = await this.rpc({ type: 'lookupAt', id: this.id, at });
    if (reply.type !== 'lookupAt') {
      throw new Error(`unexpected RPC reply`);
    }
    return reply.lookup;
  });

  info() {
    return this._info;
  }

  purge(): void {
    this.readAtCache.clear();
    this.lookupAtCache.clear();
  }

  load(columnNo: number, groupNo: number): AsyncGenerator<Part, void, void> {
    const g = this._info.groups[groupNo];
    return this.loadRange(columnNo, g.start, g.end);
  }

  async *loadRange(columnNo: number, start: number, end: number): AsyncGenerator<Part, void, void> {
    const reply = await this.rpc({ type: 'loadRange', id: this.id, columnNo, start, end });
    if (reply.type !== 'loadRange') {
      throw new Error(`unexpected RPC reply`);
    }

    for (const part of reply.parts) {
      yield part;
    }
  }

  readAt(at: number): Promise<Data> {
    return this.readAtCache.get(at);
  }

  lookupAt(at: number): Promise<UintArray> {
    return this.lookupAtCache.get(at);
  }
}
