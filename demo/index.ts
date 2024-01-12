import type { Data, ParquetInfo, ParquetReader, Part, UintArray } from '../types';
import { DemoTableElement } from './demo-table';
import type { WorkerRequest, WorkerReply } from './worker';
import { buildRpcClient } from './worker-api';
import * as thorish from 'thorish';

// @ts-ignore
import workerUrl from './worker?worker';

// @ts-ignore
import sampleDataUrl from './data/userdata1.parquet?url';

const helpNode = document.querySelector('#help')!;
const sampleNode = document.querySelector('#sample')!;

async function addReaderTable(blob: Blob, name: string) {
  helpNode.remove();

  const w = workerUrl();
  const pr = await RemoteParquetReader.create(w, blob, 'userdata1.parquet');

  const table = new DemoTableElement(pr);
  document.body.append(table);

  table.signal.addEventListener('abort', () => {
    table.remove();
    w.terminate();
  });
}

sampleNode.addEventListener('click', async () => {
  const r = await fetch(sampleDataUrl);
  const blob = await r.blob();
  await addReaderTable(blob, 'userdata1.parquet');
});

window.addEventListener('dragover', (e) => e.preventDefault());

window.addEventListener('drop', async (e) => {
  e.preventDefault();
  const files = e.dataTransfer?.files;
  if (!files) {
    return;
  }

  for (let i = 0; i < files.length; ++i) {
    const f = files[i];
    await addReaderTable(f, f.name);
  }
});

export class RemoteParquetReader implements ParquetReader {
  static async create(w: Worker, blob: Blob, name: string) {
    const rpc = buildRpcClient<WorkerRequest, WorkerReply>(w);
    const reply = await rpc({
      type: 'init',
      blob,
      name,
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
