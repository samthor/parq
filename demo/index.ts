import { ColumnInfo, GroupInfo, ParquetReader } from '../types';
import { WorkerRequest, WorkerReply, ParquetInfo } from './worker';
import { buildRpcClient } from './worker-api';

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
    const rpc = buildRpcClient<WorkerRequest, WorkerReply>(w);

    const reply = await rpc({
      type: 'init',
      blob: f,
      name: f.name,
    });
    console.info('got reply', reply);
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
    return new RemoteParquetReader(rpc, reply.info);
  }

  private constructor(
    private rpc: (r: WorkerRequest) => Promise<WorkerReply>,
    private info: ParquetInfo,
  ) {}

  async dictFor(columnNo: number, groupNo: number): Promise<ReadDictPart | null> {
    const reply = await this.rpc({ type: 'dict', id: this.info.id, columnNo, groupNo });
    if (reply.type !== 'dict') {
      throw new Error(`unexpected RPC reply`);
    }
    return {
      ...reply.r,
      async read() {
        return reply.r.data;
      },
    };
  }

  async load(columnNo: number, groupNo: number): AsyncGenerator<ReadColumnPart, void, void> {
    const reply = await this.rpc({ type: 'load', id: this.info.id, columnNo, groupNo });
    if (reply.type !== 'dict') {
      throw new Error(`unexpected RPC reply`);
    }

    throw new Error('Method not implemented.');
  }

  columns(): ColumnInfo[] {
    return this.info.columns;
  }

  groups(): GroupInfo[] {
    return this.info.groups;
  }
}
