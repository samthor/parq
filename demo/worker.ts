import { buildReader } from '../src';
import type { ColumnData, ColumnInfo, GroupInfo, ParquetReader, ReadDictPart, Reader } from '../types';
import { RpcHostContext, buildRpcHost } from './worker-api';

export type WorkerRequest =
  | {
      type: 'init';
      blob: Blob;
      name: string;
    }
  | {
      type: 'dict' | 'load';
      id: string;
      columnNo: number;
      groupNo: number;
    }
  | {
      type: 'read';
      id: string;
      column: number;
      start: number;
      end: number;
    };

export type ParquetInfo = {
  id: string;
  columns: ColumnInfo[];
  groups: GroupInfo[];
};

export type WorkerReply =
  | {
      type: 'init';
      info: ParquetInfo;
    }
  | {
      type: 'dict';
      r: Omit<ReadDictPart, 'read'> & { data: ColumnData },
    };

function readerFromBlob(b: Blob): Reader {
  return async (start, end) => {
    // reading ~50mb takes ~30ms
    const bytes = await b.slice(start, end).arrayBuffer();
    return new Uint8Array(bytes);
  };
}

let globalId = 0;
const readers = new Map<string, ParquetReader>();

async function handleMessage(
  message: WorkerRequest,
  context: RpcHostContext,
): Promise<WorkerReply> {
  if (message.type === 'init') {
    const id = String(++globalId);

    const reader = await buildReader(readerFromBlob(message.blob));
    readers.set(id, reader);
    return {
      type: 'init',
      info: {
        id,
        columns: reader.columns(),
        groups: reader.groups(),
      },
    };
  }

  const reader = readers.get(message.id);
  if (reader === undefined) {
    throw new Error(`missing reader: ${message.id}`);
  }

  switch (message.type) {
    case 'dict': {
      const out = await reader.dictFor(message.columnNo, message.groupNo);
      if (!out) {
        throw new Error(`no dict for c=${message.columnNo} g=${message.groupNo}`);
      }
      const data = await out.read();

      return {
        type: 'dict',
        r: { ...out, data },
      };
    }
  }

  throw new Error(`unhandled message type: ${message.type}`);
}

self.addEventListener('message', buildRpcHost(handleMessage));
