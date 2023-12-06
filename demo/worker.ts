import { buildReader } from '../src';
import { flattenAsyncIterator } from '../src/helper/it';
import type { Data, ParquetInfo, ParquetReader, Part, Reader, UintArray } from '../types';
import { RpcHostContext, buildRpcHost } from './worker-api';

export type WorkerRequest =
  | {
      type: 'init';
      blob: Blob;
      name: string;
    }
  | {
      type: 'readAt' | 'lookupAt';
      id: string;
      at: number;
    }
  | {
      type: 'loadRange';
      id: string;
      columnNo: number;
      start: number;
      end: number;
    };

export type WorkerReply =
  | {
      type: 'init';
      info: ParquetInfo;
      id: string;
    }
  | {
      type: 'loadRange';
      parts: Part[];
    }
  | {
      type: 'readAt';
      data: Data;
    }
  | {
      type: 'lookupAt';
      lookup: UintArray;
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
      info: reader.info(),
      id,
    };
  }

  const reader = readers.get(message.id);
  if (reader === undefined) {
    throw new Error(`missing reader: ${message.id}`);
  }

  switch (message.type) {
    case 'loadRange': {
      const parts = [
        ...(await flattenAsyncIterator(
          reader.loadRange(message.columnNo, message.start, message.end),
        )),
      ];
      return { type: 'loadRange', parts };
    }

    case 'readAt':
      return { type: 'readAt', data: await reader.readAt(message.at) };

    case 'lookupAt':
      return { type: 'lookupAt', lookup: await reader.lookupAt(message.at) };
  }

  throw new Error(`unhandled message type`);
}

self.addEventListener('message', buildRpcHost(handleMessage));
