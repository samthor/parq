type MessageRequest = {
  message: any;
  port: MessagePort;
};

type MessageResponse = {
  message?: any;
  error?: any;
  time: number;
};

export type RpcHostContext = {
  transfer(t: Transferable);
};

/**
 * Builds a lazy RPC method that can call something with a `postMessage` function.
 */
export function buildRpcClient<Request, Reply>(target: {
  postMessage: MessagePort['postMessage'];
}) {
  const { postMessage } = target;

  return async (
    message: Request,
    args?: { signal?: AbortSignal; transfer?: Transferable[] },
  ): Promise<Reply> => {
    if (args?.signal?.aborted) {
      throw new Error(`aborted`);
    }

    const ch = new MessageChannel();
    const request: MessageRequest = { message, port: ch.port1 };
    postMessage.apply(target, [request, [ch.port1]]);

    return new Promise((resolve, reject) => {
      args?.signal?.addEventListener('abort', () => {
        reject(new Error('aborted'));
      });

      ch.port2.start();
      ch.port2.addEventListener(
        'message',
        (e) => {
          const data = e.data as MessageResponse;
          if (data.error) {
            reject(data.error);
          } else {
            resolve(data.message);
          }
        },
        { signal: args?.signal, once: true },
      );
    });
  };
}

export function buildRpcHost<Request, Reply>(
  method: (data: Request, args: RpcHostContext) => Reply | Promise<Reply>,
): (e: MessageEvent) => any {
  return async (e) => {
    const request = e.data as MessageRequest;
    let reply: MessageResponse = { time: 0 };
    const start = performance.now();

    const tr: Transferable[] = [];

    const context: RpcHostContext = {
      transfer(object) {
        tr.push(object);
      },
    };

    try {
      reply.message = await method(request.message, context);
    } catch (error: any) {
      reply.error = error;
    }

    reply.time = performance.now() - start;
    request.port.postMessage(reply, tr);
  };
}
