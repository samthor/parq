import * as pq from '../../dep/thrift/parquet-code.js';

export type SchemaNode = {
  nested: true;
  name: string;
  repeated: boolean;
  optional: boolean;
  children: Map<string, SchemaNode | SchemaLeafNode>;
};

export type SchemaLeafNode = {
  nested: false;
  name: string;
  repeated: boolean;
  optional: boolean;
  type: pq.Type;

  /**
   * Type length for {@link CompressionCodec.RLE}. Zero by default.
   */
  typeLength: number;

  /**
   * Repetition levels
   */
  rl: number;

  /**
   * Definition levels
   */
  dl: number;

  raw: pq.SchemaElement;
};

export function decodeSchema(elements: pq.SchemaElement[]) {
  const columns: SchemaLeafNode[] = [];
  const { node, consumed } = internalDecodeSchema(elements, columns, { rl: 0, dl: 0 });
  if (consumed !== elements.length) {
    throw new Error(
      `Could not decode entire schema: consumed=${consumed} length=${elements.length}`,
    );
  }
  return { root: node, columns };
}

function internalDecodeSchema(
  elements: pq.SchemaElement[],
  columns: SchemaLeafNode[],
  { rl, dl }: { rl: number; dl: number },
): {
  node: SchemaNode | SchemaLeafNode;
  consumed: number;
} {
  const raw = elements[0];
  if (raw === undefined) {
    throw new Error(`missing expected schema raw`);
  }

  let optional = false;
  let repeated = false;

  switch (raw.repetition_type) {
    case pq.FieldRepetitionType.REQUIRED:
      break;
    case pq.FieldRepetitionType.OPTIONAL:
      optional = true;
      ++dl;
      break;
    case pq.FieldRepetitionType.REPEATED:
      repeated = true;
      ++rl;
      break;
  }

  if (!raw.num_children) {
    if (raw.type === undefined) {
      throw new TypeError(`Got no type for non-leaf schema node`);
    }

    const node: SchemaLeafNode = {
      nested: false,
      name: raw.name,
      optional,
      repeated,
      rl,
      dl,
      type: raw.type,
      // Just used for RLE.
      typeLength: raw.type_length ?? 0,
      raw,
    };
    columns.push(node);
    return {
      node,
      consumed: 1,
    };
  }

  let remainingIndex = 1;
  const children = new Map<string, SchemaNode | SchemaLeafNode>();

  for (let i = 0; i < raw.num_children; ++i) {
    const remainingElements = elements.slice(remainingIndex);
    const { node: childNode, consumed } = internalDecodeSchema(remainingElements, columns, {
      rl,
      dl,
    });
    children.set(childNode.name, childNode);
    remainingIndex += consumed;
  }

  return {
    node: {
      nested: true,
      name: raw.name,
      optional,
      repeated,
      children,
    },
    consumed: remainingIndex,
  };
}
