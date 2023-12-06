import { LogicalType } from '../dep/thrift/parquet-code';
import { flatIterate } from '../src/flat';
import { flattenAsyncIterator } from '../src/helper/it';
import { typedArrayView } from '../src/view';
import { ParquetReader, PhysicalType } from '../types';

export class DemoTableElement extends HTMLElement {
  private root: ShadowRoot;
  private tbody: HTMLTableSectionElement;

  constructor(private pq: ParquetReader) {
    super();

    this.root = this.attachShadow({ mode: 'open' });

    const table = document.createElement('table');

    const thead = document.createElement('thead');
    const tbody = document.createElement('tbody');
    table.append(thead, tbody);

    const theadRow = document.createElement('tr');
    thead.append(theadRow);

    theadRow.append(
      ...this.pq.info().columns.map((col) => {
        const el = document.createElement('th');
        el.textContent = col.name;
        return el;
      }),
    );

    this.root.textContent = '';
    this.root.append(table);

    this.tbody = tbody;

    // TODO
    this.update(0, 100).catch((err) => {
      console.warn(err);
    });
  }

  async update(start: number, end: number) {
    this.tbody.textContent = '';

    const trBase = document.createElement('tr');
    let i = 0;
    for (const c of this.pq.info().columns) {
      const td = document.createElement('td');
      td.innerHTML = '&mdash;';
      trBase.append(td);
      ++i;
    }

    const rows: HTMLTableRowElement[] = [];
    for (let i = start; i < end; ++i) {
      rows.push(trBase.cloneNode(true) as HTMLTableRowElement);
    }
    this.tbody.append(...rows);

    const readTasks = this.pq.info().columns.map(async (c, i) => {
      const it = flatIterate(this.pq, i, start, end);
      let index = 0; // TODO: wrong
      for await (const value of it) {
        const el = rows[index].children[i];
        el.textContent = String(renderValue(value, c.physicalType, c.logicalType));
        ++index;

        if (index === rows.length) {
          return;
        }
      }
    });
//    await Promise.all(readTasks);
  }
}

customElements.define('demo-table', DemoTableElement);

const dec = new TextDecoder();

function renderValue(raw: Uint8Array, t: PhysicalType, l?: LogicalType) {
  const dv = typedArrayView(raw);

  // datetime
  if (l?.TIMESTAMP && t === PhysicalType.INT64) {
    let v = Number(dv.getBigInt64(0, true));

    const info = l.TIMESTAMP;

    if (info.unit.MICROS) {
      v /= 1_000;
    } else if (info.unit.NANOS) {
      v /= (1_000 * 1_000);
    }

    if (info.isAdjustedToUTC) {
      // TODO: ???
    }

    const d = new Date(v);
    return d.toISOString();
  }

  // string
  if (l?.STRING && t === PhysicalType.BYTE_ARRAY) {
    return dec.decode(raw);
  }

  if (l) {
    console.info('got unhandled LogicalType', l, 'for', t);
  }

  switch (t) {
    case PhysicalType.BYTE_ARRAY:
      return `<b=${raw.join(',')}>`;

    case PhysicalType.INT32:
      return dv.getInt32(0, true);

    case PhysicalType.INT64:
      return dv.getBigInt64(0, true);

    case PhysicalType.FLOAT:
      return dv.getFloat32(0, true);

    case PhysicalType.DOUBLE:
      return dv.getFloat64(0, true);

    case PhysicalType.BOOLEAN:
      return raw[0] ? 'true' : 'false';

    case PhysicalType.FIXED_LEN_BYTE_ARRAY:
      return `<b=${raw.join(',')}>`;

    case PhysicalType.INT96: {
      const parts = [dv.getInt32(0, true), dv.getInt32(4, true), dv.getInt32(8, true)];
      return `<int96=${parts.join(',')}>`;
    }
  }

  return '?';
}
