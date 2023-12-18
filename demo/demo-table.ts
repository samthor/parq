import { LogicalType } from '../dep/thrift/parquet-code';
import { flatIterate } from '../src/flat';
import { typedArrayView } from '../src/view';
import { ParquetReader, PhysicalType } from '../types';
import * as thorish from 'thorish';

export class DemoTableElement extends HTMLElement {
  private root: ShadowRoot;
  private tbody: HTMLTableSectionElement;
  private theadRow: HTMLTableRowElement;
  private target = { start: 0, end: 0 };

  private wq: thorish.TaskType<undefined>;

  public readonly signal: AbortSignal;

  constructor(private pq: ParquetReader) {
    super();

    this.root = this.attachShadow({ mode: 'open' });

    this.root.innerHTML = `
<fieldset>
  <label>
    <span>Start</span>
    <input type="number" min="0" value="0" />
  </label>
  <label>
    <span>Rows</span>
    <select>
      <option>10</option>
      <option selected>100</option>
      <option>500</option>
    </select>
  </label>
  <label>
    Rows=<span id="rows"></span>
    <button>Close</button>
  </label>
</fieldset>
    `;

    const c = new AbortController();
    this.signal = c.signal;

    const fieldset = this.root.querySelector('fieldset')!;
    const startInput = fieldset.querySelector('input[type="number"]') as HTMLInputElement;
    const rowsInput = fieldset.querySelector('select')!;

    this.root.querySelector('#rows')!.textContent = String(pq.info().rows);
    this.root.querySelector('button')!.addEventListener('click', () => c.abort());

    const table = document.createElement('table');

    const thead = document.createElement('thead');
    const tbody = document.createElement('tbody');
    table.append(thead, tbody);

    const theadRow = document.createElement('tr');
    thead.append(theadRow);

    this.theadRow = theadRow;
    this.tbody = tbody;

    this.root.append(table);

    this.renderHeader();

    this.wq = thorish.workTask<undefined>(async () => {
      await this.internalUpdate();
    });
    fieldset.addEventListener('change', () => {
      const start = startInput.valueAsNumber;
      this.target = { start, end: start + +rowsInput.value };
      this.wq.queue(undefined);
    });
    fieldset.dispatchEvent(new CustomEvent('change'));
  }

  private renderHeader() {
    this.theadRow.append(
      document.createElement('th'),
      ...this.pq.info().columns.map((col) => {
        const el = document.createElement('th');
        el.textContent = col.name;
        return el;
      }),
    );
  }

  async internalUpdate() {
    let { start, end } = this.target;
    start = Math.max(0, start);

    this.tbody.textContent = '';

    const trBase = document.createElement('tr');
    trBase.append(document.createElement('th'));
    let i = 0;
    for (const c of this.pq.info().columns) {
      const td = document.createElement('td');
      td.innerHTML = '&mdash;';
      trBase.append(td);
      ++i;
    }

    // TODO: reuse all existing rows, cost is mostly layout. This means either:
    //  - zero overlap, restart
    //  - add/remove from start
    //  - add/remove from end

    const rows: HTMLTableRowElement[] = [];
    for (let i = start; i < end; ++i) {
      rows.push(trBase.cloneNode(true) as HTMLTableRowElement);
    }
    this.tbody.append(...rows);

    for (let i = 0; i < end - start; ++i) {
      rows[i].children[0].textContent = `#${i + start}`;
    }

    const readTasks = this.pq.info().columns.map(async (c, i) => {
      const it = flatIterate(this.pq, i, start, end);
      let index = 0;
      for await (const value of it) {
        const el = rows[index].children[i + 1];
        el.textContent = String(renderValue(value, c.physicalType, c.logicalType));
        ++index;
      }
    });
    await Promise.all(readTasks);
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
      v /= 1_000 * 1_000;
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

  // integer
  if (l?.INTEGER) {
    const integerIsSigned = l?.INTEGER?.isSigned ?? false;
    if (t === PhysicalType.INT32) {
      return integerIsSigned ? dv.getInt32(0, true) : dv.getUint32(0, true);
    } else if (t === PhysicalType.INT64) {
      if (!integerIsSigned) {
        console.debug('! unsigned int64')
      }
      return dv.getBigInt64(0, true);
    }
  }

  if (l?.JSON) {
    // TODO: string?
  }

  if (l?.UUID) {
    // TODO: uuid? (maybe FIXED_LEN_BYTE_ARRAY)
  }

  if (l) {
    console.debug('got unhandled LogicalType', l, 'for', t);
  }

  // TODO: some files don't have logicalType. Assume STRING on their behalf?

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
