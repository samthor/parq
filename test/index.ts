import test, { afterEach } from 'node:test';
import * as assert from 'node:assert';
import { cleanupHandles, flattenAsyncIterator, readerForData } from './helper.js';
import { iterateLengthByteArray } from '../src/length-array.js';
import { Type } from '../dep/thrift/parquet-code.js';
import { ColumnInfo } from '../types.js';
import { buildReader } from '../src/read.js';
import { flatRead } from '../src/flat.js';

const dec = new TextDecoder();

afterEach(async () => {
  await cleanupHandles();
});

test('columns', async () => {
  const pr = await buildReader(readerForData('userdata1.parquet'));

  const expected: ColumnInfo[] = [
    {
      name: 'registration_dttm',
      typeLength: 0,
      physicalType: Type.INT96,
      logicalType: undefined,
    },
    {
      name: 'id',
      typeLength: 0,
      physicalType: Type.INT32,
      logicalType: undefined,
    },
    {
      name: 'first_name',
      typeLength: 0,
      physicalType: Type.BYTE_ARRAY,
      logicalType: undefined,
    },
    {
      name: 'last_name',
      typeLength: 0,
      physicalType: Type.BYTE_ARRAY,
      logicalType: undefined,
    },
    {
      name: 'email',
      typeLength: 0,
      physicalType: Type.BYTE_ARRAY,
      logicalType: undefined,
    },
    {
      name: 'gender',
      typeLength: 0,
      physicalType: Type.BYTE_ARRAY,
      logicalType: undefined,
    },
    {
      name: 'ip_address',
      typeLength: 0,
      physicalType: Type.BYTE_ARRAY,
      logicalType: undefined,
    },
    {
      name: 'cc',
      typeLength: 0,
      physicalType: Type.BYTE_ARRAY,
      logicalType: undefined,
    },
    {
      name: 'country',
      typeLength: 0,
      physicalType: Type.BYTE_ARRAY,
      logicalType: undefined,
    },
    {
      name: 'birthdate',
      typeLength: 0,
      physicalType: Type.BYTE_ARRAY,
      logicalType: undefined,
    },
    {
      name: 'salary',
      typeLength: 0,
      physicalType: Type.DOUBLE,
      logicalType: undefined,
    },
    {
      name: 'title',
      typeLength: 0,
      physicalType: Type.BYTE_ARRAY,
      logicalType: undefined,
    },
    {
      name: 'comments',
      typeLength: 0,
      physicalType: Type.BYTE_ARRAY,
      logicalType: undefined,
    },
  ];
  assert.deepStrictEqual(pr.info().columns, expected);
});

test('newread', async () => {
  const pr = await buildReader(readerForData('userdata1.parquet'));
  const out = await flattenAsyncIterator(pr.load(0, 0));

  const dict = await pr.readAt(out[0].at);
  const lookup = await pr.lookupAt(out[0].lookup);
});

test('range', async () => {
  const pr = await buildReader(readerForData('userdata1.parquet'));
  console.info('info', pr.info());

  const l = pr.info().columns.length;
  for (let i = 0; i < l; ++i) {
    const r = await flattenAsyncIterator(pr.loadRange(0, 500, 500_000));
    console.info('got r for col', {col: i }, r);
  }
});

test('duration', async () => {
  const pr = await buildReader(readerForData('duration.parquet'));
  const out = await flattenAsyncIterator(pr.load(0, 0));

  const dict = await pr.readAt(out[0].at);
  const lookup = await pr.lookupAt(out[0].lookup);
});

test('read', async () => {
  const pr = await buildReader(readerForData('userdata1.parquet'));

  const columns = pr.info().columns;

  assert.strictEqual(pr.info().groups.length, 1);
  assert.strictEqual(columns.length, 13);

  // This has type INT96, which isn't really seen much (timestamp).
  // It indexes into some underlying data.
  const part0 = await flattenAsyncIterator(pr.load(0, 0));
  assert.notStrictEqual(part0[0].lookup, 0);
  const firstRead0 = await pr.readAt(part0[0].at);
  assert.strictEqual(firstRead0.count, 995); // dict has 995 values

  // Check BYTE_ARRAY, which involves later indexing into the variable-length data (for strings).
  const part8 = await flattenAsyncIterator(pr.load(8, 0));
  assert.notStrictEqual(part0[0].lookup, 0);
  const firstRead8 = await pr.readAt(part8[0].at);
  const dict8values = [...iterateLengthByteArray(firstRead8.raw)];

  const lookup8 = await pr.lookupAt(part8[0].lookup);

  assert.strictEqual(dec.decode(dict8values[lookup8[0]]), 'Indonesia');
  assert.strictEqual(dec.decode(dict8values[lookup8[1]]), 'Canada');
  assert.strictEqual(dec.decode(dict8values[lookup8[5]]), 'Indonesia');
});

test('flat', async () => {
  const pr = await buildReader(readerForData('userdata1.parquet'));
  const data = await flatRead(pr, 0, 0, 100);

  assert.strictEqual(data.bitLength, 96);
  assert.strictEqual(data.raw?.length, 12_000);
});

