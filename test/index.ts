import test from 'node:test';
import * as assert from 'node:assert';
import { flattenAsyncIterator, readerForData } from './helper.js';
import { iterateLengthByteArray } from '../src/length-array.js';
import { ParquetIndexer } from '../src/indexer.js';
import { ConvertedType, Type } from '../dep/thrift/parquet-code.js';
import { ColumnInfo } from '../types.js';
import { buildReader } from '../src/read.js';

const dec = new TextDecoder();

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
  assert.deepStrictEqual(pr.columns(), expected);
});

test('read', async () => {
  const pr = await buildReader(readerForData('userdata1.parquet'));

  const columns = pr.columns();

  assert.strictEqual(pr.groups().length, 1);
  assert.strictEqual(columns.length, 13);

  // This has type INT96, which isn't really seen much (timestamp).
  // It indexes into some underlying data.
  const dict0 = (await pr.dictFor(0, 0))!;
  assert.notStrictEqual(null, dict0);
  assert.strictEqual(dict0.count, 995);
  const data0 = await dict0.read();
  assert.strictEqual(data0.type, Type.INT96);
  assert.strictEqual(data0.raw.length, (dict0.count * data0.bitLength) / 8);

  const index0 = await flattenAsyncIterator(pr.load(0, 0));
  const firstRead0 = await index0[0].read();
  if (!firstRead0.index) {
    throw new Error(`expected index=true`);
  }

  // Check BYTE_ARRAY, which involves later indexing into the variable-length data (for strings).
  const dict8 = (await pr.dictFor(8, 0))!;
  assert.notStrictEqual(null, dict8);
  assert.strictEqual(dict8.count, 120);
  const data8 = await dict8.read();
  assert.strictEqual(data8.type, Type.BYTE_ARRAY);

  const dict8values = [...iterateLengthByteArray(data8.raw)];
  assert.strictEqual(dict8values.length, dict8.count);

  const index8 = await flattenAsyncIterator(pr.load(8, 0));
  const firstRead8 = await index8[0].read();
  if (!firstRead8.index) {
    throw new Error(`expected index=true`);
  }

  assert.strictEqual(dec.decode(dict8values[firstRead8.ptr[0]]), 'Indonesia');
  assert.strictEqual(dec.decode(dict8values[firstRead8.ptr[1]]), 'Canada');
  assert.strictEqual(dec.decode(dict8values[firstRead8.ptr[5]]), 'Indonesia');
});

test('indexer', async () => {
  const pr = await buildReader(readerForData('userdata1.parquet'));
   const i = new ParquetIndexer(pr, 0);

  const out = await i.findRange({ start: 0, end: 1 });

  assert.strictEqual(out.start, 0);
  assert.strictEqual(out.end, 1_000);
  assert.strictEqual(out.data.length, 1);
  assert.strictEqual(out.data[0].dict, false);
});
