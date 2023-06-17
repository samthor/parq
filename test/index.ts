import test from 'node:test';
import * as assert from 'node:assert';
import { ParquetReader } from '../src/read.js';
import { readerForData } from './helper.js';
import { DataType } from '../types.js';
import { arrayLengthTypeArray } from '../src/length-array.js';

test('read', async () => {
  const pr = new ParquetReader(readerForData('userdata1.parquet'));
  await pr.init();

  assert.strictEqual(pr.groups, 1);
  assert.strictEqual((await pr.columns()).length, 13);

  // This has type INT96, which isn't really seen much (timestamp).
  const dict0 = await pr.dictForColumnGroup(0, 0);
  assert.notStrictEqual(null, dict0);
  assert.strictEqual(dict0!.count, 995);
  const data0 = await dict0!.read();
  assert.strictEqual(data0.type, DataType.BIG_BYTE_ARRAY);
  assert.strictEqual(data0.arr.length, dict0!.count * data0.size);

  const dict8 = await pr.dictForColumnGroup(8, 0);
  assert.notStrictEqual(null, dict8);
  assert.strictEqual(dict8!.count, 120);
  const data8 = await dict8!.read();
  assert.strictEqual(data8.type, DataType.LENGTH_BYTE_ARRAY);

  const index = arrayLengthTypeArray(data8.arr);
  assert.strictEqual(index.length, dict8!.count);
});

test('index', async () => {

});
