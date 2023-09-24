import { ParquetReader } from '../src';
import type { Reader } from '../types';

async function prepareReader(r: Reader) {
  const reader = new ParquetReader(r);
  console.time('init');
  try {
    await reader.init();
  } finally {
    console.timeEnd('init');
  }
  console.info('got reader with', { rows: reader.rows(), cols: reader.columnLength() });
}

window.addEventListener('dragover', (e) => e.preventDefault());

window.addEventListener('drop', async (e) => {
  e.preventDefault();
  const files = e.dataTransfer?.files;
  if (!files) {
    return;
  }
  for (let i = 0; i < files.length; ++i) {
    const f = files[i];
    const reader = prepareReader(readerFromBlob(f));
    await reader;
  }
});

const readerFromBlob = (b: Blob): Reader => async (start, end) => {
  // reading ~50mb takes ~30ms
  const bytes = await b.slice(start, end).arrayBuffer();
  return new Uint8Array(bytes);
};
