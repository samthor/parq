import { type ThriftReader, readList } from 'thrift-tools';

// enum Type
export enum Type {
  BOOLEAN = 0,
  INT32 = 1,
  INT64 = 2,
  INT96 = 3,
  FLOAT = 4,
  DOUBLE = 5,
  BYTE_ARRAY = 6,
  FIXED_LEN_BYTE_ARRAY = 7,
}

// enum ConvertedType
export enum ConvertedType {
  UTF8 = 0,
  MAP = 1,
  MAP_KEY_VALUE = 2,
  LIST = 3,
  ENUM = 4,
  DECIMAL = 5,
  DATE = 6,
  TIME_MILLIS = 7,
  TIME_MICROS = 8,
  TIMESTAMP_MILLIS = 9,
  TIMESTAMP_MICROS = 10,
  UINT_8 = 11,
  UINT_16 = 12,
  UINT_32 = 13,
  UINT_64 = 14,
  INT_8 = 15,
  INT_16 = 16,
  INT_32 = 17,
  INT_64 = 18,
  JSON = 19,
  BSON = 20,
  INTERVAL = 21,
}

// enum FieldRepetitionType
export enum FieldRepetitionType {
  REQUIRED = 0,
  OPTIONAL = 1,
  REPEATED = 2,
}

// struct Statistics
export class Statistics {
  max?: Uint8Array;
  min?: Uint8Array;
  null_count?: number;
  distinct_count?: number;
  max_value?: Uint8Array;
  min_value?: Uint8Array;
  read(input: ThriftReader): Statistics {
    input.readStructBegin();
    for (;;) {
      const key = input.readStructKey();
      switch (key) {
        case 0:
          return this;
        case 264:
          this.max = input.readBinary();
          break;
        case 520:
          this.min = input.readBinary();
          break;
        case 774:
          this.null_count = input.readI64();
          break;
        case 1030:
          this.distinct_count = input.readI64();
          break;
        case 1288:
          this.max_value = input.readBinary();
          break;
        case 1544:
          this.min_value = input.readBinary();
          break;
        default:
          input.skip(key & 0xff);
      }
    }
  }
}

// struct StringType
export class StringType {
  read(input: ThriftReader): StringType {
    input.skip(12);
    return this;
  }
}
const _StringType_zeroInstance = new StringType();

// struct UUIDType
export class UUIDType {
  read(input: ThriftReader): UUIDType {
    input.skip(12);
    return this;
  }
}
const _UUIDType_zeroInstance = new UUIDType();

// struct MapType
export class MapType {
  read(input: ThriftReader): MapType {
    input.skip(12);
    return this;
  }
}
const _MapType_zeroInstance = new MapType();

// struct ListType
export class ListType {
  read(input: ThriftReader): ListType {
    input.skip(12);
    return this;
  }
}
const _ListType_zeroInstance = new ListType();

// struct EnumType
export class EnumType {
  read(input: ThriftReader): EnumType {
    input.skip(12);
    return this;
  }
}
const _EnumType_zeroInstance = new EnumType();

// struct DateType
export class DateType {
  read(input: ThriftReader): DateType {
    input.skip(12);
    return this;
  }
}
const _DateType_zeroInstance = new DateType();

// struct NullType
export class NullType {
  read(input: ThriftReader): NullType {
    input.skip(12);
    return this;
  }
}
const _NullType_zeroInstance = new NullType();

// struct DecimalType
export class DecimalType {
  scale: number = 0;
  precision: number = 0;
  read(input: ThriftReader): DecimalType {
    input.readStructBegin();
    for (;;) {
      const key = input.readStructKey();
      switch (key) {
        case 0:
          return this;
        case 261:
          this.scale = input.readI32();
          break;
        case 517:
          this.precision = input.readI32();
          break;
        default:
          input.skip(key & 0xff);
      }
    }
  }
}

// struct MilliSeconds
export class MilliSeconds {
  read(input: ThriftReader): MilliSeconds {
    input.skip(12);
    return this;
  }
}
const _MilliSeconds_zeroInstance = new MilliSeconds();

// struct MicroSeconds
export class MicroSeconds {
  read(input: ThriftReader): MicroSeconds {
    input.skip(12);
    return this;
  }
}
const _MicroSeconds_zeroInstance = new MicroSeconds();

// struct NanoSeconds
export class NanoSeconds {
  read(input: ThriftReader): NanoSeconds {
    input.skip(12);
    return this;
  }
}
const _NanoSeconds_zeroInstance = new NanoSeconds();

// union TimeUnit
export class TimeUnit {
  MILLIS?: MilliSeconds;
  MICROS?: MicroSeconds;
  NANOS?: NanoSeconds;
  read(input: ThriftReader): TimeUnit {
    input.readStructBegin();
    for (;;) {
      const key = input.readStructKey();
      switch (key) {
        case 0:
          return this;
        case 268:
          this.MILLIS = _MilliSeconds_zeroInstance.read(input);
          break;
        case 524:
          this.MICROS = _MicroSeconds_zeroInstance.read(input);
          break;
        case 780:
          this.NANOS = _NanoSeconds_zeroInstance.read(input);
          break;
        default:
          input.skip(key & 0xff);
      }
    }
  }
}

// struct TimestampType
export class TimestampType {
  isAdjustedToUTC: boolean = true;
  unit: TimeUnit = new TimeUnit();
  read(input: ThriftReader): TimestampType {
    input.readStructBegin();
    for (;;) {
      const key = input.readStructKey();
      switch (key) {
        case 0:
          return this;
        case 258:
          this.isAdjustedToUTC = input.readBool();
          break;
        case 524:
          this.unit = new TimeUnit().read(input);
          break;
        default:
          input.skip(key & 0xff);
      }
    }
  }
}

// struct TimeType
export class TimeType {
  isAdjustedToUTC: boolean = true;
  unit: TimeUnit = new TimeUnit();
  read(input: ThriftReader): TimeType {
    input.readStructBegin();
    for (;;) {
      const key = input.readStructKey();
      switch (key) {
        case 0:
          return this;
        case 258:
          this.isAdjustedToUTC = input.readBool();
          break;
        case 524:
          this.unit = new TimeUnit().read(input);
          break;
        default:
          input.skip(key & 0xff);
      }
    }
  }
}

// struct IntType
export class IntType {
  bitWidth: number = 0;
  isSigned: boolean = true;
  read(input: ThriftReader): IntType {
    input.readStructBegin();
    for (;;) {
      const key = input.readStructKey();
      switch (key) {
        case 0:
          return this;
        case 259:
          this.bitWidth = input.readByte();
          break;
        case 514:
          this.isSigned = input.readBool();
          break;
        default:
          input.skip(key & 0xff);
      }
    }
  }
}

// struct JsonType
export class JsonType {
  read(input: ThriftReader): JsonType {
    input.skip(12);
    return this;
  }
}
const _JsonType_zeroInstance = new JsonType();

// struct BsonType
export class BsonType {
  read(input: ThriftReader): BsonType {
    input.skip(12);
    return this;
  }
}
const _BsonType_zeroInstance = new BsonType();

// union LogicalType
export class LogicalType {
  STRING?: StringType;
  MAP?: MapType;
  LIST?: ListType;
  ENUM?: EnumType;
  DECIMAL?: DecimalType;
  DATE?: DateType;
  TIME?: TimeType;
  TIMESTAMP?: TimestampType;
  INTEGER?: IntType;
  UNKNOWN?: NullType;
  JSON?: JsonType;
  BSON?: BsonType;
  UUID?: UUIDType;
  read(input: ThriftReader): LogicalType {
    input.readStructBegin();
    for (;;) {
      const key = input.readStructKey();
      switch (key) {
        case 0:
          return this;
        case 268:
          this.STRING = _StringType_zeroInstance.read(input);
          break;
        case 524:
          this.MAP = _MapType_zeroInstance.read(input);
          break;
        case 780:
          this.LIST = _ListType_zeroInstance.read(input);
          break;
        case 1036:
          this.ENUM = _EnumType_zeroInstance.read(input);
          break;
        case 1292:
          this.DECIMAL = new DecimalType().read(input);
          break;
        case 1548:
          this.DATE = _DateType_zeroInstance.read(input);
          break;
        case 1804:
          this.TIME = new TimeType().read(input);
          break;
        case 2060:
          this.TIMESTAMP = new TimestampType().read(input);
          break;
        case 2572:
          this.INTEGER = new IntType().read(input);
          break;
        case 2828:
          this.UNKNOWN = _NullType_zeroInstance.read(input);
          break;
        case 3084:
          this.JSON = _JsonType_zeroInstance.read(input);
          break;
        case 3340:
          this.BSON = _BsonType_zeroInstance.read(input);
          break;
        case 3596:
          this.UUID = _UUIDType_zeroInstance.read(input);
          break;
        default:
          input.skip(key & 0xff);
      }
    }
  }
}

// struct SchemaElement
export class SchemaElement {
  type?: Type;
  type_length?: number;
  repetition_type?: FieldRepetitionType;
  name: string = '';
  num_children?: number;
  converted_type?: ConvertedType;
  scale?: number;
  precision?: number;
  field_id?: number;
  logicalType?: LogicalType;
  read(input: ThriftReader): SchemaElement {
    input.readStructBegin();
    for (;;) {
      const key = input.readStructKey();
      switch (key) {
        case 0:
          return this;
        case 261:
          this.type = input.readI32();
          break;
        case 517:
          this.type_length = input.readI32();
          break;
        case 773:
          this.repetition_type = input.readI32();
          break;
        case 1032:
          this.name = input.readString();
          break;
        case 1285:
          this.num_children = input.readI32();
          break;
        case 1541:
          this.converted_type = input.readI32();
          break;
        case 1797:
          this.scale = input.readI32();
          break;
        case 2053:
          this.precision = input.readI32();
          break;
        case 2309:
          this.field_id = input.readI32();
          break;
        case 2572:
          this.logicalType = new LogicalType().read(input);
          break;
        default:
          input.skip(key & 0xff);
      }
    }
  }
}

// enum Encoding
export enum Encoding {
  PLAIN = 0,
  PLAIN_DICTIONARY = 2,
  RLE = 3,
  BIT_PACKED = 4,
  DELTA_BINARY_PACKED = 5,
  DELTA_LENGTH_BYTE_ARRAY = 6,
  DELTA_BYTE_ARRAY = 7,
  RLE_DICTIONARY = 8,
  BYTE_STREAM_SPLIT = 9,
}

// enum CompressionCodec
export enum CompressionCodec {
  UNCOMPRESSED = 0,
  SNAPPY = 1,
  GZIP = 2,
  LZO = 3,
  BROTLI = 4,
  LZ4 = 5,
  ZSTD = 6,
  LZ4_RAW = 7,
}

// enum PageType
export enum PageType {
  DATA_PAGE = 0,
  INDEX_PAGE = 1,
  DICTIONARY_PAGE = 2,
  DATA_PAGE_V2 = 3,
}

// enum BoundaryOrder
export enum BoundaryOrder {
  UNORDERED = 0,
  ASCENDING = 1,
  DESCENDING = 2,
}

// struct DataPageHeader
export class DataPageHeader {
  num_values: number = 0;
  encoding: Encoding = Encoding.PLAIN;
  definition_level_encoding: Encoding = Encoding.PLAIN;
  repetition_level_encoding: Encoding = Encoding.PLAIN;
  statistics?: Statistics;
  read(input: ThriftReader): DataPageHeader {
    input.readStructBegin();
    for (;;) {
      const key = input.readStructKey();
      switch (key) {
        case 0:
          return this;
        case 261:
          this.num_values = input.readI32();
          break;
        case 517:
          this.encoding = input.readI32();
          break;
        case 773:
          this.definition_level_encoding = input.readI32();
          break;
        case 1029:
          this.repetition_level_encoding = input.readI32();
          break;
        case 1292:
          this.statistics = new Statistics().read(input);
          break;
        default:
          input.skip(key & 0xff);
      }
    }
  }
}

// struct IndexPageHeader
export class IndexPageHeader {
  read(input: ThriftReader): IndexPageHeader {
    input.skip(12);
    return this;
  }
}
const _IndexPageHeader_zeroInstance = new IndexPageHeader();

// struct DictionaryPageHeader
export class DictionaryPageHeader {
  num_values: number = 0;
  encoding: Encoding = Encoding.PLAIN;
  is_sorted?: boolean;
  read(input: ThriftReader): DictionaryPageHeader {
    input.readStructBegin();
    for (;;) {
      const key = input.readStructKey();
      switch (key) {
        case 0:
          return this;
        case 261:
          this.num_values = input.readI32();
          break;
        case 517:
          this.encoding = input.readI32();
          break;
        case 770:
          this.is_sorted = input.readBool();
          break;
        default:
          input.skip(key & 0xff);
      }
    }
  }
}

// struct DataPageHeaderV2
export class DataPageHeaderV2 {
  num_values: number = 0;
  num_nulls: number = 0;
  num_rows: number = 0;
  encoding: Encoding = Encoding.PLAIN;
  definition_levels_byte_length: number = 0;
  repetition_levels_byte_length: number = 0;
  is_compressed: boolean = true;
  statistics?: Statistics;
  read(input: ThriftReader): DataPageHeaderV2 {
    input.readStructBegin();
    for (;;) {
      const key = input.readStructKey();
      switch (key) {
        case 0:
          return this;
        case 261:
          this.num_values = input.readI32();
          break;
        case 517:
          this.num_nulls = input.readI32();
          break;
        case 773:
          this.num_rows = input.readI32();
          break;
        case 1029:
          this.encoding = input.readI32();
          break;
        case 1285:
          this.definition_levels_byte_length = input.readI32();
          break;
        case 1541:
          this.repetition_levels_byte_length = input.readI32();
          break;
        case 1794:
          this.is_compressed = input.readBool();
          break;
        case 2060:
          this.statistics = new Statistics().read(input);
          break;
        default:
          input.skip(key & 0xff);
      }
    }
  }
}

// struct SplitBlockAlgorithm
export class SplitBlockAlgorithm {
  read(input: ThriftReader): SplitBlockAlgorithm {
    input.skip(12);
    return this;
  }
}
const _SplitBlockAlgorithm_zeroInstance = new SplitBlockAlgorithm();

// union BloomFilterAlgorithm
export class BloomFilterAlgorithm {
  BLOCK?: SplitBlockAlgorithm;
  read(input: ThriftReader): BloomFilterAlgorithm {
    input.readStructBegin();
    for (;;) {
      const key = input.readStructKey();
      switch (key) {
        case 0:
          return this;
        case 268:
          this.BLOCK = _SplitBlockAlgorithm_zeroInstance.read(input);
          break;
        default:
          input.skip(key & 0xff);
      }
    }
  }
}

// struct XxHash
export class XxHash {
  read(input: ThriftReader): XxHash {
    input.skip(12);
    return this;
  }
}
const _XxHash_zeroInstance = new XxHash();

// union BloomFilterHash
export class BloomFilterHash {
  XXHASH?: XxHash;
  read(input: ThriftReader): BloomFilterHash {
    input.readStructBegin();
    for (;;) {
      const key = input.readStructKey();
      switch (key) {
        case 0:
          return this;
        case 268:
          this.XXHASH = _XxHash_zeroInstance.read(input);
          break;
        default:
          input.skip(key & 0xff);
      }
    }
  }
}

// struct Uncompressed
export class Uncompressed {
  read(input: ThriftReader): Uncompressed {
    input.skip(12);
    return this;
  }
}
const _Uncompressed_zeroInstance = new Uncompressed();

// union BloomFilterCompression
export class BloomFilterCompression {
  UNCOMPRESSED?: Uncompressed;
  read(input: ThriftReader): BloomFilterCompression {
    input.readStructBegin();
    for (;;) {
      const key = input.readStructKey();
      switch (key) {
        case 0:
          return this;
        case 268:
          this.UNCOMPRESSED = _Uncompressed_zeroInstance.read(input);
          break;
        default:
          input.skip(key & 0xff);
      }
    }
  }
}

// struct BloomFilterHeader
export class BloomFilterHeader {
  numBytes: number = 0;
  algorithm: BloomFilterAlgorithm = new BloomFilterAlgorithm();
  hash: BloomFilterHash = new BloomFilterHash();
  compression: BloomFilterCompression = new BloomFilterCompression();
  read(input: ThriftReader): BloomFilterHeader {
    input.readStructBegin();
    for (;;) {
      const key = input.readStructKey();
      switch (key) {
        case 0:
          return this;
        case 261:
          this.numBytes = input.readI32();
          break;
        case 524:
          this.algorithm = new BloomFilterAlgorithm().read(input);
          break;
        case 780:
          this.hash = new BloomFilterHash().read(input);
          break;
        case 1036:
          this.compression = new BloomFilterCompression().read(input);
          break;
        default:
          input.skip(key & 0xff);
      }
    }
  }
}

// struct PageHeader
export class PageHeader {
  type: PageType = PageType.DATA_PAGE;
  uncompressed_page_size: number = 0;
  compressed_page_size: number = 0;
  crc?: number;
  data_page_header?: DataPageHeader;
  index_page_header?: IndexPageHeader;
  dictionary_page_header?: DictionaryPageHeader;
  data_page_header_v2?: DataPageHeaderV2;
  read(input: ThriftReader): PageHeader {
    input.readStructBegin();
    for (;;) {
      const key = input.readStructKey();
      switch (key) {
        case 0:
          return this;
        case 261:
          this.type = input.readI32();
          break;
        case 517:
          this.uncompressed_page_size = input.readI32();
          break;
        case 773:
          this.compressed_page_size = input.readI32();
          break;
        case 1029:
          this.crc = input.readI32();
          break;
        case 1292:
          this.data_page_header = new DataPageHeader().read(input);
          break;
        case 1548:
          this.index_page_header = _IndexPageHeader_zeroInstance.read(input);
          break;
        case 1804:
          this.dictionary_page_header = new DictionaryPageHeader().read(input);
          break;
        case 2060:
          this.data_page_header_v2 = new DataPageHeaderV2().read(input);
          break;
        default:
          input.skip(key & 0xff);
      }
    }
  }
}

// struct KeyValue
export class KeyValue {
  key: string = '';
  value?: string;
  read(input: ThriftReader): KeyValue {
    input.readStructBegin();
    for (;;) {
      const key = input.readStructKey();
      switch (key) {
        case 0:
          return this;
        case 264:
          this.key = input.readString();
          break;
        case 520:
          this.value = input.readString();
          break;
        default:
          input.skip(key & 0xff);
      }
    }
  }
}

// struct SortingColumn
export class SortingColumn {
  column_idx: number = 0;
  descending: boolean = true;
  nulls_first: boolean = true;
  read(input: ThriftReader): SortingColumn {
    input.readStructBegin();
    for (;;) {
      const key = input.readStructKey();
      switch (key) {
        case 0:
          return this;
        case 261:
          this.column_idx = input.readI32();
          break;
        case 514:
          this.descending = input.readBool();
          break;
        case 770:
          this.nulls_first = input.readBool();
          break;
        default:
          input.skip(key & 0xff);
      }
    }
  }
}

// struct PageEncodingStats
export class PageEncodingStats {
  page_type: PageType = PageType.DATA_PAGE;
  encoding: Encoding = Encoding.PLAIN;
  count: number = 0;
  read(input: ThriftReader): PageEncodingStats {
    input.readStructBegin();
    for (;;) {
      const key = input.readStructKey();
      switch (key) {
        case 0:
          return this;
        case 261:
          this.page_type = input.readI32();
          break;
        case 517:
          this.encoding = input.readI32();
          break;
        case 773:
          this.count = input.readI32();
          break;
        default:
          input.skip(key & 0xff);
      }
    }
  }
}

// struct ColumnMetaData
export class ColumnMetaData {
  type: Type = Type.BOOLEAN;
  encodings: Array<Encoding> = [];
  path_in_schema: Array<string> = [];
  codec: CompressionCodec = CompressionCodec.UNCOMPRESSED;
  num_values: number = 0;
  total_uncompressed_size: number = 0;
  total_compressed_size: number = 0;
  key_value_metadata?: Array<KeyValue>;
  data_page_offset: number = 0;
  index_page_offset?: number;
  dictionary_page_offset?: number;
  statistics?: Statistics;
  encoding_stats?: Array<PageEncodingStats>;
  bloom_filter_offset?: number;
  read(input: ThriftReader): ColumnMetaData {
    input.readStructBegin();
    for (;;) {
      const key = input.readStructKey();
      switch (key) {
        case 0:
          return this;
        case 261:
          this.type = input.readI32();
          break;
        case 521:
          this.encodings = readList(input, 5, () => input.readI32());
          break;
        case 777:
          this.path_in_schema = readList(input, 8, () => input.readString());
          break;
        case 1029:
          this.codec = input.readI32();
          break;
        case 1286:
          this.num_values = input.readI64();
          break;
        case 1542:
          this.total_uncompressed_size = input.readI64();
          break;
        case 1798:
          this.total_compressed_size = input.readI64();
          break;
        case 2057:
          this.key_value_metadata = readList(input, 12, () => new KeyValue().read(input));
          break;
        case 2310:
          this.data_page_offset = input.readI64();
          break;
        case 2566:
          this.index_page_offset = input.readI64();
          break;
        case 2822:
          this.dictionary_page_offset = input.readI64();
          break;
        case 3084:
          this.statistics = new Statistics().read(input);
          break;
        case 3337:
          this.encoding_stats = readList(input, 12, () => new PageEncodingStats().read(input));
          break;
        case 3590:
          this.bloom_filter_offset = input.readI64();
          break;
        default:
          input.skip(key & 0xff);
      }
    }
  }
}

// struct EncryptionWithFooterKey
export class EncryptionWithFooterKey {
  read(input: ThriftReader): EncryptionWithFooterKey {
    input.skip(12);
    return this;
  }
}
const _EncryptionWithFooterKey_zeroInstance = new EncryptionWithFooterKey();

// struct EncryptionWithColumnKey
export class EncryptionWithColumnKey {
  path_in_schema: Array<string> = [];
  key_metadata?: Uint8Array;
  read(input: ThriftReader): EncryptionWithColumnKey {
    input.readStructBegin();
    for (;;) {
      const key = input.readStructKey();
      switch (key) {
        case 0:
          return this;
        case 265:
          this.path_in_schema = readList(input, 8, () => input.readString());
          break;
        case 520:
          this.key_metadata = input.readBinary();
          break;
        default:
          input.skip(key & 0xff);
      }
    }
  }
}

// union ColumnCryptoMetaData
export class ColumnCryptoMetaData {
  ENCRYPTION_WITH_FOOTER_KEY?: EncryptionWithFooterKey;
  ENCRYPTION_WITH_COLUMN_KEY?: EncryptionWithColumnKey;
  read(input: ThriftReader): ColumnCryptoMetaData {
    input.readStructBegin();
    for (;;) {
      const key = input.readStructKey();
      switch (key) {
        case 0:
          return this;
        case 268:
          this.ENCRYPTION_WITH_FOOTER_KEY = _EncryptionWithFooterKey_zeroInstance.read(input);
          break;
        case 524:
          this.ENCRYPTION_WITH_COLUMN_KEY = new EncryptionWithColumnKey().read(input);
          break;
        default:
          input.skip(key & 0xff);
      }
    }
  }
}

// struct ColumnChunk
export class ColumnChunk {
  file_path?: string;
  file_offset: number = 0;
  meta_data?: ColumnMetaData;
  offset_index_offset?: number;
  offset_index_length?: number;
  column_index_offset?: number;
  column_index_length?: number;
  crypto_metadata?: ColumnCryptoMetaData;
  encrypted_column_metadata?: Uint8Array;
  read(input: ThriftReader): ColumnChunk {
    input.readStructBegin();
    for (;;) {
      const key = input.readStructKey();
      switch (key) {
        case 0:
          return this;
        case 264:
          this.file_path = input.readString();
          break;
        case 518:
          this.file_offset = input.readI64();
          break;
        case 780:
          this.meta_data = new ColumnMetaData().read(input);
          break;
        case 1030:
          this.offset_index_offset = input.readI64();
          break;
        case 1285:
          this.offset_index_length = input.readI32();
          break;
        case 1542:
          this.column_index_offset = input.readI64();
          break;
        case 1797:
          this.column_index_length = input.readI32();
          break;
        case 2060:
          this.crypto_metadata = new ColumnCryptoMetaData().read(input);
          break;
        case 2312:
          this.encrypted_column_metadata = input.readBinary();
          break;
        default:
          input.skip(key & 0xff);
      }
    }
  }
}

// struct RowGroup
export class RowGroup {
  columns: Array<ColumnChunk> = [];
  total_byte_size: number = 0;
  num_rows: number = 0;
  sorting_columns?: Array<SortingColumn>;
  file_offset?: number;
  total_compressed_size?: number;
  ordinal?: number;
  read(input: ThriftReader): RowGroup {
    input.readStructBegin();
    for (;;) {
      const key = input.readStructKey();
      switch (key) {
        case 0:
          return this;
        case 265:
          this.columns = readList(input, 12, () => new ColumnChunk().read(input));
          break;
        case 518:
          this.total_byte_size = input.readI64();
          break;
        case 774:
          this.num_rows = input.readI64();
          break;
        case 1033:
          this.sorting_columns = readList(input, 12, () => new SortingColumn().read(input));
          break;
        case 1286:
          this.file_offset = input.readI64();
          break;
        case 1542:
          this.total_compressed_size = input.readI64();
          break;
        case 1796:
          this.ordinal = input.readI16();
          break;
        default:
          input.skip(key & 0xff);
      }
    }
  }
}

// struct TypeDefinedOrder
export class TypeDefinedOrder {
  read(input: ThriftReader): TypeDefinedOrder {
    input.skip(12);
    return this;
  }
}
const _TypeDefinedOrder_zeroInstance = new TypeDefinedOrder();

// union ColumnOrder
export class ColumnOrder {
  TYPE_ORDER?: TypeDefinedOrder;
  read(input: ThriftReader): ColumnOrder {
    input.readStructBegin();
    for (;;) {
      const key = input.readStructKey();
      switch (key) {
        case 0:
          return this;
        case 268:
          this.TYPE_ORDER = _TypeDefinedOrder_zeroInstance.read(input);
          break;
        default:
          input.skip(key & 0xff);
      }
    }
  }
}

// struct PageLocation
export class PageLocation {
  offset: number = 0;
  compressed_page_size: number = 0;
  first_row_index: number = 0;
  read(input: ThriftReader): PageLocation {
    input.readStructBegin();
    for (;;) {
      const key = input.readStructKey();
      switch (key) {
        case 0:
          return this;
        case 262:
          this.offset = input.readI64();
          break;
        case 517:
          this.compressed_page_size = input.readI32();
          break;
        case 774:
          this.first_row_index = input.readI64();
          break;
        default:
          input.skip(key & 0xff);
      }
    }
  }
}

// struct OffsetIndex
export class OffsetIndex {
  page_locations: Array<PageLocation> = [];
  read(input: ThriftReader): OffsetIndex {
    input.readStructBegin();
    for (;;) {
      const key = input.readStructKey();
      switch (key) {
        case 0:
          return this;
        case 265:
          this.page_locations = readList(input, 12, () => new PageLocation().read(input));
          break;
        default:
          input.skip(key & 0xff);
      }
    }
  }
}

// struct ColumnIndex
export class ColumnIndex {
  null_pages: Array<boolean> = [];
  min_values: Array<Uint8Array> = [];
  max_values: Array<Uint8Array> = [];
  boundary_order: BoundaryOrder = BoundaryOrder.UNORDERED;
  null_counts?: Array<number>;
  read(input: ThriftReader): ColumnIndex {
    input.readStructBegin();
    for (;;) {
      const key = input.readStructKey();
      switch (key) {
        case 0:
          return this;
        case 265:
          this.null_pages = readList(input, 2, () => input.readBool());
          break;
        case 521:
          this.min_values = readList(input, 8, () => input.readBinary());
          break;
        case 777:
          this.max_values = readList(input, 8, () => input.readBinary());
          break;
        case 1029:
          this.boundary_order = input.readI32();
          break;
        case 1289:
          this.null_counts = readList(input, 6, () => input.readI64());
          break;
        default:
          input.skip(key & 0xff);
      }
    }
  }
}

// struct AesGcmV1
export class AesGcmV1 {
  aad_prefix?: Uint8Array;
  aad_file_unique?: Uint8Array;
  supply_aad_prefix?: boolean;
  read(input: ThriftReader): AesGcmV1 {
    input.readStructBegin();
    for (;;) {
      const key = input.readStructKey();
      switch (key) {
        case 0:
          return this;
        case 264:
          this.aad_prefix = input.readBinary();
          break;
        case 520:
          this.aad_file_unique = input.readBinary();
          break;
        case 770:
          this.supply_aad_prefix = input.readBool();
          break;
        default:
          input.skip(key & 0xff);
      }
    }
  }
}

// struct AesGcmCtrV1
export class AesGcmCtrV1 {
  aad_prefix?: Uint8Array;
  aad_file_unique?: Uint8Array;
  supply_aad_prefix?: boolean;
  read(input: ThriftReader): AesGcmCtrV1 {
    input.readStructBegin();
    for (;;) {
      const key = input.readStructKey();
      switch (key) {
        case 0:
          return this;
        case 264:
          this.aad_prefix = input.readBinary();
          break;
        case 520:
          this.aad_file_unique = input.readBinary();
          break;
        case 770:
          this.supply_aad_prefix = input.readBool();
          break;
        default:
          input.skip(key & 0xff);
      }
    }
  }
}

// union EncryptionAlgorithm
export class EncryptionAlgorithm {
  AES_GCM_V1?: AesGcmV1;
  AES_GCM_CTR_V1?: AesGcmCtrV1;
  read(input: ThriftReader): EncryptionAlgorithm {
    input.readStructBegin();
    for (;;) {
      const key = input.readStructKey();
      switch (key) {
        case 0:
          return this;
        case 268:
          this.AES_GCM_V1 = new AesGcmV1().read(input);
          break;
        case 524:
          this.AES_GCM_CTR_V1 = new AesGcmCtrV1().read(input);
          break;
        default:
          input.skip(key & 0xff);
      }
    }
  }
}

// struct FileMetaData
export class FileMetaData {
  version: number = 0;
  schema: Array<SchemaElement> = [];
  num_rows: number = 0;
  row_groups: Array<RowGroup> = [];
  key_value_metadata?: Array<KeyValue>;
  created_by?: string;
  column_orders?: Array<ColumnOrder>;
  encryption_algorithm?: EncryptionAlgorithm;
  footer_signing_key_metadata?: Uint8Array;
  read(input: ThriftReader): FileMetaData {
    input.readStructBegin();
    for (;;) {
      const key = input.readStructKey();
      switch (key) {
        case 0:
          return this;
        case 261:
          this.version = input.readI32();
          break;
        case 521:
          this.schema = readList(input, 12, () => new SchemaElement().read(input));
          break;
        case 774:
          this.num_rows = input.readI64();
          break;
        case 1033:
          this.row_groups = readList(input, 12, () => new RowGroup().read(input));
          break;
        case 1289:
          this.key_value_metadata = readList(input, 12, () => new KeyValue().read(input));
          break;
        case 1544:
          this.created_by = input.readString();
          break;
        case 1801:
          this.column_orders = readList(input, 12, () => new ColumnOrder().read(input));
          break;
        case 2060:
          this.encryption_algorithm = new EncryptionAlgorithm().read(input);
          break;
        case 2312:
          this.footer_signing_key_metadata = input.readBinary();
          break;
        default:
          input.skip(key & 0xff);
      }
    }
  }
}

// struct FileCryptoMetaData
export class FileCryptoMetaData {
  encryption_algorithm: EncryptionAlgorithm = new EncryptionAlgorithm();
  key_metadata?: Uint8Array;
  read(input: ThriftReader): FileCryptoMetaData {
    input.readStructBegin();
    for (;;) {
      const key = input.readStructKey();
      switch (key) {
        case 0:
          return this;
        case 268:
          this.encryption_algorithm = new EncryptionAlgorithm().read(input);
          break;
        case 520:
          this.key_metadata = input.readBinary();
          break;
        default:
          input.skip(key & 0xff);
      }
    }
  }
}
