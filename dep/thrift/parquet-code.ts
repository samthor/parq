import { type ThriftReader, readFastList } from './compiler-deps.ts';

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
      const { ftype, fid } = input.readFieldBegin();
      const key = (ftype << 8) + fid;
      switch (key) {
        case 0: {
          input.readStructEnd();
          return this;
        }
        case 2817: {
          this.max = input.readBinary();
          break;
        }
        case 2818: {
          this.min = input.readBinary();
          break;
        }
        case 2563: {
          this.null_count = input.readI64();
          break;
        }
        case 2564: {
          this.distinct_count = input.readI64();
          break;
        }
        case 2821: {
          this.max_value = input.readBinary();
          break;
        }
        case 2822: {
          this.min_value = input.readBinary();
          break;
        }
        default: {
          input.skip(ftype);
        }
      }
      // skip readFieldEnd
    }
  }
}

// struct StringType
export class StringType {
  read(input: ThriftReader): StringType {
    input.skip(12);
    return this;
  }
  static zeroInstance = new StringType();
}

// struct UUIDType
export class UUIDType {
  read(input: ThriftReader): UUIDType {
    input.skip(12);
    return this;
  }
  static zeroInstance = new UUIDType();
}

// struct MapType
export class MapType {
  read(input: ThriftReader): MapType {
    input.skip(12);
    return this;
  }
  static zeroInstance = new MapType();
}

// struct ListType
export class ListType {
  read(input: ThriftReader): ListType {
    input.skip(12);
    return this;
  }
  static zeroInstance = new ListType();
}

// struct EnumType
export class EnumType {
  read(input: ThriftReader): EnumType {
    input.skip(12);
    return this;
  }
  static zeroInstance = new EnumType();
}

// struct DateType
export class DateType {
  read(input: ThriftReader): DateType {
    input.skip(12);
    return this;
  }
  static zeroInstance = new DateType();
}

// struct NullType
export class NullType {
  read(input: ThriftReader): NullType {
    input.skip(12);
    return this;
  }
  static zeroInstance = new NullType();
}

// struct DecimalType
export class DecimalType {
  scale: number = 0;
  precision: number = 0;
  read(input: ThriftReader): DecimalType {
    input.readStructBegin();
    for (;;) {
      const { ftype, fid } = input.readFieldBegin();
      const key = (ftype << 8) + fid;
      switch (key) {
        case 0: {
          input.readStructEnd();
          return this;
        }
        case 2049: {
          this.scale = input.readI32();
          break;
        }
        case 2050: {
          this.precision = input.readI32();
          break;
        }
        default: {
          input.skip(ftype);
        }
      }
      // skip readFieldEnd
    }
  }
}

// struct MilliSeconds
export class MilliSeconds {
  read(input: ThriftReader): MilliSeconds {
    input.skip(12);
    return this;
  }
  static zeroInstance = new MilliSeconds();
}

// struct MicroSeconds
export class MicroSeconds {
  read(input: ThriftReader): MicroSeconds {
    input.skip(12);
    return this;
  }
  static zeroInstance = new MicroSeconds();
}

// struct NanoSeconds
export class NanoSeconds {
  read(input: ThriftReader): NanoSeconds {
    input.skip(12);
    return this;
  }
  static zeroInstance = new NanoSeconds();
}

// union TimeUnit
export class TimeUnit {
  MILLIS?: MilliSeconds;
  MICROS?: MicroSeconds;
  NANOS?: NanoSeconds;
  read(input: ThriftReader): TimeUnit {
    input.readStructBegin();
    for (;;) {
      const { ftype, fid } = input.readFieldBegin();
      const key = (ftype << 8) + fid;
      switch (key) {
        case 0: {
          input.readStructEnd();
          return this;
        }
        case 3073: {
          this.MILLIS = MilliSeconds.zeroInstance;
          break;
        }
        case 3074: {
          this.MICROS = MicroSeconds.zeroInstance;
          break;
        }
        case 3075: {
          this.NANOS = NanoSeconds.zeroInstance;
          break;
        }
        default: {
          input.skip(ftype);
        }
      }
      // skip readFieldEnd
    }
  }
}

// struct TimestampType
export class TimestampType {
  isAdjustedToUTC: boolean = false;
  unit: TimeUnit = new TimeUnit();
  read(input: ThriftReader): TimestampType {
    input.readStructBegin();
    for (;;) {
      const { ftype, fid } = input.readFieldBegin();
      const key = (ftype << 8) + fid;
      switch (key) {
        case 0: {
          input.readStructEnd();
          return this;
        }
        case 513: {
          this.isAdjustedToUTC = input.readBool();
          break;
        }
        case 3074: {
          this.unit = new TimeUnit().read(input);
          break;
        }
        default: {
          input.skip(ftype);
        }
      }
      // skip readFieldEnd
    }
  }
}

// struct TimeType
export class TimeType {
  isAdjustedToUTC: boolean = false;
  unit: TimeUnit = new TimeUnit();
  read(input: ThriftReader): TimeType {
    input.readStructBegin();
    for (;;) {
      const { ftype, fid } = input.readFieldBegin();
      const key = (ftype << 8) + fid;
      switch (key) {
        case 0: {
          input.readStructEnd();
          return this;
        }
        case 513: {
          this.isAdjustedToUTC = input.readBool();
          break;
        }
        case 3074: {
          this.unit = new TimeUnit().read(input);
          break;
        }
        default: {
          input.skip(ftype);
        }
      }
      // skip readFieldEnd
    }
  }
}

// struct IntType
export class IntType {
  bitWidth: number = 0;
  isSigned: boolean = false;
  read(input: ThriftReader): IntType {
    input.readStructBegin();
    for (;;) {
      const { ftype, fid } = input.readFieldBegin();
      const key = (ftype << 8) + fid;
      switch (key) {
        case 0: {
          input.readStructEnd();
          return this;
        }
        case 769: {
          this.bitWidth = input.readByte();
          break;
        }
        case 514: {
          this.isSigned = input.readBool();
          break;
        }
        default: {
          input.skip(ftype);
        }
      }
      // skip readFieldEnd
    }
  }
}

// struct JsonType
export class JsonType {
  read(input: ThriftReader): JsonType {
    input.skip(12);
    return this;
  }
  static zeroInstance = new JsonType();
}

// struct BsonType
export class BsonType {
  read(input: ThriftReader): BsonType {
    input.skip(12);
    return this;
  }
  static zeroInstance = new BsonType();
}

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
      const { ftype, fid } = input.readFieldBegin();
      const key = (ftype << 8) + fid;
      switch (key) {
        case 0: {
          input.readStructEnd();
          return this;
        }
        case 3073: {
          this.STRING = StringType.zeroInstance;
          break;
        }
        case 3074: {
          this.MAP = MapType.zeroInstance;
          break;
        }
        case 3075: {
          this.LIST = ListType.zeroInstance;
          break;
        }
        case 3076: {
          this.ENUM = EnumType.zeroInstance;
          break;
        }
        case 3077: {
          this.DECIMAL = new DecimalType().read(input);
          break;
        }
        case 3078: {
          this.DATE = DateType.zeroInstance;
          break;
        }
        case 3079: {
          this.TIME = new TimeType().read(input);
          break;
        }
        case 3080: {
          this.TIMESTAMP = new TimestampType().read(input);
          break;
        }
        case 3082: {
          this.INTEGER = new IntType().read(input);
          break;
        }
        case 3083: {
          this.UNKNOWN = NullType.zeroInstance;
          break;
        }
        case 3084: {
          this.JSON = JsonType.zeroInstance;
          break;
        }
        case 3085: {
          this.BSON = BsonType.zeroInstance;
          break;
        }
        case 3086: {
          this.UUID = UUIDType.zeroInstance;
          break;
        }
        default: {
          input.skip(ftype);
        }
      }
      // skip readFieldEnd
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
      const { ftype, fid } = input.readFieldBegin();
      const key = (ftype << 8) + fid;
      switch (key) {
        case 0: {
          input.readStructEnd();
          return this;
        }
        case 2049: {
          this.type = input.readI32();
          break;
        }
        case 2050: {
          this.type_length = input.readI32();
          break;
        }
        case 2051: {
          this.repetition_type = input.readI32();
          break;
        }
        case 2820: {
          this.name = input.readString();
          break;
        }
        case 2053: {
          this.num_children = input.readI32();
          break;
        }
        case 2054: {
          this.converted_type = input.readI32();
          break;
        }
        case 2055: {
          this.scale = input.readI32();
          break;
        }
        case 2056: {
          this.precision = input.readI32();
          break;
        }
        case 2057: {
          this.field_id = input.readI32();
          break;
        }
        case 3082: {
          this.logicalType = new LogicalType().read(input);
          break;
        }
        default: {
          input.skip(ftype);
        }
      }
      // skip readFieldEnd
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
      const { ftype, fid } = input.readFieldBegin();
      const key = (ftype << 8) + fid;
      switch (key) {
        case 0: {
          input.readStructEnd();
          return this;
        }
        case 2049: {
          this.num_values = input.readI32();
          break;
        }
        case 2050: {
          this.encoding = input.readI32();
          break;
        }
        case 2051: {
          this.definition_level_encoding = input.readI32();
          break;
        }
        case 2052: {
          this.repetition_level_encoding = input.readI32();
          break;
        }
        case 3077: {
          this.statistics = new Statistics().read(input);
          break;
        }
        default: {
          input.skip(ftype);
        }
      }
      // skip readFieldEnd
    }
  }
}

// struct IndexPageHeader
export class IndexPageHeader {
  read(input: ThriftReader): IndexPageHeader {
    input.skip(12);
    return this;
  }
  static zeroInstance = new IndexPageHeader();
}

// struct DictionaryPageHeader
export class DictionaryPageHeader {
  num_values: number = 0;
  encoding: Encoding = Encoding.PLAIN;
  is_sorted?: boolean;
  read(input: ThriftReader): DictionaryPageHeader {
    input.readStructBegin();
    for (;;) {
      const { ftype, fid } = input.readFieldBegin();
      const key = (ftype << 8) + fid;
      switch (key) {
        case 0: {
          input.readStructEnd();
          return this;
        }
        case 2049: {
          this.num_values = input.readI32();
          break;
        }
        case 2050: {
          this.encoding = input.readI32();
          break;
        }
        case 515: {
          this.is_sorted = input.readBool();
          break;
        }
        default: {
          input.skip(ftype);
        }
      }
      // skip readFieldEnd
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
  is_compressed?: boolean;
  statistics?: Statistics;
  read(input: ThriftReader): DataPageHeaderV2 {
    input.readStructBegin();
    for (;;) {
      const { ftype, fid } = input.readFieldBegin();
      const key = (ftype << 8) + fid;
      switch (key) {
        case 0: {
          input.readStructEnd();
          return this;
        }
        case 2049: {
          this.num_values = input.readI32();
          break;
        }
        case 2050: {
          this.num_nulls = input.readI32();
          break;
        }
        case 2051: {
          this.num_rows = input.readI32();
          break;
        }
        case 2052: {
          this.encoding = input.readI32();
          break;
        }
        case 2053: {
          this.definition_levels_byte_length = input.readI32();
          break;
        }
        case 2054: {
          this.repetition_levels_byte_length = input.readI32();
          break;
        }
        case 519: {
          this.is_compressed = input.readBool();
          break;
        }
        case 3080: {
          this.statistics = new Statistics().read(input);
          break;
        }
        default: {
          input.skip(ftype);
        }
      }
      // skip readFieldEnd
    }
  }
}

// struct SplitBlockAlgorithm
export class SplitBlockAlgorithm {
  read(input: ThriftReader): SplitBlockAlgorithm {
    input.skip(12);
    return this;
  }
  static zeroInstance = new SplitBlockAlgorithm();
}

// union BloomFilterAlgorithm
export class BloomFilterAlgorithm {
  BLOCK?: SplitBlockAlgorithm;
  read(input: ThriftReader): BloomFilterAlgorithm {
    input.readStructBegin();
    for (;;) {
      const { ftype, fid } = input.readFieldBegin();
      const key = (ftype << 8) + fid;
      switch (key) {
        case 0: {
          input.readStructEnd();
          return this;
        }
        case 3073: {
          this.BLOCK = SplitBlockAlgorithm.zeroInstance;
          break;
        }
        default: {
          input.skip(ftype);
        }
      }
      // skip readFieldEnd
    }
  }
}

// struct XxHash
export class XxHash {
  read(input: ThriftReader): XxHash {
    input.skip(12);
    return this;
  }
  static zeroInstance = new XxHash();
}

// union BloomFilterHash
export class BloomFilterHash {
  XXHASH?: XxHash;
  read(input: ThriftReader): BloomFilterHash {
    input.readStructBegin();
    for (;;) {
      const { ftype, fid } = input.readFieldBegin();
      const key = (ftype << 8) + fid;
      switch (key) {
        case 0: {
          input.readStructEnd();
          return this;
        }
        case 3073: {
          this.XXHASH = XxHash.zeroInstance;
          break;
        }
        default: {
          input.skip(ftype);
        }
      }
      // skip readFieldEnd
    }
  }
}

// struct Uncompressed
export class Uncompressed {
  read(input: ThriftReader): Uncompressed {
    input.skip(12);
    return this;
  }
  static zeroInstance = new Uncompressed();
}

// union BloomFilterCompression
export class BloomFilterCompression {
  UNCOMPRESSED?: Uncompressed;
  read(input: ThriftReader): BloomFilterCompression {
    input.readStructBegin();
    for (;;) {
      const { ftype, fid } = input.readFieldBegin();
      const key = (ftype << 8) + fid;
      switch (key) {
        case 0: {
          input.readStructEnd();
          return this;
        }
        case 3073: {
          this.UNCOMPRESSED = Uncompressed.zeroInstance;
          break;
        }
        default: {
          input.skip(ftype);
        }
      }
      // skip readFieldEnd
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
      const { ftype, fid } = input.readFieldBegin();
      const key = (ftype << 8) + fid;
      switch (key) {
        case 0: {
          input.readStructEnd();
          return this;
        }
        case 2049: {
          this.numBytes = input.readI32();
          break;
        }
        case 3074: {
          this.algorithm = new BloomFilterAlgorithm().read(input);
          break;
        }
        case 3075: {
          this.hash = new BloomFilterHash().read(input);
          break;
        }
        case 3076: {
          this.compression = new BloomFilterCompression().read(input);
          break;
        }
        default: {
          input.skip(ftype);
        }
      }
      // skip readFieldEnd
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
      const { ftype, fid } = input.readFieldBegin();
      const key = (ftype << 8) + fid;
      switch (key) {
        case 0: {
          input.readStructEnd();
          return this;
        }
        case 2049: {
          this.type = input.readI32();
          break;
        }
        case 2050: {
          this.uncompressed_page_size = input.readI32();
          break;
        }
        case 2051: {
          this.compressed_page_size = input.readI32();
          break;
        }
        case 2052: {
          this.crc = input.readI32();
          break;
        }
        case 3077: {
          this.data_page_header = new DataPageHeader().read(input);
          break;
        }
        case 3078: {
          this.index_page_header = IndexPageHeader.zeroInstance;
          break;
        }
        case 3079: {
          this.dictionary_page_header = new DictionaryPageHeader().read(input);
          break;
        }
        case 3080: {
          this.data_page_header_v2 = new DataPageHeaderV2().read(input);
          break;
        }
        default: {
          input.skip(ftype);
        }
      }
      // skip readFieldEnd
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
      const { ftype, fid } = input.readFieldBegin();
      const key = (ftype << 8) + fid;
      switch (key) {
        case 0: {
          input.readStructEnd();
          return this;
        }
        case 2817: {
          this.key = input.readString();
          break;
        }
        case 2818: {
          this.value = input.readString();
          break;
        }
        default: {
          input.skip(ftype);
        }
      }
      // skip readFieldEnd
    }
  }
}

// struct SortingColumn
export class SortingColumn {
  column_idx: number = 0;
  descending: boolean = false;
  nulls_first: boolean = false;
  read(input: ThriftReader): SortingColumn {
    input.readStructBegin();
    for (;;) {
      const { ftype, fid } = input.readFieldBegin();
      const key = (ftype << 8) + fid;
      switch (key) {
        case 0: {
          input.readStructEnd();
          return this;
        }
        case 2049: {
          this.column_idx = input.readI32();
          break;
        }
        case 514: {
          this.descending = input.readBool();
          break;
        }
        case 515: {
          this.nulls_first = input.readBool();
          break;
        }
        default: {
          input.skip(ftype);
        }
      }
      // skip readFieldEnd
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
      const { ftype, fid } = input.readFieldBegin();
      const key = (ftype << 8) + fid;
      switch (key) {
        case 0: {
          input.readStructEnd();
          return this;
        }
        case 2049: {
          this.page_type = input.readI32();
          break;
        }
        case 2050: {
          this.encoding = input.readI32();
          break;
        }
        case 2051: {
          this.count = input.readI32();
          break;
        }
        default: {
          input.skip(ftype);
        }
      }
      // skip readFieldEnd
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
      const { ftype, fid } = input.readFieldBegin();
      const key = (ftype << 8) + fid;
      switch (key) {
        case 0: {
          input.readStructEnd();
          return this;
        }
        case 2049: {
          this.type = input.readI32();
          break;
        }
        case 3842: {
          this.encodings = readFastList(input, 8, () => input.readI32());
          break;
        }
        case 3843: {
          this.path_in_schema = readFastList(input, 11, () => input.readString());
          break;
        }
        case 2052: {
          this.codec = input.readI32();
          break;
        }
        case 2565: {
          this.num_values = input.readI64();
          break;
        }
        case 2566: {
          this.total_uncompressed_size = input.readI64();
          break;
        }
        case 2567: {
          this.total_compressed_size = input.readI64();
          break;
        }
        case 3848: {
          this.key_value_metadata = readFastList(input, 12, () => new KeyValue().read(input));
          break;
        }
        case 2569: {
          this.data_page_offset = input.readI64();
          break;
        }
        case 2570: {
          this.index_page_offset = input.readI64();
          break;
        }
        case 2571: {
          this.dictionary_page_offset = input.readI64();
          break;
        }
        case 3084: {
          this.statistics = new Statistics().read(input);
          break;
        }
        case 3853: {
          this.encoding_stats = readFastList(input, 12, () => new PageEncodingStats().read(input));
          break;
        }
        case 2574: {
          this.bloom_filter_offset = input.readI64();
          break;
        }
        default: {
          input.skip(ftype);
        }
      }
      // skip readFieldEnd
    }
  }
}

// struct EncryptionWithFooterKey
export class EncryptionWithFooterKey {
  read(input: ThriftReader): EncryptionWithFooterKey {
    input.skip(12);
    return this;
  }
  static zeroInstance = new EncryptionWithFooterKey();
}

// struct EncryptionWithColumnKey
export class EncryptionWithColumnKey {
  path_in_schema: Array<string> = [];
  key_metadata?: Uint8Array;
  read(input: ThriftReader): EncryptionWithColumnKey {
    input.readStructBegin();
    for (;;) {
      const { ftype, fid } = input.readFieldBegin();
      const key = (ftype << 8) + fid;
      switch (key) {
        case 0: {
          input.readStructEnd();
          return this;
        }
        case 3841: {
          this.path_in_schema = readFastList(input, 11, () => input.readString());
          break;
        }
        case 2818: {
          this.key_metadata = input.readBinary();
          break;
        }
        default: {
          input.skip(ftype);
        }
      }
      // skip readFieldEnd
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
      const { ftype, fid } = input.readFieldBegin();
      const key = (ftype << 8) + fid;
      switch (key) {
        case 0: {
          input.readStructEnd();
          return this;
        }
        case 3073: {
          this.ENCRYPTION_WITH_FOOTER_KEY = EncryptionWithFooterKey.zeroInstance;
          break;
        }
        case 3074: {
          this.ENCRYPTION_WITH_COLUMN_KEY = new EncryptionWithColumnKey().read(input);
          break;
        }
        default: {
          input.skip(ftype);
        }
      }
      // skip readFieldEnd
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
      const { ftype, fid } = input.readFieldBegin();
      const key = (ftype << 8) + fid;
      switch (key) {
        case 0: {
          input.readStructEnd();
          return this;
        }
        case 2817: {
          this.file_path = input.readString();
          break;
        }
        case 2562: {
          this.file_offset = input.readI64();
          break;
        }
        case 3075: {
          this.meta_data = new ColumnMetaData().read(input);
          break;
        }
        case 2564: {
          this.offset_index_offset = input.readI64();
          break;
        }
        case 2053: {
          this.offset_index_length = input.readI32();
          break;
        }
        case 2566: {
          this.column_index_offset = input.readI64();
          break;
        }
        case 2055: {
          this.column_index_length = input.readI32();
          break;
        }
        case 3080: {
          this.crypto_metadata = new ColumnCryptoMetaData().read(input);
          break;
        }
        case 2825: {
          this.encrypted_column_metadata = input.readBinary();
          break;
        }
        default: {
          input.skip(ftype);
        }
      }
      // skip readFieldEnd
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
      const { ftype, fid } = input.readFieldBegin();
      const key = (ftype << 8) + fid;
      switch (key) {
        case 0: {
          input.readStructEnd();
          return this;
        }
        case 3841: {
          this.columns = readFastList(input, 12, () => new ColumnChunk().read(input));
          break;
        }
        case 2562: {
          this.total_byte_size = input.readI64();
          break;
        }
        case 2563: {
          this.num_rows = input.readI64();
          break;
        }
        case 3844: {
          this.sorting_columns = readFastList(input, 12, () => new SortingColumn().read(input));
          break;
        }
        case 2565: {
          this.file_offset = input.readI64();
          break;
        }
        case 2566: {
          this.total_compressed_size = input.readI64();
          break;
        }
        case 1543: {
          this.ordinal = input.readI16();
          break;
        }
        default: {
          input.skip(ftype);
        }
      }
      // skip readFieldEnd
    }
  }
}

// struct TypeDefinedOrder
export class TypeDefinedOrder {
  read(input: ThriftReader): TypeDefinedOrder {
    input.skip(12);
    return this;
  }
  static zeroInstance = new TypeDefinedOrder();
}

// union ColumnOrder
export class ColumnOrder {
  TYPE_ORDER?: TypeDefinedOrder;
  read(input: ThriftReader): ColumnOrder {
    input.readStructBegin();
    for (;;) {
      const { ftype, fid } = input.readFieldBegin();
      const key = (ftype << 8) + fid;
      switch (key) {
        case 0: {
          input.readStructEnd();
          return this;
        }
        case 3073: {
          this.TYPE_ORDER = TypeDefinedOrder.zeroInstance;
          break;
        }
        default: {
          input.skip(ftype);
        }
      }
      // skip readFieldEnd
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
      const { ftype, fid } = input.readFieldBegin();
      const key = (ftype << 8) + fid;
      switch (key) {
        case 0: {
          input.readStructEnd();
          return this;
        }
        case 2561: {
          this.offset = input.readI64();
          break;
        }
        case 2050: {
          this.compressed_page_size = input.readI32();
          break;
        }
        case 2563: {
          this.first_row_index = input.readI64();
          break;
        }
        default: {
          input.skip(ftype);
        }
      }
      // skip readFieldEnd
    }
  }
}

// struct OffsetIndex
export class OffsetIndex {
  page_locations: Array<PageLocation> = [];
  read(input: ThriftReader): OffsetIndex {
    input.readStructBegin();
    for (;;) {
      const { ftype, fid } = input.readFieldBegin();
      const key = (ftype << 8) + fid;
      switch (key) {
        case 0: {
          input.readStructEnd();
          return this;
        }
        case 3841: {
          this.page_locations = readFastList(input, 12, () => new PageLocation().read(input));
          break;
        }
        default: {
          input.skip(ftype);
        }
      }
      // skip readFieldEnd
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
      const { ftype, fid } = input.readFieldBegin();
      const key = (ftype << 8) + fid;
      switch (key) {
        case 0: {
          input.readStructEnd();
          return this;
        }
        case 3841: {
          this.null_pages = readFastList(input, 2, () => input.readBool());
          break;
        }
        case 3842: {
          this.min_values = readFastList(input, 11, () => input.readBinary());
          break;
        }
        case 3843: {
          this.max_values = readFastList(input, 11, () => input.readBinary());
          break;
        }
        case 2052: {
          this.boundary_order = input.readI32();
          break;
        }
        case 3845: {
          this.null_counts = readFastList(input, 10, () => input.readI64());
          break;
        }
        default: {
          input.skip(ftype);
        }
      }
      // skip readFieldEnd
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
      const { ftype, fid } = input.readFieldBegin();
      const key = (ftype << 8) + fid;
      switch (key) {
        case 0: {
          input.readStructEnd();
          return this;
        }
        case 2817: {
          this.aad_prefix = input.readBinary();
          break;
        }
        case 2818: {
          this.aad_file_unique = input.readBinary();
          break;
        }
        case 515: {
          this.supply_aad_prefix = input.readBool();
          break;
        }
        default: {
          input.skip(ftype);
        }
      }
      // skip readFieldEnd
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
      const { ftype, fid } = input.readFieldBegin();
      const key = (ftype << 8) + fid;
      switch (key) {
        case 0: {
          input.readStructEnd();
          return this;
        }
        case 2817: {
          this.aad_prefix = input.readBinary();
          break;
        }
        case 2818: {
          this.aad_file_unique = input.readBinary();
          break;
        }
        case 515: {
          this.supply_aad_prefix = input.readBool();
          break;
        }
        default: {
          input.skip(ftype);
        }
      }
      // skip readFieldEnd
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
      const { ftype, fid } = input.readFieldBegin();
      const key = (ftype << 8) + fid;
      switch (key) {
        case 0: {
          input.readStructEnd();
          return this;
        }
        case 3073: {
          this.AES_GCM_V1 = new AesGcmV1().read(input);
          break;
        }
        case 3074: {
          this.AES_GCM_CTR_V1 = new AesGcmCtrV1().read(input);
          break;
        }
        default: {
          input.skip(ftype);
        }
      }
      // skip readFieldEnd
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
      const { ftype, fid } = input.readFieldBegin();
      const key = (ftype << 8) + fid;
      switch (key) {
        case 0: {
          input.readStructEnd();
          return this;
        }
        case 2049: {
          this.version = input.readI32();
          break;
        }
        case 3842: {
          this.schema = readFastList(input, 12, () => new SchemaElement().read(input));
          break;
        }
        case 2563: {
          this.num_rows = input.readI64();
          break;
        }
        case 3844: {
          this.row_groups = readFastList(input, 12, () => new RowGroup().read(input));
          break;
        }
        case 3845: {
          this.key_value_metadata = readFastList(input, 12, () => new KeyValue().read(input));
          break;
        }
        case 2822: {
          this.created_by = input.readString();
          break;
        }
        case 3847: {
          this.column_orders = readFastList(input, 12, () => new ColumnOrder().read(input));
          break;
        }
        case 3080: {
          this.encryption_algorithm = new EncryptionAlgorithm().read(input);
          break;
        }
        case 2825: {
          this.footer_signing_key_metadata = input.readBinary();
          break;
        }
        default: {
          input.skip(ftype);
        }
      }
      // skip readFieldEnd
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
      const { ftype, fid } = input.readFieldBegin();
      const key = (ftype << 8) + fid;
      switch (key) {
        case 0: {
          input.readStructEnd();
          return this;
        }
        case 3073: {
          this.encryption_algorithm = new EncryptionAlgorithm().read(input);
          break;
        }
        case 2818: {
          this.key_metadata = input.readBinary();
          break;
        }
        default: {
          input.skip(ftype);
        }
      }
      // skip readFieldEnd
    }
  }
}


