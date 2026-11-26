# Field mappings and data fidelity

Mappings are deterministic projections of an event's key, before-image and after-image. They run after the original event is durable in the local journal and before destination delivery. The same mapping is applied on replay. A mapping is part of the saved task fingerprint: change it only through a reviewed new task generation with a reconciled destination.

The supported configuration is exactly:

```json
{
  "rename": {"source_field": "target_field"},
  "conversions": {"target_field": "STRING"},
  "exclude": ["source_field_to_remove"]
}
```

Empty maps/lists select identity projection. Field names are literal top-level names. There is no SQL expression language, JavaScript, row filter, computed column, default-value injection, JSONPath traversal, nested flattening or wildcard rename. A dot in a name is not a path expression.

## Projection order

For each present source field, the mapper:

1. Checks `exclude` using the **original source name**.
2. Resolves the destination name through `rename`.
3. Applies any conversion keyed by that **destination name**.
4. Inserts the result into the mapped row while retaining document iteration order.

Example: to turn MongoDB `_id` into an SQL `id`, the conversion is under `id`, not `_id`:

```json
{
  "rename": {"_id": "id"},
  "conversions": {"id": "LONG", "amount": "DECIMAL"},
  "exclude": ["debug_notes"]
}
```

Key fields cannot be excluded. Two source fields cannot produce the same destination name, including a rename that collides with an unchanged field. The mapper does not fill absent fields with null. A present null remains null through every conversion; a missing field remains absent. SQL sinks require a complete row of the expected columns for state writes. MongoDB replacement owns the entire mapped document, so an omitted field is removed from the destination document.

Mapped events use the sink's configured `database` and `table` when those options exist. PostgreSQL's `schema` remains an endpoint routing setting. Source position, operation, event identity and source timestamp are retained. A Kafka sink normally preserves the incoming database/table namespace because its endpoint is a topic, not a database table.

## Supported conversions

Conversion names are uppercase and case-sensitive. Unknown names fail configuration validation. Conversions are explicit; ordinary numeric strings are not automatically accepted by a numeric SQL sink.

| Name | Result | Accepted input and failure boundary |
| --- | --- | --- |
| `STRING` | Scalar text | Scalar `toString()` after supported BSON-tag unwrapping; maps/arrays require `JSON` instead |
| `LONG` | Signed 64-bit integer | Exact integral numeric scalar/string; fractions and overflow fail |
| `INTEGER` | Signed 32-bit integer | Exact integral numeric scalar/string; fractions and overflow fail |
| `DECIMAL` | Arbitrary-precision decimal | Numeric scalar/string parsed exactly; boolean, map and array inputs fail |
| `BOOLEAN` | Boolean | Boolean input or the exact strings `true` / `false`; numeric 0/1 and other spellings fail |
| `JSON` | JSON-encoded string | Encodes the original input, including nested objects, arrays and BSON tags |
| `BSON_INT64` | `{"$numberLong":"..."}` | Exact signed 64-bit integer; fractions and overflow fail |
| `BSON_DECIMAL` | `{"$numberDecimal":"..."}` | Exact decimal tag; a MongoDB destination additionally validates representability as Decimal128 |
| `BSON_OBJECT_ID` | `{"$oid":"..."}` | Exactly 24 hexadecimal characters, normalized to lowercase |

The unwrapping helper recognizes canonical `$numberInt`, `$numberLong`, `$numberDecimal` and `$oid` scalars. Canonical `$date` can be rendered as an ISO instant through `STRING`; it is not a general SQL temporal conversion. Other wrappers, including binary and `$numberDouble`, do not acquire arbitrary numeric or temporal meaning through the task mapper. Preserve the native representation for a compatible destination, or choose an explicitly supported conversion such as `JSON` for a text column.

`JSON` produces a **string**, not a destination-native JSON object. MySQL/MariaDB currently support text columns for that string and reject native JSON column types. PostgreSQL can bind a JSON string to an inspected JSON/JSONB column. If a PostgreSQL source already represents JSON as a string, applying `JSON` again adds another string-encoding layer; leave it unchanged when that is not intended.

An explicit conversion does not relax the destination's precision, width, nullability or type checks. `DECIMAL` does not round to fit a smaller SQL column. `STRING` does not truncate text. A destination failure retains the event instead of silently coercing it.

## Keys, updates and collisions

Every state destination needs a stable non-null logical key. Fixed-schema endpoints check the mapped primary-key names during preflight. Dynamic MongoDB/Kafka/changelog shapes still validate the key and row at delivery. Source key columns must map onto the target key columns; a field rename alone does not create a new source key.

A conversion of key **values** must be one-to-one over the real source data. For example, source string keys `"01"` and `"1"` both convert to numeric `1`. The mapper can reject structural field collisions and numeric overflow, but it cannot infer that such a projection is injective across an unbounded source. Review real key domains before choosing a conversion; otherwise separate source entities can overwrite the same destination entity.

Updates use the same mapping on before and after images. SQL destinations require the full before-image for an `UPDATE`, so an upstream key change can delete the previous key and upsert the new key in the same delivery transaction. MongoDB handles a mapped old `_id` followed by the new complete document in a transaction. ClickHouse current-state delivery emits an old-key tombstone before the new-key row. Kafka splits a changed-key update into an old-key DELETE and a new-key INSERT; these can be routed to different partitions and are not an atomic cross-partition transaction.

MySQL/MariaDB text primary-key columns require `binary` or a collation ending in `_bin`. Text keys ending in an ASCII space are rejected, including with binary collation, to prevent database comparison/padding rules from merging identities. Trailing ASCII spaces are also rejected for fixed `CHAR` values. An ObjectId target commonly uses:

```sql
id VARCHAR(24) COLLATE utf8mb4_bin NOT NULL PRIMARY KEY
```

This does not grant support for arbitrary collations, binary SQL columns or locale-dependent key normalization.

## Preserve precision through JSON

The Java event codec parses JSON integers as arbitrary-precision integers and decimal values as arbitrary-precision decimals. A value such as `9007199254740993` is preserved through the journal and supported Java connector paths. Destination checks then enforce the actual signed range and decimal precision/scale. ClickHouse's `UInt64` source representation can preserve `18446744073709551615`; mapping that value to a signed SQL `BIGINT` must fail.

Precision already lost by an external producer cannot be recovered. For example, do not first load a large integer into a JavaScript `Number` and then expect JSON serialization or a later `LONG` mapping to restore it. Use an exact-number encoder, or deliberately send a string with a matching explicit numeric conversion. If sending a Kafka envelope, its key and after-image must use the same representation and its Kafka record key must match the envelope key.

Missing, null and an empty string remain distinct. Decimal scale can be relevant to a destination even when two numeric values compare equal. Preserve explicit scale where needed and size the target schema rather than relying on implicit database rounding.

## MongoDB BSON representation

Native MongoDB events use canonical Extended JSON so BSON widths and types survive a generic JSON journal. For example:

```json
{
  "_id": {"$oid": "507f1f77bcf86cd799439011"},
  "count": {"$numberLong": "9007199254740993"},
  "amount": {"$numberDecimal": "1234567890123456.78"},
  "observedAt": {"$date": {"$numberLong": "1735689600123"}},
  "optional": null
}
```

Tagged scalar values are not ordinary application subdocuments. Native MongoDB-to-MongoDB copying keeps the tags, scalar types and nested document ordering. SQL-to-MongoDB mappings should create the intended numeric tags explicitly. ObjectId is not silently turned into a relational string, and nested objects are not silently flattened.

The MongoDB provider rejects dollar-prefixed application field names that would be ambiguous with Extended JSON type tags, unsupported BSON types, null `_id` and document/array-valued `_id`. Its destination requires `documentMode=replace`: every event replaces the complete mapped document, rather than merging arbitrary target-owned fields. See [MongoDB contract](M3_MONGODB.md) for the exact supported BSON types and image requirements.

## Worked mappings

### SQL signed key and decimal to MongoDB

Input row: `id BIGINT`, `name VARCHAR(120)`, `amount DECIMAL(18,2)`.

```json
{
  "rename": {"id": "_id"},
  "conversions": {"_id": "BSON_INT64", "amount": "BSON_DECIMAL"},
  "exclude": []
}
```

The destination receives an Int64 `_id` and Decimal128 amount. See the complete [MySQL-to-MongoDB task](../examples/tasks/mysql-to-mongodb.json).

### MongoDB numeric `_id` to SQL

```json
{
  "rename": {"_id": "id"},
  "conversions": {"id": "LONG", "amount": "DECIMAL"},
  "exclude": []
}
```

The source `_id` must fit signed 64-bit and `amount` must fit the target decimal. Use `"id":"STRING"` instead for a native ObjectId, with the binary-collated text key described above. These are different key domains; do not change between them inside a saved task.

### Nested document to an SQL text column

```json
{
  "rename": {"_id": "id", "profile": "profile_json"},
  "conversions": {"id": "STRING", "profile_json": "JSON"},
  "exclude": ["temporary_notes"]
}
```

The target must contain exactly the mapped SQL row shape, with enough text capacity. A missing required field still blocks; the mapper does not supply defaults. The `profile_json` string contains the original object's JSON, including any typed wrappers and array order.

### SQL to Kafka, and Kafka to PostgreSQL

An identity mapping publishes the normalized SQL row in a CDC_EX envelope. The [MySQL-to-Kafka example](../examples/tasks/mysql-to-kafka.json) and [Kafka-to-PostgreSQL example](../examples/tasks/kafka-to-postgresql.json) use the common signed-integer/text/fixed-decimal subset. An arbitrary JSON or Debezium envelope cannot be consumed merely by selecting identity mapping; the Kafka connector requires its explicit CDC_EX/1 format and matching record key.

### SQL to ClickHouse

An identity mapping can deliver a SQL row into ClickHouse's current-state **event-envelope schema**. It does not turn a business table with arbitrary columns into an accepted target. `payload` holds the mapped row JSON, `entity_key` holds the canonical mapped key, and durable journal sequence numbers determine versions. See the [task example](../examples/tasks/mysql-to-clickhouse.json), [DDL template](../examples/clickhouse-current-state.sql) and [ClickHouse contract](M5_CLICKHOUSE.md).

## Preflight, DDL preview and comparison

Fixed-schema preflight checks projected field sets, mapped key names and compatible type families. Without a conversion, it also checks declared decimal capacity and text width. The destination performs value-level checks again. Dynamic schemas can pass preflight and later block on a missing field, invalid tag or incompatible value; inspect representative data before starting.

Task JSON rejects unknown structured properties and the mapper rejects unsupported conversion names. Connection `options` is a connector-specific string map; unrecognized arbitrary driver settings are not a supported extension mechanism. Only documented options have meaning.

DDL preview is deliberately non-executable and may contain placeholder widths or scales. It does not provision tables or authorize lossy conversion. Comparisons apply the same mapping before hashing current rows, require an inactive task with no pending events and an explicitly quiescent source, and enforce row/byte bounds. Document order and BSON type tags are not discarded to make an incorrect comparison pass.

## 简体中文

映射顺序是：按原字段名排除 → 重命名 → 按目标字段名转换。`exclude` 写源字段名，`conversions` 写重命名后的目标字段名。主键不能排除，字段不能映射重名；缺失字段和显式 `null` 不会被混为一谈，也不会自动补默认值。当前只支持顶层字段，不支持脚本、表达式、行过滤和嵌套展开。

数值转换使用精确整数和十进制数，溢出、小数转整数、目标精度不足或字符串过长都会停止投递。大整数在进入工具前若已被外部 JavaScript `Number` 舍入，工具无法恢复原值。主键值转换还必须保持一一对应，例如字符串 `"01"` 和 `"1"` 都转成整数 `1` 会合并两个实体，需要在设计映射时避免。

MongoDB 使用规范 Extended JSON 保存 BSON 类型。SQL 的 `id` 转 MongoDB `_id` 时，应在目标字段 `_id` 上配置 `BSON_INT64`；ObjectId 转 SQL 时明确选择 `STRING`，并为 MySQL/MariaDB 文本主键使用二进制排序规则。整个映射属于任务的固定代际配置，不能修改后直接沿用旧位点。

## Implementation references

- [MappingSpec: projection, conversion and preflight](../cdc-runtime/src/main/java/com/cdcex/runtime/MappingSpec.java)
- [EventCodec: exact JSON values and ordered payload digests](../cdc-core/src/main/java/com/cdcex/core/EventCodec.java)
- [MongoDB BSON representation](../cdc-mongodb/src/main/java/com/cdcex/mongodb/BsonValues.java)
- [MySQL/MariaDB destination validation](../cdc-mysql/src/main/java/com/cdcex/mysql/MysqlSink.java)
- [Kafka envelope and key contract](../cdc-kafka/src/main/java/com/cdcex/kafka/KafkaEnvelope.java)
