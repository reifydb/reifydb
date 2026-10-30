# Comparison

## AI Instructions

**CRITICAL: NEVER SKIP ANY TEST CASES** - every case runs in every language on every transport.

1. Each case stores three values and a none row in `Option(T)` in a fresh namespace `cmp_<case>`, then filters with a typed named param `$p` holding the second value.
2. `v == $p` keeps exactly row 2; `v != $p` and `not (v == $p)` keep exactly rows 1 and 3; the none row is in neither.
3. Rust compares the rendered output; TypeScript compares the typed rows. An enum param is plain text naming the variant (D1, D2).

## Test Specification
```yaml
suite: Comparison
description: Equality and its inverse with a typed param for every value type through each client and transport
tags: [types, comparison, params]
```

## Test Cases

### bool_param
```yaml
operation: query
setup: "DROP NAMESPACE IF EXISTS cmp_bool CASCADE; CREATE NAMESPACE cmp_bool; CREATE TABLE cmp_bool::o { id: int4, v: Option(bool) }; INSERT cmp_bool::o [{ id: 1, v: true }, { id: 2, v: false }, { id: 3, v: true }, { id: 9, v: none }]; OUTPUT FROM cmp_bool::o SORT { id: asc }"
param: { p: BooleanValue.parse('false') }
expected:
  'v == $p': [2]
  'v != $p': [1, 3]
  'not (v == $p)': [1, 3]
```

### int1_param
```yaml
operation: query
setup: "DROP NAMESPACE IF EXISTS cmp_int1 CASCADE; CREATE NAMESPACE cmp_int1; CREATE TABLE cmp_int1::o { id: int4, v: Option(int1) }; INSERT cmp_int1::o [{ id: 1, v: -128 }, { id: 2, v: 0 }, { id: 3, v: 127 }, { id: 9, v: none }]; OUTPUT FROM cmp_int1::o SORT { id: asc }"
param: { p: Int1Value.parse('0') }
expected:
  'v == $p': [2]
  'v != $p': [1, 3]
  'not (v == $p)': [1, 3]
```

### int2_param
```yaml
operation: query
setup: "DROP NAMESPACE IF EXISTS cmp_int2 CASCADE; CREATE NAMESPACE cmp_int2; CREATE TABLE cmp_int2::o { id: int4, v: Option(int2) }; INSERT cmp_int2::o [{ id: 1, v: -32768 }, { id: 2, v: 0 }, { id: 3, v: 32767 }, { id: 9, v: none }]; OUTPUT FROM cmp_int2::o SORT { id: asc }"
param: { p: Int2Value.parse('0') }
expected:
  'v == $p': [2]
  'v != $p': [1, 3]
  'not (v == $p)': [1, 3]
```

### int4_param
```yaml
operation: query
setup: "DROP NAMESPACE IF EXISTS cmp_int4 CASCADE; CREATE NAMESPACE cmp_int4; CREATE TABLE cmp_int4::o { id: int4, v: Option(int4) }; INSERT cmp_int4::o [{ id: 1, v: -2147483648 }, { id: 2, v: 0 }, { id: 3, v: 2147483647 }, { id: 9, v: none }]; OUTPUT FROM cmp_int4::o SORT { id: asc }"
param: { p: Int4Value.parse('0') }
expected:
  'v == $p': [2]
  'v != $p': [1, 3]
  'not (v == $p)': [1, 3]
```

### int8_param
```yaml
operation: query
setup: "DROP NAMESPACE IF EXISTS cmp_int8 CASCADE; CREATE NAMESPACE cmp_int8; CREATE TABLE cmp_int8::o { id: int4, v: Option(int8) }; INSERT cmp_int8::o [{ id: 1, v: -9223372036854775808 }, { id: 2, v: 0 }, { id: 3, v: 9223372036854775807 }, { id: 9, v: none }]; OUTPUT FROM cmp_int8::o SORT { id: asc }"
param: { p: Int8Value.parse('0') }
expected:
  'v == $p': [2]
  'v != $p': [1, 3]
  'not (v == $p)': [1, 3]
```

### int16_param
```yaml
operation: query
setup: "DROP NAMESPACE IF EXISTS cmp_int16 CASCADE; CREATE NAMESPACE cmp_int16; CREATE TABLE cmp_int16::o { id: int4, v: Option(int16) }; INSERT cmp_int16::o [{ id: 1, v: -170141183460469231731687303715884105728 }, { id: 2, v: 0 }, { id: 3, v: 170141183460469231731687303715884105727 }, { id: 9, v: none }]; OUTPUT FROM cmp_int16::o SORT { id: asc }"
param: { p: Int16Value.parse('0') }
expected:
  'v == $p': [2]
  'v != $p': [1, 3]
  'not (v == $p)': [1, 3]
```

### uint1_param
```yaml
operation: query
setup: "DROP NAMESPACE IF EXISTS cmp_uint1 CASCADE; CREATE NAMESPACE cmp_uint1; CREATE TABLE cmp_uint1::o { id: int4, v: Option(uint1) }; INSERT cmp_uint1::o [{ id: 1, v: 0 }, { id: 2, v: 1 }, { id: 3, v: 255 }, { id: 9, v: none }]; OUTPUT FROM cmp_uint1::o SORT { id: asc }"
param: { p: Uint1Value.parse('1') }
expected:
  'v == $p': [2]
  'v != $p': [1, 3]
  'not (v == $p)': [1, 3]
```

### uint2_param
```yaml
operation: query
setup: "DROP NAMESPACE IF EXISTS cmp_uint2 CASCADE; CREATE NAMESPACE cmp_uint2; CREATE TABLE cmp_uint2::o { id: int4, v: Option(uint2) }; INSERT cmp_uint2::o [{ id: 1, v: 0 }, { id: 2, v: 1 }, { id: 3, v: 65535 }, { id: 9, v: none }]; OUTPUT FROM cmp_uint2::o SORT { id: asc }"
param: { p: Uint2Value.parse('1') }
expected:
  'v == $p': [2]
  'v != $p': [1, 3]
  'not (v == $p)': [1, 3]
```

### uint4_param
```yaml
operation: query
setup: "DROP NAMESPACE IF EXISTS cmp_uint4 CASCADE; CREATE NAMESPACE cmp_uint4; CREATE TABLE cmp_uint4::o { id: int4, v: Option(uint4) }; INSERT cmp_uint4::o [{ id: 1, v: 0 }, { id: 2, v: 1 }, { id: 3, v: 4294967295 }, { id: 9, v: none }]; OUTPUT FROM cmp_uint4::o SORT { id: asc }"
param: { p: Uint4Value.parse('1') }
expected:
  'v == $p': [2]
  'v != $p': [1, 3]
  'not (v == $p)': [1, 3]
```

### uint8_param
```yaml
operation: query
setup: "DROP NAMESPACE IF EXISTS cmp_uint8 CASCADE; CREATE NAMESPACE cmp_uint8; CREATE TABLE cmp_uint8::o { id: int4, v: Option(uint8) }; INSERT cmp_uint8::o [{ id: 1, v: 0 }, { id: 2, v: 1 }, { id: 3, v: 18446744073709551615 }, { id: 9, v: none }]; OUTPUT FROM cmp_uint8::o SORT { id: asc }"
param: { p: Uint8Value.parse('1') }
expected:
  'v == $p': [2]
  'v != $p': [1, 3]
  'not (v == $p)': [1, 3]
```

### uint16_param
```yaml
operation: query
setup: "DROP NAMESPACE IF EXISTS cmp_uint16 CASCADE; CREATE NAMESPACE cmp_uint16; CREATE TABLE cmp_uint16::o { id: int4, v: Option(uint16) }; INSERT cmp_uint16::o [{ id: 1, v: 0 }, { id: 2, v: 1 }, { id: 3, v: 340282366920938463463374607431768211455 }, { id: 9, v: none }]; OUTPUT FROM cmp_uint16::o SORT { id: asc }"
param: { p: Uint16Value.parse('1') }
expected:
  'v == $p': [2]
  'v != $p': [1, 3]
  'not (v == $p)': [1, 3]
```

### float4_param
```yaml
operation: query
setup: "DROP NAMESPACE IF EXISTS cmp_float4 CASCADE; CREATE NAMESPACE cmp_float4; CREATE TABLE cmp_float4::o { id: int4, v: Option(float4) }; INSERT cmp_float4::o [{ id: 1, v: 0.5 }, { id: 2, v: -0.0 }, { id: 3, v: 1.5 }, { id: 9, v: none }]; OUTPUT FROM cmp_float4::o SORT { id: asc }"
param: { p: Float4Value.parse('-0.0') }
expected:
  'v == $p': [2]
  'v != $p': [1, 3]
  'not (v == $p)': [1, 3]
```

### float8_param
```yaml
operation: query
setup: "DROP NAMESPACE IF EXISTS cmp_float8 CASCADE; CREATE NAMESPACE cmp_float8; CREATE TABLE cmp_float8::o { id: int4, v: Option(float8) }; INSERT cmp_float8::o [{ id: 1, v: 0.5 }, { id: 2, v: -0.0 }, { id: 3, v: 0.000000000001 }, { id: 9, v: none }]; OUTPUT FROM cmp_float8::o SORT { id: asc }"
param: { p: Float8Value.parse('-0.0') }
expected:
  'v == $p': [2]
  'v != $p': [1, 3]
  'not (v == $p)': [1, 3]
```

### decimal_param
```yaml
operation: query
setup: "DROP NAMESPACE IF EXISTS cmp_decimal CASCADE; CREATE NAMESPACE cmp_decimal; CREATE TABLE cmp_decimal::o { id: int4, v: Option(decimal) }; INSERT cmp_decimal::o [{ id: 1, v: '1.5' }, { id: 2, v: '-0.25' }, { id: 3, v: '12345678901234567890.123456789' }, { id: 9, v: none }]; OUTPUT FROM cmp_decimal::o SORT { id: asc }"
param: { p: DecimalValue.parse('-0.25') }
expected:
  'v == $p': [2]
  'v != $p': [1, 3]
  'not (v == $p)': [1, 3]
```

### decimal_scaled_param
```yaml
operation: query
setup: "DROP NAMESPACE IF EXISTS cmp_decimal_scaled CASCADE; CREATE NAMESPACE cmp_decimal_scaled; CREATE TABLE cmp_decimal_scaled::o { id: int4, v: Option(decimal(30, 3)) }; INSERT cmp_decimal_scaled::o [{ id: 1, v: '1.5' }, { id: 2, v: '-0.25' }, { id: 3, v: '12345678901234567890.123' }, { id: 9, v: none }]; OUTPUT FROM cmp_decimal_scaled::o SORT { id: asc }"
param: { p: DecimalValue.parse('-0.25') }
expected:
  'v == $p': [2]
  'v != $p': [1, 3]
  'not (v == $p)': [1, 3]
```

### utf8_param
```yaml
operation: query
setup: "DROP NAMESPACE IF EXISTS cmp_utf8 CASCADE; CREATE NAMESPACE cmp_utf8; CREATE TABLE cmp_utf8::o { id: int4, v: Option(utf8) }; INSERT cmp_utf8::o [{ id: 1, v: '' }, { id: 2, v: 'héllo wörld' }, { id: 3, v: '日本語' }, { id: 9, v: none }]; OUTPUT FROM cmp_utf8::o SORT { id: asc }"
param: { p: Utf8Value.parse('héllo wörld') }
expected:
  'v == $p': [2]
  'v != $p': [1, 3]
  'not (v == $p)': [1, 3]
```

### blob_param
```yaml
operation: query
setup: "DROP NAMESPACE IF EXISTS cmp_blob CASCADE; CREATE NAMESPACE cmp_blob; CREATE TABLE cmp_blob::o { id: int4, v: Option(blob) }; INSERT cmp_blob::o [{ id: 1, v: blob::hex('') }, { id: 2, v: blob::hex('00ff10') }, { id: 3, v: blob::hex('ff') }, { id: 9, v: none }]; OUTPUT FROM cmp_blob::o SORT { id: asc }"
param: { p: BlobValue.parse('0x00ff10') }
expected:
  'v == $p': [2]
  'v != $p': [1, 3]
  'not (v == $p)': [1, 3]
```

### date_param
```yaml
operation: query
setup: "DROP NAMESPACE IF EXISTS cmp_date CASCADE; CREATE NAMESPACE cmp_date; CREATE TABLE cmp_date::o { id: int4, v: Option(date) }; INSERT cmp_date::o [{ id: 1, v: '1970-01-01' }, { id: 2, v: '2024-02-29' }, { id: 3, v: '2100-12-31' }, { id: 9, v: none }]; OUTPUT FROM cmp_date::o SORT { id: asc }"
param: { p: DateValue.parse('2024-02-29') }
expected:
  'v == $p': [2]
  'v != $p': [1, 3]
  'not (v == $p)': [1, 3]
```

### time_param
```yaml
operation: query
setup: "DROP NAMESPACE IF EXISTS cmp_time CASCADE; CREATE NAMESPACE cmp_time; CREATE TABLE cmp_time::o { id: int4, v: Option(time) }; INSERT cmp_time::o [{ id: 1, v: '00:00:00' }, { id: 2, v: '12:34:56.789' }, { id: 3, v: '23:59:59.999999999' }, { id: 9, v: none }]; OUTPUT FROM cmp_time::o SORT { id: asc }"
param: { p: TimeValue.parse('12:34:56.789') }
expected:
  'v == $p': [2]
  'v != $p': [1, 3]
  'not (v == $p)': [1, 3]
```

### datetime_param
```yaml
operation: query
setup: "DROP NAMESPACE IF EXISTS cmp_datetime CASCADE; CREATE NAMESPACE cmp_datetime; CREATE TABLE cmp_datetime::o { id: int4, v: Option(datetime) }; INSERT cmp_datetime::o [{ id: 1, v: '1970-01-01T00:00:00Z' }, { id: 2, v: '2024-02-29T12:34:56.789Z' }, { id: 3, v: '2100-12-31T23:59:59Z' }, { id: 9, v: none }]; OUTPUT FROM cmp_datetime::o SORT { id: asc }"
param: { p: DateTimeValue.parse('2024-02-29T12:34:56.789Z') }
expected:
  'v == $p': [2]
  'v != $p': [1, 3]
  'not (v == $p)': [1, 3]
```

### duration_param
```yaml
operation: query
setup: "DROP NAMESPACE IF EXISTS cmp_duration CASCADE; CREATE NAMESPACE cmp_duration; CREATE TABLE cmp_duration::o { id: int4, v: Option(duration) }; INSERT cmp_duration::o [{ id: 1, v: '0s' }, { id: 2, v: '1d2h30m' }, { id: 3, v: '30m' }, { id: 9, v: none }]; OUTPUT FROM cmp_duration::o SORT { id: asc }"
param: { p: DurationValue.parse('1d2h30m') }
expected:
  'v == $p': [2]
  'v != $p': [1, 3]
  'not (v == $p)': [1, 3]
```

### uuid4_param
```yaml
operation: query
setup: "DROP NAMESPACE IF EXISTS cmp_uuid4 CASCADE; CREATE NAMESPACE cmp_uuid4; CREATE TABLE cmp_uuid4::o { id: int4, v: Option(uuid4) }; INSERT cmp_uuid4::o [{ id: 1, v: '550e8400-e29b-41d4-a716-446655440000' }, { id: 2, v: '550e8400-e29b-41d4-a716-446655440001' }, { id: 3, v: '550e8400-e29b-41d4-a716-446655440002' }, { id: 9, v: none }]; OUTPUT FROM cmp_uuid4::o SORT { id: asc }"
param: { p: Uuid4Value.parse('550e8400-e29b-41d4-a716-446655440001') }
expected:
  'v == $p': [2]
  'v != $p': [1, 3]
  'not (v == $p)': [1, 3]
```

### uuid7_param
```yaml
operation: query
setup: "DROP NAMESPACE IF EXISTS cmp_uuid7 CASCADE; CREATE NAMESPACE cmp_uuid7; CREATE TABLE cmp_uuid7::o { id: int4, v: Option(uuid7) }; INSERT cmp_uuid7::o [{ id: 1, v: '018fad5d-f37a-7c94-a716-446655440000' }, { id: 2, v: '018fad5d-f37a-7c94-a716-446655440001' }, { id: 3, v: '018fad5d-f37a-7c94-a716-446655440002' }, { id: 9, v: none }]; OUTPUT FROM cmp_uuid7::o SORT { id: asc }"
param: { p: Uuid7Value.parse('018fad5d-f37a-7c94-a716-446655440001') }
expected:
  'v == $p': [2]
  'v != $p': [1, 3]
  'not (v == $p)': [1, 3]
```

### identity_id_param
```yaml
operation: query
setup: "DROP NAMESPACE IF EXISTS cmp_identity_id CASCADE; CREATE NAMESPACE cmp_identity_id; CREATE TABLE cmp_identity_id::o { id: int4, v: Option(identity_id) }; INSERT cmp_identity_id::o [{ id: 1, v: '018fad5d-f37a-7c94-a716-446655440000' }, { id: 2, v: '018fad5d-f37a-7c94-a716-446655440001' }, { id: 3, v: '018fad5d-f37a-7c94-a716-446655440002' }, { id: 9, v: none }]; OUTPUT FROM cmp_identity_id::o SORT { id: asc }"
param: { p: IdentityIdValue.parse('018fad5d-f37a-7c94-a716-446655440001') }
expected:
  'v == $p': [2]
  'v != $p': [1, 3]
  'not (v == $p)': [1, 3]
```

### enum_param
```yaml
operation: query
setup: "DROP NAMESPACE IF EXISTS cmp_enum CASCADE; CREATE NAMESPACE cmp_enum; CREATE ENUM cmp_enum::status { Todo, Doing, Done }; CREATE TABLE cmp_enum::o { id: int4, v: Option(cmp_enum::status) }; INSERT cmp_enum::o [{ id: 1, v: 'todo' }, { id: 2, v: 'doing' }, { id: 3, v: 'done' }, { id: 9, v: none }]; OUTPUT FROM cmp_enum::o SORT { id: asc }"
param: { p: Utf8Value.parse('doing') }
expected:
  'v == $p': [2]
  'v != $p': [1, 3]
  'not (v == $p)': [1, 3]
```
