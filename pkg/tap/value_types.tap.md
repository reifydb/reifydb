# Value Types

## AI Instructions

**CRITICAL: NEVER SKIP ANY TEST CASES** - every case runs in every language on every transport.

1. Each case stores values in a fresh namespace `vt_<case>`, dropping any leftover first, then reads them back.
2. Rust compares the rendered `output` table; TypeScript compares the typed `values` per row.
3. Ids, times and uuids are literals so the output is stable across runs.

## Test Specification
```yaml
suite: Value Types
description: Store every value type in a table and read it back through each client and transport
tags: [types, storage, roundtrip]
```

## Test Cases

### bool_values
```yaml
operation: admin
statement: "DROP NAMESPACE IF EXISTS vt_bool_values CASCADE; CREATE NAMESPACE vt_bool_values; CREATE TABLE vt_bool_values::t { id: int4, v: bool }; INSERT vt_bool_values::t [{ id: 1, v: true }, { id: 2, v: false }]; OUTPUT FROM vt_bool_values::t SORT { id: asc }"
params: null
expected:
  status: success
  shape: Shape.boolean()
  values: [true, false]
  output: |
    +-----------+------+---------+
    |  #rownum  |  id  |    v    |
    +-----------+------+---------+
    |     1     |  1   |  true   |
    |     2     |  2   |  false  |
    +-----------+------+---------+
```

### int1_bounds
```yaml
operation: admin
statement: "DROP NAMESPACE IF EXISTS vt_int1_bounds CASCADE; CREATE NAMESPACE vt_int1_bounds; CREATE TABLE vt_int1_bounds::t { id: int4, v: int1 }; INSERT vt_int1_bounds::t [{ id: 1, v: -128 }, { id: 2, v: 127 }]; OUTPUT FROM vt_int1_bounds::t SORT { id: asc }"
params: null
expected:
  status: success
  shape: Shape.int1()
  values: [-128, 127]
  output: |
    +-----------+------+--------+
    |  #rownum  |  id  |   v    |
    +-----------+------+--------+
    |     1     |  1   |  -128  |
    |     2     |  2   |  127   |
    +-----------+------+--------+
```

### int2_bounds
```yaml
operation: admin
statement: "DROP NAMESPACE IF EXISTS vt_int2_bounds CASCADE; CREATE NAMESPACE vt_int2_bounds; CREATE TABLE vt_int2_bounds::t { id: int4, v: int2 }; INSERT vt_int2_bounds::t [{ id: 1, v: -32768 }, { id: 2, v: 32767 }]; OUTPUT FROM vt_int2_bounds::t SORT { id: asc }"
params: null
expected:
  status: success
  shape: Shape.int2()
  values: [-32768, 32767]
  output: |
    +-----------+------+----------+
    |  #rownum  |  id  |    v     |
    +-----------+------+----------+
    |     1     |  1   |  -32768  |
    |     2     |  2   |  32767   |
    +-----------+------+----------+
```

### int4_bounds
```yaml
operation: admin
statement: "DROP NAMESPACE IF EXISTS vt_int4_bounds CASCADE; CREATE NAMESPACE vt_int4_bounds; CREATE TABLE vt_int4_bounds::t { id: int4, v: int4 }; INSERT vt_int4_bounds::t [{ id: 1, v: -2147483648 }, { id: 2, v: 2147483647 }]; OUTPUT FROM vt_int4_bounds::t SORT { id: asc }"
params: null
expected:
  status: success
  shape: Shape.int4()
  values: [-2147483648, 2147483647]
  output: |
    +-----------+------+---------------+
    |  #rownum  |  id  |       v       |
    +-----------+------+---------------+
    |     1     |  1   |  -2147483648  |
    |     2     |  2   |  2147483647   |
    +-----------+------+---------------+
```

### int8_bounds
```yaml
operation: admin
statement: "DROP NAMESPACE IF EXISTS vt_int8_bounds CASCADE; CREATE NAMESPACE vt_int8_bounds; CREATE TABLE vt_int8_bounds::t { id: int4, v: int8 }; INSERT vt_int8_bounds::t [{ id: 1, v: -9223372036854775808 }, { id: 2, v: 9223372036854775807 }]; OUTPUT FROM vt_int8_bounds::t SORT { id: asc }"
params: null
expected:
  status: success
  shape: Shape.int8()
  values: [-9223372036854775808n, 9223372036854775807n]
  output: |
    +-----------+------+------------------------+
    |  #rownum  |  id  |           v            |
    +-----------+------+------------------------+
    |     1     |  1   |  -9223372036854775808  |
    |     2     |  2   |  9223372036854775807   |
    +-----------+------+------------------------+
```

### int16_bounds
```yaml
operation: admin
statement: "DROP NAMESPACE IF EXISTS vt_int16_bounds CASCADE; CREATE NAMESPACE vt_int16_bounds; CREATE TABLE vt_int16_bounds::t { id: int4, v: int16 }; INSERT vt_int16_bounds::t [{ id: 1, v: -170141183460469231731687303715884105728 }, { id: 2, v: 170141183460469231731687303715884105727 }]; OUTPUT FROM vt_int16_bounds::t SORT { id: asc }"
params: null
expected:
  status: success
  shape: Shape.int16()
  values: [-170141183460469231731687303715884105728n, 170141183460469231731687303715884105727n]
  output: |
    +-----------+------+--------------------------------------------+
    |  #rownum  |  id  |                     v                      |
    +-----------+------+--------------------------------------------+
    |     1     |  1   |  -170141183460469231731687303715884105728  |
    |     2     |  2   |  170141183460469231731687303715884105727   |
    +-----------+------+--------------------------------------------+
```

### uint1_bounds
```yaml
operation: admin
statement: "DROP NAMESPACE IF EXISTS vt_uint1_bounds CASCADE; CREATE NAMESPACE vt_uint1_bounds; CREATE TABLE vt_uint1_bounds::t { id: int4, v: uint1 }; INSERT vt_uint1_bounds::t [{ id: 1, v: 0 }, { id: 2, v: 255 }]; OUTPUT FROM vt_uint1_bounds::t SORT { id: asc }"
params: null
expected:
  status: success
  shape: Shape.uint1()
  values: [0, 255]
  output: |
    +-----------+------+-------+
    |  #rownum  |  id  |   v   |
    +-----------+------+-------+
    |     1     |  1   |   0   |
    |     2     |  2   |  255  |
    +-----------+------+-------+
```

### uint2_bounds
```yaml
operation: admin
statement: "DROP NAMESPACE IF EXISTS vt_uint2_bounds CASCADE; CREATE NAMESPACE vt_uint2_bounds; CREATE TABLE vt_uint2_bounds::t { id: int4, v: uint2 }; INSERT vt_uint2_bounds::t [{ id: 1, v: 0 }, { id: 2, v: 65535 }]; OUTPUT FROM vt_uint2_bounds::t SORT { id: asc }"
params: null
expected:
  status: success
  shape: Shape.uint2()
  values: [0, 65535]
  output: |
    +-----------+------+---------+
    |  #rownum  |  id  |    v    |
    +-----------+------+---------+
    |     1     |  1   |    0    |
    |     2     |  2   |  65535  |
    +-----------+------+---------+
```

### uint4_bounds
```yaml
operation: admin
statement: "DROP NAMESPACE IF EXISTS vt_uint4_bounds CASCADE; CREATE NAMESPACE vt_uint4_bounds; CREATE TABLE vt_uint4_bounds::t { id: int4, v: uint4 }; INSERT vt_uint4_bounds::t [{ id: 1, v: 0 }, { id: 2, v: 4294967295 }]; OUTPUT FROM vt_uint4_bounds::t SORT { id: asc }"
params: null
expected:
  status: success
  shape: Shape.uint4()
  values: [0, 4294967295]
  output: |
    +-----------+------+--------------+
    |  #rownum  |  id  |      v       |
    +-----------+------+--------------+
    |     1     |  1   |      0       |
    |     2     |  2   |  4294967295  |
    +-----------+------+--------------+
```

### uint8_bounds
```yaml
operation: admin
statement: "DROP NAMESPACE IF EXISTS vt_uint8_bounds CASCADE; CREATE NAMESPACE vt_uint8_bounds; CREATE TABLE vt_uint8_bounds::t { id: int4, v: uint8 }; INSERT vt_uint8_bounds::t [{ id: 1, v: 0 }, { id: 2, v: 18446744073709551615 }]; OUTPUT FROM vt_uint8_bounds::t SORT { id: asc }"
params: null
expected:
  status: success
  shape: Shape.uint8()
  values: [0n, 18446744073709551615n]
  output: |
    +-----------+------+------------------------+
    |  #rownum  |  id  |           v            |
    +-----------+------+------------------------+
    |     1     |  1   |           0            |
    |     2     |  2   |  18446744073709551615  |
    +-----------+------+------------------------+
```

### uint16_bounds
```yaml
operation: admin
statement: "DROP NAMESPACE IF EXISTS vt_uint16_bounds CASCADE; CREATE NAMESPACE vt_uint16_bounds; CREATE TABLE vt_uint16_bounds::t { id: int4, v: uint16 }; INSERT vt_uint16_bounds::t [{ id: 1, v: 0 }, { id: 2, v: 340282366920938463463374607431768211455 }]; OUTPUT FROM vt_uint16_bounds::t SORT { id: asc }"
params: null
expected:
  status: success
  shape: Shape.uint16()
  values: [0n, 340282366920938463463374607431768211455n]
  output: |
    +-----------+------+-------------------------------------------+
    |  #rownum  |  id  |                     v                     |
    +-----------+------+-------------------------------------------+
    |     1     |  1   |                     0                     |
    |     2     |  2   |  340282366920938463463374607431768211455  |
    +-----------+------+-------------------------------------------+
```

### float4_values
```yaml
operation: admin
statement: "DROP NAMESPACE IF EXISTS vt_float4_values CASCADE; CREATE NAMESPACE vt_float4_values; CREATE TABLE vt_float4_values::t { id: int4, v: float4 }; INSERT vt_float4_values::t [{ id: 1, v: 1.5 }, { id: 2, v: -0.0 }, { id: 3, v: 0.000001 }, { id: 4, v: 300000000000000000000000000000000000000.0 }]; OUTPUT FROM vt_float4_values::t SORT { id: asc }"
params: null
expected:
  status: success
  shape: Shape.float4()
  values: [1.5, 0, Math.fround(0.000001), Math.fround(3e38)]
  output: |
    +-----------+------+-------------------------------------------+
    |  #rownum  |  id  |                     v                     |
    +-----------+------+-------------------------------------------+
    |     1     |  1   |                    1.5                    |
    |     2     |  2   |                     0                     |
    |     3     |  3   |                 0.000001                  |
    |     4     |  4   |  300000000000000000000000000000000000000  |
    +-----------+------+-------------------------------------------+
```

### float8_values
```yaml
operation: admin
statement: "DROP NAMESPACE IF EXISTS vt_float8_values CASCADE; CREATE NAMESPACE vt_float8_values; CREATE TABLE vt_float8_values::t { id: int4, v: float8 }; INSERT vt_float8_values::t [{ id: 1, v: 0.5 }, { id: 2, v: -0.0 }, { id: 3, v: 0.000000000001 }, { id: 4, v: 1000000000000000000000.0 }]; OUTPUT FROM vt_float8_values::t SORT { id: asc }"
params: null
expected:
  status: success
  shape: Shape.float8()
  values: [0.5, 0, 1e-12, 1e21]
  output: |
    +-----------+------+--------------------------+
    |  #rownum  |  id  |            v             |
    +-----------+------+--------------------------+
    |     1     |  1   |           0.5            |
    |     2     |  2   |            0             |
    |     3     |  3   |      0.000000000001      |
    |     4     |  4   |  1000000000000000000000  |
    +-----------+------+--------------------------+
```

### decimal_values
```yaml
operation: admin
statement: "DROP NAMESPACE IF EXISTS vt_decimal_values CASCADE; CREATE NAMESPACE vt_decimal_values; CREATE TABLE vt_decimal_values::t { id: int4, v: decimal }; INSERT vt_decimal_values::t [{ id: 1, v: '12345678901234567890.123456789' }, { id: 2, v: '1.500' }, { id: 3, v: '-0.25' }]; OUTPUT FROM vt_decimal_values::t SORT { id: asc }"
params: null
expected:
  status: success
  shape: Shape.decimal()
  values: ['12345678901234567890.1234567890', '1.5000000000', '-0.2500000000']
  output: |
    +-----------+------+-----------------------------------+
    |  #rownum  |  id  |                 v                 |
    +-----------+------+-----------------------------------+
    |     1     |  1   |  12345678901234567890.1234567890  |
    |     2     |  2   |           1.5000000000            |
    |     3     |  3   |           -0.2500000000           |
    +-----------+------+-----------------------------------+
```

### decimal_declared_scale
```yaml
operation: admin
statement: "DROP NAMESPACE IF EXISTS vt_decimal_declared_scale CASCADE; CREATE NAMESPACE vt_decimal_declared_scale; CREATE TABLE vt_decimal_declared_scale::t { id: int4, v: decimal(30, 3) }; INSERT vt_decimal_declared_scale::t [{ id: 1, v: '12345678901234567890.123' }, { id: 2, v: '1.500' }, { id: 3, v: '-0.25' }]; OUTPUT FROM vt_decimal_declared_scale::t SORT { id: asc }"
params: null
expected:
  status: success
  shape: Shape.decimal()
  values: ['12345678901234567890.123', '1.500', '-0.250']
  output: |
    +-----------+------+----------------------------+
    |  #rownum  |  id  |             v              |
    +-----------+------+----------------------------+
    |     1     |  1   |  12345678901234567890.123  |
    |     2     |  2   |           1.500            |
    |     3     |  3   |           -0.250           |
    +-----------+------+----------------------------+
```

### utf8_values
```yaml
operation: admin
statement: "DROP NAMESPACE IF EXISTS vt_utf8_values CASCADE; CREATE NAMESPACE vt_utf8_values; CREATE TABLE vt_utf8_values::t { id: int4, v: utf8 }; INSERT vt_utf8_values::t [{ id: 1, v: '' }, { id: 2, v: 'héllo wörld' }, { id: 3, v: '日本語' }]; OUTPUT FROM vt_utf8_values::t SORT { id: asc }"
params: null
expected:
  status: success
  shape: Shape.utf8()
  values: ['', 'héllo wörld', '日本語']
  output: |
    +-----------+------+---------------+
    |  #rownum  |  id  |       v       |
    +-----------+------+---------------+
    |     1     |  1   |               |
    |     2     |  2   |  héllo wörld  |
    |     3     |  3   |    日本語     |
    +-----------+------+---------------+
```

### blob_values
```yaml
operation: admin
statement: "DROP NAMESPACE IF EXISTS vt_blob_values CASCADE; CREATE NAMESPACE vt_blob_values; CREATE TABLE vt_blob_values::t { id: int4, v: blob }; INSERT vt_blob_values::t [{ id: 1, v: blob::hex('') }, { id: 2, v: blob::hex('00ff10') }]; OUTPUT FROM vt_blob_values::t SORT { id: asc }"
params: null
expected:
  status: success
  shape: Shape.blob()
  values: [new Uint8Array([]), new Uint8Array([0x00, 0xff, 0x10])]
  output: |
    +-----------+------+------------+
    |  #rownum  |  id  |     v      |
    +-----------+------+------------+
    |     1     |  1   |     0x     |
    |     2     |  2   |  0x00ff10  |
    +-----------+------+------------+
```

### date_values
```yaml
operation: admin
statement: "DROP NAMESPACE IF EXISTS vt_date_values CASCADE; CREATE NAMESPACE vt_date_values; CREATE TABLE vt_date_values::t { id: int4, v: date }; INSERT vt_date_values::t [{ id: 1, v: '2024-02-29' }, { id: 2, v: '1970-01-01' }]; OUTPUT FROM vt_date_values::t SORT { id: asc }"
params: null
expected:
  status: success
  shape: Shape.date()
  values: [new Date('2024-02-29T00:00:00.000Z'), new Date('1970-01-01T00:00:00.000Z')]
  output: |
    +-----------+------+--------------+
    |  #rownum  |  id  |      v       |
    +-----------+------+--------------+
    |     1     |  1   |  2024-02-29  |
    |     2     |  2   |  1970-01-01  |
    +-----------+------+--------------+
```

### time_values
```yaml
operation: admin
statement: "DROP NAMESPACE IF EXISTS vt_time_values CASCADE; CREATE NAMESPACE vt_time_values; CREATE TABLE vt_time_values::t { id: int4, v: time }; INSERT vt_time_values::t [{ id: 1, v: '00:00:00' }, { id: 2, v: '23:59:59.999999999' }]; OUTPUT FROM vt_time_values::t SORT { id: asc }"
params: null
expected:
  status: success
  shape: Shape.time()
  values: ['00:00:00.000000000', '23:59:59.999999999']
  output: |
    +-----------+------+----------------------+
    |  #rownum  |  id  |          v           |
    +-----------+------+----------------------+
    |     1     |  1   |  00:00:00.000000000  |
    |     2     |  2   |  23:59:59.999999999  |
    +-----------+------+----------------------+
```

### datetime_values
```yaml
operation: admin
statement: "DROP NAMESPACE IF EXISTS vt_datetime_values CASCADE; CREATE NAMESPACE vt_datetime_values; CREATE TABLE vt_datetime_values::t { id: int4, v: datetime }; INSERT vt_datetime_values::t [{ id: 1, v: '2024-02-29T12:34:56.789Z' }, { id: 2, v: '1970-01-01T00:00:00Z' }]; OUTPUT FROM vt_datetime_values::t SORT { id: asc }"
params: null
expected:
  status: success
  shape: Shape.datetime()
  values: [new Date('2024-02-29T12:34:56.789Z'), new Date('1970-01-01T00:00:00.000Z')]
  output: |
    +-----------+------+----------------------------------+
    |  #rownum  |  id  |                v                 |
    +-----------+------+----------------------------------+
    |     1     |  1   |  2024-02-29T12:34:56.789000000Z  |
    |     2     |  2   |  1970-01-01T00:00:00.000000000Z  |
    +-----------+------+----------------------------------+
```

### duration_values
```yaml
operation: admin
statement: "DROP NAMESPACE IF EXISTS vt_duration_values CASCADE; CREATE NAMESPACE vt_duration_values; CREATE TABLE vt_duration_values::t { id: int4, v: duration }; INSERT vt_duration_values::t [{ id: 1, v: '1d2h30m' }, { id: 2, v: '0s' }]; OUTPUT FROM vt_duration_values::t SORT { id: asc }"
params: null
expected:
  status: success
  shape: Shape.duration()
  values: ['1d2h30m', '0s']
  output: |
    +-----------+------+-----------+
    |  #rownum  |  id  |     v     |
    +-----------+------+-----------+
    |     1     |  1   |  1d2h30m  |
    |     2     |  2   |    0s     |
    +-----------+------+-----------+
```

### uuid4_values
```yaml
operation: admin
statement: "DROP NAMESPACE IF EXISTS vt_uuid4_values CASCADE; CREATE NAMESPACE vt_uuid4_values; CREATE TABLE vt_uuid4_values::t { id: int4, v: uuid4 }; INSERT vt_uuid4_values::t [{ id: 1, v: '550e8400-e29b-41d4-a716-446655440000' }]; OUTPUT FROM vt_uuid4_values::t SORT { id: asc }"
params: null
expected:
  status: success
  shape: Shape.uuid4()
  values: ['550e8400-e29b-41d4-a716-446655440000']
  output: |
    +-----------+------+----------------------------------------+
    |  #rownum  |  id  |                   v                    |
    +-----------+------+----------------------------------------+
    |     1     |  1   |  550e8400-e29b-41d4-a716-446655440000  |
    +-----------+------+----------------------------------------+
```

### uuid7_values
```yaml
operation: admin
statement: "DROP NAMESPACE IF EXISTS vt_uuid7_values CASCADE; CREATE NAMESPACE vt_uuid7_values; CREATE TABLE vt_uuid7_values::t { id: int4, v: uuid7 }; INSERT vt_uuid7_values::t [{ id: 1, v: '018fad5d-f37a-7c94-a716-446655440000' }]; OUTPUT FROM vt_uuid7_values::t SORT { id: asc }"
params: null
expected:
  status: success
  shape: Shape.uuid7()
  values: ['018fad5d-f37a-7c94-a716-446655440000']
  output: |
    +-----------+------+----------------------------------------+
    |  #rownum  |  id  |                   v                    |
    +-----------+------+----------------------------------------+
    |     1     |  1   |  018fad5d-f37a-7c94-a716-446655440000  |
    +-----------+------+----------------------------------------+
```

### identity_id_values
```yaml
operation: admin
statement: "DROP NAMESPACE IF EXISTS vt_identity_id_values CASCADE; CREATE NAMESPACE vt_identity_id_values; CREATE TABLE vt_identity_id_values::t { id: int4, v: identity_id }; INSERT vt_identity_id_values::t [{ id: 1, v: '018fad5d-f37a-7c94-a716-446655440001' }]; OUTPUT FROM vt_identity_id_values::t SORT { id: asc }"
params: null
expected:
  status: success
  shape: Shape.identityid()
  values: ['018fad5d-f37a-7c94-a716-446655440001']
  output: |
    +-----------+------+----------------------------------------+
    |  #rownum  |  id  |                   v                    |
    +-----------+------+----------------------------------------+
    |     1     |  1   |  018fad5d-f37a-7c94-a716-446655440001  |
    +-----------+------+----------------------------------------+
```

### option_int4_none
```yaml
operation: admin
statement: "DROP NAMESPACE IF EXISTS vt_option_int4_none CASCADE; CREATE NAMESPACE vt_option_int4_none; CREATE TABLE vt_option_int4_none::t { id: int4, v: Option(int4) }; INSERT vt_option_int4_none::t [{ id: 1, v: 7 }, { id: 2, v: none }]; OUTPUT FROM vt_option_int4_none::t SORT { id: asc }"
params: null
expected:
  status: success
  shape: Shape.option(Shape.int4())
  values: [Option.some(7), Option.none('Int4')]
  output: |
    +-----------+------+--------+
    |  #rownum  |  id  |   v    |
    +-----------+------+--------+
    |     1     |  1   |   7    |
    |     2     |  2   |  none  |
    +-----------+------+--------+
```

### option_utf8_none
```yaml
operation: admin
statement: "DROP NAMESPACE IF EXISTS vt_option_utf8_none CASCADE; CREATE NAMESPACE vt_option_utf8_none; CREATE TABLE vt_option_utf8_none::t { id: int4, v: Option(utf8) }; INSERT vt_option_utf8_none::t [{ id: 1, v: none }, { id: 2, v: 'x' }]; OUTPUT FROM vt_option_utf8_none::t SORT { id: asc }"
params: null
expected:
  status: success
  shape: Shape.option(Shape.utf8())
  values: [Option.none('Utf8'), Option.some('x')]
  output: |
    +-----------+------+--------+
    |  #rownum  |  id  |   v    |
    +-----------+------+--------+
    |     1     |  1   |  none  |
    |     2     |  2   |   x    |
    +-----------+------+--------+
```

### all_types
```yaml
operation: admin
statement: "DROP NAMESPACE IF EXISTS vt_all_types CASCADE; CREATE NAMESPACE vt_all_types; CREATE TABLE vt_all_types::t { id: int4, cbool: bool, cint1: int1, cint2: int2, cint4: int4, cint8: int8, cint16: int16, cuint1: uint1, cuint2: uint2, cuint4: uint4, cuint8: uint8, cuint16: uint16, cfloat4: float4, cfloat8: float8, cdecimal: decimal, cdecimalscaled: decimal(30, 3), cutf8: utf8, cblob: blob, cdate: date, ctime: time, cdatetime: datetime, cduration: duration, cuuid4: uuid4, cuuid7: uuid7, cidentityid: identity_id, coptint4: Option(int4), coptutf8: Option(utf8) }; INSERT vt_all_types::t [{ id: 1, cbool: true, cint1: -128, cint2: -32768, cint4: -2147483648, cint8: -9223372036854775808, cint16: -170141183460469231731687303715884105728, cuint1: 0, cuint2: 0, cuint4: 0, cuint8: 0, cuint16: 0, cfloat4: 1.5, cfloat8: 0.5, cdecimal: '12345678901234567890.123456789', cdecimalscaled: '12345678901234567890.123', cutf8: '', cblob: blob::hex(''), cdate: '2024-02-29', ctime: '00:00:00', cdatetime: '2024-02-29T12:34:56.789Z', cduration: '1d2h30m', cuuid4: '550e8400-e29b-41d4-a716-446655440000', cuuid7: '018fad5d-f37a-7c94-a716-446655440000', cidentityid: '018fad5d-f37a-7c94-a716-446655440001', coptint4: 7, coptutf8: none }, { id: 2, cbool: false, cint1: 127, cint2: 32767, cint4: 2147483647, cint8: 9223372036854775807, cint16: 170141183460469231731687303715884105727, cuint1: 255, cuint2: 65535, cuint4: 4294967295, cuint8: 18446744073709551615, cuint16: 340282366920938463463374607431768211455, cfloat4: -0.0, cfloat8: -0.0, cdecimal: '1.500', cdecimalscaled: '1.500', cutf8: 'héllo wörld', cblob: blob::hex('00ff10'), cdate: '1970-01-01', ctime: '23:59:59.999999999', cdatetime: '1970-01-01T00:00:00Z', cduration: '0s', cuuid4: '550e8400-e29b-41d4-a716-446655440000', cuuid7: '018fad5d-f37a-7c94-a716-446655440000', cidentityid: '018fad5d-f37a-7c94-a716-446655440001', coptint4: none, coptutf8: 'x' }, { id: 3, cbool: true, cint1: -128, cint2: -32768, cint4: -2147483648, cint8: -9223372036854775808, cint16: -170141183460469231731687303715884105728, cuint1: 0, cuint2: 0, cuint4: 0, cuint8: 0, cuint16: 0, cfloat4: 0.000001, cfloat8: 0.000000000001, cdecimal: '-0.25', cdecimalscaled: '-0.25', cutf8: '日本語', cblob: blob::hex(''), cdate: '2024-02-29', ctime: '00:00:00', cdatetime: '2024-02-29T12:34:56.789Z', cduration: '1d2h30m', cuuid4: '550e8400-e29b-41d4-a716-446655440000', cuuid7: '018fad5d-f37a-7c94-a716-446655440000', cidentityid: '018fad5d-f37a-7c94-a716-446655440001', coptint4: 7, coptutf8: none }]; OUTPUT FROM vt_all_types::t SORT { id: asc }"
params: null
expected:
  status: success
  values: one row object per id with every column below, values as in the single-type cases
  output: |
    +-----------+------+---------+---------+----------+---------------+------------------------+--------------------------------------------+----------+----------+--------------+------------------------+-------------------------------------------+------------+------------------+-----------------------------------+----------------------------+---------------+------------+--------------+----------------------+----------------------------------+-------------+----------------------------------------+----------------------------------------+----------------------------------------+------------+------------+
    |  #rownum  |  id  |  cbool  |  cint1  |  cint2   |     cint4     |         cint8          |                   cint16                   |  cuint1  |  cuint2  |    cuint4    |         cuint8         |                  cuint16                  |  cfloat4   |     cfloat8      |             cdecimal              |       cdecimalscaled       |     cutf8     |   cblob    |    cdate     |        ctime         |            cdatetime             |  cduration  |                 cuuid4                 |                 cuuid7                 |              cidentityid               |  coptint4  |  coptutf8  |
    +-----------+------+---------+---------+----------+---------------+------------------------+--------------------------------------------+----------+----------+--------------+------------------------+-------------------------------------------+------------+------------------+-----------------------------------+----------------------------+---------------+------------+--------------+----------------------+----------------------------------+-------------+----------------------------------------+----------------------------------------+----------------------------------------+------------+------------+
    |     1     |  1   |  true   |  -128   |  -32768  |  -2147483648  |  -9223372036854775808  |  -170141183460469231731687303715884105728  |    0     |    0     |      0       |           0            |                     0                     |    1.5     |       0.5        |  12345678901234567890.1234567890  |  12345678901234567890.123  |               |     0x     |  2024-02-29  |  00:00:00.000000000  |  2024-02-29T12:34:56.789000000Z  |   1d2h30m   |  550e8400-e29b-41d4-a716-446655440000  |  018fad5d-f37a-7c94-a716-446655440000  |  018fad5d-f37a-7c94-a716-446655440001  |     7      |    none    |
    |     2     |  2   |  false  |   127   |  32767   |  2147483647   |  9223372036854775807   |  170141183460469231731687303715884105727   |   255    |  65535   |  4294967295  |  18446744073709551615  |  340282366920938463463374607431768211455  |     0      |        0         |           1.5000000000            |           1.500            |  héllo wörld  |  0x00ff10  |  1970-01-01  |  23:59:59.999999999  |  1970-01-01T00:00:00.000000000Z  |     0s      |  550e8400-e29b-41d4-a716-446655440000  |  018fad5d-f37a-7c94-a716-446655440000  |  018fad5d-f37a-7c94-a716-446655440001  |    none    |     x      |
    |     3     |  3   |  true   |  -128   |  -32768  |  -2147483648  |  -9223372036854775808  |  -170141183460469231731687303715884105728  |    0     |    0     |      0       |           0            |                     0                     |  0.000001  |  0.000000000001  |           -0.2500000000           |           -0.250           |    日本語     |     0x     |  2024-02-29  |  00:00:00.000000000  |  2024-02-29T12:34:56.789000000Z  |   1d2h30m   |  550e8400-e29b-41d4-a716-446655440000  |  018fad5d-f37a-7c94-a716-446655440000  |  018fad5d-f37a-7c94-a716-446655440001  |     7      |    none    |
    +-----------+------+---------+---------+----------+---------------+------------------------+--------------------------------------------+----------+----------+--------------+------------------------+-------------------------------------------+------------+------------------+-----------------------------------+----------------------------+---------------+------------+--------------+----------------------+----------------------------------+-------------+----------------------------------------+----------------------------------------+----------------------------------------+------------+------------+
```

### enum_unit_variants
```yaml
operation: admin
statement: "DROP NAMESPACE IF EXISTS vt_enum_unit_variants CASCADE; CREATE NAMESPACE vt_enum_unit_variants; CREATE ENUM vt_enum_unit_variants::status { Todo, Doing, Done }; CREATE TABLE vt_enum_unit_variants::t { id: int4, v: vt_enum_unit_variants::status }; INSERT vt_enum_unit_variants::t [{ id: 1, v: vt_enum_unit_variants::status::Todo }, { id: 2, v: vt_enum_unit_variants::status::Doing }, { id: 3, v: vt_enum_unit_variants::status::Done }, { id: 4, v: vt_enum_unit_variants::status::Todo }]; OUTPUT FROM vt_enum_unit_variants::t SORT { id: asc }"
params: null
expected:
  status: success
  shape: Shape.utf8()
  values: ['todo', 'doing', 'done', 'todo']
  output: |
    +-----------+------+---------+
    |  #rownum  |  id  |    v    |
    +-----------+------+---------+
    |     1     |  1   |  todo   |
    |     2     |  2   |  doing  |
    |     3     |  3   |  done   |
    |     4     |  4   |  todo   |
    +-----------+------+---------+
```

### enum_unit_optional
```yaml
operation: admin
statement: "DROP NAMESPACE IF EXISTS vt_enum_unit_optional CASCADE; CREATE NAMESPACE vt_enum_unit_optional; CREATE ENUM vt_enum_unit_optional::status { Todo, Doing, Done }; CREATE TABLE vt_enum_unit_optional::t { id: int4, v: Option(vt_enum_unit_optional::status) }; INSERT vt_enum_unit_optional::t [{ id: 1, v: vt_enum_unit_optional::status::Doing }, { id: 2, v: none }, { id: 3, v: vt_enum_unit_optional::status::Done }]; OUTPUT FROM vt_enum_unit_optional::t SORT { id: asc }"
params: null
expected:
  status: success
  shape: Shape.option(Shape.utf8())
  values: [Option.some('doing'), Option.none('Utf8'), Option.some('done')]
  output: |
    +-----------+------+---------+
    |  #rownum  |  id  |    v    |
    +-----------+------+---------+
    |     1     |  1   |  doing  |
    |     2     |  2   |  none   |
    |     3     |  3   |  done   |
    +-----------+------+---------+
```

### enum_with_fields
```yaml
operation: admin
statement: "DROP NAMESPACE IF EXISTS vt_enum_with_fields CASCADE; CREATE NAMESPACE vt_enum_with_fields; CREATE ENUM vt_enum_with_fields::shape { Point, Circle { radius: float8 }, Rect { w: float8, h: float8 } }; CREATE TABLE vt_enum_with_fields::t { id: int4, v: vt_enum_with_fields::shape }; INSERT vt_enum_with_fields::t [{ id: 1, v: vt_enum_with_fields::shape::Point }, { id: 2, v: vt_enum_with_fields::shape::Circle { radius: 1.5 } }, { id: 3, v: vt_enum_with_fields::shape::Rect { w: 2.0, h: 3.0 } }]; OUTPUT FROM vt_enum_with_fields::t SORT { id: asc }"
params: null
expected:
  status: success
  shape: split: v_tag Shape.uint1(), fields Shape.option(Shape.float8())
  values: see the TS row objects
  output: |
    +-----------+------+---------+-------------------+------------+------------+
    |  #rownum  |  id  |  v_tag  |  v_circle_radius  |  v_rect_w  |  v_rect_h  |
    +-----------+------+---------+-------------------+------------+------------+
    |     1     |  1   |    0    |       none        |    none    |    none    |
    |     2     |  2   |    1    |        1.5        |    none    |    none    |
    |     3     |  3   |    2    |       none        |     2      |     3      |
    +-----------+------+---------+-------------------+------------+------------+
```
