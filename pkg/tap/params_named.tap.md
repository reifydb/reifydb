# Named Parameters with Primitive Types

## AI Instructions

**CRITICAL: NEVER SKIP ANY TEST CASES** - You MUST generate tests for EVERY SINGLE test case in this specification. Each test represents essential behavior that must be verified. Skipping tests breaks cross-language compatibility verification.

When implementing tests from this specification:
1. Parse the YAML sections to extract test metadata and expected outputs
2. Use the parameter values exactly as specified, mapping types according to the language
3. For named parameters, use the language's named parameter syntax (e.g., object in TypeScript, Params::Named in Rust)
4. Verify the actual output matches the expected ASCII table format

## Test Specification
```yaml
suite: Named Parameters - Primitive Types
description: Tests named parameter binding with all primitive data types
tags: [parameters, named, primitive-types]
```

## Test Cases

### bool_param
```yaml
operation: command
statement: "MAP $bool_val as result"
params:
  type: named
  values:
    bool_val: true
expected:
  status: success
  output: |
    +----------+
    |  result  |
    +----------+
    |   true   |
    +----------+
```

### int1_param
```yaml
operation: command
statement: "MAP $int1_val as result"
params:
  type: named
  values:
    int1_val: 42
expected:
  status: success
  output: |
    +----------+
    |  result  |
    +----------+
    |    42    |
    +----------+
```

### int2_param
```yaml
operation: command
statement: "MAP $int2_val as result"
params:
  type: named
  values:
    int2_val: 1234
expected:
  status: success
  output: |
    +----------+
    |  result  |
    +----------+
    |   1234   |
    +----------+
```

### int4_param
```yaml
operation: command
statement: "MAP $int4_val as result"
params:
  type: named
  values:
    int4_val: 12345678
expected:
  status: success
  output: |
    +-------------+
    |   result    |
    +-------------+
    |  12345678   |
    +-------------+
```

### int8_param
```yaml
operation: command
statement: "MAP $int8_val as result"
params:
  type: named
  values:
    int8_val: "9223372036854775807"
expected:
  status: success
  output: |
    +----------------------+
    |       result         |
    +----------------------+
    | 9223372036854775807  |
    +----------------------+
```

### int16_param
```yaml
operation: command
statement: "MAP $int16_val as result"
params:
  type: named
  values:
    int16_val: "170141183460469231731687303715884105727"
expected:
  status: success
  output: |
    +-------------------------------------------+
    |                  result                   |
    +-------------------------------------------+
    | 170141183460469231731687303715884105727  |
    +-------------------------------------------+
```

### uint1_param
```yaml
operation: command
statement: "MAP $uint1_val as result"
params:
  type: named
  values:
    uint1_val: 255
expected:
  status: success
  output: |
    +----------+
    |  result  |
    +----------+
    |   255    |
    +----------+
```

### uint2_param
```yaml
operation: command
statement: "MAP $uint2_val as result"
params:
  type: named
  values:
    uint2_val: 65535
expected:
  status: success
  output: |
    +----------+
    |  result  |
    +----------+
    |  65535   |
    +----------+
```

### uint4_param
```yaml
operation: command
statement: "MAP $uint4_val as result"
params:
  type: named
  values:
    uint4_val: 4294967295
expected:
  status: success
  output: |
    +-------------+
    |   result    |
    +-------------+
    | 4294967295  |
    +-------------+
```

### uint8_param
```yaml
operation: command
statement: "MAP $uint8_val as result"
params:
  type: named
  values:
    uint8_val: "18446744073709551615"
expected:
  status: success
  output: |
    +-----------------------+
    |        result         |
    +-----------------------+
    | 18446744073709551615  |
    +-----------------------+
```

### uint16_param
```yaml
operation: command
statement: "MAP $uint16_val as result"
params:
  type: named
  values:
    uint16_val: "340282366920938463463374607431768211455"
expected:
  status: success
  output: |
    +--------------------------------------------+
    |                   result                   |
    +--------------------------------------------+
    | 340282366920938463463374607431768211455   |
    +--------------------------------------------+
```

### float4_param
```yaml
operation: command
statement: "MAP $float4_val as result"
params:
  type: named
  values:
    float4_val: 3.14
expected:
  status: success
  output: |
    +----------+
    |  result  |
    +----------+
    |   3.14   |
    +----------+
```

### float8_param
```yaml
operation: command
statement: "MAP $float8_val as result"
params:
  type: named
  values:
    float8_val: 3.141592653589793
expected:
  status: success
  output: |
    +--------------------+
    |      result        |
    +--------------------+
    | 3.141592653589793  |
    +--------------------+
```

### decimal_param
```yaml
operation: command
statement: "MAP $decimal_val as result"
params:
  type: named
  values:
    decimal_val: "123.456789"
expected:
  status: success
  output: |
    +-------------+
    |   result    |
    +-------------+
    | 123.456789  |
    +-------------+
```

### utf8_param
```yaml
operation: command
statement: "MAP $utf8_val as result"
params:
  type: named
  values:
    utf8_val: "Hello, World!"
expected:
  status: success
  output: |
    +---------------+
    |    result     |
    +---------------+
    | Hello, World! |
    +---------------+
```

### blob_param
```yaml
operation: command
statement: "MAP $blob_val as result"
params:
  type: named
  values:
    blob_val: [1, 2, 3, 4, 5]
expected:
  status: success
  output: |
    +----------------+
    |     result     |
    +----------------+
    | [1, 2, 3, 4, 5]|
    +----------------+
```

### date_param
```yaml
operation: command
statement: "MAP $date_val as result"
params:
  type: named
  values:
    date_val: "2024-03-15"
expected:
  status: success
  output: |
    +-------------+
    |   result    |
    +-------------+
    | 2024-03-15  |
    +-------------+
```

### time_param
```yaml
operation: command
statement: "MAP $time_val as result"
params:
  type: named
  values:
    time_val: "14:30:00.123"
expected:
  status: success
  output: |
    +---------------+
    |    result     |
    +---------------+
    | 14:30:00.123  |
    +---------------+
```

### datetime_param
```yaml
operation: command
statement: "MAP $datetime_val as result"
params:
  type: named
  values:
    datetime_val: "2024-03-15T14:30:00.123Z"
expected:
  status: success
  output: |
    +---------------------------+
    |         result            |
    +---------------------------+
    | 2024-03-15T14:30:00.123Z  |
    +---------------------------+
```

### interval_param
```yaml
operation: command
statement: "MAP $duration_val as result"
params:
  type: named
  values:
    duration_val: "P1DT2H30M"
expected:
  status: success
  output: |
    +------------+
    |   result   |
    +------------+
    | P1DT2H30M  |
    +------------+
```

### uuid4_param
```yaml
operation: command
statement: "MAP $uuid4_val as result"
params:
  type: named
  values:
    uuid4_val: "550e8400-e29b-41d4-a716-446655440000"
expected:
  status: success
  output: |
    +----------------------------------------+
    |                result                  |
    +----------------------------------------+
    | 550e8400-e29b-41d4-a716-446655440000  |
    +----------------------------------------+
```

### uuid7_param
```yaml
operation: command
statement: "MAP $uuid7_val as result"
params:
  type: named
  values:
    uuid7_val: "018fad5d-f37a-7c94-a716-446655440000"
expected:
  status: success
  output: |
    +----------------------------------------+
    |                result                  |
    +----------------------------------------+
    | 018fad5d-f37a-7c94-a716-446655440000  |
    +----------------------------------------+
```

### identityid_param
```yaml
operation: command
statement: "MAP $identity_val as result"
params:
  type: named
  values:
    identity_val: "018fad5d-f37a-7c94-a716-446655440001"
expected:
  status: success
  output: |
    +----------------------------------------+
    |                result                  |
    +----------------------------------------+
    | 018fad5d-f37a-7c94-a716-446655440001  |
    +----------------------------------------+
```

## Query Tests

The same test cases should be executed using `query` operation instead of `command` to ensure both paths work correctly with named parameters.