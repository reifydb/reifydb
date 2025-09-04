# Positional Parameters with Primitive Types

## AI Instructions

**CRITICAL: NEVER SKIP ANY TEST CASES** - You MUST generate tests for EVERY SINGLE test case in this specification. Each test represents essential behavior that must be verified. Skipping tests breaks cross-language compatibility verification.

When implementing tests from this specification:
1. Parse the YAML sections to extract test metadata and expected outputs
2. Use the parameter values exactly as specified, mapping types according to the language
3. For positional parameters, use the language's positional parameter syntax (e.g., array in TypeScript, Params::Positional in Rust)
4. Parameters are referenced by position: $1, $2, $3, etc.
5. Verify the actual output matches the expected ASCII table format

## Test Specification
```yaml
suite: Positional Parameters - Primitive Types
description: Tests positional parameter binding with all primitive data types
tags: [parameters, positional, primitive-types]
```

## Test Cases

### bool_param
```yaml
operation: command
statement: "MAP $1 as result"
params:
  type: positional
  values:
    - true
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
statement: "MAP $1 as result"
params:
  type: positional
  values:
    - 42
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
statement: "MAP $1 as result"
params:
  type: positional
  values:
    - 1234
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
statement: "MAP $1 as result"
params:
  type: positional
  values:
    - 12345678
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
statement: "MAP $1 as result"
params:
  type: positional
  values:
    - "9223372036854775807"
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
statement: "MAP $1 as result"
params:
  type: positional
  values:
    - "170141183460469231731687303715884105727"
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
statement: "MAP $1 as result"
params:
  type: positional
  values:
    - 255
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
statement: "MAP $1 as result"
params:
  type: positional
  values:
    - 65535
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
statement: "MAP $1 as result"
params:
  type: positional
  values:
    - 4294967295
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
statement: "MAP $1 as result"
params:
  type: positional
  values:
    - "18446744073709551615"
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
statement: "MAP $1 as result"
params:
  type: positional
  values:
    - "340282366920938463463374607431768211455"
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
statement: "MAP $1 as result"
params:
  type: positional
  values:
    - 3.14
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
statement: "MAP $1 as result"
params:
  type: positional
  values:
    - 3.141592653589793
expected:
  status: success
  output: |
    +--------------------+
    |      result        |
    +--------------------+
    | 3.141592653589793  |
    +--------------------+
```

### utf8_param
```yaml
operation: command
statement: "MAP $1 as result"
params:
  type: positional
  values:
    - "Hello, World!"
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
statement: "MAP $1 as result"
params:
  type: positional
  values:
    - [1, 2, 3, 4, 5]
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
statement: "MAP $1 as result"
params:
  type: positional
  values:
    - "2024-03-15"
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
statement: "MAP $1 as result"
params:
  type: positional
  values:
    - "14:30:00.123"
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
statement: "MAP $1 as result"
params:
  type: positional
  values:
    - "2024-03-15T14:30:00.123Z"
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
statement: "MAP $1 as result"
params:
  type: positional
  values:
    - "P1DT2H30M"
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
statement: "MAP $1 as result"
params:
  type: positional
  values:
    - "550e8400-e29b-41d4-a716-446655440000"
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
statement: "MAP $1 as result"
params:
  type: positional
  values:
    - "018fad5d-f37a-7c94-a716-446655440000"
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
statement: "MAP $1 as result"
params:
  type: positional
  values:
    - "018fad5d-f37a-7c94-a716-446655440001"
expected:
  status: success
  output: |
    +----------------------------------------+
    |                result                  |
    +----------------------------------------+
    | 018fad5d-f37a-7c94-a716-446655440001  |
    +----------------------------------------+
```

### rownumber_param
```yaml
operation: command
statement: "MAP $1 as result"
params:
  type: positional
  values:
    - "123456789"
expected:
  status: success
  output: |
    +------------+
    |   result   |
    +------------+
    | 123456789  |
    +------------+
```

### multiple_params
```yaml
operation: command
statement: "MAP { first: $1, second: $2, third: $3 }"
params:
  type: positional
  values:
    - 100
    - "test"
    - true
expected:
  status: success
  output: |
    +---------+----------+---------+
    |  first  |  second  |  third  |
    +---------+----------+---------+
    |   100   |   test   |  true   |
    +---------+----------+---------+
```

## Query Tests

The same test cases should be executed using `query` operation instead of `command` to ensure both paths work correctly with positional parameters.