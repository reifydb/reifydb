# TAP Test Specifications for ReifyDB Clients

## AI Agent Instructions

### How to Read This Documentation

This directory contains language-agnostic test specifications for ReifyDB client implementations. The TAP (Test Anything Protocol) inspired markdown format ensures consistent testing across TypeScript, Rust, and future client implementations.

### For AI Agents Processing These Files

When working with TAP test specifications:

1. **Reading TAP Files**:
   - Each `.tap.md` file contains test cases in a structured format
   - Test cases include: operation type, RQL statement, parameters, and expected output
   - Expected output uses ASCII table format for human readability

2. **Adding New Test Cases**:
   - Follow the existing structure in each TAP file
   - Use lowercase with underscores for test IDs
   - Ensure expected output matches ReifyDB's actual output format
   - Test both success and error cases where applicable

3. **Generating Language-Specific Tests**:
   - Parse the YAML/Markdown sections to extract test metadata
   - Map data types according to language conventions (see Data Type Mapping below)
   - Generate idiomatic test code for each language
   - Preserve test IDs for traceability
   - **IMPORTANT**: Follow the Language-Specific Test Generation section for each language's conventions

### AI Generation Instructions by Language

#### When asked to generate TypeScript tests from TAP specs:
1. Place tests in `/pkg/typescript/client/tests/integration/tap/` directory
2. Create ONE test file per TAP suite with `.tap.ts` extension (e.g., `params_named_primitive.tap.md` → `params_named_primitive.tap.ts`)
3. Use `describe` blocks with the suite name from the TAP file
4. Create one `it` block per test case, using the test case ID as the test name
5. Map parameters according to TypeScript conventions (objects for named, arrays for positional)
6. Use appropriate Schema types from `@reifydb/core`
7. Include proper setup/teardown with `beforeEach`/`afterEach`

#### When asked to generate Rust tests from TAP specs:
1. Place tests in `/pkg/rust/reifydb-client/tests/scripts/tap/` directory
2. Create a DIRECTORY per TAP suite with `_tap` suffix (e.g., `params_named_primitive.tap.md` → `params_named_primitive_tap/`)
3. Create ONE FILE per test case within that directory
4. Use numeric prefixes for file ordering (e.g., `001_bool_param`, `002_int1_param`)
5. Each file should contain:
   - Header comments with source reference and description
   - The appropriate testscript command (`command`, `query`, `command_named`, etc.)
   - Expected output in the exact ASCII table format from the TAP spec
6. Convert parameter values to Rust syntax:
   - Named: `'param=value'` format
   - Positional: Direct values or appropriate Rust literals
7. Preserve exact whitespace and formatting in expected output sections

## TAP File Format

### Structure

```markdown
# Test Suite Name

## AI Instructions
[Specific instructions for this test suite]

## Test Specification
```yaml
suite: Suite Name
description: Suite description
tags: [tag1, tag2]
```

## Test Cases

### test_case_id
```yaml
operation: query|command
statement: "RQL statement with $params"
params:
  type: named|positional
  values:
    # For named:
    param_name: value
    # For positional:
    - value1
    - value2
expected:
  status: success|error
  # For success:
  output: |
    +--------+--------+
    | col1   | col2   |
    +--------+--------+
    | value1 | value2 |
    +--------+--------+
  # For error:
  error_code: "ERROR_CODE"
  error_pattern: "error message pattern"
```
```

## Data Type Mapping

### Primitive Types

| TAP Type | TypeScript | Rust | RQL Type | Notes |
|----------|------------|------|----------|-------|
| bool | boolean | bool | BOOL | true/false |
| int1 | number | i8 | INT1 | -128 to 127 |
| int2 | number | i16 | INT2 | -32,768 to 32,767 |
| int4 | number | i32 | INT4 | -2,147,483,648 to 2,147,483,647 |
| int8 | bigint | i64 | INT8 | Requires BigInt in TypeScript |
| int16 | bigint | i128 | INT16 | Requires BigInt in TypeScript |
| uint1 | number | u8 | UINT1 | 0 to 255 |
| uint2 | number | u16 | UINT2 | 0 to 65,535 |
| uint4 | number/bigint | u32 | UINT4 | >2^31 requires BigInt in TypeScript |
| uint8 | bigint | u64 | UINT8 | Requires BigInt in TypeScript |
| uint16 | bigint | u128 | UINT16 | Requires BigInt in TypeScript |
| float4 | number | f32 | FLOAT4 | Single precision |
| float8 | number | f64 | FLOAT8 | Double precision |
| utf8 | string | String | UTF8 | Unicode text |
| blob | Uint8Array | Vec<u8> | BLOB | Binary data |
| date | Date | Date | DATE | Date only |
| time | Date | Time | TIME | Time only |
| datetime | Date | DateTime | DATETIME | Date and time |
| interval | string | String | INTERVAL | ISO 8601 duration |
| uuid4 | string | String | UUID4 | UUID version 4 |
| uuid7 | string | String | UUID7 | UUID version 7 |
| identity_id | string | String | IDENTITYID | UUID7-based identity |
| row_number | bigint | u64 | ROWNUMBER | Row identifier |

### Parameter Formats

#### Named Parameters
```yaml
params:
  type: named
  values:
    param1: value1
    param2: value2
```

RQL: `MAP $param1 + $param2 as result`

#### Positional Parameters
```yaml
params:
  type: positional
  values:
    - value1
    - value2
```

RQL: `MAP $1 + $2 as result`

## Language-Specific Test Generation

### Generation Strategy by Language

Each language should generate tests that follow its idiomatic patterns while maintaining behavioral consistency across all implementations.

#### TypeScript Generation

**Test Structure**: One test file per TAP suite
- Each TAP file (e.g., `params_named_primitive.tap.md`) generates one TypeScript test file with `.tap.ts` extension
- Place in `/pkg/typescript/client/tests/integration/tap/` directory
- Use Vitest or similar test framework
- Group all test cases in `describe` blocks
- Each test case becomes an `it` block

**Example Generation**:
```typescript
// File: /pkg/typescript/client/tests/integration/tap/params_named_primitive.tap.ts
// Generated from: params_named_primitive.tap.md
import {describe, expect, it} from "vitest";
import {Client} from "@reifydb/client";
import {Schema} from "@reifydb/core";

describe('Named Parameters - Primitive Types', () => {
    let client: Client;
    
    beforeEach(async () => {
        client = await Client.connect_ws(url, options);
    });

    it('bool_param', async () => {
        const frames = await client.command(
            'MAP $bool_val as result',
            {bool_val: true},
            [Schema.object({result: Schema.boolean()})]
        );
        expectSingleResult(frames, true, 'boolean');
    });

    it('int1_param', async () => {
        const frames = await client.command(
            'MAP $int1_val as result',
            {int1_val: 42},
            [Schema.object({result: Schema.int1()})]
        );
        expectSingleResult(frames, 42, 'number');
    });
    // ... more test cases
});
```

#### Rust Generation

**Test Structure**: Directory-based snapshot tests
- Each TAP suite becomes a directory with `_tap` suffix (e.g., `params_named_primitive_tap/`)
- Place in `/pkg/rust/reifydb-client/tests/scripts/tap/` directory
- Each test case becomes a separate file within that directory
- Use testscript format for snapshot testing
- Files can be numbered for ordering if needed

**Example Generation**:
```
/pkg/rust/reifydb-client/tests/scripts/tap/
├── params_named_primitive_tap/
│   ├── 001_bool_param
│   ├── 002_int1_param
│   ├── 003_int2_param
│   └── ...
├── params_positional_primitive_tap/
│   ├── 001_bool_param
│   ├── 002_int1_param
│   └── ...
└── basic_queries_tap/
    ├── 001_simple_map
    ├── 002_map_multiple_fields
    └── ...
```

**File Content Example**:
```rust
# File: /pkg/rust/reifydb-client/tests/scripts/tap/params_named_primitive_tap/001_bool_param
# Generated from: params_named_primitive.tap.md#bool_param
# Test: Named parameter with boolean type

command_named 'MAP $bool_val as result' 'bool_val=true'
---
> +----------+
> |  result  |
> +----------+
> |   true   |
> +----------+
> 
```

### Directory Structure Examples

#### TypeScript Output Structure
```
pkg/typescript/client/tests/integration/
├── tap/                                      # TAP-generated tests
│   ├── params_named_primitive.tap.ts        # Named param tests (note .tap.ts extension)
│   ├── params_positional_primitive.tap.ts   # Positional param tests
│   ├── basic_queries.tap.ts                 # Query tests
│   └── basic_commands.tap.ts                # Command tests
└── ws/                                       # Language-specific tests
    ├── params_named_value.test.ts           # TypeScript Value type tests
    ├── params_positional_value.test.ts      
    ├── error.test.ts
    └── test-helper.ts                       # Shared test utilities
```

#### Rust Output Structure
```
pkg/rust/reifydb-client/tests/scripts/
├── tap/                                      # TAP-generated tests
│   ├── params_named_primitive_tap/          # Note _tap suffix
│   │   ├── 001_bool_param
│   │   ├── 002_int1_param
│   │   ├── 003_int2_param
│   │   └── ...
│   ├── params_positional_primitive_tap/
│   │   ├── 001_bool_param
│   │   ├── 002_int1_param
│   │   └── ...
│   ├── basic_queries_tap/
│   │   ├── 001_simple_map
│   │   ├── 002_map_multiple_fields
│   │   └── ...
│   └── basic_commands_tap/
│       ├── 001_create_schema
│       ├── 002_create_table
│       └── ...
├── 000000001                                 # Language-specific tests
├── 000000002
├── 000000003
└── ...
```

### Parameter Value Conversion

#### TypeScript Parameter Mapping
```typescript
// Named parameters from TAP:
params:
  type: named
  values:
    int_val: 42
    str_val: "hello"
    bool_val: true

// Becomes TypeScript object:
{int_val: 42, str_val: "hello", bool_val: true}

// Positional parameters from TAP:
params:
  type: positional
  values:
    - 42
    - "hello"
    - true

// Becomes TypeScript array:
[42, "hello", true]
```

#### Rust Parameter Mapping
```rust
// Named parameters from TAP:
params:
  type: named
  values:
    int_val: 42
    str_val: "hello"
    bool_val: true

// Becomes Rust testscript arguments:
command_named 'RQL' 'int_val=42' 'str_val=hello' 'bool_val=true'

// Positional parameters from TAP:
params:
  type: positional
  values:
    - 42
    - "hello"
    - true

// Becomes Rust testscript arguments:
command_positional 'RQL' '42' 'hello' 'true'
```

### Special Data Type Handling

#### BigInt values (int8, int16, uint8, uint16):
- TypeScript: Use `BigInt("value")` constructor
- Rust: Pass as string literals in testscript

#### Binary data (blob):
- TypeScript: `new Uint8Array([1, 2, 3])`
- Rust: Represented as array syntax in testscript

#### Date/Time types:
- TypeScript: `new Date('2024-03-15')` or ISO string
- Rust: Pass as ISO string literals

#### UUID and IdentityId:
- Both languages: Pass as string literals

## TAP vs Language-Specific Tests

### TAP-Generated Tests
- **Purpose**: Cross-language compatibility tests that must work identically across all client implementations
- **Location**: Always in `tap/` subdirectories
- **Naming**: TypeScript uses `.tap.ts` extension, Rust uses `_tap` directory suffix
- **Content**: Generated from TAP specifications, ensuring behavioral consistency
- **Examples**: Primitive type parameters, basic queries, standard commands

### Language-Specific Tests
- **Purpose**: Tests for features unique to a particular language implementation
- **Location**: In language-specific directories (e.g., `ws/` for TypeScript, root `scripts/` for Rust)
- **Naming**: Standard test naming for each language (`.test.ts` for TypeScript, numbered files for Rust)
- **Content**: Hand-written tests for language-specific features
- **Examples**:
  - TypeScript: Value type wrappers (`BoolValue`, `Int1Value`, etc.)
  - Rust: Specific error handling, performance tests
  - Language-specific API features

### Important Notes
- When generating from TAP specs, ONLY create files in the `tap/` subdirectory
- Never modify language-specific tests when generating from TAP
- The separation ensures TAP tests can be regenerated without affecting custom tests

## Test Files

- `params_named_primitive.tap.md` - Named parameters with primitive types
- `params_positional_primitive.tap.md` - Positional parameters with primitive types  
- `basic_queries.tap.md` - Basic query operations (MAP, SELECT)
- `basic_commands.tap.md` - Basic command operations (CREATE, INSERT)
- `statement_handling.tap.md` - Multiple statements, empty statements, and statement separators

## Adding New Test Suites

1. Create a new `.tap.md` file following the naming convention
2. Include AI instructions specific to the test suite
3. Define test cases with clear IDs and descriptions
4. Specify expected outputs in ASCII table format
5. Update this README with the new test file

## Validation

To ensure consistency:
1. All clients must pass the same TAP specifications
2. Test IDs must be unique within a suite
3. Expected outputs must match ReifyDB's actual format
4. Error patterns should be regex-compatible for flexible matching