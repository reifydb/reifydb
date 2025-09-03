# Statement Handling

## AI Instructions

When implementing tests from this specification:
1. These tests verify how the client handles multiple statements, empty statements, and statement separators
2. Test both `command` and `query` operations as they should behave identically for statement parsing
3. Pay attention to the frame structure - each statement produces its own frame
4. Empty statements should not produce frames

## Test Specification
```yaml
suite: Statement Handling
description: Tests for multiple statement execution, empty statements, and statement separators
tags: [statements, parsing, multiple-results]
```

## Test Cases

### no_statements
```yaml
operation: command
statement: ""
params: null
expected:
  status: success
  frames: 0
  output: |
    (empty)
```

### single_empty_statement
```yaml
operation: command
statement: ";"
params: null
expected:
  status: success
  frames: 0
  output: |
    (empty)
```

### many_empty_statements
```yaml
operation: command
statement: ";;;;;"
params: null
expected:
  status: success
  frames: 0
  output: |
    (empty)
```

### mixed_empty_and_non_empty
```yaml
operation: command
statement: ";MAP 1 as one ;;;MAP 2 as two"
params: null
expected:
  status: success
  frames: 2
  output: |
    Frame 1:
    +-------+
    |  one  |
    +-------+
    |   1   |
    +-------+
    
    Frame 2:
    +-------+
    |  two  |
    +-------+
    |   2   |
    +-------+
```

### single_statement_with_semicolon
```yaml
operation: command
statement: "MAP 1 as result;"
params: null
expected:
  status: success
  frames: 1
  output: |
    +----------+
    |  result  |
    +----------+
    |     1    |
    +----------+
```

### multiple_statements_same_structure
```yaml
operation: command
statement: "MAP 1 as result;MAP 2 as result;MAP 3 as result;"
params: null
expected:
  status: success
  frames: 3
  output: |
    Frame 1:
    +----------+
    |  result  |
    +----------+
    |     1    |
    +----------+
    
    Frame 2:
    +----------+
    |  result  |
    +----------+
    |     2    |
    +----------+
    
    Frame 3:
    +----------+
    |  result  |
    +----------+
    |     3    |
    +----------+
```

### multiple_statements_different_structure
```yaml
operation: command
statement: "MAP 1 as result;MAP { 2 as a, 3 as b };MAP 'ReifyDB' as result;"
params: null
expected:
  status: success
  frames: 3
  output: |
    Frame 1:
    +----------+
    |  result  |
    +----------+
    |     1    |
    +----------+
    
    Frame 2:
    +------+------+
    |  a   |  b   |
    +------+------+
    |  2   |  3   |
    +------+------+
    
    Frame 3:
    +----------+
    |  result  |
    +----------+
    | ReifyDB  |
    +----------+
```

### statement_without_trailing_semicolon
```yaml
operation: command
statement: "MAP 1 as x"
params: null
expected:
  status: success
  frames: 1
  output: |
    +------+
    |  x   |
    +------+
    |  1   |
    +------+
```

### multiple_statements_no_trailing_semicolon
```yaml
operation: command
statement: "MAP 1 as x;MAP 2 as y"
params: null
expected:
  status: success
  frames: 2
  output: |
    Frame 1:
    +------+
    |  x   |
    +------+
    |  1   |
    +------+
    
    Frame 2:
    +------+
    |  y   |
    +------+
    |  2   |
    +------+
```

### statement_with_whitespace
```yaml
operation: command
statement: "  MAP 1 as result  ;  MAP 2 as result  "
params: null
expected:
  status: success
  frames: 2
  output: |
    Frame 1:
    +----------+
    |  result  |
    +----------+
    |     1    |
    +----------+
    
    Frame 2:
    +----------+
    |  result  |
    +----------+
    |     2    |
    +----------+
```

## Query Tests

All the above test cases should also be executed using `query` operation instead of `command` to ensure both operations handle statements identically.

### query_no_statements
```yaml
operation: query
statement: ""
params: null
expected:
  status: success
  frames: 0
  output: |
    (empty)
```

### query_single_empty_statement
```yaml
operation: query
statement: ";"
params: null
expected:
  status: success
  frames: 0
  output: |
    (empty)
```

### query_many_empty_statements
```yaml
operation: query
statement: ";;;;;"
params: null
expected:
  status: success
  frames: 0
  output: |
    (empty)
```

### query_mixed_empty_and_non_empty
```yaml
operation: query
statement: ";MAP 1 as one ;;;MAP 2 as two"
params: null
expected:
  status: success
  frames: 2
  output: |
    Frame 1:
    +-------+
    |  one  |
    +-------+
    |   1   |
    +-------+
    
    Frame 2:
    +-------+
    |  two  |
    +-------+
    |   2   |
    +-------+
```

### query_single_statement_with_semicolon
```yaml
operation: query
statement: "MAP 1 as result;"
params: null
expected:
  status: success
  frames: 1
  output: |
    +----------+
    |  result  |
    +----------+
    |     1    |
    +----------+
```

### query_multiple_statements_same_structure
```yaml
operation: query
statement: "MAP 1 as result;MAP 2 as result;MAP 3 as result;"
params: null
expected:
  status: success
  frames: 3
  output: |
    Frame 1:
    +----------+
    |  result  |
    +----------+
    |     1    |
    +----------+
    
    Frame 2:
    +----------+
    |  result  |
    +----------+
    |     2    |
    +----------+
    
    Frame 3:
    +----------+
    |  result  |
    +----------+
    |     3    |
    +----------+
```

### query_multiple_statements_different_structure
```yaml
operation: query
statement: "MAP 1 as result;MAP { 2 as a, 3 as b };MAP 'ReifyDB' as result;"
params: null
expected:
  status: success
  frames: 3
  output: |
    Frame 1:
    +----------+
    |  result  |
    +----------+
    |     1    |
    +----------+
    
    Frame 2:
    +------+------+
    |  a   |  b   |
    +------+------+
    |  2   |  3   |
    +------+------+
    
    Frame 3:
    +----------+
    |  result  |
    +----------+
    | ReifyDB  |
    +----------+
```

## Implementation Notes

### TypeScript
- Each statement produces a separate frame in the frames array
- Empty statements should not produce frames (frames.length should match non-empty statements)
- Use appropriate Schema definitions for each frame structure

### Rust
- For snapshot tests, consider how to represent multiple frames
- May need special handling for empty results vs. multiple results
- Ensure frame ordering is preserved