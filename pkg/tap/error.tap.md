# Error Handling Tests

## AI Instructions

**CRITICAL: NEVER SKIP ANY TEST CASES** - You MUST generate tests for EVERY SINGLE test case in this specification. Each test represents essential behavior that must be verified. Skipping tests breaks cross-language compatibility verification.

This specification tests error handling across ReifyDB client implementations. Tests verify that clients properly handle and report casting errors that result in out-of-range values.

When generating tests from this specification:
1. Ensure error matching is flexible (use pattern matching rather than exact string comparison)
2. Verify both error codes and error messages are present
3. Check that statement context (line, column, text) is provided when applicable
4. For nested errors, verify the cause chain is properly exposed

## Test Specification

```yaml
suite: Error Handling
description: Tests for cast errors with out-of-range values
tags: [errors, casting, type-safety]
```

## Test Cases

### command_out_of_range

Tests casting a value that exceeds the positive range of INT1 type in a command operation.

```yaml
operation: command
statement: "MAP cast(129, int1) as result;"
params:
  type: none
expected:
  status: error
  error_code: "CAST_002"
  error_pattern: "cast error"
  cause:
    error_code: "NUMBER_002"
    error_pattern: "value '129' exceeds the valid range for type Int1 \\(-128 to 127\\)"
  fragment:
    text: "129"
    has_location: true
```

### query_out_of_range

Tests casting a value that exceeds the positive range of INT1 type in a query operation.

```yaml
operation: query
statement: "MAP cast(129, int1) as result;"
params:
  type: none
expected:
  status: error
  error_code: "CAST_002"
  error_pattern: "cast error"
  cause:
    error_code: "NUMBER_002"
    error_pattern: "value '129' exceeds the valid range for type Int1 \\(-128 to 127\\)"
  fragment:
    text: "129"
    has_location: true
```

## Implementation Notes

### Error Structure

All errors should include:
- `code`: The specific error code (e.g., "CAST_002")
- `message`: A human-readable error message
- `statement`: The full statement that caused the error (when applicable)
- `fragment`: Location information about where the error occurred
  - `text`: The specific text that caused the error
  - `line`: Line number (1-based)
  - `column`: Column number (1-based)
- `cause`: Nested error information for errors with underlying causes

### Error Code Categories

- `CAST_xxx`: Type casting errors
- `NUMBER_xxx`: Numeric range/overflow errors
- `SYNTAX_xxx`: SQL/RQL syntax errors
- `PARAM_xxx`: Parameter binding errors
- `TYPE_xxx`: Type system errors
- `MATH_xxx`: Mathematical operation errors
- `FORMAT_xxx`: Data format errors
- `NULL_xxx`: Null handling errors

### Testing Guidelines

1. **Pattern Matching**: Use regex patterns for error messages as exact text may vary
2. **Location Info**: Verify that line/column information is provided when applicable
3. **Error Chains**: For nested errors, verify the full cause chain
4. **Both Operations**: Test errors in both `command` and `query` operations
5. **Parameter Types**: Test errors with named, positional, and no parameters