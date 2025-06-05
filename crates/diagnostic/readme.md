## 🧱 ReifyDB Error Classes

| Code | Layer       | Description                                                                 |
|------|-------------|-----------------------------------------------------------------------------|
| LX   | **Lexer**   | Errors during lexical analysis. Covers invalid characters, malformed literals (e.g. `12abc`), unterminated strings, and non-UTF8 input fragments. |
| PA   | **Parser**  | Syntax-level issues while building the AST. Includes unexpected tokens, incomplete expressions, unclosed parentheses, or misuse of keywords. |
| PL   | **Planner** | Semantic validation errors during query planning. Examples include type mismatches, undefined columns or tables, overflow of typed values, and invalid function calls. |
| CO   | **Constraints** | Violations of user-defined constraints. Includes `NOT NULL`, `UNIQUE`, `CHECK`, and `DEFAULT` expression violations during validation or insertion. |
| PO   | **Policies** | Errors related to ReifyDB’s policy system. Includes unauthorized field access, overflow/underflow handling behavior, and invalid policy definitions. |
| EX   | **Executor** | Runtime errors during query execution. Includes division by zero, invalid function evaluation, or unexpected runtime state (e.g. empty frame stack). |