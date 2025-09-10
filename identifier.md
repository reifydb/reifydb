# Qualified Identifier System - Implementation Plan

## Executive Summary

This document outlines the design and implementation plan for a new qualified identifier system in ReifyDB. The system will replace the current simple `AstIdentifier` with a comprehensive type-safe identifier hierarchy that supports full qualification, aliases, and default schema injection.

## Current State Problems

1. **Ambiguous Identifiers**: Current `AstIdentifier` is just a token wrapper with no semantic information
2. **Lost Context**: Schema/table/column relationships are not preserved through plan transformations
3. **Alias Confusion**: No clear distinction between aliases and actual source names
4. **Inconsistent Qualification**: Different parts of the codebase handle qualification differently
5. **No Default Schema Support**: Cannot inject default schema when not specified by user

## Design Goals

- **Type Safety**: Different identifier types cannot be confused
- **Full Qualification**: All identifiers fully qualified at plan level
- **Extensibility**: Easy to add new source types (streams, external tables, etc.)
- **Clear Semantics**: Always know if using alias vs actual name
- **Default Schema Support**: Seamlessly inject default schema when not provided
- **Better Diagnostics**: Track origin of identifiers for better error messages

## Core Design

### 1. Identifier Type Hierarchy

```rust
// crates/core/src/interface/identifier.rs

use reifydb_type::{Fragment, OwnedFragment};
use serde::{Serialize, Deserialize};

/// Root enum for all qualified identifier types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QualifiedIdentifier<'a> {
    Schema(SchemaIdentifier<'a>),
    Source(SourceIdentifier<'a>),
    Column(ColumnIdentifier<'a>),
    Function(FunctionIdentifier<'a>),
    Sequence(SequenceIdentifier<'a>),
    Index(IndexIdentifier<'a>),
}

/// Schema identifier - always unqualified
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaIdentifier<'a> {
    pub name: Fragment<'a>,
}

/// Source identifier for tables, views, and future source types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceIdentifier<'a> {
    /// Schema containing this source (None means needs resolution)
    pub schema: Option<Fragment<'a>>,
    /// Source name
    pub name: Fragment<'a>,
    /// Alias for this source in query context
    pub alias: Option<Fragment<'a>>,
    /// Type of source (determined during resolution)
    pub kind: SourceKind,
}

/// Types of sources that can be referenced
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceKind {
    Table,
    View,
    MaterializedView,
    DeferredView,
    TransactionalView,
    ExternalTable,
    Subquery,
    CTE,  // Common Table Expression
    Unknown,  // Before resolution
}

/// Column identifier with source qualification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnIdentifier<'a> {
    pub source: ColumnSource<'a>,
    pub name: Fragment<'a>,
}

/// How a column is qualified
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ColumnSource<'a> {
    /// Qualified by source name (table/view)
    Source {
        schema: Option<Fragment<'a>>,
        source: Fragment<'a>,
    },
    /// Qualified by alias
    Alias(Fragment<'a>),
    /// Not qualified (needs resolution based on context)
    Unqualified,
}

/// Function identifier with namespace support
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionIdentifier<'a> {
    /// Namespace chain (e.g., ["pg_catalog", "string"] for pg_catalog::string::substr)
    pub namespaces: Vec<Fragment<'a>>,
    /// Function name
    pub name: Fragment<'a>,
}

/// Sequence identifier
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SequenceIdentifier<'a> {
    pub schema: Option<Fragment<'a>>,
    pub name: Fragment<'a>,
}

/// Index identifier
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexIdentifier<'a> {
    pub schema: Option<Fragment<'a>>,
    pub table: Fragment<'a>,
    pub name: Fragment<'a>,
}
```

### 2. Identifier Resolution System

```rust
// crates/rql/src/plan/logical/resolver.rs

use std::collections::HashMap;
use reifydb_catalog::MaterializedCatalog;
use reifydb_core::interface::identifier::*;
use reifydb_type::{Fragment, OwnedFragment};

/// Context for resolving identifiers during logical planning
pub struct IdentifierResolver {
    /// User's default schema (from session/connection)
    default_schema: Option<String>,
    
    /// Maps aliases to fully qualified source identifiers
    /// Built up as FROM/JOIN clauses are processed
    aliases: HashMap<String, SourceIdentifier<'static>>,
    
    /// Available columns in current scope
    /// Maps (source_alias_or_name, column_name) -> ColumnIdentifier
    available_columns: HashMap<(String, String), ColumnIdentifier<'static>>,
    
    /// Stack of CTE definitions for WITH clauses
    cte_stack: Vec<HashMap<String, SourceIdentifier<'static>>>,
    
    /// Catalog for validation and type determination
    catalog: Arc<MaterializedCatalog>,
    
    /// Current transaction version for catalog lookups
    version: Version,
}

impl IdentifierResolver {
    pub fn new(
        catalog: Arc<MaterializedCatalog>,
        version: Version,
        default_schema: Option<String>,
    ) -> Self {
        Self {
            default_schema,
            aliases: HashMap::new(),
            available_columns: HashMap::new(),
            cte_stack: Vec::new(),
            catalog,
            version,
        }
    }
    
    /// Push a new CTE scope
    pub fn push_cte_scope(&mut self) {
        self.cte_stack.push(HashMap::new());
    }
    
    /// Pop CTE scope
    pub fn pop_cte_scope(&mut self) {
        self.cte_stack.pop();
    }
    
    /// Register a CTE
    pub fn register_cte(&mut self, name: String, source: SourceIdentifier<'static>) {
        if let Some(scope) = self.cte_stack.last_mut() {
            scope.insert(name, source);
        }
    }
    
    /// Register an alias for a source
    pub fn register_alias(&mut self, alias: String, source: SourceIdentifier<'static>) {
        self.aliases.insert(alias, source);
    }
    
    /// Resolve a source identifier to fully qualified form
    pub fn resolve_source(&self, source: &SourceIdentifier<'a>) -> Result<SourceIdentifier<'static>> {
        // First check if this references a CTE
        for cte_scope in self.cte_stack.iter().rev() {
            if let Some(cte_source) = cte_scope.get(source.name.text()) {
                return Ok(cte_source.clone());
            }
        }
        
        // Determine schema to use
        let resolved_schema = match &source.schema {
            Some(schema) => {
                // User provided explicit schema - validate it exists
                let schema_name = schema.text();
                if !self.catalog.schema_exists(schema_name, self.version) {
                    return Err(Error::SchemaNotFound(schema_name.to_string()));
                }
                Some(Fragment::Owned(schema.into_owned()))
            }
            None => {
                // No schema provided - inject default if available
                self.default_schema.as_ref().map(|default| {
                    // Use Internal fragment type to indicate this was injected
                    Fragment::Owned(OwnedFragment::Internal {
                        text: default.clone(),
                    })
                })
            }
        };
        
        // Determine source type from catalog
        let source_kind = self.determine_source_kind(
            resolved_schema.as_ref().map(|f| f.text()),
            source.name.text(),
        )?;
        
        Ok(SourceIdentifier {
            schema: resolved_schema,
            name: Fragment::Owned(source.name.into_owned()),
            alias: source.alias.as_ref().map(|a| Fragment::Owned(a.into_owned())),
            kind: source_kind,
        })
    }
    
    /// Resolve a column identifier to fully qualified form
    pub fn resolve_column(&self, column: &ColumnIdentifier<'a>) -> Result<ColumnIdentifier<'static>> {
        let resolved_source = match &column.source {
            ColumnSource::Source { schema, source } => {
                // Column qualified by source name
                let resolved_schema = self.resolve_schema_fragment(schema)?;
                
                // Validate source exists
                self.validate_source_exists(
                    resolved_schema.as_ref().map(|f| f.text()),
                    source.text(),
                )?;
                
                ColumnSource::Source {
                    schema: resolved_schema,
                    source: Fragment::Owned(source.into_owned()),
                }
            }
            ColumnSource::Alias(alias) => {
                // Column qualified by alias - check it exists
                if !self.aliases.contains_key(alias.text()) {
                    return Err(Error::UnknownAlias(alias.text().to_string()));
                }
                ColumnSource::Alias(Fragment::Owned(alias.into_owned()))
            }
            ColumnSource::Unqualified => {
                // Unqualified column - resolve based on available sources
                // This requires checking all sources in scope for this column
                let matching_sources = self.find_column_sources(column.name.text());
                
                match matching_sources.len() {
                    0 => return Err(Error::ColumnNotFound(column.name.text().to_string())),
                    1 => {
                        // Unambiguous - qualify with the single source
                        let (source_name, source_id) = &matching_sources[0];
                        ColumnSource::Source {
                            schema: source_id.schema.clone(),
                            source: source_id.name.clone(),
                        }
                    }
                    _ => {
                        // Ambiguous - report error with all possibilities
                        let sources: Vec<String> = matching_sources
                            .iter()
                            .map(|(name, _)| name.clone())
                            .collect();
                        return Err(Error::AmbiguousColumn {
                            column: column.name.text().to_string(),
                            sources,
                        });
                    }
                }
            }
        };
        
        Ok(ColumnIdentifier {
            source: resolved_source,
            name: Fragment::Owned(column.name.into_owned()),
        })
    }
    
    /// Resolve a function identifier
    pub fn resolve_function(&self, func: &FunctionIdentifier<'a>) -> Result<FunctionIdentifier<'static>> {
        // Validate function exists in catalog
        let namespaces: Vec<String> = func.namespaces.iter().map(|f| f.text().to_string()).collect();
        let function_name = func.name.text();
        
        if !self.catalog.function_exists(&namespaces, function_name, self.version) {
            return Err(Error::FunctionNotFound {
                namespaces,
                name: function_name.to_string(),
            });
        }
        
        Ok(FunctionIdentifier {
            namespaces: func.namespaces.iter().map(|f| Fragment::Owned(f.into_owned())).collect(),
            name: Fragment::Owned(func.name.into_owned()),
        })
    }
    
    // Helper methods
    
    fn determine_source_kind(&self, schema: Option<&str>, name: &str) -> Result<SourceKind> {
        let schema = schema.unwrap_or_else(|| {
            self.default_schema.as_deref().unwrap_or("public")
        });
        
        // Check catalog for source type
        if self.catalog.table_exists(schema, name, self.version) {
            Ok(SourceKind::Table)
        } else if self.catalog.view_exists(schema, name, self.version) {
            // Determine specific view type
            let view_def = self.catalog.get_view(schema, name, self.version)?;
            Ok(match view_def.kind {
                ViewKind::Materialized => SourceKind::MaterializedView,
                ViewKind::Deferred => SourceKind::DeferredView,
                ViewKind::Transactional => SourceKind::TransactionalView,
                _ => SourceKind::View,
            })
        } else {
            Err(Error::SourceNotFound {
                schema: schema.to_string(),
                name: name.to_string(),
            })
        }
    }
    
    fn resolve_schema_fragment(&self, schema: &Option<Fragment<'a>>) -> Result<Option<Fragment<'static>>> {
        Ok(match schema {
            Some(s) => Some(Fragment::Owned(s.into_owned())),
            None => {
                // Inject default schema if available
                self.default_schema.as_ref().map(|default| {
                    Fragment::Owned(OwnedFragment::Internal {
                        text: default.clone(),
                    })
                })
            }
        })
    }
    
    fn validate_source_exists(&self, schema: Option<&str>, name: &str) -> Result<()> {
        let schema = schema.unwrap_or_else(|| {
            self.default_schema.as_deref().unwrap_or("public")
        });
        
        if !self.catalog.source_exists(schema, name, self.version) {
            return Err(Error::SourceNotFound {
                schema: schema.to_string(),
                name: name.to_string(),
            });
        }
        
        Ok(())
    }
    
    fn find_column_sources(&self, column_name: &str) -> Vec<(String, &SourceIdentifier<'static>)> {
        let mut sources = Vec::new();
        
        // Check all registered aliases
        for (alias, source) in &self.aliases {
            if self.source_has_column(source, column_name) {
                sources.push((alias.clone(), source));
            }
        }
        
        // Check CTEs
        for cte_scope in &self.cte_stack {
            for (name, source) in cte_scope {
                if self.source_has_column(source, column_name) {
                    sources.push((name.clone(), source));
                }
            }
        }
        
        sources
    }
    
    fn source_has_column(&self, source: &SourceIdentifier<'static>, column_name: &str) -> bool {
        let schema = source.schema.as_ref().map(|f| f.text()).unwrap_or("public");
        let source_name = source.name.text();
        
        match source.kind {
            SourceKind::Table => {
                self.catalog.table_has_column(schema, source_name, column_name, self.version)
            }
            SourceKind::View | SourceKind::MaterializedView | 
            SourceKind::DeferredView | SourceKind::TransactionalView => {
                self.catalog.view_has_column(schema, source_name, column_name, self.version)
            }
            _ => false,
        }
    }
}
```

### 3. AST Updates

Update AST types to use qualified identifiers:

```rust
// crates/rql/src/ast/ast.rs

// Replace current AstIdentifier usages with:

pub enum AstFrom<'a> {
    Source {
        token: Token<'a>,
        source: SourceIdentifier<'a>,  // Replaces schema + source + alias
        index: Option<IndexIdentifier<'a>>,  // Replaces index_name
    },
    Inline {
        token: Token<'a>,
        list: AstList<'a>,
    },
}

pub struct AstCallFunction<'a> {
    pub token: Token<'a>,
    pub function: FunctionIdentifier<'a>,  // Replaces namespaces + function
    pub arguments: AstTuple<'a>,
}

pub struct AstCreate<'a> {
    pub token: Token<'a>,
    pub object: CreateObject<'a>,
}

pub enum CreateObject<'a> {
    Schema(SchemaIdentifier<'a>),
    Table {
        table: SourceIdentifier<'a>,
        columns: Vec<ColumnDef<'a>>,
        // ...
    },
    View {
        view: SourceIdentifier<'a>,
        query: Box<Ast<'a>>,
        // ...
    },
    Sequence(SequenceIdentifier<'a>),
    Index(IndexIdentifier<'a>),
}
```

### 4. Expression Updates

Update expression types to use qualified identifiers:

```rust
// crates/core/src/interface/evaluate/expression/mod.rs

// Update existing types:

pub struct AccessSourceExpression<'a> {
    pub column: ColumnIdentifier<'a>,  // Replaces source + column
}

pub struct CallExpression<'a> {
    pub func: FunctionIdentifier<'a>,  // Replaces IdentExpression
    pub args: Vec<Expression<'a>>,
    pub fragment: Fragment<'a>,
}

pub struct ColumnExpression<'a> {
    pub column: ColumnIdentifier<'a>,  // Replaces Fragment
}
```

### 5. Plan Updates

Update logical and physical plan types:

```rust
// crates/rql/src/plan/logical/mod.rs

pub struct SourceScanNode<'a> {
    pub source: SourceIdentifier<'a>,  // Replaces schema + source + alias
    pub index: Option<IndexIdentifier<'a>>,
}

pub struct CreateTableNode<'a> {
    pub table: SourceIdentifier<'a>,
    pub columns: Vec<TableColumnToCreate>,
    // ...
}

pub struct UpdateNode<'a> {
    pub target: SourceIdentifier<'a>,  // Replaces schema + table
    pub input: Option<Box<LogicalPlan<'a>>>,
}

pub struct DeleteNode<'a> {
    pub target: SourceIdentifier<'a>,  // Replaces schema + table
    pub input: Option<Box<LogicalPlan<'a>>>,
}
```

## Implementation Plan

### Phase 1: Core Infrastructure (Week 1)
1. **Day 1-2**: Create identifier module with all type definitions
   - File: `crates/core/src/interface/identifier.rs`
   - Implement all identifier structs and enums
   - Add serialization support
   - Add conversion methods (to_owned, etc.)

2. **Day 3-4**: Implement identifier resolver
   - File: `crates/rql/src/plan/logical/resolver.rs`
   - Basic resolution logic
   - Default schema injection
   - Alias tracking

3. **Day 5**: Add error types and tests
   - File: `crates/core/src/error/identifier.rs`
   - Comprehensive error messages
   - Unit tests for resolver

### Phase 2: AST Migration (Week 2)
1. **Day 1-2**: Update AST types
   - Modify `AstFrom`, `AstCallFunction`, etc.
   - Update parser to create new identifier types
   - Maintain backward compatibility temporarily

2. **Day 3-4**: Update expression compiler
   - Modify expression types to use new identifiers
   - Update expression compilation logic
   - Add identifier resolution calls

3. **Day 5**: Integration tests
   - Test parsing with new identifiers
   - Test expression compilation
   - Validate error messages

### Phase 3: Logical Plan Integration (Week 3)
1. **Day 1-2**: Update logical plan types
   - Modify all node types to use qualified identifiers
   - Update plan builder to use resolver

2. **Day 3-4**: Update plan compilation
   - Integrate resolver into compilation pipeline
   - Add resolution context management
   - Handle CTEs and subqueries

3. **Day 5**: Comprehensive testing
   - Test all SQL patterns
   - Test default schema injection
   - Test error cases

### Phase 4: Physical Plan and Execution (Week 4)
1. **Day 1-2**: Update physical plan types
   - Ensure all identifiers are fully qualified
   - Remove resolution logic from physical layer

2. **Day 3-4**: Update execution engine
   - Modify catalog lookups to use new identifiers
   - Update row access patterns

3. **Day 5**: Performance testing
   - Benchmark identifier resolution
   - Optimize hot paths
   - Profile memory usage

### Phase 5: Cleanup and Polish (Week 5)
1. **Day 1-2**: Remove old identifier system
   - Delete `AstIdentifier`
   - Remove compatibility shims
   - Update all tests

2. **Day 3-4**: Documentation
   - Write developer guide
   - Update SQL reference
   - Add examples

3. **Day 5**: Final testing
   - Full regression test suite
   - Performance validation
   - Edge case testing

## Testing Strategy

### Unit Tests
- Test each identifier type's construction and methods
- Test resolver with various scenarios
- Test error cases and messages

### Integration Tests
- Test full SQL statements with various qualification patterns
- Test default schema injection
- Test alias resolution
- Test CTE handling

### Test Cases

```sql
-- Test default schema injection
CREATE TABLE users (id INT);
SELECT * FROM users;  -- Should inject default schema

-- Test explicit schema
SELECT * FROM public.users;

-- Test aliases
SELECT u.id FROM users AS u;
SELECT u.id FROM public.users u;

-- Test column qualification
SELECT users.id FROM users;
SELECT u.id FROM users u;
SELECT id FROM users;  -- Unqualified

-- Test ambiguous columns
SELECT id FROM users, profiles;  -- Should error

-- Test CTEs
WITH recent AS (SELECT * FROM users)
SELECT * FROM recent;

-- Test function calls
SELECT pg_catalog::string::length(name) FROM users;

-- Test cross-schema references
SELECT * FROM schema1.table1 t1
JOIN schema2.table2 t2 ON t1.id = t2.id;
```

## Migration Considerations

### Backward Compatibility
- Not required per specification
- Can make breaking changes

### Rollout Strategy
1. Deploy identifier types
2. Update parser incrementally
3. Migrate plan types
4. Remove old system

### Risk Mitigation
- Extensive testing at each phase
- Keep old system temporarily for comparison
- Performance benchmarks to catch regressions

## Performance Considerations

### Memory Usage
- Qualified identifiers use more memory than simple strings
- Use string interning for common schema/table names
- Share Fragment instances where possible

### Resolution Performance
- Cache resolution results within a statement
- Use efficient lookup structures in resolver
- Minimize catalog access

### Optimization Opportunities
- Pre-resolve identifiers in prepared statements
- Cache fully qualified identifiers in plan cache
- Use identifier hash for fast equality checks

## Future Extensions

### Additional Source Types
- External tables (foreign data wrappers)
- Streams (for real-time data)
- Federated queries
- Time-series sources

### Advanced Features
- Identifier search paths (like PostgreSQL)
- Synonym support
- Cross-database references
- Dynamic identifier resolution

### Tooling Integration
- IDE support with qualified identifier info
- Better EXPLAIN output showing qualification
- Debugging tools showing resolution steps

## Success Metrics

1. **Correctness**: All existing tests pass with new system
2. **Performance**: No regression in query compilation time
3. **Usability**: Clear error messages for identifier issues
4. **Maintainability**: Easier to add new identifier types
5. **Diagnostics**: Better error messages with full context

## Conclusion

This qualified identifier system provides a robust foundation for ReifyDB's query processing. It solves current ambiguity issues while providing extensibility for future features. The phased implementation approach minimizes risk while allowing for incremental validation of the design.