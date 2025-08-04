// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::IntoOwnedSpan;
use crate::result::error::diagnostic::Diagnostic;

pub fn schema_already_exists(span: Option<impl IntoOwnedSpan>, schema: &str) -> Diagnostic {
    Diagnostic {
        code: "CA_001".to_string(),
        statement: None,
        message: format!("schema `{}` already exists", schema),
        span: span.map(|s| s.into_span()),
        label: Some("duplicate schema definition".to_string()),
        help: Some("choose a different name or drop the existing schema first".to_string()),
        column: None,
        notes: vec![],
        cause: None,
    }
}

pub fn schema_not_found(span: Option<impl IntoOwnedSpan>, schema: &str) -> Diagnostic {
    Diagnostic {
        code: "CA_002".to_string(),
        statement: None,
        message: format!("schema `{}` not found", schema),
        span: span.map(|s| s.into_span()),
        label: Some("undefined schema reference".to_string()),
        help: Some("make sure the schema exists before using it or create it first".to_string()),
        column: None,
        notes: vec![],
        cause: None,
    }
}

pub fn table_already_exists(
    span: Option<impl IntoOwnedSpan>,
    schema: &str,
    table: &str,
) -> Diagnostic {
    Diagnostic {
        code: "CA_003".to_string(),
        statement: None,
        message: format!("table `{}.{}` already exists", schema, table),
        span: span.map(|s| s.into_span()),
        label: Some("duplicate table definition".to_string()),
        help: Some("choose a different name, drop the existing table or create table in a different schema".to_string()),
        column: None,
        notes: vec![],
        cause: None,
    }
}

pub fn table_not_found(span: impl IntoOwnedSpan, schema: &str, table: &str) -> Diagnostic {
    let owned_span = span.into_span();
    Diagnostic {
        code: "CA_004".to_string(),
        statement: None,
        message: format!("table `{}.{}` not found", schema, table),
        span: Some(owned_span),
        label: Some("unknown table reference".to_string()),
        help: Some("ensure the table exists or create it first using `CREATE TABLE`".to_string()),
        column: None,
        notes: vec![],
        cause: None,
    }
}

pub fn column_already_exists(
    span: Option<impl IntoOwnedSpan>,
    schema: &str,
    table: &str,
    column: &str,
) -> Diagnostic {
    Diagnostic {
        code: "CA_005".to_string(),
        statement: None,
        message: format!("column `{}` already exists in table `{}`.`{}`", column, schema, table),
        span: span.map(|s| s.into_span()),
        label: Some("duplicate column definition".to_string()),
        help: Some("choose a different column name or drop the existing one first".to_string()),
        column: None,
        notes: vec![],
        cause: None,
    }
}

pub fn column_policy_already_exists(policy: &str, column: &str) -> Diagnostic {
    Diagnostic {
        code: "CA_008".to_string(),
        statement: None,
        message: format!("policy `{policy:?}` already exists for column `{}`", column),
        span: None,
        label: Some("duplicate column policy".to_string()),
        help: Some("remove the existing policy first".to_string()),
        column: None,
        notes: vec![],
        cause: None,
    }
}

pub fn index_variable_length_not_supported() -> Diagnostic {
    Diagnostic {
        code: "CA_009".to_string(),
        statement: None,
        message: "variable-length types (UTF8, BLOB) are not supported in indexes".to_string(),
        span: None,
        label: Some("unsupported type for indexing".to_string()),
        help: Some("only fixed-size types can be indexed currently".to_string()),
        column: None,
        notes: vec![],
        cause: None,
    }
}

pub fn index_types_directions_mismatch(types_len: usize, directions_len: usize) -> Diagnostic {
    Diagnostic {
        code: "CA_010".to_string(),
        statement: None,
        message: format!(
            "mismatch between number of types ({}) and directions ({})",
            types_len, directions_len
        ),
        span: None,
        label: Some("length mismatch".to_string()),
        help: Some("each indexed field must have a corresponding sort direction".to_string()),
        column: None,
        notes: vec![],
        cause: None,
    }
}
