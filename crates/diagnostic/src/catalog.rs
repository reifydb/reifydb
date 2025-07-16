// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{Diagnostic, Span};

pub fn schema_already_exists(span: Option<Span>, schema: &str) -> Diagnostic {
    Diagnostic {
        code: "CA_001".to_string(),
        statement: None,
        message: format!("schema `{}` already exists", schema),
        span,
        label: Some("duplicate schema definition".to_string()),
        help: Some("choose a different name or drop the existing schema first".to_string()),
        column: None,
        notes: vec![],
        caused_by: None,
    }
}

pub fn schema_not_found(span: Option<Span>, schema: &str) -> Diagnostic {
    Diagnostic {
        code: "CA_002".to_string(),
        statement: None,
        message: format!("schema `{}` not found", schema),
        span,
        label: Some("undefined schema reference".to_string()),
        help: Some("make sure the schema exists before using it or create it first".to_string()),
        column: None,
        notes: vec![],
        caused_by: None,
    }
}

pub fn table_already_exists(span: Option<Span>, schema: &str, table: &str) -> Diagnostic {
    Diagnostic {
        code: "CA_003".to_string(),
        statement: None,
        message: format!("table `{}.{}` already exists", schema, table),
        span,
        label: Some("duplicate table definition".to_string()),
        help: Some("choose a different name, drop the existing table or create table in a different schema".to_string()),
        column: None,
        notes: vec![],
        caused_by: None,
    }
}

pub fn table_not_found(span: Span, schema: &str, table: &str) -> Diagnostic {
    Diagnostic {
        code: "CA_004".to_string(),
        statement: None,
        message: format!("table `{}.{}` not found", schema, table),
        span: Some(span),
        label: Some("unknown table reference".to_string()),
        help: Some("ensure the table exists or create it first using `CREATE TABLE`".to_string()),
        column: None,
        notes: vec![],
        caused_by: None,
    }
}

pub fn column_already_exists(
    span: Option<Span>,
    schema: &str,
    table: &str,
    column: &str,
) -> Diagnostic {
    Diagnostic {
        code: "CA_005".to_string(),
        statement: None,
        message: format!("column `{}` already exists in table `{}`.`{}`", column, schema, table),
        span,
        label: Some("duplicate column definition".to_string()),
        help: Some("choose a different column name or drop the existing one first".to_string()),
        column: None,
        notes: vec![],
        caused_by: None,
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
        caused_by: None,
    }
}
