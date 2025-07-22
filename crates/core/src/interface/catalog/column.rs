// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::Type;
use super::policy::{ColumnPolicyKind, ColumnSaturationPolicy, DEFAULT_COLUMN_SATURATION_POLICY};

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDescriptor<'a> {
    // Location information
    pub schema: Option<&'a str>,
    pub table: Option<&'a str>, 
    pub column: Option<&'a str>,
    
    // Column metadata
    pub column_type: Option<Type>,
    pub policies: Vec<ColumnPolicyKind>,
}

impl<'a> ColumnDescriptor<'a> {
    pub fn new() -> Self {
        Self { 
            schema: None, 
            table: None, 
            column: None,
            column_type: None,
            policies: Vec::new(),
        }
    }
    
    pub fn with_schema(mut self, schema: &'a str) -> Self {
        self.schema = Some(schema);
        self
    }
    
    pub fn with_table(mut self, table: &'a str) -> Self {
        self.table = Some(table);
        self
    }
    
    pub fn with_column(mut self, column: &'a str) -> Self {
        self.column = Some(column);
        self
    }
    
    pub fn with_column_type(mut self, column_type: Type) -> Self {
        self.column_type = Some(column_type);
        self
    }
    
    pub fn with_policies(mut self, policies: Vec<ColumnPolicyKind>) -> Self {
        self.policies = policies;
        self
    }
    
    // Location formatting
    pub fn location_string(&self) -> String {
        match (self.schema, self.table, self.column) {
            (Some(s), Some(t), Some(c)) => format!("{}.{}.{}", s, t, c),
            (Some(s), Some(t), None) => format!("{}.{}", s, t),
            (None, Some(t), Some(c)) => format!("{}.{}", t, c),
            (Some(s), None, Some(c)) => format!("{}.{}", s, c),
            (Some(s), None, None) => s.to_string(),
            (None, Some(t), None) => t.to_string(),
            (None, None, Some(c)) => c.to_string(),
            (None, None, None) => "unknown location".to_string(),
        }
    }
    
    // Policy methods
    pub fn saturation_policy(&self) -> &ColumnSaturationPolicy {
        self.policies
            .iter()
            .find_map(|p| match p {
                ColumnPolicyKind::Saturation(policy) => Some(policy),
            })
            .unwrap_or(&DEFAULT_COLUMN_SATURATION_POLICY)
    }
}