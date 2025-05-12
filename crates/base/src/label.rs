// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ValueType;
use crate::schema::{ColumnName, SchemaName, StoreName};
use std::fmt::Display;

/// A column label, used in query results and plans.
#[derive(Clone, Debug)]
pub enum Label {
    /// A custom label
    Custom { value: ValueType, label: String },
    /// Just the column name
    Column { value: ValueType, column: ColumnName },
    /// Store and column name
    StoreAndColumn { value: ValueType, store: StoreName, column: ColumnName },
    /// Full name consisting of schema, store and column
    Full { value: ValueType, schema: SchemaName, store: StoreName, column: ColumnName },
}

impl Display for Label {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Custom { label, .. } => write!(f, "{label}"),
            Self::Column { column, .. } => write!(f, "{column}"),
            Self::StoreAndColumn { store, column, .. } => write!(f, "{store}.{column}"),
            Self::Full { schema, store, column, .. } => write!(f, "{schema}.{store}.{column}"),
        }
    }
}
