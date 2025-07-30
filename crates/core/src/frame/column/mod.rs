// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::Type;
pub use layout::FrameColumnLayout;
use serde::{Deserialize, Serialize};
pub use values::ColumnValues;

mod layout;
mod values;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FullyQualified {
    pub schema: String,
    pub table: String,
    pub name: String,
    pub values: ColumnValues,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TableQualified {
    pub table: String,
    pub name: String,
    pub values: ColumnValues,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ColumnQualified {
    pub name: String,
    pub values: ColumnValues,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Unqualified {
    pub name: String,
    pub values: ColumnValues,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum FrameColumn {
    FullyQualified(FullyQualified),
    TableQualified(TableQualified),
    ColumnQualified(ColumnQualified),
    Unqualified(Unqualified),
}

impl FrameColumn {
    pub fn get_type(&self) -> Type {
        match self {
            Self::FullyQualified(col) => col.values.get_type(),
            Self::TableQualified(col) => col.values.get_type(),
            Self::ColumnQualified(col) => col.values.get_type(),
            Self::Unqualified(col) => col.values.get_type(),
        }
    }

    pub fn qualified_name(&self) -> String {
        match self {
            Self::FullyQualified(col) => format!("{}.{}.{}", col.schema, col.table, col.name),
            Self::TableQualified(col) => format!("{}.{}", col.table, col.name),
            Self::ColumnQualified(col) => col.name.clone(),
            Self::Unqualified(col) => col.name.clone(),
        }
    }

    pub fn with_new_values(&self, values: ColumnValues) -> FrameColumn {
        match self {
            Self::FullyQualified(col) => Self::FullyQualified(FullyQualified {
                schema: col.schema.clone(),
                table: col.table.clone(),
                name: col.name.clone(),
                values,
            }),
            Self::TableQualified(col) => Self::TableQualified(TableQualified {
                table: col.table.clone(),
                name: col.name.clone(),
                values,
            }),
            Self::ColumnQualified(col) => {
                Self::ColumnQualified(ColumnQualified { name: col.name.clone(), values })
            }
            Self::Unqualified(col) => {
                Self::Unqualified(Unqualified { name: col.name.clone(), values })
            }
        }
    }

    pub fn name(&self) -> &str {
        match self {
            Self::FullyQualified(col) => &col.name,
            Self::TableQualified(col) => &col.name,
            Self::ColumnQualified(col) => &col.name,
            Self::Unqualified(col) => &col.name,
        }
    }

    pub fn table(&self) -> Option<&str> {
        match self {
            Self::FullyQualified(col) => Some(&col.table),
            Self::TableQualified(col) => Some(&col.table),
            Self::ColumnQualified(_) => None,
            Self::Unqualified(_) => None,
        }
    }

    pub fn schema(&self) -> Option<&str> {
        match self {
            Self::FullyQualified(col) => Some(&col.schema),
            Self::TableQualified(_) => None,
            Self::ColumnQualified(_) => None,
            Self::Unqualified(_) => None,
        }
    }

    pub fn values(&self) -> &ColumnValues {
        match self {
            Self::FullyQualified(col) => &col.values,
            Self::TableQualified(col) => &col.values,
            Self::ColumnQualified(col) => &col.values,
            Self::Unqualified(col) => &col.values,
        }
    }

    pub fn values_mut(&mut self) -> &mut ColumnValues {
        match self {
            Self::FullyQualified(col) => &mut col.values,
            Self::TableQualified(col) => &mut col.values,
            Self::ColumnQualified(col) => &mut col.values,
            Self::Unqualified(col) => &mut col.values,
        }
    }
}
