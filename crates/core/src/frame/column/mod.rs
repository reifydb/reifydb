// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::Type;
pub use layout::FrameColumnLayout;
pub use push::Push;
use serde::{Deserialize, Serialize};
pub use values::ColumnValues;

mod extend;
mod filter;
mod get;
mod layout;
// pub mod old_pool;
pub mod container;
// mod pool; // TODO: Re-enable after container trait refactor
pub(crate) mod pooled;
mod push;
mod qualification;
mod reorder;
mod slice;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_qualified_column() {
        let column = TableQualified::int4("test_frame", "normal_column", [1, 2, 3]);
        assert_eq!(column.qualified_name(), "test_frame.normal_column");
        match column {
            FrameColumn::TableQualified(col) => {
                assert_eq!(col.table, "test_frame");
                assert_eq!(col.name, "normal_column");
            }
            _ => panic!("Expected TableQualified variant"),
        }
    }

    #[test]
    fn test_fully_qualified_column() {
        let column = FullyQualified::int4("public", "users", "id", [1, 2, 3]);
        assert_eq!(column.qualified_name(), "public.users.id");
        match column {
            FrameColumn::FullyQualified(col) => {
                assert_eq!(col.schema, "public");
                assert_eq!(col.table, "users");
                assert_eq!(col.name, "id");
            }
            _ => panic!("Expected FullyQualified variant"),
        }
    }

    #[test]
    fn test_column_qualified() {
        let column = ColumnQualified::int4("expr_result", [1, 2, 3]);
        assert_eq!(column.qualified_name(), "expr_result");
        match column {
            FrameColumn::ColumnQualified(col) => {
                assert_eq!(col.name, "expr_result");
            }
            _ => panic!("Expected ColumnQualified variant"),
        }
    }

    #[test]
    fn test_unqualified_expression() {
        let column = Unqualified::int4("sum(a+b)", [1, 2, 3]);
        assert_eq!(column.qualified_name(), "sum(a+b)");
        match column {
            FrameColumn::Unqualified(col) => {
                assert_eq!(col.name, "sum(a+b)");
            }
            _ => panic!("Expected Unqualified variant"),
        }
    }
}
