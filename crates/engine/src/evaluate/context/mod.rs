// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub use convert::Convert;
pub use demote::Demote;
pub use promote::Promote;

use reifydb_catalog::column_policy::{
    ColumnPolicyKind, ColumnSaturationPolicy, DEFAULT_COLUMN_SATURATION_POLICY,
};

mod arith;
mod convert;
mod demote;
mod promote;

use crate::frame::Column;
use reifydb_core::{BitVec, DataType};

#[derive(Clone, Debug)]
pub(crate) struct EvaluationColumn {
    pub(crate) name: Option<String>,
    pub(crate) data_type: Option<DataType>,
    pub(crate) policies: Vec<ColumnPolicyKind>,
}

impl EvaluationColumn {
    pub(crate) fn saturation_policy(&self) -> &ColumnSaturationPolicy {
        self.policies
            .iter()
            .find_map(|p| match p {
                ColumnPolicyKind::Saturation(policy) => Some(policy),
            })
            .unwrap_or(&DEFAULT_COLUMN_SATURATION_POLICY)
    }
}

#[derive(Debug)]
pub(crate) struct Context {
    pub(crate) column: Option<EvaluationColumn>,
    pub(crate) mask: BitVec,
    pub(crate) columns: Vec<Column>,
    pub(crate) row_count: usize,
    pub(crate) take: Option<usize>,
}

impl Context {
    #[cfg(test)]
    pub fn testing() -> Self {
        Self {
            column: None,
            mask: BitVec::new(0, false),
            columns: vec![],
            row_count: 1,
            take: None,
        }
    }
}

impl Context {
    pub(crate) fn saturation_policy(&self) -> &ColumnSaturationPolicy {
        self.column
            .as_ref()
            .map(|c| c.saturation_policy())
            .unwrap_or(&DEFAULT_COLUMN_SATURATION_POLICY)
    }
}
