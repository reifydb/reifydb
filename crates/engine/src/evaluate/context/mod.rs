// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub use convert::Convert;
pub use demote::Demote;
pub use promote::Promote;
use reifydb_catalog::column::Column;
use reifydb_catalog::column_policy::{
    ColumnPolicyKind, ColumnSaturationPolicy, DEFAULT_COLUMN_SATURATION_POLICY,
};

mod arith;
mod convert;
mod demote;
mod promote;

use reifydb_core::frame::FrameColumn;
use reifydb_core::{BitVec, Type};

#[derive(Clone, Debug)]
pub(crate) struct EvaluationColumn {
    pub(crate) ty: Option<Type>,
    pub(crate) policies: Vec<ColumnPolicyKind>,
}

impl From<Column> for EvaluationColumn {
    fn from(value: Column) -> Self {
        Self {
            ty: Some(value.ty),
            policies: value.policies.into_iter().map(|cp| cp.policy).collect(),
        }
    }
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
pub(crate) struct EvaluationContext {
    pub(crate) column: Option<EvaluationColumn>,
    pub(crate) mask: BitVec,
    pub(crate) columns: Vec<FrameColumn>,
    pub(crate) row_count: usize,
    pub(crate) take: Option<usize>,
}

impl EvaluationContext {
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

impl EvaluationContext {
    pub(crate) fn saturation_policy(&self) -> &ColumnSaturationPolicy {
        self.column
            .as_ref()
            .map(|c| c.saturation_policy())
            .unwrap_or(&DEFAULT_COLUMN_SATURATION_POLICY)
    }
}
