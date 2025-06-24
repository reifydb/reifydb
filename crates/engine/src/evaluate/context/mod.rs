// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

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
use reifydb_core::num::SafeSubtract;
use reifydb_core::{BitVec, Kind};
use reifydb_diagnostic::r#type::TypeOutOfRange;
use reifydb_diagnostic::{Diagnostic, IntoSpan};

#[derive(Clone, Debug)]
pub(crate) struct EvaluationColumn {
    pub(crate) name: Option<String>,
    pub(crate) kind: Option<Kind>,
    pub(crate) policies: Vec<ColumnPolicyKind>,
}

impl EvaluationColumn {
    pub(crate) fn saturation_policy(&self) -> &ColumnSaturationPolicy {
        self.policies
            .iter()
            .find_map(|p| match p {
                ColumnPolicyKind::Saturation(policy) => Some(policy),
                _ => None,
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
    pub(crate) limit: Option<usize>,
}

impl Context {
    pub fn testing() -> Self {
        Self {
            column: None,
            mask: BitVec::new(0, false),
            columns: vec![],
            row_count: 1,
            limit: None,
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

impl Context {
    pub(crate) fn sub<T: SafeSubtract>(
        &self,
        l: T,
        r: T,
        span: impl IntoSpan,
    ) -> crate::evaluate::Result<Option<T>> {
        match self.saturation_policy() {
            ColumnSaturationPolicy::Error => l
                .checked_sub(r)
                .ok_or_else(|| {
                    if let Some(column) = &self.column {
                        return crate::evaluate::Error(Diagnostic::type_out_of_range(
                            TypeOutOfRange {
                                span: span.into_span(),
                                column: column.name.clone(),
                                ty: column.kind,
                            },
                        ));
                    }
                    // expression_saturation
                    unimplemented!()
                })
                .map(Some),
            // SaturationPolicy::Saturate => Ok(a.saturating_add(b)),
            // SaturationPolicy::Wrap => Ok(a.wrapping_add(b)),
            ColumnSaturationPolicy::Undefined => Ok(None),
        }
    }
}
