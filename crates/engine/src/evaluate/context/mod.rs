// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

pub use demote::Demote;
pub use promote::Promote;
use reifydb_catalog::column::{
    ColumnPolicy, ColumnSaturationPolicy, DEFAULT_COLUMN_SATURATION_POLICY,
};

mod demote;
mod promote;

use reifydb_core::ValueKind;
use reifydb_core::num::{SafeAdd, SafeSubtract};
use reifydb_diagnostic::IntoSpan;
use reifydb_diagnostic::policy::{ColumnSaturation, column_saturation};
use reifydb_frame::Frame;

#[derive(Debug)]
pub(crate) struct EvaluationColumn {
    pub(crate) name: String,
    pub(crate) value: ValueKind,
    pub(crate) policies: Vec<ColumnPolicy>,
}

impl EvaluationColumn {
    pub(crate) fn saturation_policy(&self) -> &ColumnSaturationPolicy {
        self.policies
            .iter()
            .find_map(|p| match p {
                ColumnPolicy::Saturation(policy) => Some(policy),
                _ => None,
            })
            .unwrap_or(&DEFAULT_COLUMN_SATURATION_POLICY)
    }
}

#[derive(Debug)]
pub(crate) struct Context {
    pub(crate) column: Option<EvaluationColumn>,
    pub(crate) frame: Option<Frame>,
}

impl Context {
    pub(crate) fn row_count_or_one(&self) -> usize {
        self.frame.as_ref().map(|f| f.row_count()).unwrap_or(1)
    }

    pub(crate) fn saturation_policy(&self) -> &ColumnSaturationPolicy {
        self.column
            .as_ref()
            .map(|c| c.saturation_policy())
            .unwrap_or(&DEFAULT_COLUMN_SATURATION_POLICY)
    }
}

impl Context {
    pub(crate) fn add<T: SafeAdd>(
        &self,
        l: T,
        r: T,
        span: impl IntoSpan,
    ) -> crate::evaluate::Result<Option<T>> {
        match self.saturation_policy() {
            ColumnSaturationPolicy::Error => l
                .checked_add(r)
                .ok_or_else(|| {
                    if let Some(column) = &self.column {
                        return crate::evaluate::Error(column_saturation(ColumnSaturation {
                            span: span.into_span(),
                            column: column.name.to_string(),
                            value: column.value,
                        }));
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
                        return crate::evaluate::Error(column_saturation(ColumnSaturation {
                            span: span.into_span(),
                            column: column.name.to_string(),
                            value: column.value,
                        }));
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
