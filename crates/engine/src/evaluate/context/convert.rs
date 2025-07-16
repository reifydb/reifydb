// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::evaluate::EvaluationContext;
use reifydb_catalog::column_policy::ColumnSaturationPolicy;
use reifydb_core::IntoSpan;
use reifydb_core::num::SafeConvert;
use reifydb_core::diagnostic::r#type::{out_of_range, OutOfRange};

pub trait Convert {
    fn convert<From, To>(
        &self,
        from: From,
        span: impl IntoSpan,
    ) -> crate::evaluate::Result<Option<To>>
    where
        From: SafeConvert<To>;
}

impl Convert for EvaluationContext {
    fn convert<From, To>(
        &self,
        from: From,
        span: impl IntoSpan,
    ) -> crate::evaluate::Result<Option<To>>
    where
        From: SafeConvert<To>,
    {
        Convert::convert(&self, from, span)
    }
}

impl Convert for &EvaluationContext {
    fn convert<From, To>(
        &self,
        from: From,
        span: impl IntoSpan,
    ) -> crate::evaluate::Result<Option<To>>
    where
        From: SafeConvert<To>,
    {
        match self.saturation_policy() {
            ColumnSaturationPolicy::Error => from
                .checked_convert()
                .ok_or_else(|| {
                    if let Some(column) = &self.column {
                        return crate::evaluate::Error(out_of_range(OutOfRange {
                            span: span.into_span(),
                            column: column.name.clone(),
                            data_type: column.data_type,
                        }));
                    }
                    return crate::evaluate::Error(out_of_range(OutOfRange {
                        span: span.into_span(),
                        column: None,
                        data_type: None,
                    }));
                })
                .map(Some),
            ColumnSaturationPolicy::Undefined => match from.checked_convert() {
                None => Ok(None),
                Some(value) => Ok(Some(value)),
            },
        }
    }
}
