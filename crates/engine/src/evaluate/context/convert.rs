// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::evaluate::Context;
use reifydb_catalog::column_policy::ColumnSaturationPolicy;
use reifydb_core::IntoSpan;
use reifydb_core::num::SafeConvert;
use reifydb_diagnostic::Diagnostic;
use reifydb_diagnostic::r#type::OutOfRange;

pub trait Convert {
    fn convert<From, To>(
        &self,
        from: From,
        span: impl IntoSpan,
    ) -> crate::evaluate::Result<Option<To>>
    where
        From: SafeConvert<To>;
}

impl Convert for Context {
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

impl Convert for &Context {
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
                        return crate::evaluate::Error(Diagnostic::out_of_range(OutOfRange {
                            span: span.into_span(),
                            column: column.name.clone(),
                            kind: column.kind,
                        }));
                    }
                    return crate::evaluate::Error(Diagnostic::out_of_range(OutOfRange {
                        span: span.into_span(),
                        column: None,
                        kind: None,
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
