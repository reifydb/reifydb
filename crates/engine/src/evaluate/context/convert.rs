// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::evaluate::Context;
use reifydb_catalog::column_policy::ColumnSaturationPolicy;
use reifydb_core::num::SafeConvert;
use reifydb_diagnostic::r#type::TypeOutOfRange;
use reifydb_diagnostic::{Diagnostic, IntoSpan};

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
                        return crate::evaluate::Error(Diagnostic::type_out_of_range(
                            TypeOutOfRange {
                                span: span.into_span(),
                                column: column.name.clone(),
                                ty: column.kind,
                            },
                        ));
                    }
                    return crate::evaluate::Error(Diagnostic::type_out_of_range(
                        TypeOutOfRange { span: span.into_span(), column: None, ty: None },
                    ));
                })
                .map(Some),
            // SaturationPolicy::Saturate => Ok(a.saturating_convert(b)),
            // SaturationPolicy::Wrap => Ok(a.wrapping_convert(b)),
            ColumnSaturationPolicy::Undefined => Ok(None),
        }
    }
}
