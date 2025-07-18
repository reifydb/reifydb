// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::evaluate::EvaluationContext;
use reifydb_catalog::column_policy::ColumnSaturationPolicy;
use reifydb_core::diagnostic::number::number_out_of_range;
use reifydb_core::value::number::SafeConvert;
use reifydb_core::{GetType, IntoSpan};

pub trait Convert {
    fn convert<From, To>(
        &self,
        from: From,
        span: impl IntoSpan,
    ) -> crate::evaluate::Result<Option<To>>
    where
        From: SafeConvert<To>,
        To: GetType;
}

impl Convert for EvaluationContext {
    fn convert<From, To>(
        &self,
        from: From,
        span: impl IntoSpan,
    ) -> crate::evaluate::Result<Option<To>>
    where
        From: SafeConvert<To>,
        To: GetType,
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
        To: GetType,
    {
        match self.saturation_policy() {
            ColumnSaturationPolicy::Error => from
                .checked_convert()
                .ok_or_else(|| {
                    return crate::evaluate::Error(number_out_of_range(
                        span.into_span(),
                        To::get_type(),
                    ));
                })
                .map(Some),
            ColumnSaturationPolicy::Undefined => match from.checked_convert() {
                None => Ok(None),
                Some(value) => Ok(Some(value)),
            },
        }
    }
}
