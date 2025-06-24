// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::evaluate::Context;
use reifydb_catalog::column_policy::ColumnSaturationPolicy;
use reifydb_core::num::{IsNumber, Promote, SafeAdd};
use reifydb_diagnostic::r#type::TypeOutOfRange;
use reifydb_diagnostic::{Diagnostic, IntoSpan};

impl Context {
    pub(crate) fn add<L, R>(
        &self,
        l: L,
        r: R,
        span: impl IntoSpan,
    ) -> crate::evaluate::Result<Option<<L as Promote<R>>::Output>>
    where
        L: Promote<R>,
        R: IsNumber,
        <L as Promote<R>>::Output: IsNumber,
        <L as Promote<R>>::Output: SafeAdd,
    {
        let (lp, rp) = l.promote(r);
        match self.saturation_policy() {
            ColumnSaturationPolicy::Error => {
                lp.checked_add(rp)
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
                    .map(Some)
            }
            // SaturationPolicy::Saturate => Ok(a.saturating_add(b)),
            // SaturationPolicy::Wrap => Ok(a.wrapping_add(b)),
            ColumnSaturationPolicy::Undefined => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::evaluate::Context;
    use reifydb_diagnostic::Span;

    #[test]
    fn test_add() {
        let test_instance = Context::testing();
        let result = test_instance.add(1i8, 255i16, Span::testing_empty());
        assert_eq!(result, Ok(Some(256i16)));
    }
}
