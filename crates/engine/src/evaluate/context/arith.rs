// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::evaluate::Context;
use reifydb_catalog::column_policy::ColumnSaturationPolicy;
use reifydb_core::num::{IsNumber, Promote, SafeAdd, SafeSubtract};
use reifydb_diagnostic::r#type::OutOfRange;
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
        match self.saturation_policy() {
            ColumnSaturationPolicy::Error => {
                let Some((lp, rp)) = l.checked_promote(r) else {
                    return Err(crate::evaluate::Error(Diagnostic::out_of_range(
                        OutOfRange { span: span.into_span(), column: None, kind: None },
                    )));
                };

                lp.checked_add(rp)
                    .ok_or_else(|| {
                        if let Some(column) = &self.column {
                            return crate::evaluate::Error(Diagnostic::out_of_range(
                                OutOfRange {
                                    span: span.into_span(),
                                    column: column.name.clone(),
                                    kind: column.kind,
                                },
                            ));
                        }
                        return crate::evaluate::Error(Diagnostic::out_of_range(
                            OutOfRange { span: span.into_span(), column: None, kind: None },
                        ));
                    })
                    .map(Some)
            }
            ColumnSaturationPolicy::Undefined => {
                let Some((lp, rp)) = l.checked_promote(r) else {
                    return Ok(None);
                };
                
                match lp.checked_add(rp) {
                    None => Ok(None),
                    Some(value) => Ok(Some(value)),
                }
            }
        }
    }
}

impl Context {
    pub(crate) fn sub<L, R>(
        &self,
        l: L,
        r: R,
        span: impl IntoSpan,
    ) -> crate::evaluate::Result<Option<<L as Promote<R>>::Output>>
    where
        L: Promote<R>,
        R: IsNumber,
        <L as Promote<R>>::Output: IsNumber,
        <L as Promote<R>>::Output: SafeSubtract,
    {
        match self.saturation_policy() {
            ColumnSaturationPolicy::Error => {
                let Some((lp, rp)) = l.checked_promote(r) else {
                    return Err(crate::evaluate::Error(Diagnostic::out_of_range(
                        OutOfRange { span: span.into_span(), column: None, kind: None },
                    )));
                };

                lp.checked_sub(rp)
                    .ok_or_else(|| {
                        if let Some(column) = &self.column {
                            return crate::evaluate::Error(Diagnostic::out_of_range(
                                OutOfRange {
                                    span: span.into_span(),
                                    column: column.name.clone(),
                                    kind: column.kind,
                                },
                            ));
                        }
                        return crate::evaluate::Error(Diagnostic::out_of_range(
                            OutOfRange { span: span.into_span(), column: None, kind: None },
                        ));
                    })
                    .map(Some)
            }
            ColumnSaturationPolicy::Undefined => {
                let Some((lp, rp)) = l.checked_promote(r) else {
                    return Ok(None);
                };

                match lp.checked_sub(rp) {
                    None => Ok(None),
                    Some(value) => Ok(Some(value)),
                }
            }
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
        assert_eq!(result, Ok(Some(256i128)));
    }
}
