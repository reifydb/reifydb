// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::evaluate::EvaluationContext;
use reifydb_catalog::column_policy::ColumnSaturationPolicy;
use reifydb_core::IntoSpan;
use reifydb_core::num::{IsNumber, Promote, SafeAdd, SafeDiv, SafeRemainder, SafeMul, SafeSub};
use reifydb_diagnostic::r#type::{OutOfRange, out_of_range};

impl EvaluationContext {
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
                    return Err(crate::evaluate::Error(out_of_range(OutOfRange {
                        span: span.into_span(),
                        column: None,
                        data_type: None,
                    })));
                };

                lp.checked_add(rp)
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

impl EvaluationContext {
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
        <L as Promote<R>>::Output: SafeSub,
    {
        match self.saturation_policy() {
            ColumnSaturationPolicy::Error => {
                let Some((lp, rp)) = l.checked_promote(r) else {
                    return Err(crate::evaluate::Error(out_of_range(OutOfRange {
                        span: span.into_span(),
                        column: None,
                        data_type: None,
                    })));
                };

                lp.checked_sub(rp)
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

impl EvaluationContext {
    pub(crate) fn mul<L, R>(
        &self,
        l: L,
        r: R,
        span: impl IntoSpan,
    ) -> crate::evaluate::Result<Option<<L as Promote<R>>::Output>>
    where
        L: Promote<R>,
        R: IsNumber,
        <L as Promote<R>>::Output: IsNumber,
        <L as Promote<R>>::Output: SafeMul,
    {
        match self.saturation_policy() {
            ColumnSaturationPolicy::Error => {
                let Some((lp, rp)) = l.checked_promote(r) else {
                    return Err(crate::evaluate::Error(out_of_range(OutOfRange {
                        span: span.into_span(),
                        column: None,
                        data_type: None,
                    })));
                };

                lp.checked_mul(rp)
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
                    .map(Some)
            }
            ColumnSaturationPolicy::Undefined => {
                let Some((lp, rp)) = l.checked_promote(r) else {
                    return Ok(None);
                };

                match lp.checked_mul(rp) {
                    None => Ok(None),
                    Some(value) => Ok(Some(value)),
                }
            }
        }
    }
}

impl EvaluationContext {
    pub(crate) fn div<L, R>(
        &self,
        l: L,
        r: R,
        span: impl IntoSpan,
    ) -> crate::evaluate::Result<Option<<L as Promote<R>>::Output>>
    where
        L: Promote<R>,
        R: IsNumber,
        <L as Promote<R>>::Output: IsNumber,
        <L as Promote<R>>::Output: SafeDiv,
    {
        match self.saturation_policy() {
            ColumnSaturationPolicy::Error => {
                let Some((lp, rp)) = l.checked_promote(r) else {
                    return Err(crate::evaluate::Error(out_of_range(OutOfRange {
                        span: span.into_span(),
                        column: None,
                        data_type: None,
                    })));
                };

                lp.checked_div(rp)
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
                    .map(Some)
            }
            ColumnSaturationPolicy::Undefined => {
                let Some((lp, rp)) = l.checked_promote(r) else {
                    return Ok(None);
                };

                match lp.checked_div(rp) {
                    None => Ok(None),
                    Some(value) => Ok(Some(value)),
                }
            }
        }
    }
}

impl EvaluationContext {
    pub(crate) fn remainder<L, R>(
        &self,
        l: L,
        r: R,
        span: impl IntoSpan,
    ) -> crate::evaluate::Result<Option<<L as Promote<R>>::Output>>
    where
        L: Promote<R>,
        R: IsNumber,
        <L as Promote<R>>::Output: IsNumber,
        <L as Promote<R>>::Output: SafeRemainder,
    {
        match self.saturation_policy() {
            ColumnSaturationPolicy::Error => {
                let Some((lp, rp)) = l.checked_promote(r) else {
                    return Err(crate::evaluate::Error(out_of_range(OutOfRange {
                        span: span.into_span(),
                        column: None,
                        data_type: None,
                    })));
                };

                lp.checked_rem(rp)
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
                    .map(Some)
            }
            ColumnSaturationPolicy::Undefined => {
                let Some((lp, rp)) = l.checked_promote(r) else {
                    return Ok(None);
                };

                match lp.checked_rem(rp) {
                    None => Ok(None),
                    Some(value) => Ok(Some(value)),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::evaluate::EvaluationContext;
    use reifydb_core::Span;

    #[test]
    fn test_add() {
        let test_instance = EvaluationContext::testing();
        let result = test_instance.add(1i8, 255i16, Span::testing_empty());
        assert_eq!(result, Ok(Some(256i128)));
    }

    #[test]
    fn test_sub() {
        let test_instance = EvaluationContext::testing();
        let result = test_instance.sub(1i8, 255i16, Span::testing_empty());
        assert_eq!(result, Ok(Some(-254i128)));
    }

    #[test]
    fn test_mul() {
        let test_instance = EvaluationContext::testing();
        let result = test_instance.mul(23i8, 255i16, Span::testing_empty());
        assert_eq!(result, Ok(Some(5865i128)));
    }

    #[test]
    fn test_div() {
        let test_instance = EvaluationContext::testing();
        let result = test_instance.div(120i8, 20i16, Span::testing_empty());
        assert_eq!(result, Ok(Some(6i128)));
    }

    #[test]
    fn test_remainder() {
        let test_instance = EvaluationContext::testing();
        let result = test_instance.remainder(120i8, 21i16, Span::testing_empty());
        assert_eq!(result, Ok(Some(15i128)));
    }
}
