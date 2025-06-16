// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later.

use crate::evaluate::Context;
use reifydb_catalog::DepColumnSaturationPolicy;
use reifydb_core::num::SafeDemote;
use reifydb_diagnostic::IntoSpan;
use reifydb_diagnostic::policy::{ColumnSaturation, column_saturation};

pub trait Demote {
    fn demote<From, To>(
        &self,
        val: From,
        span: impl IntoSpan,
    ) -> crate::evaluate::Result<Option<To>>
    where
        From: SafeDemote<To>;
}

impl Demote for Context {
    fn demote<From, To>(
        &self,
        val: From,
        span: impl IntoSpan,
    ) -> crate::evaluate::Result<Option<To>>
    where
        From: SafeDemote<To>,
    {
        Demote::demote(&self, val, span)
    }
}

impl Demote for &Context {
    fn demote<From, To>(
        &self,
        val: From,
        span: impl IntoSpan,
    ) -> crate::evaluate::Result<Option<To>>
    where
        From: SafeDemote<To>,
    {
        match val.demote() {
            Some(v) => Ok(Some(v)),
            None => match self.saturation_policy() {
                DepColumnSaturationPolicy::Error => {
                    if let Some(column) = &self.column {
                        Err(crate::evaluate::Error(column_saturation(ColumnSaturation {
                            span: span.into_span(),
                            column: column.name.to_string(),
                            value: column.value,
                        })))
                    } else {
                        unimplemented!()
                    }
                }
                DepColumnSaturationPolicy::Undefined => Ok(None),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::evaluate::{Context, Demote, EvaluationColumn};
    use DepColumnPolicy::Saturation;
    use reifydb_catalog::DepColumnPolicy;
    use reifydb_catalog::DepColumnSaturationPolicy::{Error, Undefined};
    use reifydb_core::ValueKind;
    use reifydb_core::num::SafeDemote;
    use reifydb_testing::make_test_span;

    #[test]
    fn test_demote_ok() {
        let ctx = Context {
            column: Some(EvaluationColumn {
                name: "test_column".to_string(),
                value: ValueKind::Int1,
                policies: vec![Saturation(Error)],
            }),
            frame: None,
        };

        let result = ctx.demote::<i16, i8>(1i16, || make_test_span());
        assert_eq!(result, Ok(Some(1i8)));
    }

    #[test]
    fn test_demote_fail_with_error_policy() {
        let ctx = Context {
            column: Some(EvaluationColumn {
                name: "test_column".to_string(),
                value: ValueKind::Int1,
                policies: vec![Saturation(Error)],
            }),
            frame: None,
        };

        let err = ctx.demote::<TestI16, TestI8>(TestI16 {}, || make_test_span()).err().unwrap();

        let diagnostic = err.diagnostic();
        assert_eq!(diagnostic.code, "PO_001");
    }

    #[test]
    fn test_demote_fail_with_undefined_policy() {
        let ctx = Context {
            column: Some(EvaluationColumn {
                name: "test_column".to_string(),
                value: ValueKind::Int1,
                policies: vec![Saturation(Undefined)],
            }),
            frame: None,
        };

        let result = ctx.demote::<TestI16, TestI8>(TestI16 {}, || make_test_span()).unwrap();
        assert!(result.is_none());
    }

    pub struct TestI16 {}

    pub struct TestI8 {}

    impl SafeDemote<TestI8> for TestI16 {
        fn demote(self) -> Option<TestI8> {
            None
        }
    }
}
