// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later.

use crate::evaluate::Context;
use reifydb_catalog::column_policy::ColumnSaturationPolicy;
use reifydb_core::num::SafePromote;
use reifydb_diagnostic::IntoSpan;
use reifydb_diagnostic::policy::{ColumnSaturation, column_saturation};

pub trait Promote {
    fn promote<From, To>(
        &self,
        val: From,
        span: impl IntoSpan,
    ) -> crate::evaluate::Result<Option<To>>
    where
        From: SafePromote<To>;
}

impl Promote for Context<'_> {
    fn promote<From, To>(
        &self,
        val: From,
        span: impl IntoSpan,
    ) -> crate::evaluate::Result<Option<To>>
    where
        From: SafePromote<To>,
    {
        Promote::promote(&self, val, span)
    }
}

impl Promote for &Context<'_> {
    fn promote<From, To>(
        &self,
        val: From,
        span: impl IntoSpan,
    ) -> crate::evaluate::Result<Option<To>>
    where
        From: SafePromote<To>,
    {
        match val.promote() {
            Some(v) => Ok(Some(v)),
            None => match self.saturation_policy() {
                ColumnSaturationPolicy::Error => {
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
                ColumnSaturationPolicy::Undefined => Ok(None),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::evaluate::{Context, EvaluationColumn, Promote};
    use reifydb_catalog::column_policy::ColumnPolicyKind::Saturation;
    use reifydb_catalog::column_policy::ColumnSaturationPolicy::{Error, Undefined};
    use reifydb_core::num::SafePromote;
    use reifydb_core::{BitVec, ValueKind};
    use reifydb_testing::make_test_span;

    #[test]
    fn test_promote_ok() {
        let ctx = Context {
            column: Some(EvaluationColumn {
                name: "test_column".to_string(),
                value: ValueKind::Int2,
                policies: vec![Saturation(Error)],
            }),
            mask: &BitVec::empty(),
            columns: &[],
            row_count: 0,
        };

        let result = ctx.promote::<i8, i16>(1i8, || make_test_span());
        assert_eq!(result, Ok(Some(1i16)));
    }

    #[test]
    fn test_promote_fail_with_error_policy() {
        let ctx = Context {
            column: Some(EvaluationColumn {
                name: "test_column".to_string(),
                value: ValueKind::Int2,
                policies: vec![Saturation(Error)],
            }),
            mask: &BitVec::empty(),
            columns: &[],
            row_count: 0,
        };

        let err = ctx.promote::<TestI8, TestI16>(TestI8 {}, || make_test_span()).err().unwrap();
        let diagnostic = err.diagnostic();
        assert_eq!(diagnostic.code, "PO_001")
    }

    #[test]
    fn test_promote_fail_with_undefined_policy() {
        let ctx = Context {
            column: Some(EvaluationColumn {
                name: "test_column".to_string(),
                value: ValueKind::Int2,
                policies: vec![Saturation(Undefined)],
            }),
            mask: &BitVec::empty(),
            columns: &[],
            row_count: 0,
        };

        let result = ctx.promote::<TestI8, TestI16>(TestI8 {}, || make_test_span()).unwrap();
        assert!(result.is_none());
    }

    pub struct TestI8 {}

    impl SafePromote<TestI16> for TestI8 {
        fn promote(self) -> Option<TestI16> {
            None
        }
    }

    pub struct TestI16 {}
}
