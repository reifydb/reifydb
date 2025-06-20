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
        from: From,
        span: impl IntoSpan,
    ) -> crate::evaluate::Result<Option<To>>
    where
        From: SafePromote<To>;
}

impl Promote for Context<'_> {
    fn promote<From, To>(
        &self,
        from: From,
        span: impl IntoSpan,
    ) -> crate::evaluate::Result<Option<To>>
    where
        From: SafePromote<To>,
    {
        Promote::promote(&self, from, span)
    }
}

impl Promote for &Context<'_> {
    fn promote<From, To>(
        &self,
        from: From,
        span: impl IntoSpan,
    ) -> crate::evaluate::Result<Option<To>>
    where
        From: SafePromote<To>,
    {
        match self.saturation_policy() {
            ColumnSaturationPolicy::Error => from
                .checked_promote()
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
            // SaturationPolicy::Saturate => Ok(a.saturating_promote(b)),
            // SaturationPolicy::Wrap => Ok(a.wrapping_promote(b)),
            ColumnSaturationPolicy::Undefined => Ok(None),
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
            limit: None,
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
            limit: None,
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
            limit: None,
        };

        let result = ctx.promote::<TestI8, TestI16>(TestI8 {}, || make_test_span()).unwrap();
        assert!(result.is_none());
    }

    pub struct TestI8 {}

    impl SafePromote<TestI16> for TestI8 {
        fn checked_promote(self) -> Option<TestI16> {
            None
        }

        fn saturating_promote(self) -> TestI16 {
            unreachable!()
        }

        fn wrapping_promote(self) -> TestI16 {
            unreachable!()
        }
    }

    pub struct TestI16 {}
}
