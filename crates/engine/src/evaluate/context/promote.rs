// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use crate::evaluate::Context;
use reifydb_catalog::column_policy::ColumnSaturationPolicy;
use reifydb_core::IntoSpan;
use reifydb_core::num::SafePromote;
use reifydb_diagnostic::r#type::{out_of_range, OutOfRange};

pub trait Promote {
    fn promote<From, To>(
        &self,
        from: From,
        span: impl IntoSpan,
    ) -> crate::evaluate::Result<Option<To>>
    where
        From: SafePromote<To>;
}

impl Promote for Context {
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

impl Promote for &Context {
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
            ColumnSaturationPolicy::Undefined => match from.checked_promote() {
                None => Ok(None),
                Some(value) => Ok(Some(value)),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::evaluate::{Context, EvaluationColumn, Promote};
    use reifydb_catalog::column_policy::ColumnPolicyKind::Saturation;
    use reifydb_catalog::column_policy::ColumnSaturationPolicy::{Error, Undefined};
    use reifydb_core::DataType;
    use reifydb_core::Span;
    use reifydb_core::num::SafePromote;

    #[test]
    fn test_promote_ok() {
        let mut ctx = Context::testing();
        ctx.column = Some(EvaluationColumn {
            name: Some("test_column".to_string()),
            data_type: Some(DataType::Int2),
            policies: vec![Saturation(Error)],
        });

        let result = ctx.promote::<i8, i16>(1i8, || Span::testing_empty());
        assert_eq!(result, Ok(Some(1i16)));
    }

    #[test]
    fn test_promote_fail_with_error_policy() {
        let mut ctx = Context::testing();
        ctx.column = Some(EvaluationColumn {
            name: Some("test_column".to_string()),
            data_type: Some(DataType::Int2),
            policies: vec![Saturation(Error)],
        });

        let err =
            ctx.promote::<TestI8, TestI16>(TestI8 {}, || Span::testing_empty()).err().unwrap();
        let diagnostic = err.diagnostic();
        assert_eq!(diagnostic.code, "TYPE_001")
    }

    #[test]
    fn test_promote_fail_with_undefined_policy() {
        let mut ctx = Context::testing();
        ctx.column = Some(EvaluationColumn {
            name: Some("test_column".to_string()),
            data_type: Some(DataType::Int2),
            policies: vec![Saturation(Undefined)],
        });

        let result = ctx.promote::<TestI8, TestI16>(TestI8 {}, || Span::testing_empty()).unwrap();
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
