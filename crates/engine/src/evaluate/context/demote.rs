// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later.

use crate::evaluate::Context;
use reifydb_catalog::column_policy::ColumnSaturationPolicy;
use reifydb_core::IntoSpan;
use reifydb_core::num::SafeDemote;
use reifydb_diagnostic::Diagnostic;
use reifydb_diagnostic::r#type::OutOfRange;

pub trait Demote {
    fn demote<From, To>(
        &self,
        from: From,
        span: impl IntoSpan,
    ) -> crate::evaluate::Result<Option<To>>
    where
        From: SafeDemote<To>;
}

impl Demote for Context {
    fn demote<From, To>(
        &self,
        from: From,
        span: impl IntoSpan,
    ) -> crate::evaluate::Result<Option<To>>
    where
        From: SafeDemote<To>,
    {
        Demote::demote(&self, from, span)
    }
}

impl Demote for &Context {
    fn demote<From, To>(
        &self,
        from: From,
        span: impl IntoSpan,
    ) -> crate::evaluate::Result<Option<To>>
    where
        From: SafeDemote<To>,
    {
        match self.saturation_policy() {
            ColumnSaturationPolicy::Error => from
                .checked_demote()
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
            ColumnSaturationPolicy::Undefined => match from.checked_demote() {
                None => Ok(None),
                Some(value) => Ok(Some(value)),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::evaluate::{Context, Demote, EvaluationColumn};
    use reifydb_catalog::column_policy::ColumnPolicyKind::Saturation;
    use reifydb_catalog::column_policy::ColumnSaturationPolicy::{Error, Undefined};
    use reifydb_core::Kind;
    use reifydb_core::Span;
    use reifydb_core::num::SafeDemote;

    #[test]
    fn test_demote_ok() {
        let mut ctx = Context::testing();
        ctx.column = Some(EvaluationColumn {
            name: Some("test_column".to_string()),
            kind: Some(Kind::Int1),
            policies: vec![Saturation(Error)],
        });

        let result = ctx.demote::<i16, i8>(1i16, || Span::testing_empty());
        assert_eq!(result, Ok(Some(1i8)));
    }

    #[test]
    fn test_demote_fail_with_error_policy() {
        let mut ctx = Context::testing();
        ctx.column = Some(EvaluationColumn {
            name: Some("test_column".to_string()),
            kind: Some(Kind::Int1),
            policies: vec![Saturation(Error)],
        });

        let err =
            ctx.demote::<TestI16, TestI8>(TestI16 {}, || Span::testing_empty()).err().unwrap();

        let diagnostic = err.diagnostic();
        assert_eq!(diagnostic.code, "TYPE_001");
    }

    #[test]
    fn test_demote_fail_with_undefined_policy() {
        let mut ctx = Context::testing();
        ctx.column = Some(EvaluationColumn {
            name: Some("test_column".to_string()),
            kind: Some(Kind::Int1),
            policies: vec![Saturation(Undefined)],
        });

        let result = ctx.demote::<TestI16, TestI8>(TestI16 {}, || Span::testing_empty()).unwrap();
        assert!(result.is_none());
    }

    pub struct TestI16 {}

    pub struct TestI8 {}

    impl SafeDemote<TestI8> for TestI16 {
        fn checked_demote(self) -> Option<TestI8> {
            None
        }

        fn saturating_demote(self) -> TestI8 {
            unreachable!()
        }

        fn wrapping_demote(self) -> TestI8 {
            unreachable!()
        }
    }
}
